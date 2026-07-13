//! Crash-safe persistence for Raft's hard state and log.
//!
//! On-disk layout inside the node's data directory:
//! - `hard_state.json` — current_term + voted_for; rewritten atomically
//!   (temp file → fsync → rename → fsync dir) on every change.
//! - `log.jsonl` — the log, one JSON entry per line, append-only. Appends are
//!   fsynced before returning, so an entry is only ever acknowledged after it
//!   is durable. Suffix truncation (log conflicts, phase 4) atomically
//!   rewrites the whole file — acceptable while logs stay small.
//!   TODO(compaction): snapshotting/log compaction would replace the
//!   whole-file rewrite and bound replay time. Deliberately out of scope.
//!
//! Crash tolerance on replay: a torn *final* line (a crash mid-append) is by
//! construction un-acknowledged, so it is dropped and the file truncated back
//! to the last complete entry. A malformed line anywhere *else*, or a gap in
//! indexes, is real corruption and fails loudly.
//!
//! All I/O here is synchronous `std::fs`. The Raft core (phase 3+) owns its
//! storage from a dedicated task, so blocking writes never sit on a shared
//! executor thread pool's hot path.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use super::types::{HardState, LogEntry, LogIndex, Term};

const HARD_STATE_FILE: &str = "hard_state.json";
const LOG_FILE: &str = "log.jsonl";

#[derive(Debug)]
pub enum StorageError {
    Io(std::io::Error),
    /// Unrecoverable on-disk damage (bad non-final line, index gap, ...).
    Corrupt(String),
    /// An append would create a hole in the log — a caller bug, not disk damage.
    NonContiguous {
        expected: LogIndex,
        got: LogIndex,
    },
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::Io(e) => write!(f, "storage I/O error: {e}"),
            StorageError::Corrupt(msg) => write!(f, "storage corrupt: {msg}"),
            StorageError::NonContiguous { expected, got } => {
                write!(
                    f,
                    "non-contiguous append: expected index {expected}, got {got}"
                )
            }
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StorageError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for StorageError {
    fn from(e: std::io::Error) -> Self {
        StorageError::Io(e)
    }
}

/// Durable hard state + log for one node, with the log mirrored in memory.
///
/// Mutations return only after the data is fsynced, which is what lets the
/// Raft core answer RPCs immediately after a `save_hard_state`/`append`.
pub struct Storage {
    dir: PathBuf,
    /// Open append handle to `log.jsonl`; recreated after truncation.
    log_file: File,
    hard_state: HardState,
    /// In-memory mirror of the log. Entry with index `i` lives at `log[i-1]`.
    log: Vec<LogEntry>,
}

impl Storage {
    /// Opens (or initializes) the storage in `dir`, replaying the log.
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;

        let hard_state = load_hard_state(&dir)?;
        let log = replay_log(&dir)?;
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(LOG_FILE))?;

        tracing::info!(
            dir = %dir.display(),
            term = hard_state.current_term,
            voted_for = ?hard_state.voted_for,
            log_len = log.len(),
            "storage opened"
        );
        Ok(Self {
            dir,
            log_file,
            hard_state,
            log,
        })
    }

    pub fn hard_state(&self) -> HardState {
        self.hard_state
    }

    /// Durably replaces the hard state. Must be called (and must succeed)
    /// before responding to any RPC that changed term or vote.
    pub fn save_hard_state(&mut self, hs: HardState) -> Result<(), StorageError> {
        let bytes = serde_json::to_vec(&hs).expect("HardState serialization cannot fail");
        write_atomic(&self.dir, HARD_STATE_FILE, &bytes)?;
        self.hard_state = hs;
        Ok(())
    }

    /// Durably appends entries, which must continue the log contiguously
    /// (`entries[0].index == last_index() + 1`, then +1 each).
    pub fn append(&mut self, entries: &[LogEntry]) -> Result<(), StorageError> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut expected = self.last_index() + 1;
        for entry in entries {
            if entry.index != expected {
                return Err(StorageError::NonContiguous {
                    expected,
                    got: entry.index,
                });
            }
            expected += 1;
        }

        let mut buf = Vec::new();
        for entry in entries {
            serde_json::to_writer(&mut buf, entry).expect("LogEntry serialization cannot fail");
            buf.push(b'\n');
        }
        self.log_file.write_all(&buf)?;
        self.log_file.sync_all()?;
        self.log.extend_from_slice(entries);
        Ok(())
    }

    /// Durably drops every entry with `index >= from` (log-conflict handling,
    /// phase 4). No-op if `from` is past the end of the log.
    ///
    /// TODO(compaction): with snapshots this would also guard the snapshot
    /// boundary; today the whole remaining prefix is rewritten atomically.
    pub fn truncate_from(&mut self, from: LogIndex) -> Result<(), StorageError> {
        if from == 0 {
            return Err(StorageError::Corrupt(
                "truncate_from(0): index 0 is a sentinel".into(),
            ));
        }
        if from > self.last_index() {
            return Ok(());
        }
        let keep = usize::try_from(from - 1).expect("log index fits in usize");

        let mut buf = Vec::new();
        for entry in &self.log[..keep] {
            serde_json::to_writer(&mut buf, entry).expect("LogEntry serialization cannot fail");
            buf.push(b'\n');
        }
        write_atomic(&self.dir, LOG_FILE, &buf)?;
        // The old append handle points at the replaced inode; reopen.
        self.log_file = OpenOptions::new()
            .append(true)
            .open(self.dir.join(LOG_FILE))?;
        self.log.truncate(keep);
        Ok(())
    }

    pub fn entries(&self) -> &[LogEntry] {
        &self.log
    }

    /// All entries with `index >= from` (1-based); empty if past the end.
    pub fn entries_from(&self, from: LogIndex) -> &[LogEntry] {
        let start = usize::try_from(from.saturating_sub(1)).expect("log index fits in usize");
        self.log.get(start..).unwrap_or(&[])
    }

    /// The entry at 1-based `index`, if present. `index` 0 is the sentinel
    /// "before the log" and never has an entry.
    pub fn entry(&self, index: LogIndex) -> Option<&LogEntry> {
        if index == 0 {
            return None;
        }
        self.log
            .get(usize::try_from(index - 1).expect("log index fits in usize"))
    }

    /// The term of the entry at `index`; `Some(0)` for the index-0 sentinel
    /// (what AppendEntries consistency checks need), `None` past the end.
    pub fn term(&self, index: LogIndex) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }
        self.entry(index).map(|e| e.term)
    }

    /// Index of the last entry, or 0 if the log is empty.
    pub fn last_index(&self) -> LogIndex {
        self.log.len() as LogIndex
    }

    /// Term of the last entry, or 0 if the log is empty.
    pub fn last_term(&self) -> Term {
        self.log.last().map_or(0, |e| e.term)
    }
}

fn load_hard_state(dir: &Path) -> Result<HardState, StorageError> {
    let path = dir.join(HARD_STATE_FILE);
    match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(|e| StorageError::Corrupt(format!("{}: {e}", path.display()))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(HardState::default()),
        Err(e) => Err(e.into()),
    }
}

/// Reads and validates `log.jsonl`, repairing a torn final line if a crash
/// interrupted the last append.
fn replay_log(dir: &Path) -> Result<Vec<LogEntry>, StorageError> {
    let path = dir.join(LOG_FILE);
    let bytes = match fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };

    let mut log: Vec<LogEntry> = Vec::new();
    let mut good_end = 0usize; // byte offset just past the last valid line
    let mut offset = 0usize;

    for chunk in bytes.split_inclusive(|&b| b == b'\n') {
        let complete = chunk.ends_with(b"\n");
        let line_start = offset;
        offset += chunk.len();

        match serde_json::from_slice::<LogEntry>(chunk) {
            Ok(entry) if complete => {
                let expected = log.len() as LogIndex + 1;
                if entry.index != expected {
                    return Err(StorageError::Corrupt(format!(
                        "{}: expected index {expected} at byte {line_start}, found {}",
                        path.display(),
                        entry.index
                    )));
                }
                log.push(entry);
                good_end = offset;
            }
            // A parse failure or missing newline on the FINAL line is a torn
            // append: the entry was never acked, so drop it. Anywhere else it
            // is corruption.
            Ok(_) | Err(_) if offset == bytes.len() => {
                tracing::warn!(
                    path = %path.display(),
                    dropped_bytes = bytes.len() - good_end,
                    "dropping torn final log line from interrupted append"
                );
                let file = OpenOptions::new().write(true).open(&path)?;
                file.set_len(good_end as u64)?;
                file.sync_all()?;
                break;
            }
            Ok(_) => unreachable!("complete non-final lines are handled above"),
            Err(e) => {
                return Err(StorageError::Corrupt(format!(
                    "{}: bad line at byte {line_start}: {e}",
                    path.display()
                )));
            }
        }
    }
    Ok(log)
}

/// Writes `bytes` to `dir/name` atomically: temp file → fsync → rename →
/// fsync of the directory, so a crash leaves either the old or the new file.
fn write_atomic(dir: &Path, name: &str, bytes: &[u8]) -> Result<(), StorageError> {
    let tmp = dir.join(format!("{name}.tmp"));
    let mut f = File::create(&tmp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    fs::rename(&tmp, dir.join(name))?;
    File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::Command;
    use serde_json::json;

    fn entry(term: Term, index: LogIndex) -> LogEntry {
        LogEntry {
            term,
            index,
            command: Command::Put {
                key: format!("k{index}"),
                value: json!({ "i": index }),
            },
        }
    }

    #[test]
    fn fresh_dir_starts_empty_with_default_hard_state() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::open(dir.path().join("node")).unwrap();
        assert_eq!(storage.hard_state(), HardState::default());
        assert_eq!(storage.last_index(), 0);
        assert_eq!(storage.last_term(), 0);
        assert_eq!(storage.entries(), &[]);
        assert_eq!(storage.term(0), Some(0));
        assert_eq!(storage.term(1), None);
    }

    #[test]
    fn entries_from_slices_the_tail() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(dir.path()).unwrap();
        storage
            .append(&[entry(1, 1), entry(1, 2), entry(2, 3)])
            .unwrap();

        assert_eq!(storage.entries_from(1), storage.entries());
        assert_eq!(storage.entries_from(3).len(), 1);
        assert_eq!(storage.entries_from(3)[0].index, 3);
        assert_eq!(storage.entries_from(4), &[]);
        assert_eq!(storage.entries_from(100), &[]);
    }

    #[test]
    fn hard_state_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let hs = HardState {
            current_term: 7,
            voted_for: Some(2),
        };
        {
            let mut storage = Storage::open(dir.path()).unwrap();
            storage.save_hard_state(hs).unwrap();
            assert_eq!(storage.hard_state(), hs);
        }
        let storage = Storage::open(dir.path()).unwrap();
        assert_eq!(storage.hard_state(), hs);
    }

    #[test]
    fn appended_entries_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let entries = vec![entry(1, 1), entry(1, 2), entry(2, 3)];
        {
            let mut storage = Storage::open(dir.path()).unwrap();
            storage.append(&entries[..2]).unwrap();
            storage.append(&entries[2..]).unwrap();
        }
        let storage = Storage::open(dir.path()).unwrap();
        assert_eq!(storage.entries(), &entries);
        assert_eq!(storage.last_index(), 3);
        assert_eq!(storage.last_term(), 2);
        assert_eq!(storage.term(2), Some(1));
        assert_eq!(storage.entry(3), Some(&entries[2]));
        assert_eq!(storage.entry(4), None);
    }

    #[test]
    fn non_contiguous_append_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(dir.path()).unwrap();
        storage.append(&[entry(1, 1)]).unwrap();

        let err = storage.append(&[entry(1, 3)]).unwrap_err();
        assert!(matches!(
            err,
            StorageError::NonContiguous {
                expected: 2,
                got: 3
            }
        ));
        // Gap *inside* the batch is rejected too, and nothing is written.
        let err = storage.append(&[entry(1, 2), entry(1, 4)]).unwrap_err();
        assert!(matches!(
            err,
            StorageError::NonContiguous {
                expected: 3,
                got: 4
            }
        ));
        assert_eq!(storage.last_index(), 1);
    }

    #[test]
    fn truncate_from_drops_suffix_durably() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut storage = Storage::open(dir.path()).unwrap();
            storage
                .append(&[entry(1, 1), entry(1, 2), entry(2, 3)])
                .unwrap();
            storage.truncate_from(2).unwrap();
            assert_eq!(storage.last_index(), 1);
            // Appending after truncation reuses the freed indexes.
            storage.append(&[entry(3, 2)]).unwrap();
        }
        let storage = Storage::open(dir.path()).unwrap();
        assert_eq!(storage.last_index(), 2);
        assert_eq!(storage.term(2), Some(3));
    }

    #[test]
    fn truncate_past_end_is_a_noop_and_zero_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(dir.path()).unwrap();
        storage.append(&[entry(1, 1)]).unwrap();
        storage.truncate_from(5).unwrap();
        assert_eq!(storage.last_index(), 1);
        assert!(matches!(
            storage.truncate_from(0),
            Err(StorageError::Corrupt(_))
        ));
    }

    #[test]
    fn torn_final_line_is_dropped_on_replay() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut storage = Storage::open(dir.path()).unwrap();
            storage.append(&[entry(1, 1), entry(1, 2)]).unwrap();
        }
        // Simulate a crash mid-append: a partial JSON fragment, no newline.
        let path = dir.path().join(LOG_FILE);
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(br#"{"term":1,"index":3,"comm"#).unwrap();
        f.sync_all().unwrap();

        let mut storage = Storage::open(dir.path()).unwrap();
        assert_eq!(storage.last_index(), 2);
        // The file was repaired: appending and reopening again works cleanly.
        storage.append(&[entry(1, 3)]).unwrap();
        drop(storage);
        let storage = Storage::open(dir.path()).unwrap();
        assert_eq!(storage.last_index(), 3);
    }

    #[test]
    fn complete_but_unparsable_final_line_is_also_dropped() {
        // A final line that ends in '\n' but doesn't parse (e.g. torn write
        // that happened to end on a newline byte) is still un-acked → dropped.
        let dir = tempfile::tempdir().unwrap();
        {
            let mut storage = Storage::open(dir.path()).unwrap();
            storage.append(&[entry(1, 1)]).unwrap();
        }
        let path = dir.path().join(LOG_FILE);
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(b"garbage\n").unwrap();
        f.sync_all().unwrap();

        let storage = Storage::open(dir.path()).unwrap();
        assert_eq!(storage.last_index(), 1);
    }

    #[test]
    fn corrupt_middle_line_fails_loudly() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut storage = Storage::open(dir.path()).unwrap();
            storage.append(&[entry(1, 1), entry(1, 2)]).unwrap();
        }
        let path = dir.path().join(LOG_FILE);
        let contents = fs::read_to_string(&path).unwrap();
        let corrupted = contents.replacen(r#""term":1,"index":1"#, r#""term":1,"index":9"#, 1);
        fs::write(&path, corrupted).unwrap();

        assert!(matches!(
            Storage::open(dir.path()),
            Err(StorageError::Corrupt(_))
        ));
    }

    #[test]
    fn index_gap_on_disk_fails_loudly() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(dir.path()).unwrap();
        storage
            .append(&[entry(1, 1), entry(1, 2), entry(1, 3)])
            .unwrap();
        drop(storage);

        // Remove the middle line to create a gap 1 → 3.
        let path = dir.path().join(LOG_FILE);
        let contents = fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        fs::write(&path, format!("{}\n{}\n", lines[0], lines[2])).unwrap();

        assert!(matches!(
            Storage::open(dir.path()),
            Err(StorageError::Corrupt(_))
        ));
    }
}
