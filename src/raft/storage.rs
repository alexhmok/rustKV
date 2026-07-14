//! Crash-safe persistence for Raft's hard state, log, and snapshot.
//!
//! On-disk layout inside the node's data directory:
//! - `hard_state.json` — current_term + voted_for; rewritten atomically
//!   (temp file → fsync → rename → fsync dir) on every change.
//! - `log.jsonl` — the log, one JSON entry per line, append-only. Appends are
//!   fsynced before returning, so an entry is only ever acknowledged after it
//!   is durable. Suffix truncation (log conflicts, phase 4) atomically
//!   rewrites the whole file — acceptable while retained logs stay small.
//! - `snapshot.json` (phase 14, optional) — a [`Snapshot`] of the state
//!   machine at `last_included_index`; every log entry at or below that
//!   boundary is compacted away. Absent = boundary 0 = the pre-snapshot
//!   behavior, byte-identical (old data dirs open unchanged).
//!
//! Compaction ([`Storage::compact_to`], [`Storage::install_snapshot`]) is
//! crash-safe by ordering: the snapshot is written atomically FIRST, then
//! the log is rewritten without the compacted prefix. A crash between the
//! two leaves a log overlapping the boundary; replay skips entries at or
//! below it, so reopening completes the compaction's effect idempotently.
//!
//! Crash tolerance on replay: a torn *final* line (a crash mid-append) is by
//! construction un-acknowledged, so it is dropped and the file truncated back
//! to the last complete entry. A malformed line anywhere *else*, a gap in
//! indexes, or a log that doesn't continue from the snapshot boundary is real
//! corruption and fails loudly.
//!
//! All I/O here is synchronous `std::fs`. The Raft core (phase 3+) owns its
//! storage from a dedicated task, so blocking writes never sit on a shared
//! executor thread pool's hot path.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde_json::Value;

use super::types::{HardState, LogEntry, LogIndex, Snapshot, Term};

const HARD_STATE_FILE: &str = "hard_state.json";
const LOG_FILE: &str = "log.jsonl";
const SNAPSHOT_FILE: &str = "snapshot.json";

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

/// Durable hard state + log + snapshot for one node, with the retained log
/// (and the snapshot) mirrored in memory.
///
/// Mutations return only after the data is fsynced, which is what lets the
/// Raft core answer RPCs immediately after a `save_hard_state`/`append`.
///
/// Index arithmetic: log indexes stay global and 1-based, but entries at or
/// below the snapshot boundary ([`Self::snapshot_index`]) are compacted away;
/// the retained entry at index `i` lives at `log[pos(i)]`. With no snapshot
/// the boundary is 0 and everything behaves exactly as before phase 14.
pub struct Storage {
    dir: PathBuf,
    /// Open append handle to `log.jsonl`; recreated after truncation/compaction.
    log_file: File,
    hard_state: HardState,
    /// The current snapshot, if any. Kept in memory whole (payloads are small
    /// by scope) — it doubles as the leader's InstallSnapshot payload cache,
    /// naturally invalidated whenever a compaction replaces it.
    snapshot: Option<Snapshot>,
    /// In-memory mirror of the retained log (entries above the boundary).
    log: Vec<LogEntry>,
}

impl Storage {
    /// Opens (or initializes) the storage in `dir`, loading the snapshot (if
    /// any) and replaying the retained log.
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;

        let hard_state = load_hard_state(&dir)?;
        let snapshot = load_snapshot(&dir)?;
        let snapshot_index = snapshot.as_ref().map_or(0, |s| s.last_included_index);
        let log = replay_log(&dir, snapshot_index)?;
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(LOG_FILE))?;

        tracing::info!(
            dir = %dir.display(),
            term = hard_state.current_term,
            voted_for = ?hard_state.voted_for,
            snapshot_index,
            log_len = log.len(),
            "storage opened"
        );
        Ok(Self {
            dir,
            log_file,
            hard_state,
            snapshot,
            log,
        })
    }

    pub fn hard_state(&self) -> HardState {
        self.hard_state
    }

    /// The current snapshot, if the log has ever been compacted.
    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    /// Index of the last compacted entry; 0 if never compacted. Every entry
    /// at or below it lives only in the snapshot.
    pub fn snapshot_index(&self) -> LogIndex {
        self.snapshot.as_ref().map_or(0, |s| s.last_included_index)
    }

    /// Term of the entry at the snapshot boundary; 0 if never compacted
    /// (matching the index-0 sentinel this boundary generalizes).
    pub fn snapshot_term(&self) -> Term {
        self.snapshot.as_ref().map_or(0, |s| s.last_included_term)
    }

    /// Position of global `index` in the retained in-memory log, or `None`
    /// for the boundary and everything below it. The single place index
    /// arithmetic happens.
    fn pos(&self, index: LogIndex) -> Option<usize> {
        index
            .checked_sub(self.snapshot_index() + 1)
            .map(|p| usize::try_from(p).expect("log position fits in usize"))
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
    /// phase 4). No-op if `from` is past the end of the log. Truncating at or
    /// below the snapshot boundary is an error: compacted entries are
    /// committed and must never be rewound (index 0 is the degenerate case —
    /// the pre-snapshot sentinel).
    pub fn truncate_from(&mut self, from: LogIndex) -> Result<(), StorageError> {
        let Some(keep) = self.pos(from) else {
            return Err(StorageError::Corrupt(format!(
                "truncate_from({from}): at or below the snapshot boundary {}",
                self.snapshot_index()
            )));
        };
        if from > self.last_index() {
            return Ok(());
        }
        let retained = self.log[..keep].to_vec();
        self.rewrite_log(retained)
    }

    /// Compacts the log up to and including `last_included_index`, durably
    /// replacing that prefix with a snapshot carrying `state` (the state
    /// machine's export at exactly that index — the caller must pass
    /// `last_applied`, never `commit_index`: committed-but-unapplied entries
    /// are not in the state yet).
    ///
    /// Crash-safe ordering: snapshot written atomically first, then the log
    /// rewritten without the prefix; replay tolerates the in-between state
    /// (see module docs).
    pub fn compact_to(
        &mut self,
        last_included_index: LogIndex,
        state: Value,
    ) -> Result<(), StorageError> {
        // Capture the boundary entry's term while the entry still exists.
        let Some(last_included_term) = self
            .pos(last_included_index)
            .and_then(|p| self.log.get(p))
            .map(|e| e.term)
        else {
            return Err(StorageError::Corrupt(format!(
                "compact_to({last_included_index}): outside the retained log \
                 ({}, {}]",
                self.snapshot_index(),
                self.last_index()
            )));
        };
        let snapshot = Snapshot {
            last_included_index,
            last_included_term,
            membership: None,
            state,
        };
        // Compute the retained tail BEFORE the snapshot moves the boundary
        // (entries_from's arithmetic shifts with it), and write the snapshot
        // BEFORE rewriting the log (the crash-safe ordering).
        let retained = self.entries_from(last_included_index + 1).to_vec();
        self.write_snapshot(snapshot)?;
        self.rewrite_log(retained)
    }

    /// Persists a snapshot received from the leader (InstallSnapshot,
    /// phase 14), replacing everything up to its boundary. The log suffix
    /// beyond the boundary is retained only if our entry AT the boundary
    /// matches the snapshot's term (log matching then guarantees the suffix
    /// continues the committed history); otherwise the whole log is stale
    /// divergent junk and is cleared. The caller must have verified the
    /// boundary is above both `commit_index` and the current snapshot.
    pub fn install_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), StorageError> {
        if snapshot.last_included_index <= self.snapshot_index() {
            return Err(StorageError::Corrupt(format!(
                "install_snapshot({}): at or below the current boundary {}",
                snapshot.last_included_index,
                self.snapshot_index()
            )));
        }
        let retained =
            if self.term(snapshot.last_included_index) == Some(snapshot.last_included_term) {
                self.entries_from(snapshot.last_included_index + 1).to_vec()
            } else {
                Vec::new()
            };
        self.write_snapshot(snapshot.clone())?;
        self.rewrite_log(retained)
    }

    /// Durably writes `snapshot.json` and adopts it as the boundary.
    fn write_snapshot(&mut self, snapshot: Snapshot) -> Result<(), StorageError> {
        let bytes = serde_json::to_vec(&snapshot).expect("Snapshot serialization cannot fail");
        write_atomic(&self.dir, SNAPSHOT_FILE, &bytes)?;
        tracing::info!(
            dir = %self.dir.display(),
            last_included_index = snapshot.last_included_index,
            last_included_term = snapshot.last_included_term,
            "snapshot written"
        );
        self.snapshot = Some(snapshot);
        Ok(())
    }

    /// Atomically replaces `log.jsonl` (and the in-memory mirror) with
    /// exactly `entries`, reopening the append handle.
    fn rewrite_log(&mut self, entries: Vec<LogEntry>) -> Result<(), StorageError> {
        let mut buf = Vec::new();
        for entry in &entries {
            serde_json::to_writer(&mut buf, entry).expect("LogEntry serialization cannot fail");
            buf.push(b'\n');
        }
        write_atomic(&self.dir, LOG_FILE, &buf)?;
        // The old append handle points at the replaced inode; reopen.
        self.log_file = OpenOptions::new()
            .append(true)
            .open(self.dir.join(LOG_FILE))?;
        self.log = entries;
        Ok(())
    }

    /// The retained entries (everything above the snapshot boundary).
    pub fn entries(&self) -> &[LogEntry] {
        &self.log
    }

    /// All retained entries with `index >= from`; empty if past the end.
    /// Asking below the boundary yields the whole retained log (what lies
    /// below is compacted and cannot be returned).
    pub fn entries_from(&self, from: LogIndex) -> &[LogEntry] {
        let start = self.pos(from).unwrap_or(0);
        self.log.get(start..).unwrap_or(&[])
    }

    /// The entry at 1-based `index`, if retained. Index 0 (the "before the
    /// log" sentinel) and compacted indexes never have one.
    pub fn entry(&self, index: LogIndex) -> Option<&LogEntry> {
        self.pos(index).and_then(|p| self.log.get(p))
    }

    /// The term of the entry at `index`: `Some(snapshot_term)` at the
    /// boundary itself (`Some(0)` at the index-0 sentinel the boundary
    /// generalizes — what AppendEntries consistency checks need), `None`
    /// past the end AND below the boundary — the latter meaning "compacted;
    /// only the snapshot can answer" (a leader seeing this for a peer's
    /// next_index must send a snapshot, not entries).
    pub fn term(&self, index: LogIndex) -> Option<Term> {
        if index == self.snapshot_index() {
            return Some(self.snapshot_term());
        }
        self.entry(index).map(|e| e.term)
    }

    /// Index of the last entry (retained or compacted), or 0 if nothing was
    /// ever appended.
    pub fn last_index(&self) -> LogIndex {
        self.snapshot_index() + self.log.len() as LogIndex
    }

    /// Term of the last entry; falls back to the snapshot boundary's term
    /// when the retained log is empty (0 if nothing was ever appended).
    pub fn last_term(&self) -> Term {
        self.log
            .last()
            .map_or_else(|| self.snapshot_term(), |e| e.term)
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

/// Loads `snapshot.json` if present. Written atomically, so it is either
/// absent (boundary 0, the pre-phase-14 behavior) or complete.
fn load_snapshot(dir: &Path) -> Result<Option<Snapshot>, StorageError> {
    let path = dir.join(SNAPSHOT_FILE);
    match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| StorageError::Corrupt(format!("{}: {e}", path.display()))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Reads and validates `log.jsonl`, repairing a torn final line if a crash
/// interrupted the last append.
///
/// Entries at or below `snapshot_index` are skipped (still validated): a
/// crash between writing the snapshot and rewriting the log leaves the old
/// log overlapping the boundary, and reopening must complete the
/// compaction's effect idempotently. The retained log must then continue
/// exactly at `snapshot_index + 1`.
fn replay_log(dir: &Path, snapshot_index: LogIndex) -> Result<Vec<LogEntry>, StorageError> {
    let path = dir.join(LOG_FILE);
    let bytes = match fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };

    let mut log: Vec<LogEntry> = Vec::new();
    let mut previous_index: Option<LogIndex> = None;
    let mut good_end = 0usize; // byte offset just past the last valid line
    let mut offset = 0usize;

    for chunk in bytes.split_inclusive(|&b| b == b'\n') {
        let complete = chunk.ends_with(b"\n");
        let line_start = offset;
        offset += chunk.len();

        match serde_json::from_slice::<LogEntry>(chunk) {
            Ok(entry) if complete => {
                if let Some(previous) = previous_index
                    && entry.index != previous + 1
                {
                    return Err(StorageError::Corrupt(format!(
                        "{}: expected index {} at byte {line_start}, found {}",
                        path.display(),
                        previous + 1,
                        entry.index
                    )));
                }
                previous_index = Some(entry.index);
                if entry.index > snapshot_index {
                    log.push(entry);
                }
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
    if let Some(first) = log.first()
        && first.index != snapshot_index + 1
    {
        return Err(StorageError::Corrupt(format!(
            "{}: log starts at index {} but the snapshot boundary is {snapshot_index}",
            path.display(),
            first.index
        )));
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
                session: None,
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

    // ---- phase 14: snapshot boundary ----

    fn state(marker: u64) -> Value {
        json!({ "marker": marker })
    }

    #[test]
    fn compact_reopen_preserves_boundary_arithmetic() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut storage = Storage::open(dir.path()).unwrap();
            storage
                .append(&[
                    entry(1, 1),
                    entry(1, 2),
                    entry(2, 3),
                    entry(2, 4),
                    entry(3, 5),
                ])
                .unwrap();
            storage.compact_to(3, state(3)).unwrap();

            // In-memory arithmetic immediately after compaction...
            assert_eq!(storage.snapshot_index(), 3);
            assert_eq!(storage.snapshot_term(), 2);
            assert_eq!(storage.last_index(), 5);
            assert_eq!(storage.last_term(), 3);
            assert_eq!(storage.term(2), None, "compacted");
            assert_eq!(storage.term(3), Some(2), "the boundary answers");
            assert_eq!(storage.term(4), Some(2));
            assert_eq!(storage.term(6), None, "past the end");
            assert_eq!(storage.entry(3), None);
            assert_eq!(storage.entry(4), Some(&entry(2, 4)));
            assert_eq!(storage.entries_from(4).len(), 2);
            assert_eq!(
                storage.entries_from(1).len(),
                2,
                "below-boundary = whole tail"
            );
            assert_eq!(storage.entries_from(6), &[]);
            // ...and appends continue contiguously past the retained tail.
            storage.append(&[entry(3, 6)]).unwrap();
        }
        // ...and identically after reopening from disk.
        let storage = Storage::open(dir.path()).unwrap();
        assert_eq!(storage.snapshot_index(), 3);
        assert_eq!(storage.snapshot_term(), 2);
        assert_eq!(storage.last_index(), 6);
        assert_eq!(storage.entries().len(), 3);
        assert_eq!(storage.term(3), Some(2));
        assert_eq!(storage.term(1), None);
        assert_eq!(storage.snapshot().unwrap().state, state(3));
        assert_eq!(storage.snapshot().unwrap().membership, None);
    }

    #[test]
    fn compacting_the_whole_log_leaves_boundary_answers_only() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut storage = Storage::open(dir.path()).unwrap();
            storage.append(&[entry(1, 1), entry(2, 2)]).unwrap();
            storage.compact_to(2, state(2)).unwrap();
            assert_eq!(storage.entries(), &[]);
            assert_eq!(storage.last_index(), 2);
            assert_eq!(storage.last_term(), 2, "falls back to the snapshot term");
            // Appending after a full compaction continues at boundary + 1.
            storage.append(&[entry(3, 3)]).unwrap();
        }
        let storage = Storage::open(dir.path()).unwrap();
        assert_eq!(storage.last_index(), 3);
        assert_eq!(storage.term(3), Some(3));
    }

    #[test]
    fn truncate_at_or_below_the_boundary_errors() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(dir.path()).unwrap();
        storage
            .append(&[entry(1, 1), entry(1, 2), entry(1, 3), entry(1, 4)])
            .unwrap();
        storage.compact_to(3, state(3)).unwrap();

        for from in [0, 1, 3] {
            assert!(
                matches!(storage.truncate_from(from), Err(StorageError::Corrupt(_))),
                "truncate_from({from}) must refuse to rewind compacted entries"
            );
        }
        // Just above the boundary still works.
        storage.truncate_from(4).unwrap();
        assert_eq!(storage.last_index(), 3);
        assert_eq!(storage.entries(), &[]);
    }

    #[test]
    fn compact_outside_the_retained_log_errors() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(dir.path()).unwrap();
        storage.append(&[entry(1, 1), entry(1, 2)]).unwrap();
        storage.compact_to(1, state(1)).unwrap();

        // At/below the current boundary, at the 0 sentinel, and past the end.
        for index in [0, 1, 3] {
            assert!(
                matches!(
                    storage.compact_to(index, state(index)),
                    Err(StorageError::Corrupt(_))
                ),
                "compact_to({index}) must be rejected"
            );
        }
    }

    /// The crash window: snapshot.json written, but the crash hit before the
    /// log rewrite — the old log still overlaps the boundary. Reopening must
    /// skip the overlap and land in exactly the fully-compacted state.
    #[test]
    fn crash_between_snapshot_write_and_log_rewrite_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut storage = Storage::open(dir.path()).unwrap();
            storage
                .append(&[entry(1, 1), entry(1, 2), entry(2, 3), entry(2, 4)])
                .unwrap();
        }
        // Hand-build the window: a snapshot claiming boundary 3 next to the
        // ORIGINAL log file still holding entries 1..=4.
        let snapshot = Snapshot {
            last_included_index: 3,
            last_included_term: 2,
            membership: None,
            state: state(3),
        };
        fs::write(
            dir.path().join(SNAPSHOT_FILE),
            serde_json::to_vec(&snapshot).unwrap(),
        )
        .unwrap();

        let mut storage = Storage::open(dir.path()).unwrap();
        assert_eq!(storage.snapshot_index(), 3);
        assert_eq!(storage.entries(), &[entry(2, 4)]);
        assert_eq!(storage.last_index(), 4);
        assert_eq!(storage.term(3), Some(2));
        assert_eq!(storage.term(1), None);
        // Appends after the repair stay contiguous and a second reopen is
        // clean (idempotence).
        storage.append(&[entry(2, 5)]).unwrap();
        drop(storage);
        let storage = Storage::open(dir.path()).unwrap();
        assert_eq!(storage.last_index(), 5);
        assert_eq!(storage.entries().first(), Some(&entry(2, 4)));
    }

    /// The extreme crash window: the snapshot covers the ENTIRE old log.
    #[test]
    fn crash_window_covering_the_whole_log_reopens_empty() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut storage = Storage::open(dir.path()).unwrap();
            storage.append(&[entry(1, 1), entry(1, 2)]).unwrap();
        }
        let snapshot = Snapshot {
            last_included_index: 2,
            last_included_term: 1,
            membership: None,
            state: state(2),
        };
        fs::write(
            dir.path().join(SNAPSHOT_FILE),
            serde_json::to_vec(&snapshot).unwrap(),
        )
        .unwrap();

        let storage = Storage::open(dir.path()).unwrap();
        assert_eq!(storage.entries(), &[]);
        assert_eq!(storage.last_index(), 2);
        assert_eq!(storage.last_term(), 1);
    }

    /// A retained log that does NOT continue from the snapshot boundary is
    /// corruption, not a legal crash window.
    #[test]
    fn log_gap_after_the_boundary_fails_loudly() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut storage = Storage::open(dir.path()).unwrap();
            storage.append(&[entry(1, 1), entry(1, 2)]).unwrap();
            storage.compact_to(2, state(2)).unwrap();
            storage.append(&[entry(2, 3)]).unwrap();
        }
        // Forge a snapshot claiming a LOWER boundary than the log continues
        // from: the retained log would start at 3 against a boundary of 1.
        let snapshot = Snapshot {
            last_included_index: 1,
            last_included_term: 1,
            membership: None,
            state: state(1),
        };
        fs::write(
            dir.path().join(SNAPSHOT_FILE),
            serde_json::to_vec(&snapshot).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            Storage::open(dir.path()),
            Err(StorageError::Corrupt(_))
        ));
    }

    #[test]
    fn install_snapshot_retains_a_matching_suffix_and_clears_a_divergent_log() {
        // Matching boundary entry: the suffix beyond it survives.
        let dir = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(dir.path()).unwrap();
        storage
            .append(&[entry(1, 1), entry(1, 2), entry(2, 3), entry(2, 4)])
            .unwrap();
        storage
            .install_snapshot(&Snapshot {
                last_included_index: 3,
                last_included_term: 2,
                membership: None,
                state: state(3),
            })
            .unwrap();
        assert_eq!(storage.entries(), &[entry(2, 4)]);
        assert_eq!(storage.last_index(), 4);

        // Divergent boundary entry (or none at all): the log is cleared.
        let dir = tempfile::tempdir().unwrap();
        let mut storage = Storage::open(dir.path()).unwrap();
        storage.append(&[entry(1, 1), entry(1, 2)]).unwrap();
        storage
            .install_snapshot(&Snapshot {
                last_included_index: 5,
                last_included_term: 4,
                membership: None,
                state: state(5),
            })
            .unwrap();
        assert_eq!(storage.entries(), &[]);
        assert_eq!(storage.last_index(), 5);
        assert_eq!(storage.last_term(), 4);
        // Regressing the boundary is refused (the caller guards on
        // commit_index, so reaching this would be a bug).
        assert!(matches!(
            storage.install_snapshot(&Snapshot {
                last_included_index: 5,
                last_included_term: 4,
                membership: None,
                state: state(5),
            }),
            Err(StorageError::Corrupt(_))
        ));
        // And the installed snapshot survives reopen.
        drop(storage);
        let storage = Storage::open(dir.path()).unwrap();
        assert_eq!(storage.snapshot_index(), 5);
        assert_eq!(storage.snapshot().unwrap().state, state(5));
    }

    /// Old data dirs (no snapshot.json) must open byte-identically: same
    /// contents, same arithmetic, no snapshot file conjured into existence.
    #[test]
    fn pre_snapshot_data_dirs_open_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let entries = vec![entry(1, 1), entry(1, 2), entry(2, 3)];
        {
            let mut storage = Storage::open(dir.path()).unwrap();
            storage.append(&entries).unwrap();
        }
        let log_bytes = fs::read(dir.path().join(LOG_FILE)).unwrap();

        let storage = Storage::open(dir.path()).unwrap();
        assert!(storage.snapshot().is_none());
        assert_eq!(storage.snapshot_index(), 0);
        assert_eq!(storage.snapshot_term(), 0);
        assert_eq!(storage.entries(), &entries);
        assert_eq!(storage.term(0), Some(0), "the index-0 sentinel");
        assert_eq!(storage.last_index(), 3);
        drop(storage);
        assert!(!dir.path().join(SNAPSHOT_FILE).exists());
        assert_eq!(
            fs::read(dir.path().join(LOG_FILE)).unwrap(),
            log_bytes,
            "opening must not rewrite the log"
        );
    }

    #[test]
    fn torn_final_line_is_repaired_even_past_a_snapshot_boundary() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut storage = Storage::open(dir.path()).unwrap();
            storage
                .append(&[entry(1, 1), entry(1, 2), entry(1, 3)])
                .unwrap();
            storage.compact_to(2, state(2)).unwrap();
        }
        let path = dir.path().join(LOG_FILE);
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(br#"{"term":1,"index":4,"comm"#).unwrap();
        f.sync_all().unwrap();

        let mut storage = Storage::open(dir.path()).unwrap();
        assert_eq!(storage.last_index(), 3);
        storage.append(&[entry(1, 4)]).unwrap();
        drop(storage);
        let storage = Storage::open(dir.path()).unwrap();
        assert_eq!(storage.last_index(), 4);
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
