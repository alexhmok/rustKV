//! Black-box linearizability checker for a per-key last-write-wins register
//! (Put / Delete / Get), in the style of Jepsen's Knossos: given a history
//! of operations with invocation/return times, decide whether some
//! linearization is consistent with the register semantics and real time.
//!
//! Algorithm: Wing & Gong depth-first search with memoization on
//! (linearized-set, register state). Linearizability is compositional, so
//! each key is checked independently — per-key histories stay small enough
//! for a u64 op mask.
//!
//! Outcome semantics (mirrors Jepsen):
//! - `Ok` write / read: definitely happened; must be linearized.
//! - `Fail` write: definitely did NOT happen; excluded before checking.
//! - `Unknown` write (client timeout): may take effect at any time after
//!   its invocation — or never. Modeled as return time = ∞ and optional.

use std::collections::{HashMap, HashSet};

pub type Value = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteKind {
    Put(Value),
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    Ok,
    Fail,
    Unknown,
}

#[derive(Debug, Clone)]
pub enum OpKind {
    Write {
        kind: WriteKind,
        outcome: WriteOutcome,
        /// (term, index) assigned by the leader, when the proposal was
        /// accepted — used by white-box witness checks, not by this checker.
        log_pos: Option<(u64, u64)>,
    },
    Read {
        result: Option<Value>,
    },
}

#[derive(Debug, Clone)]
pub struct Recorded {
    pub process: u64,
    pub key: String,
    pub op: OpKind,
    pub invoked_us: u64,
    /// `u64::MAX` when the outcome is unknown.
    pub returned_us: u64,
}

/// Checks every key of `history` independently. `Err` carries the first
/// offending key and its rendered history.
pub fn check_linearizable(history: &[Recorded]) -> Result<(), String> {
    let mut by_key: HashMap<&str, Vec<&Recorded>> = HashMap::new();
    for record in history {
        by_key.entry(&record.key).or_default().push(record);
    }
    let mut keys: Vec<&str> = by_key.keys().copied().collect();
    keys.sort_unstable();
    for key in keys {
        check_key(&by_key[key])
            .map_err(|reason| format!("key {key:?}: {reason}\n{}", render(&by_key[key])))?;
    }
    Ok(())
}

enum Action {
    Set(Option<Value>),
    Expect(Option<Value>),
}

struct Entry {
    inv: u64,
    ret: u64,
    action: Action,
    required: bool,
}

fn check_key(history: &[&Recorded]) -> Result<(), String> {
    let mut ops = Vec::new();
    for record in history {
        match &record.op {
            OpKind::Write {
                outcome: WriteOutcome::Fail,
                ..
            } => continue, // definitely never happened
            OpKind::Write { kind, outcome, .. } => ops.push(Entry {
                inv: record.invoked_us,
                ret: if *outcome == WriteOutcome::Unknown {
                    u64::MAX
                } else {
                    record.returned_us
                },
                action: Action::Set(match kind {
                    WriteKind::Put(value) => Some(*value),
                    WriteKind::Delete => None,
                }),
                required: *outcome == WriteOutcome::Ok,
            }),
            OpKind::Read { result } => ops.push(Entry {
                inv: record.invoked_us,
                ret: record.returned_us,
                action: Action::Expect(*result),
                required: true,
            }),
        }
    }
    assert!(
        ops.len() <= 63,
        "history too large for the u64-mask checker"
    );
    let required_mask: u64 = ops
        .iter()
        .enumerate()
        .filter(|(_, e)| e.required)
        .map(|(i, _)| 1u64 << i)
        .sum();

    let mut memo = HashSet::new();
    if search(&ops, required_mask, &mut memo, 0, None) {
        Ok(())
    } else {
        Err("no valid linearization exists".to_string())
    }
}

fn search(
    ops: &[Entry],
    required_mask: u64,
    memo: &mut HashSet<(u64, Option<Value>)>,
    mask: u64,
    state: Option<Value>,
) -> bool {
    if mask & required_mask == required_mask {
        // Leftover unknown writes "never happened" — legal.
        return true;
    }
    if !memo.insert((mask, state)) {
        return false;
    }
    // An op is a candidate iff its invocation does not come after the
    // return of any other un-linearized op (`<=` tolerates timestamp ties).
    let min_ret = ops
        .iter()
        .enumerate()
        .filter(|(i, _)| mask & (1 << i) == 0)
        .map(|(_, e)| e.ret)
        .min()
        .unwrap_or(u64::MAX);
    for (i, entry) in ops.iter().enumerate() {
        if mask & (1 << i) != 0 || entry.inv > min_ret {
            continue;
        }
        let next = mask | (1 << i);
        let feasible = match entry.action {
            Action::Expect(expected) => {
                state == expected && search(ops, required_mask, memo, next, state)
            }
            Action::Set(new_state) => search(ops, required_mask, memo, next, new_state),
        };
        if feasible {
            return true;
        }
    }
    false
}

/// Human-readable history dump for failure reports, ordered by invocation.
pub fn render(history: &[&Recorded]) -> String {
    let mut sorted: Vec<&&Recorded> = history.iter().collect();
    sorted.sort_by_key(|r| r.invoked_us);
    let mut out = String::new();
    for r in sorted {
        let ret = if r.returned_us == u64::MAX {
            "?".to_string()
        } else {
            r.returned_us.to_string()
        };
        out.push_str(&format!(
            "  p{} [{} .. {ret}µs] {:?}\n",
            r.process, r.invoked_us, r.op
        ));
    }
    out
}
