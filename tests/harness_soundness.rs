//! Testing-regime phase T2: sensitivity battery for the WGL
//! linearizability checker (`common::lin`).
//!
//! These are negative tests of the CHECKER, not of rustkv: each history
//! encodes one of the violations the harness claims to guard against — a
//! stale read, a lost acknowledged write, a double-applied write — and the
//! checker must REJECT it. A checker that never fires is indistinguishable
//! from one that works. Each rejection is paired with the nearest legal
//! history the checker must ACCEPT, so a probe can't pass by the checker
//! simply rejecting everything.
//!
//! (The basic accept/reject cases from phase 8 live in tests/jepsen.rs;
//! this battery adds the violation classes T2 calls out explicitly, the
//! sharp real-time boundaries between them, and the guard rails of the
//! checker's own representation limits.)

mod common;

use common::lin::{OpKind, Recorded, WriteKind, WriteOutcome, check_linearizable};

fn read(process: u64, key: &str, result: Option<u64>, invoked_us: u64) -> Recorded {
    Recorded {
        process,
        key: key.to_string(),
        op: OpKind::Read { result },
        invoked_us,
        returned_us: invoked_us + 1,
    }
}

fn write(
    process: u64,
    key: &str,
    kind: WriteKind,
    outcome: WriteOutcome,
    invoked_us: u64,
    returned_us: u64,
) -> Recorded {
    Recorded {
        process,
        key: key.to_string(),
        op: OpKind::Write {
            kind,
            outcome,
            log_pos: None,
        },
        invoked_us,
        returned_us: if outcome == WriteOutcome::Unknown {
            u64::MAX
        } else {
            returned_us
        },
    }
}

fn put(process: u64, key: &str, value: u64, invoked_us: u64, returned_us: u64) -> Recorded {
    write(
        process,
        key,
        WriteKind::Put(value),
        WriteOutcome::Ok,
        invoked_us,
        returned_us,
    )
}

// ---- lost acknowledged write ----

/// The client was told the write committed; no read ever sees it. In a
/// delete-free history the register can only hold the written value after
/// an Ok put, so a later `None` read means the acknowledged write is gone.
#[test]
fn a_lost_acknowledged_write_is_rejected() {
    let h = vec![put(0, "k", 1, 0, 10), read(1, "k", None, 20)];
    assert!(check_linearizable(&h).is_err());

    // Nearest legal history: the same shape but the write was only ever
    // ambiguous — "never happened" is then a legal fate.
    let h = vec![
        write(0, "k", WriteKind::Put(1), WriteOutcome::Unknown, 0, 0),
        read(1, "k", None, 20),
    ];
    check_linearizable(&h).unwrap();
}

/// A lost acknowledged write observed the other way round: a later
/// acknowledged write is visible, but rolling back to the FIRST write's
/// value afterwards means the second was lost mid-history (resurrection of
/// the overwritten value).
#[test]
fn a_lost_acknowledged_overwrite_is_rejected() {
    let h = vec![
        put(0, "k", 1, 0, 10),
        put(0, "k", 2, 20, 30),
        read(1, "k", Some(2), 40),
        read(1, "k", Some(1), 60),
    ];
    assert!(check_linearizable(&h).is_err());
}

// ---- stale read ----

/// The sharp real-time boundary: a read overlapping the second write may
/// legally see either value; the same read moved past the write's return
/// is stale and must be rejected. The accept leg keeps the reject leg
/// honest — the checker distinguishes the two by timing alone.
#[test]
fn staleness_is_judged_by_real_time_not_by_value() {
    // Read invoked at 25, while put(2) [20..30] is still in flight: 1 is
    // a legal result (the read linearizes before the write).
    let h = vec![
        put(0, "k", 1, 0, 10),
        put(0, "k", 2, 20, 30),
        read(1, "k", Some(1), 25),
    ];
    check_linearizable(&h).unwrap();

    // The identical read invoked at 31 — after put(2) returned — is stale.
    let h = vec![
        put(0, "k", 1, 0, 10),
        put(0, "k", 2, 20, 30),
        read(1, "k", Some(1), 31),
    ];
    assert!(check_linearizable(&h).is_err());
}

/// Compositionality: per-key checking must still find a violation buried
/// among healthy keys — one stale read on "b" fails the whole history even
/// though "a" and "c" linearize fine.
#[test]
fn a_violation_on_one_key_among_many_is_still_found() {
    let h = vec![
        put(0, "a", 1, 0, 10),
        put(0, "b", 1, 0, 10),
        put(0, "c", 1, 0, 10),
        put(0, "b", 2, 20, 30),
        read(1, "a", Some(1), 40),
        read(1, "b", Some(1), 40), // stale: b=2 returned at 30
        read(1, "c", Some(1), 40),
    ];
    let reason = check_linearizable(&h).unwrap_err();
    assert!(reason.contains("key \"b\""), "wrong key blamed: {reason}");
}

// ---- double-applied write ----

/// The dedup violation the tokened workloads exist to prevent, as the
/// checker sees it: client A's write is recorded ONCE as Ok (the
/// exactly-once claim), client B overwrites it, and then A's value
/// reappears — a duplicate application of A's command surfacing after B.
/// With A's op recorded Ok-and-returned there is no linearization that
/// explains the reappearance, so the checker must reject.
#[test]
fn a_double_applied_write_surfacing_late_is_rejected() {
    let h = vec![
        put(0, "k", 1, 0, 10),     // A, acked, recorded exactly once
        put(1, "k", 2, 20, 30),    // B, acked
        read(2, "k", Some(2), 40), // B's write visible...
        read(2, "k", Some(1), 60), // ...then A's value resurrects
    ];
    assert!(check_linearizable(&h).is_err());

    // The honesty boundary: if A's outcome had stayed Unknown (the
    // UNtokened at-least-once contract), the late reappearance is legal —
    // an unknown write may take effect at any time after its invocation.
    // This pair is exactly why the tokened workload's tighter Ok-once
    // recording carries the exactly-once claim: the checker can only
    // reject the anomaly because dedup lets the history say "returned".
    let h = vec![
        write(0, "k", WriteKind::Put(1), WriteOutcome::Unknown, 0, 0),
        put(1, "k", 2, 20, 30),
        read(2, "k", Some(2), 40),
        read(2, "k", Some(1), 60),
    ];
    check_linearizable(&h).unwrap();
}

/// A double-applied DELETE: the key vanishes again after a confirmed
/// re-put. Same class as the put case, on the other write kind.
#[test]
fn a_double_applied_delete_is_rejected() {
    let h = vec![
        put(0, "k", 1, 0, 10),
        write(0, "k", WriteKind::Delete, WriteOutcome::Ok, 20, 30),
        put(0, "k", 2, 40, 50),
        read(1, "k", Some(2), 60),
        read(1, "k", None, 80), // the delete "applies again"
    ];
    assert!(check_linearizable(&h).is_err());
}

// ---- unknowns are bounded, not a free pass ----

/// An unknown write can explain ITS value appearing — never a value nobody
/// wrote, and never at a time before its invocation.
#[test]
fn unknown_writes_do_not_excuse_arbitrary_values() {
    // A value nobody wrote, with an unknown write in the history.
    let h = vec![
        write(0, "k", WriteKind::Put(9), WriteOutcome::Unknown, 0, 0),
        read(1, "k", Some(8), 20),
    ];
    assert!(check_linearizable(&h).is_err());

    // The unknown write's value read BEFORE the write was even invoked.
    let h = vec![
        read(1, "k", Some(9), 0),
        write(0, "k", WriteKind::Put(9), WriteOutcome::Unknown, 10, 0),
    ];
    assert!(check_linearizable(&h).is_err());
}

// ---- representation guard rails ----

/// The checker's documented 63-ops-per-key cap must trip loudly, not
/// silently mis-check a truncated history.
#[test]
#[should_panic(expected = "history too large")]
fn oversized_per_key_histories_are_refused_not_mischecked() {
    let mut h = Vec::new();
    for i in 0..64u64 {
        h.push(put(0, "k", i + 1, i * 10, i * 10 + 5));
    }
    let _ = check_linearizable(&h);
}
