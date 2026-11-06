// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! Tests for `crates/store-memory/src/v3.rs`: the store v3 slots' dedupe (S1-S5) and the count batches.

use super::*;
use crate::v3::Stage;
use busbar_contract::abi::sdk::store::OpRefused;
use busbar_contract::abi::store::{OpId, OP_ID_RETENTION_SECS};
use busbar_contract::records::{ModelTokensDelta, RecordStore, UsageDelta};

fn op(n: u64) -> OpId {
    OpId::from_parts(7, n)
}

fn delta(requests: i64, input: i128) -> UsageDelta {
    UsageDelta {
        requests,
        billable_requests: requests,
        models: vec![ModelTokensDelta {
            scale: 6,
            model: "m".to_string(),
            usage_units: [("input".to_string(), input)].into_iter().collect(),
        }],
    }
}

type BatchObserver = Box<dyn FnMut(&MemoryStore)>;

thread_local! {
    static BATCH_OBSERVER: std::cell::RefCell<Option<BatchObserver>> =
        const { std::cell::RefCell::new(None) };
}

fn batch_item_applied(store: &MemoryStore, stage: Stage) {
    if stage != Stage::BatchItem {
        return;
    }
    BATCH_OBSERVER.with_borrow_mut(|observer| {
        if let Some(observer) = observer {
            observer(store);
        }
    });
}

/// A store whose count batches report each applied item to [`observe_batch`]'s observer.
fn observed() -> MemoryStore {
    let mut store = MemoryStore::new();
    store.observe_stages(batch_item_applied);
    store
}

fn observe_batch(observer: impl FnMut(&MemoryStore) + 'static, run: impl FnOnce()) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            BATCH_OBSERVER.with_borrow_mut(|observer| *observer = None);
        }
    }
    BATCH_OBSERVER.with_borrow_mut(|slot| {
        assert!(slot.replace(Box::new(observer)).is_none());
    });
    let _reset = Reset;
    run();
}

/// Start a real reader between each pair of cells.
fn concurrent_batch_reads(
    store: std::sync::Arc<MemoryStore>,
    available: fn(&MemoryStore) -> bool,
    snapshot: fn(&MemoryStore) -> u64,
    run: impl FnOnce(&MemoryStore),
) -> Vec<u64> {
    use std::{cell::RefCell, rc::Rc, sync::mpsc};

    let pending = Rc::new(RefCell::new(Vec::new()));
    let readers = pending.clone();
    let seen = Rc::new(RefCell::new(Vec::new()));
    let observations = seen.clone();
    let shared = store.clone();
    observe_batch(
        move |_| {
            let store = shared.clone();
            let (started_tx, started_rx) = mpsc::sync_channel(0);
            let (result_tx, result_rx) = mpsc::channel();
            let reader = std::thread::spawn(move || {
                started_tx.send(available(&store)).unwrap();
                result_tx.send(snapshot(&store)).unwrap();
            });
            if started_rx.recv().unwrap() {
                observations.borrow_mut().push(result_rx.recv().unwrap());
                reader.join().unwrap();
            } else {
                readers.borrow_mut().push((result_rx, reader));
            }
        },
        || run(&store),
    );
    for (result, reader) in pending.borrow_mut().drain(..) {
        seen.borrow_mut().push(result.recv().unwrap());
        reader.join().unwrap();
    }
    let values = seen.borrow().clone();
    assert_eq!(values.len(), 2, "both between-cell readers must finish");
    values
}

#[test]
fn readers_never_observe_part_of_a_usage_batch() {
    use busbar_contract::abi::sdk::store::{Op, Step};
    use std::sync::Arc;

    let store = Arc::new(observed());
    let seen = concurrent_batch_reads(
        store.clone(),
        |store| store.usage.try_read().is_ok(),
        |store| {
            store.get_usage("k", 0).unwrap().requests
                + store.get_usage("other", 0).unwrap().requests
        },
        |store| match busbar_contract::abi::sdk::store::StoreSlots::add_usage_batch(
            store,
            &mut Op::detached(),
            op(101),
            // Staging coalesces repeated keys. Use two rows to exercise the lock between
            // separate publications, rather than observing a single coalesced row twice.
            &[("k", 0, delta(3, 0)), ("other", 0, delta(4, 0))],
        ) {
            Step::Ready(result) => result.expect("apply usage batch"),
            Step::Pending { .. } => panic!("memory must complete inline"),
        },
    );
    assert_eq!(store.get_usage("k", 0).unwrap().requests, 3);
    assert_eq!(store.get_usage("other", 0).unwrap().requests, 4);
    assert_eq!(seen, vec![7, 7], "readers saw partial usage batches");
}

#[test]
fn readers_never_observe_part_of_a_metering_batch() {
    use busbar_contract::abi::sdk::store::{Op, Step};
    use std::sync::Arc;

    let store = Arc::new(observed());
    let mut a = metering(0);
    a.requests = 3;
    let mut b = a.clone();
    b.requests = 4;
    b.key_id = "other".into();
    let seen = concurrent_batch_reads(
        store.clone(),
        |store| store.metering.try_read().is_ok(),
        |store| {
            store
                .list_metering(86_400)
                .unwrap()
                .iter()
                .map(|row| row.requests)
                .sum()
        },
        |store| match busbar_contract::abi::sdk::store::StoreSlots::add_metering_batch(
            store,
            &mut Op::detached(),
            op(102),
            &[a, b],
        ) {
            Step::Ready(result) => result.expect("apply metering batch"),
            Step::Pending { .. } => panic!("memory must complete inline"),
        },
    );
    assert_eq!(store.list_metering(86_400).unwrap().len(), 2);
    assert_eq!(seen, vec![7, 7], "readers saw partial metering batches");
}

#[test]
fn usage_purge_cannot_split_a_batch() {
    use busbar_contract::abi::sdk::store::{Op, Step};
    use std::{cell::Cell, rc::Rc};

    let s = observed();
    s.pin_clock(1);
    let purged = Rc::new(Cell::new(false));
    let ran = purged.clone();
    observe_batch(
        move |store| {
            let available = store.usage.try_write().is_ok();
            if available && !ran.replace(true) {
                match busbar_contract::abi::sdk::store::StoreSlots::purge_usage_before(
                    store,
                    &mut Op::detached(),
                    2,
                ) {
                    Step::Ready(result) => assert_eq!(result.unwrap(), 1),
                    Step::Pending { .. } => panic!("memory must complete inline"),
                }
            }
        },
        || match busbar_contract::abi::sdk::store::StoreSlots::add_usage_batch(
            &s,
            &mut Op::detached(),
            op(103),
            &[("k", 1, delta(3, 0)), ("k", 1, delta(4, 0))],
        ) {
            Step::Ready(result) => result.expect("apply usage batch"),
            Step::Pending { .. } => panic!("memory must complete inline"),
        },
    );
    let requests = s.get_usage("k", 1).unwrap().requests;
    assert!(
        requests == 0 || requests == 7,
        "purge interleaved with batch cells, leaving {requests} requests"
    );
    if !purged.get() {
        assert_eq!(s.v3_purge_usage_before(2).unwrap(), 1);
    }
    assert_eq!(s.get_usage("k", 1).unwrap().requests, 0);
}

#[test]
fn the_usage_sweep_runs_after_the_whole_batch() {
    use busbar_contract::abi::sdk::store::{Op, Step};

    let s = MemoryStore::new();
    s.pin_clock(MAX_RETENTION_SECS + 2);
    s.usage_sweep_ticker
        .store(SWEEP_INTERVAL - 1, Ordering::Relaxed);
    match busbar_contract::abi::sdk::store::StoreSlots::add_usage_batch(
        &s,
        &mut Op::detached(),
        op(104),
        &[
            ("k", 1, delta(3, 0)),
            ("other", 1, delta(5, 0)),
            ("k", 1, delta(4, 0)),
        ],
    ) {
        Step::Ready(result) => result.expect("apply usage batch"),
        Step::Pending { .. } => panic!("memory must complete inline"),
    }
    assert_eq!(
        s.get_usage("k", 1).unwrap().requests,
        0,
        "an expired row must not be recreated from only the batch's trailing cells"
    );
    assert_eq!(s.get_usage("other", 1).unwrap().requests, 0);
    // Three deltas coalesce to two published rows, each counted once by the sweep ticker.
    assert_eq!(
        s.usage_sweep_ticker.load(Ordering::Relaxed),
        SWEEP_INTERVAL + 1
    );
}

#[test]
fn a_late_invalid_metering_cell_preserves_existing_rows_and_the_op_id() {
    let store = MemoryStore::new();
    let initial = metering(1);
    store.add_metering(&initial).unwrap();
    let before = store.list_metering(initial.bucket).unwrap();
    let mut valid = initial.clone();
    valid.requests = 20;
    let invalid = metering(i64::MAX as u64 + 1);
    assert!(matches!(
        store.v3_add_metering_batch(op(105), &[valid.clone(), invalid]),
        Err(OpRefused::Refused(_))
    ));
    assert!(store.list_metering(initial.bucket).unwrap() == before);
    store
        .v3_add_metering_batch(op(105), std::slice::from_ref(&valid))
        .expect("refusal must not remember the operation id");
    let applied = store.list_metering(initial.bucket).unwrap();
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].requests, initial.requests + valid.requests);
    store
        .v3_add_metering_batch(op(105), std::slice::from_ref(&valid))
        .expect("replay after a lost success reply must apply nothing");
    assert!(store.list_metering(initial.bucket).unwrap() == applied);
    assert_eq!(
        store.v3_add_metering_batch(op(105), &[initial]),
        Err(OpRefused::Conflict)
    );
    assert!(store.list_metering(valid.bucket).unwrap() == applied);
}

#[test]
fn empty_batches_do_not_change_rows_or_run_usage_retention() {
    let store = MemoryStore::new();
    store.pin_clock(MAX_RETENTION_SECS + 2);
    store.put_usage("k", 1, &UsageLedger::default()).unwrap();
    store
        .usage_sweep_ticker
        .store(SWEEP_INTERVAL - 1, Ordering::Relaxed);
    store.v3_add_usage_batch(op(106), &[]).unwrap();
    store.v3_add_metering_batch(op(107), &[]).unwrap();
    assert_eq!(store.v3_purge_usage_before(2).unwrap(), 1);
    assert_eq!(
        store.usage_sweep_ticker.load(Ordering::Relaxed),
        SWEEP_INTERVAL - 1
    );
    assert!(store.list_metering(86_400).unwrap().is_empty());
}

#[test]
fn batch_retention_preserves_all_time_usage_and_operator_billing_history() {
    let store = MemoryStore::new();
    store.pin_clock(MAX_RETENTION_SECS + 2);
    store
        .usage_sweep_ticker
        .store(SWEEP_INTERVAL - 1, Ordering::Relaxed);
    store
        .v3_add_usage_batch(op(108), &[("k", 0, delta(3, 0)), ("k", 1, delta(4, 0))])
        .unwrap();
    assert_eq!(store.get_usage("k", 0).unwrap().requests, 3);
    assert_eq!(store.get_usage("k", 1).unwrap().requests, 0);
    let mut old = metering(1);
    old.bucket = 1;
    let mut recent = old.clone();
    recent.bucket = MAX_RETENTION_SECS + 2;
    store
        .v3_add_metering_batch(op(109), &[old, recent])
        .unwrap();
    assert_eq!(store.list_metering(1).unwrap()[0].requests, 1);
    assert_eq!(
        store.list_metering(MAX_RETENTION_SECS + 2).unwrap()[0].requests,
        1
    );
    assert_eq!(store.v3_purge_usage_before(u64::MAX).unwrap(), 0);
    assert_eq!(store.get_usage("k", 0).unwrap().requests, 3);
    assert_eq!(store.list_metering(1).unwrap()[0].requests, 1);
}

#[test]
fn the_memory_store_states_it_is_ephemeral_and_refuses_forks() {
    assert_eq!(
        <MemoryStore as busbar_contract::abi::sdk::store::StoreSlots>::TAIL,
        busbar_contract::abi::sdk::store::Tail {
            ephemeral: true,
            durable_plane: false,
            fork_refusal: true,
        }
    );
}

#[test]
fn a_replayed_usage_batch_applies_once() {
    let s = MemoryStore::new();
    let cells = [("k", 60, delta(1, 10))];
    s.v3_add_usage_batch(op(1), &cells).expect("first");
    s.v3_add_usage_batch(op(1), &cells)
        .expect("replay answers the original");
    assert_eq!(s.get_usage("k", 60).expect("read").requests, 1);
}

#[test]
fn equal_bodies_under_distinct_op_ids_both_apply() {
    let s = MemoryStore::new();
    let cells = [("k", 60, delta(2, 4))];
    s.v3_add_usage_batch(op(1), &cells).expect("a");
    s.v3_add_usage_batch(op(2), &cells).expect("b");
    assert_eq!(s.get_usage("k", 60).expect("read").requests, 4);
}

#[test]
fn a_reused_op_id_with_a_different_body_is_a_conflict_and_applies_nothing() {
    let s = MemoryStore::new();
    s.v3_add_usage_batch(op(1), &[("k", 60, delta(1, 1))])
        .expect("first");
    assert_eq!(
        s.v3_add_usage_batch(op(1), &[("k", 60, delta(5, 5))]),
        Err(OpRefused::Conflict)
    );
    assert_eq!(s.get_usage("k", 60).expect("read").requests, 1);
}

#[test]
fn an_op_id_reused_across_slots_is_a_conflict() {
    let s = MemoryStore::new();
    s.v3_add_usage_batch(op(1), &[("k", 60, delta(1, 1))])
        .expect("usage");
    assert_eq!(
        s.v3_add_metering_batch(op(1), &[metering(5)]),
        Err(OpRefused::Conflict)
    );
    assert!(s.list_metering(86_400).expect("list").is_empty());
}

#[test]
fn a_usage_batch_applies_its_cells_in_order() {
    let s = MemoryStore::new();
    let neg = UsageDelta {
        requests: 0,
        billable_requests: -1,
        models: vec![],
    };
    let pos = UsageDelta {
        requests: 0,
        billable_requests: 1,
        models: vec![],
    };
    s.v3_add_usage_batch(op(1), &[("k", 60, neg), ("k", 60, pos)])
        .expect("batch");

    assert_eq!(s.get_usage("k", 60).expect("read").billable_requests, 1);
}

#[test]
fn a_metering_batch_replay_applies_once() {
    let s = MemoryStore::new();
    let d = MeteringDelta {
        scale: 6,
        key_id: "k".into(),
        bucket: 86_400,
        model: "m".into(),
        provider: "p".into(),
        tokens_input: 3,
        tokens_output: 0,
        tokens_cache_read: 0,
        tokens_cache_write: 0,
        requests: 2,
        billable_requests: 2,
        key_group_at_use: String::new(),
        pricing_version: String::new(),
        priced_from_ms: 0,
        usage_units: Default::default(),
    };
    s.v3_add_metering_batch(op(1), std::slice::from_ref(&d))
        .expect("a");
    s.v3_add_metering_batch(op(1), std::slice::from_ref(&d))
        .expect("replay");
    let rows = s.list_metering(86_400).expect("list");
    assert_eq!(rows.iter().map(|r| r.requests).sum::<u64>(), 2);
}

#[test]
fn an_op_id_is_forgotten_after_its_retention() {
    let s = MemoryStore::new();
    s.pin_clock(1_000);
    s.v3_add_usage_batch(op(1), &[("k", 60, delta(1, 1))])
        .expect("a");
    s.pin_clock(1_000 + OP_ID_RETENTION_SECS);

    s.v3_add_usage_batch(op(2), &[("j", 60, delta(1, 1))])
        .expect("b");
    s.v3_add_usage_batch(op(1), &[("k", 60, delta(1, 1))])
        .expect("new again");
    assert_eq!(s.get_usage("k", 60).expect("read").requests, 2);
}

#[test]
fn an_op_id_is_remembered_inside_its_retention() {
    let s = MemoryStore::new();
    s.pin_clock(1_000);
    s.v3_add_usage_batch(op(1), &[("k", 60, delta(1, 1))])
        .expect("a");
    s.pin_clock(1_000 + OP_ID_RETENTION_SECS - 1);

    s.v3_add_usage_batch(op(2), &[("j", 60, delta(1, 1))])
        .expect("b");
    s.v3_add_usage_batch(op(1), &[("k", 60, delta(1, 1))])
        .expect("replay");
    assert_eq!(s.get_usage("k", 60).expect("read").requests, 1);
}

/// THE DEDUPE LOG HOLDS A FIXED-SIZE RECORD PER OP (the store dedupe rule: "same" means the op's value fields, kept at least `OP_ID_RETENTION_SECS`).
#[test]
fn the_dedupe_log_keeps_a_fixed_size_record_per_op() {
    let s = MemoryStore::new();
    let big: Vec<(&str, u64, UsageDelta)> = (0..64).map(|w| ("k", w, delta(1, 500))).collect();
    s.v3_add_usage_batch(op(1), &big).expect("apply");
    let held = s.v3.op_bytes();
    assert!(held <= 64, "one op keeps {held} bytes in the dedupe log");

    s.v3_add_usage_batch(op(1), &big).expect("replay");
    assert!(matches!(
        s.v3_add_usage_batch(op(1), &big[..1]),
        Err(OpRefused::Conflict)
    ));
}

#[test]
fn a_plane_append_replays_once_and_a_reused_op_id_conflicts() {
    use busbar_contract::records::{PlaneDisposition, PlaneRecord, PlaneSelector};
    let s = MemoryStore::new();
    let event = |seq: u64, body: &[u8]| PlaneRecord {
        kind: "task_event".to_string(),
        id: "t1".to_string(),
        parent: Some("t1".to_string()),
        seq,
        ts: 0,
        disposition: PlaneDisposition::Active,
        body: body.to_vec(),
    };
    let (a, b) = (event(1, b"a"), event(2, b"b"));
    s.v3_append_plane_record_op(op(1), a.view()).expect("first");
    s.v3_append_plane_record_op(op(1), a.view())
        .expect("replay");
    assert_eq!(
        s.v3_append_plane_record_op(op(1), b.view()),
        Err(OpRefused::Conflict)
    );
    let rows = s
        .list_plane_records("task_event", &PlaneSelector::All)
        .expect("list");
    assert_eq!(rows, vec![b"a".to_vec()]);
}

/// A NEW op_id whose append forks an occupied `(parent, seq)` answers `Failed` and records nothing: the same op_id retried with a non-forking record applies (not Replay/Conflict).
#[test]
fn a_failed_append_does_not_record_its_op_id() {
    use busbar_contract::records::{PlaneDisposition, PlaneRecord, PlaneSelector};
    let s = MemoryStore::new();
    let event = |seq: u64, body: &[u8]| PlaneRecord {
        kind: "task_event".to_string(),
        id: "t1".to_string(),
        parent: Some("t1".to_string()),
        seq,
        ts: 0,
        disposition: PlaneDisposition::Active,
        body: body.to_vec(),
    };
    s.v3_append_plane_record_op(op(1), event(1, b"a").view())
        .expect("first");
    assert!(matches!(
        s.v3_append_plane_record_op(op(2), event(1, b"fork").view()),
        Err(OpRefused::Failed(_))
    ));
    s.v3_append_plane_record_op(op(2), event(2, b"b").view())
        .expect("the retry applies: the failed op was never recorded");
    let rows = s
        .list_plane_records("task_event", &PlaneSelector::All)
        .expect("list");
    assert_eq!(rows, vec![b"a".to_vec(), b"b".to_vec()]);
}

fn metering(priced_from_ms: u64) -> MeteringDelta {
    MeteringDelta {
        scale: 6,
        key_id: "k".into(),
        bucket: 86_400,
        model: "m".into(),
        provider: "p".into(),
        tokens_input: 1,
        tokens_output: 0,
        tokens_cache_read: 0,
        tokens_cache_write: 0,
        requests: 1,
        billable_requests: 1,
        key_group_at_use: String::new(),
        pricing_version: String::new(),
        priced_from_ms,
        usage_units: Default::default(),
    }
}

fn plane(
    kind: &str,
    parent: Option<&str>,
    seq: u64,
    ts: u64,
) -> busbar_contract::records::PlaneRecord {
    busbar_contract::records::PlaneRecord {
        kind: kind.to_string(),
        id: "r".to_string(),
        parent: parent.map(str::to_string),
        seq,
        ts,
        disposition: busbar_contract::records::PlaneDisposition::Active,
        body: b"b".to_vec(),
    }
}

/// A u64 a signed column cannot hold is refused before any write, on the trait methods (an error naming the method and field) and on the op slots (`OpRefused::Refused`, nothing recorded).
#[test]
fn a_value_a_signed_column_cannot_hold_is_refused_before_any_write() {
    let over = i64::MAX as u64 + 1;
    let s = MemoryStore::new();

    for (rec, field) in [
        (plane("task_event", Some("p"), over, 0), "seq"),
        (plane("task_event", Some("p"), 1, over), "ts"),
    ] {
        let e = s.append_plane_record(rec.view()).expect_err("append");
        assert!(
            e.0.starts_with(&format!("append_plane_record: {field} {over} exceeds")),
            "{}",
            e.0
        );
        assert!(matches!(
            s.v3_append_plane_record_op(op(1), rec.view()),
            Err(OpRefused::Refused(_))
        ));
    }
    for (rec, field) in [
        (plane("task", None, over, 0), "seq"),
        (plane("task", None, 0, over), "ts"),
    ] {
        let e = s.upsert_plane_record(rec.view()).expect_err("upsert");
        assert!(
            e.0.starts_with(&format!("upsert_plane_record: {field} {over} exceeds")),
            "{}",
            e.0
        );
    }
    assert!(s.get_plane_record("task", "r").unwrap().is_none());
    assert!(s
        .list_plane_records("task_event", &PlaneSelector::All)
        .unwrap()
        .is_empty());

    s.v3_append_plane_record_op(op(1), plane("task_event", Some("p"), 1, 1).view())
        .expect("op id reusable");

    let e = s.add_metering(&metering(over)).expect_err("metering");
    assert!(
        e.0.starts_with(&format!("add_metering: priced_from_ms {over} exceeds")),
        "{}",
        e.0
    );
    assert!(matches!(
        s.v3_add_metering_batch(op(2), &[metering(1), metering(over)]),
        Err(OpRefused::Refused(_))
    ));
    assert!(
        s.list_metering(86_400).unwrap().is_empty(),
        "a refused batch applied part of itself"
    );
    s.v3_add_metering_batch(op(2), &[metering(1)])
        .expect("op id reusable");

    assert!(s
        .redeem_plane_token("ask", "unsigned-expiry", over, 0)
        .unwrap());
    assert!(!s
        .redeem_plane_token("ask", "unsigned-expiry", over, over)
        .unwrap());
    assert!(s
        .redeem_plane_token("ask", "unsigned-now", u64::MAX, over)
        .unwrap());
    assert!(!s
        .redeem_plane_token("ask", "unsigned-now", u64::MAX, u64::MAX)
        .unwrap());
}

/// The host's explicit usage purge removes only windows strictly before its cutoff; the all-time ledger, cutoff equality, future windows and billing history survive.
#[test]
fn usage_purge_counts_removed_windows_and_preserves_all_time_and_cutoff() {
    use busbar_contract::abi::sdk::store::{Op, Step};
    let s = MemoryStore::new();
    let ledger = UsageLedger {
        requests: 7,
        ..UsageLedger::default()
    };
    for (bucket, window) in [
        ("a", 0),
        ("a", 10),
        ("b", 19),
        ("a", 20),
        ("b", 21),
        ("a", u64::MAX),
    ] {
        s.put_usage(bucket, window, &ledger).unwrap();
    }
    let purge = |before| match busbar_contract::abi::sdk::store::StoreSlots::purge_usage_before(
        &s,
        &mut Op::detached(),
        before,
    ) {
        Step::Ready(result) => result.expect("memory implements the usage-purge slot"),
        Step::Pending { .. } => panic!("memory purge must complete inline"),
    };
    assert_eq!(purge(20), 2);
    assert_eq!(s.get_usage("a", 10).unwrap().requests, 0);
    assert_eq!(s.get_usage("b", 19).unwrap().requests, 0);
    for (bucket, window) in [("a", 0), ("a", 20), ("b", 21), ("a", u64::MAX)] {
        assert_eq!(s.get_usage(bucket, window).unwrap().requests, 7);
    }
    assert_eq!(purge(20), 0, "repeating a purge is idempotent");
    assert_eq!(purge(0), 0, "zero cutoff removes nothing");
    assert_eq!(purge(u64::MAX), 2);
    assert_eq!(s.get_usage("a", 0).unwrap().requests, 7);
    assert_eq!(s.get_usage("a", u64::MAX).unwrap().requests, 7);
}

/// Billing history belongs to the operator: unrelated writes must not silently erase old rows.
#[test]
fn billing_history_survives_repeated_writes_past_the_usage_retention_horizon() {
    let s = MemoryStore::new();
    let n = 1_800_000_000;
    s.pin_clock(n);
    let old = n - 40 * 86_400;
    let delta = MeteringDelta {
        scale: 6,
        key_id: "old-bill".into(),
        bucket: old,
        model: "m".into(),
        provider: "p".into(),
        tokens_input: 11,
        tokens_output: 0,
        tokens_cache_read: 0,
        tokens_cache_write: 0,
        requests: 1,
        billable_requests: 1,
        key_group_at_use: "at-use".into(),
        pricing_version: "original".into(),
        priced_from_ms: 42,
        usage_units: Default::default(),
    };
    s.add_metering(&delta).unwrap();
    let original = s.list_metering(old).unwrap();
    let fresh = MeteringDelta {
        scale: 6,
        key_id: "new-bill".into(),
        bucket: n,
        ..delta
    };
    for _ in 0..(2 * SWEEP_INTERVAL) {
        s.add_metering(&fresh).unwrap();
    }
    assert_eq!(
        s.list_metering(old).unwrap(),
        original,
        "old billing rows are never aged out"
    );
    assert_eq!(original.len(), 1);
}

#[path = "conditional_records_tests.rs"]
pub(crate) mod conditional_records_tests;

#[test]
fn racing_usage_batches_with_one_op_id_apply_once() {
    let store = std::sync::Arc::new(MemoryStore::new());
    let start = std::sync::Arc::new(std::sync::Barrier::new(3));
    std::thread::scope(|scope| {
        let mut writers = Vec::new();
        for _ in 0..2 {
            let store = store.clone();
            let start = start.clone();
            writers.push(scope.spawn(move || {
                start.wait();
                store.v3_add_usage_batch(op(901), &[("raced", 0, delta(7, 3))])
            }));
        }
        start.wait();
        for writer in writers {
            writer.join().unwrap().unwrap();
        }
    });
    assert_eq!(store.get_usage("raced", 0).unwrap().requests, 7);
}

#[test]
fn poisoned_usage_and_dedupe_locks_recover_for_the_next_operation() {
    let store = observed();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        observe_batch(
            |_| panic!("fixture poisons held guards"),
            || {
                store
                    .v3_add_usage_batch(op(902), &[("recovered", 0, delta(0, 0))])
                    .unwrap();
            },
        );
    }));
    assert!(panic.is_err());
    store
        .v3_add_usage_batch(op(903), &[("recovered", 0, delta(2, 3))])
        .unwrap();
    assert_eq!(store.get_usage("recovered", 0).unwrap().requests, 2);
    store
        .v3_add_usage_batch(op(903), &[("recovered", 0, delta(2, 3))])
        .unwrap();
    assert_eq!(store.get_usage("recovered", 0).unwrap().requests, 2);
}

#[test]
fn metering_preserves_each_groups_and_versions_counts_for_single_and_batch_writes() {
    use busbar_contract::abi::sdk::store::{Op, Step};
    use std::collections::BTreeMap;

    for batched in [false, true] {
        let store = MemoryStore::new();
        let mut first = metering(42);
        first.key_group_at_use = "group-a".into();
        first.pricing_version = "version-a".into();
        first.requests = 2;
        first.usage_units.insert("audio".into(), 3);
        let mut regrouped = first.clone();
        regrouped.key_group_at_use = "group-b".into();
        regrouped.requests = 3;
        regrouped.usage_units.insert("audio".into(), 5);
        let mut versioned = first.clone();
        versioned.pricing_version = "version-b".into();
        versioned.requests = 5;
        versioned.usage_units.insert("audio".into(), 7);
        let mut untracked = first.clone();
        untracked.key_group_at_use.clear();
        untracked.pricing_version.clear();
        untracked.requests = 7;
        untracked.usage_units.insert("audio".into(), 11);
        let cells = [first.clone(), regrouped, versioned, untracked, first];
        if batched {
            for _ in 0..2 {
                match busbar_contract::abi::sdk::store::StoreSlots::add_metering_batch(
                    &store,
                    &mut Op::detached(),
                    op(904),
                    &cells,
                ) {
                    Step::Ready(result) => result.unwrap(),
                    Step::Pending { .. } => panic!("memory completes inline"),
                }
            }
        } else {
            for cell in &cells {
                store.add_metering(cell).unwrap();
            }
        }
        let rows = store.list_metering(86_400).unwrap();
        assert_eq!(
            rows.len(),
            4,
            "group/version attribution must not be collapsed"
        );
        let counts: BTreeMap<_, _> = rows
            .into_iter()
            .map(|row| {
                assert_eq!(
                    (
                        row.key_id.as_str(),
                        row.model.as_str(),
                        row.provider.as_str()
                    ),
                    ("k", "m", "p")
                );
                assert_eq!(row.priced_from_ms, 42);
                (
                    (row.key_group_at_use, row.pricing_version),
                    (row.requests, row.tokens_input, row.usage_units["audio"]),
                )
            })
            .collect();
        assert_eq!(
            counts,
            [
                (("group-a".into(), "version-a".into()), (4, 2, 6)),
                (("group-b".into(), "version-a".into()), (3, 1, 5)),
                (("group-a".into(), "version-b".into()), (5, 1, 7)),
                ((String::new(), String::new()), (7, 1, 11)),
            ]
            .into(),
            "each row keeps the attribution of its own counts"
        );
    }
}

#[test]
fn receipt_expiry_stays_ordered_after_a_backward_clock_step() {
    let s = MemoryStore::new();
    s.pin_clock(2_000);
    s.v3_add_usage_batch(op(901), &[("newer", 60, delta(1, 0))])
        .unwrap();
    s.pin_clock(1_000);
    s.v3_add_usage_batch(op(902), &[("older", 60, delta(1, 0))])
        .unwrap();
    s.pin_clock(1_000 + OP_ID_RETENTION_SECS);
    s.v3_add_usage_batch(op(903), &[("sweep", 60, delta(1, 0))])
        .unwrap();
    s.v3_add_usage_batch(op(902), &[("older", 60, delta(1, 0))])
        .unwrap();
    s.v3_add_usage_batch(op(901), &[("newer", 60, delta(1, 0))])
        .unwrap();
    assert_eq!(s.get_usage("older", 60).unwrap().requests, 2);
    assert_eq!(s.get_usage("newer", 60).unwrap().requests, 1);
}

#[test]
fn linked_store_advertises_and_honors_usage_batch_dedupe() {
    let s = MemoryStore::new();
    let store: &dyn RecordStore = &s;
    assert!(store.supports_usage_batch_deduped());
    for _ in 0..2 {
        store
            .add_usage_batch_deduped(op(904), &[("k", 60, delta(1, 0))])
            .unwrap();
    }
    assert_eq!(store.get_usage("k", 60).unwrap().requests, 1);
}
