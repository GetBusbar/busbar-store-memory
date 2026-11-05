// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! Real memory correction controls. These need the correction carrier; a missing API is not RED.

use super::{
    correction, write, Body, Dedupe, MemoryStore, MeteringDelta, MeteringRow, Op, OpId, OpRefused,
    OpResult, RecordStore, Stage, State, Step, UsageDelta, UsageLedger, VirtualKey,
    OP_ID_RETENTION_SECS,
};
use crate::MAX_RETENTION_SECS;
use busbar_contract::abi::sdk::store::correction::{Decision, Request, UsageAddress, UsageChange};
use busbar_contract::records::correction::{
    canonical_metering, ADJUST_DOWN_BUCKET, ADJUST_UP_BUCKET, VERSION,
};
use busbar_contract::records::{ModelTokens, ModelTokensDelta, UNIT_INPUT};
use std::sync::{mpsc, Arc, Barrier};
use std::time::Duration;

const OWNER: &str = "correction-owner";
const DAY_START: u64 = 86_400;
const WAIT: Duration = Duration::from_secs(5);

impl State {
    /// The bytes the dedupe log keeps per remembered op, summed (shared by every v3 test module).
    pub(crate) fn op_bytes(&self) -> usize {
        self.lock().ops.values().map(|body| body.len()).sum()
    }
}

impl MemoryStore {
    /// Report each [`Stage`] of this store's atomic ops to `probe` (shared by every v3 test module).
    pub(crate) fn observe_stages(&mut self, probe: fn(&MemoryStore, Stage)) {
        self.v3.probe = probe;
    }
}

type Observer = Box<dyn FnMut(Stage)>;
thread_local! {
    static OBSERVER: std::cell::RefCell<Option<Observer>> = const { std::cell::RefCell::new(None) };
}
fn at_stage(_: &MemoryStore, stage: Stage) {
    OBSERVER.with_borrow_mut(|observer| {
        if let Some(observer) = observer {
            observer(stage);
        }
    });
}
fn observe<T>(observer: impl FnMut(Stage) + 'static, run: impl FnOnce() -> T) -> T {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            OBSERVER.with_borrow_mut(|observer| {
                observer.take();
            });
        }
    }
    OBSERVER.with_borrow_mut(|slot| assert!(slot.replace(Box::new(observer)).is_none()));
    let _reset = Reset;
    run()
}
fn op(sequence: u64) -> OpId {
    OpId::from_parts(11, sequence)
}
fn store() -> MemoryStore {
    let mut store = MemoryStore::new();
    store.observe_stages(at_stage);
    store.pin_clock(1);
    store
        .put_key(&VirtualKey {
            id: OWNER.into(),
            enabled: true,
            ..Default::default()
        })
        .unwrap();
    store
}
fn ledger(count: u64) -> UsageLedger {
    ledger_parts(count, count)
}

/// Request width and quantity width are independent: these fixtures state both explicitly.
fn ledger_parts(requests: u64, quantity: u64) -> UsageLedger {
    UsageLedger {
        requests,
        billable_requests: requests,
        models: vec![ModelTokens {
            scale: 6,
            model: "model".into(),
            usage_units: [
                (UNIT_INPUT.into(), quantity),
                ("open-class".into(), quantity),
            ]
            .into(),
        }],
    }
}
fn change(bucket: &str, expected: UsageLedger, replacement: UsageLedger) -> UsageChange {
    UsageChange {
        address: UsageAddress {
            bucket_id: bucket.into(),
            window_start: 0,
        },
        expected,
        replacement,
    }
}
fn request(sequence: u64, usage: Vec<UsageChange>) -> Request {
    Request {
        version: VERSION,
        op_id: op(sequence),
        key_id: OWNER.into(),
        day_start: DAY_START,
        metering: vec![],
        usage,
    }
}
fn correct(store: &MemoryStore, request: &Request) -> OpResult<Decision> {
    match busbar_contract::abi::sdk::store::StoreSlots::apply_usage_correction(
        store,
        &mut Op::detached(),
        request,
    ) {
        Step::Ready(result) => result,
        Step::Pending { .. } => panic!("memory correction must answer inline"),
    }
}
fn batch(store: &MemoryStore, id: OpId, cells: &[(&str, u64, UsageDelta)]) -> OpResult<()> {
    match busbar_contract::abi::sdk::store::StoreSlots::add_usage_batch(
        store,
        &mut Op::detached(),
        id,
        cells,
    ) {
        Step::Ready(result) => result,
        Step::Pending { .. } => panic!("memory usage batch must answer inline"),
    }
}
fn delta(count: i64) -> UsageDelta {
    UsageDelta {
        requests: count,
        billable_requests: count,
        models: vec![ModelTokensDelta {
            scale: 6,
            model: "model".into(),
            usage_units: [(UNIT_INPUT.into(), count), ("open-class".into(), count)].into(),
        }],
    }
}
fn metering() -> MeteringDelta {
    MeteringDelta {
        scale: 6,
        key_id: OWNER.into(),
        bucket: DAY_START,
        model: "model".into(),
        provider: "provider".into(),
        tokens_input: 7,
        tokens_output: 5,
        tokens_cache_read: 3,
        tokens_cache_write: 2,
        requests: 4,
        billable_requests: 3,
        key_group_at_use: "original-group".into(),
        pricing_version: "original-version".into(),
        priced_from_ms: DAY_START * 1_000,
        usage_units: [("open-class".into(), 13)].into(),
    }
}
fn snapshot_metering(store: &MemoryStore) -> Vec<MeteringRow> {
    let mut rows = store.list_metering(DAY_START).unwrap();
    rows.retain(|row| row.key_id == OWNER);
    canonical_metering(&mut rows);
    rows
}
fn receipts(store: &MemoryStore) -> usize {
    store.v3.op_bytes()
}

#[test]
fn direct_provided_id_batch_replays_a_lost_answer_and_preserves_empty_model_identity() {
    let store = store();
    let mut accrual = delta(7);
    accrual.billable_requests = 3;
    accrual.models[0].model.clear();
    let cells = [("plain", 0, accrual)];
    RecordStore::add_usage_batch_deduped(&store, op(1), &cells).unwrap();
    let mut expected = ledger(7);
    expected.billable_requests = 3;
    expected.models[0].model.clear();
    assert_eq!(store.get_usage("plain", 0).unwrap(), expected);
    RecordStore::add_usage_batch_deduped(&store, op(1), &cells).unwrap();
    assert_eq!(store.get_usage("plain", 0).unwrap(), expected);
    let changed = [("plain", 0, delta(9))];
    assert_eq!(
        RecordStore::add_usage_batch_deduped(&store, op(1), &changed)
            .unwrap_err()
            .0,
        busbar_contract::abi::store::DIAG_OPID_CONFLICT
    );
    assert_eq!(store.get_usage("plain", 0).unwrap(), expected);
    assert_eq!(receipts(&store), std::mem::size_of::<Body>());
    let mut replacement = expected.clone();
    replacement.models[0]
        .usage_units
        .insert(UNIT_INPUT.into(), 11);
    let candidate = request(2, vec![change("plain", expected, replacement.clone())]);
    assert_eq!(correct(&store, &candidate), Ok(Decision::Applied));
    assert_eq!(store.get_usage("plain", 0).unwrap(), replacement);
    RecordStore::add_usage_batch_deduped(&store, op(1), &cells).unwrap();
    assert_eq!(store.get_usage("plain", 0).unwrap(), replacement);
    store.put_usage("copied", 0, &replacement).unwrap();
    assert_eq!(store.get_usage("copied", 0).unwrap(), replacement);
}

#[test]
fn public_slot_opts_in_and_two_sixty_refunds_from_one_hundred_accept_only_one() {
    const { assert!(<MemoryStore as busbar_contract::abi::sdk::store::StoreSlots>::USAGE_CORRECTION) };
    assert_eq!(
        correction::capability::<MemoryStore>(),
        Some((correction::CAPABILITY, correction::CAPABILITY_VALUE))
    );
    let store = Arc::new(store());
    assert!(RecordStore::supports_usage_correction(&*store));
    store.put_usage("plain", 0, &ledger(100)).unwrap();
    let gate = Arc::new(Barrier::new(3));
    let workers: Vec<_> = [1, 2]
        .into_iter()
        .map(|id| {
            let store = Arc::clone(&store);
            let gate = Arc::clone(&gate);
            std::thread::spawn(move || {
                let request = request(
                    id,
                    vec![
                        change(ADJUST_DOWN_BUCKET, UsageLedger::default(), ledger(60)),
                        change("plain", ledger(100), ledger(40)),
                    ],
                );
                gate.wait();
                correct(&store, &request).unwrap()
            })
        })
        .collect();
    gate.wait();
    let results: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(
        results.iter().filter(|d| **d == Decision::Applied).count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|d| **d == Decision::ConditionConflict)
            .count(),
        1
    );
    assert_eq!(store.get_usage("plain", 0).unwrap(), ledger(40));
    assert_eq!(store.get_usage(ADJUST_DOWN_BUCKET, 0).unwrap(), ledger(60));
    assert_eq!(receipts(&store), std::mem::size_of::<Body>());
}

#[test]
fn lost_answer_replays_before_key_metering_and_rows_and_any_body_change_conflicts() {
    let store = store();
    let candidate = request(1, vec![change("plain", UsageLedger::default(), ledger(40))]);
    let _lost_answer = correct(&store, &candidate).unwrap();
    store.put_usage("plain", 0, &ledger(45)).unwrap();
    store.add_metering(&metering()).unwrap();
    store.delete_key(OWNER).unwrap();
    assert_eq!(correct(&store, &candidate), Ok(Decision::Applied));
    assert_eq!(
        RecordStore::apply_usage_correction(&store, &candidate).unwrap(),
        Decision::Applied
    );
    let mut bodies = vec![];
    let mut changed = candidate.clone();
    changed.key_id = "another-key".into();
    bodies.push(changed);
    let mut changed = candidate.clone();
    changed.day_start += DAY_START;
    bodies.push(changed);
    let mut changed = candidate.clone();
    changed.metering = snapshot_metering(&store);
    bodies.push(changed);
    let mut changed = candidate.clone();
    changed.usage[0].expected = ledger(1);
    bodies.push(changed);
    let mut changed = candidate.clone();
    changed.usage[0].replacement = ledger(41);
    bodies.push(changed);
    let mut changed = candidate.clone();
    changed.usage[0].address.bucket_id = "other-row".into();
    bodies.push(changed);
    for changed in bodies {
        assert_eq!(correct(&store, &changed), Err(OpRefused::Conflict));
    }
    assert_eq!(store.get_usage("plain", 0).unwrap(), ledger(45));
    assert_eq!(receipts(&store), std::mem::size_of::<Body>());
}

#[test]
fn late_conflict_and_late_invalid_row_have_no_effect_or_receipt_and_allow_same_id_retry() {
    let store = store();
    store.put_usage("a", 0, &ledger(100)).unwrap();
    store.put_usage("z", 0, &ledger(2)).unwrap();
    let mut candidate = request(
        1,
        vec![
            change("a", ledger(100), ledger(40)),
            change("z", ledger(1), ledger(3)),
        ],
    );
    let revision = store.revision.load(std::sync::atomic::Ordering::Relaxed);
    store.pin_clock(OP_ID_RETENTION_SECS + 100);
    assert_eq!(correct(&store, &candidate), Ok(Decision::ConditionConflict));
    assert_eq!(store.get_usage("a", 0).unwrap(), ledger(100));
    assert_eq!(store.get_usage("z", 0).unwrap(), ledger(2));
    assert_eq!(receipts(&store), 0);
    assert_eq!(
        store.revision.load(std::sync::atomic::Ordering::Relaxed),
        revision
    );
    candidate.usage[1].expected = ledger(2);
    let duplicate = candidate.usage[1].replacement.models[0].clone();
    candidate.usage[1].replacement.models.push(duplicate);
    assert!(matches!(
        correct(&store, &candidate),
        Err(OpRefused::Refused(_))
    ));
    assert_eq!(store.get_usage("a", 0).unwrap(), ledger(100));
    assert_eq!(receipts(&store), 0);
    candidate.usage[1].replacement.models.pop();
    assert_eq!(correct(&store, &candidate), Ok(Decision::Applied));
    assert_eq!(store.get_usage("a", 0).unwrap(), ledger(40));
    assert_eq!(store.get_usage("z", 0).unwrap(), ledger(3));
}

#[test]
fn exact_unsigned_replacement_and_canonical_models_do_not_narrow_or_saturate() {
    let store = store();
    let high = u64::MAX - 1;
    let seed = request(
        1,
        vec![change(
            "plain",
            UsageLedger::default(),
            ledger_parts(high, i64::MAX as u64 - 1),
        )],
    );
    assert_eq!(correct(&store, &seed), Ok(Decision::Applied));
    assert_eq!(
        store.get_usage("plain", 0).unwrap(),
        ledger_parts(high, i64::MAX as u64 - 1)
    );
    let mut stale = request(
        2,
        vec![change(
            "plain",
            ledger(i64::MAX as u64),
            ledger_parts(high - 1, i64::MAX as u64 - 2),
        )],
    );
    assert_eq!(correct(&store, &stale), Ok(Decision::ConditionConflict));
    stale.usage[0].expected = ledger_parts(high, i64::MAX as u64 - 1);
    assert_eq!(correct(&store, &stale), Ok(Decision::Applied));
    assert_eq!(
        store.get_usage("plain", 0).unwrap(),
        ledger_parts(high - 1, i64::MAX as u64 - 2)
    );
    let mut unordered = ledger(1);
    let mut second = unordered.models[0].clone();
    second.model = "a-model".into();
    unordered.models.push(second);
    store.put_usage("unordered", 0, &unordered).unwrap();
    unordered.models.reverse();
    let canonical = request(3, vec![change("unordered", unordered, ledger(2))]);
    assert_eq!(correct(&store, &canonical), Ok(Decision::Applied));
    store.add_usage("zeroed", 0, &delta(1)).unwrap();
    store.add_usage("zeroed", 0, &delta(-1)).unwrap();
    let empty = request(4, vec![change("zeroed", UsageLedger::default(), ledger(2))]);
    assert_eq!(correct(&store, &empty), Ok(Decision::Applied));
}

#[test]
fn correction_high_unsigned_counts_survive_ordinary_accrual_refund_batch_and_absolute_write() {
    let store = store();
    let high = (i64::MAX as u64) + 100;
    let candidate = request(
        1,
        vec![change(
            "plain",
            UsageLedger::default(),
            ledger_parts(high, 100),
        )],
    );
    assert_eq!(correct(&store, &candidate), Ok(Decision::Applied));
    store.add_usage("plain", 0, &delta(5)).unwrap();
    assert_eq!(
        store.get_usage("plain", 0).unwrap(),
        ledger_parts(high + 5, 100 + 5)
    );
    store.add_usage("plain", 0, &delta(-3)).unwrap();
    assert_eq!(
        store.get_usage("plain", 0).unwrap(),
        ledger_parts(high + 2, 100 + 2)
    );
    batch(
        &store,
        op(2),
        &[("plain", 0, delta(7)), ("plain", 0, delta(-4))],
    )
    .unwrap();
    assert_eq!(
        store.get_usage("plain", 0).unwrap(),
        ledger_parts(high + 5, 100 + 5)
    );
    store
        .put_usage("plain", 0, &ledger_parts(u64::MAX - 1, i64::MAX as u64 - 1))
        .unwrap();
    assert!(store.add_usage("plain", 0, &delta(5)).is_err());
    store.add_usage("plain", 0, &delta(1)).unwrap();
    assert_eq!(
        store.get_usage("plain", 0).unwrap(),
        ledger_parts(u64::MAX, i64::MAX as u64)
    );
    store.add_usage("plain", 0, &delta(-5)).unwrap();
    assert_eq!(
        store.get_usage("plain", 0).unwrap(),
        ledger_parts(u64::MAX - 5, i64::MAX as u64 - 5)
    );
    store.add_usage("empty", 0, &delta(-5)).unwrap();
    assert_eq!(store.get_usage("empty", 0).unwrap().requests, 0);
    assert_eq!(store.get_usage("empty", 0).unwrap().billable_requests, 0);
    assert!(store
        .get_usage("empty", 0)
        .unwrap()
        .models
        .iter()
        .all(|model| model.usage_units.values().all(|count| *count == 0)));
    assert_eq!(correct(&store, &candidate), Ok(Decision::Applied));
    assert_eq!(
        store.get_usage("plain", 0).unwrap(),
        ledger_parts(u64::MAX - 5, i64::MAX as u64 - 5)
    );
}

#[test]
fn concurrent_same_id_replay_has_one_receipt_and_no_second_replacement() {
    let store = Arc::new(store());
    let gate = Arc::new(Barrier::new(3));
    let candidate = request(1, vec![change("plain", UsageLedger::default(), ledger(40))]);
    let workers: Vec<_> = (0..2)
        .map(|_| {
            let store = Arc::clone(&store);
            let gate = Arc::clone(&gate);
            let candidate = candidate.clone();
            std::thread::spawn(move || {
                gate.wait();
                correct(&store, &candidate)
            })
        })
        .collect();
    gate.wait();
    for worker in workers {
        assert_eq!(worker.join().unwrap(), Ok(Decision::Applied));
    }
    assert_eq!(store.get_usage("plain", 0).unwrap(), ledger(40));
    assert_eq!(receipts(&store), std::mem::size_of::<Body>());
}

#[test]
fn condition_conflict_does_not_sweep_an_aged_receipt_or_advance_usage_retention() {
    let store = store();
    let applied = request(1, vec![change("plain", UsageLedger::default(), ledger(40))]);
    assert_eq!(correct(&store, &applied), Ok(Decision::Applied));
    let receipt_bytes = receipts(&store);
    let ticker = store
        .usage_sweep_ticker
        .load(std::sync::atomic::Ordering::Relaxed);
    store.pin_clock(OP_ID_RETENTION_SECS + MAX_RETENTION_SECS);
    let conflict = request(2, vec![change("plain", ledger(41), ledger(42))]);
    assert_eq!(correct(&store, &conflict), Ok(Decision::ConditionConflict));
    assert_eq!(receipts(&store), receipt_bytes);
    assert_eq!(
        store
            .usage_sweep_ticker
            .load(std::sync::atomic::Ordering::Relaxed),
        ticker
    );
    assert_eq!(correct(&store, &applied), Ok(Decision::Applied));
    assert_eq!(store.get_usage("plain", 0).unwrap(), ledger(40));
}

#[test]
fn every_metering_field_and_the_complete_key_day_selection_are_predicates() {
    let store = store();
    store.add_metering(&metering()).unwrap();
    let mut candidate = request(1, vec![change("plain", UsageLedger::default(), ledger(1))]);
    candidate.metering = snapshot_metering(&store);
    let mut mismatches = vec![];
    macro_rules! changed {
        ($field:ident, $value:expr) => {{
            let mut changed = candidate.clone();
            changed.metering[0].$field = $value;
            mismatches.push(changed);
        }};
    }
    changed!(model, "different-model".into());
    changed!(provider, "different-provider".into());
    changed!(key_group_at_use, "different-group".into());
    changed!(pricing_version, "different-version".into());
    changed!(priced_from_ms, 1);
    changed!(tokens_input, 1);
    changed!(tokens_output, 1);
    changed!(tokens_cache_read, 1);
    changed!(tokens_cache_write, 1);
    changed!(requests, 1);
    changed!(billable_requests, 1);
    changed!(usage_units, [("open-class".into(), 12)].into());
    let mut missing = candidate.clone();
    missing.metering.clear();
    mismatches.push(missing);
    for mismatch in mismatches {
        assert_eq!(correct(&store, &mismatch), Ok(Decision::ConditionConflict));
        assert_eq!(store.get_usage("plain", 0).unwrap(), UsageLedger::default());
        assert_eq!(receipts(&store), 0);
    }
    let mut other = metering();
    other.key_id = "other-key".into();
    store.add_metering(&other).unwrap();
    other = metering();
    other.bucket += DAY_START;
    store.add_metering(&other).unwrap();
    assert_eq!(correct(&store, &candidate), Ok(Decision::Applied));
    let mut extra = metering();
    extra.pricing_version = "second-era".into();
    store.add_metering(&extra).unwrap();
    let mut next = request(2, vec![change("plain", ledger(1), ledger(2))]);
    next.metering = candidate.metering;
    assert_eq!(correct(&store, &next), Ok(Decision::ConditionConflict));
}

#[test]
fn absent_insert_metering_and_key_deletion_before_acceptance_each_invalidate_the_snapshot() {
    enum Writer {
        AbsentInsert,
        Metering,
        DeleteKey,
    }
    for writer in [Writer::AbsentInsert, Writer::Metering, Writer::DeleteKey] {
        let store = store();
        let candidate = request(1, vec![change("absent", UsageLedger::default(), ledger(1))]);
        match writer {
            Writer::AbsentInsert => store.put_usage("absent", 0, &ledger(2)).unwrap(),
            Writer::Metering => store.add_metering(&metering()).unwrap(),
            Writer::DeleteKey => store.delete_key(OWNER).unwrap(),
        }
        assert_eq!(correct(&store, &candidate), Ok(Decision::ConditionConflict));
        assert_eq!(receipts(&store), 0);
    }
    let store = MemoryStore::new();
    assert_eq!(
        correct(
            &store,
            &request(1, vec![change("absent", UsageLedger::default(), ledger(1))])
        ),
        Ok(Decision::ConditionConflict)
    );
}

#[test]
fn ordinary_writers_and_readers_wait_for_all_replacements_and_the_receipt() {
    use std::{cell::RefCell, rc::Rc};
    #[derive(Clone, Copy)]
    enum Writer {
        Accrual,
        AbsentInsert,
        Metering,
        DeleteKey,
        Reader,
    }
    for writer in [
        Writer::Accrual,
        Writer::AbsentInsert,
        Writer::Metering,
        Writer::DeleteKey,
        Writer::Reader,
    ] {
        let store = Arc::new(store());
        store.put_usage("a", 0, &ledger(100)).unwrap();
        let candidate = request(
            1,
            vec![
                change("a", ledger(100), ledger(40)),
                change("z", UsageLedger::default(), ledger(60)),
            ],
        );
        let (start_tx, start_rx) = mpsc::channel();
        let (attempt_tx, attempt_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let worker_store = Arc::clone(&store);
        let worker = std::thread::spawn(move || {
            if start_rx.recv().is_err() {
                return;
            }
            let unavailable = match writer {
                Writer::Accrual | Writer::AbsentInsert => worker_store.usage.try_write().is_err(),
                Writer::Metering => worker_store.metering.try_write().is_err(),
                Writer::DeleteKey => worker_store.keys.try_write().is_err(),
                Writer::Reader => worker_store.usage.try_read().is_err(),
            };
            attempt_tx.send(unavailable).unwrap();
            match writer {
                Writer::Accrual => worker_store.add_usage("a", 0, &delta(5)).unwrap(),
                Writer::AbsentInsert => worker_store.put_usage("z", 0, &ledger(9)).unwrap(),
                Writer::Metering => worker_store.add_metering(&metering()).unwrap(),
                Writer::DeleteKey => worker_store.delete_key(OWNER).unwrap(),
                Writer::Reader => {
                    assert_eq!(worker_store.get_usage("a", 0).unwrap(), ledger(40));
                    assert_eq!(worker_store.get_usage("z", 0).unwrap(), ledger(60));
                }
            }
            done_tx.send(()).unwrap();
        });
        let done = Rc::new(RefCell::new(done_rx));
        let observing = Rc::clone(&done);
        assert_eq!(
            observe(
                move |stage| {
                    if stage == Stage::Mutation(0) {
                        start_tx.send(()).unwrap();
                        assert!(
                            attempt_rx.recv_timeout(WAIT).unwrap(),
                            "ordinary path must share correction's guard"
                        );
                    }
                    if stage == Stage::BeforeReceipt {
                        assert!(matches!(
                            observing.borrow_mut().try_recv(),
                            Err(mpsc::TryRecvError::Empty)
                        ));
                    }
                },
                || correct(&store, &candidate)
            ),
            Ok(Decision::Applied)
        );
        done.borrow_mut().recv_timeout(WAIT).unwrap();
        worker.join().unwrap();
        let snapshot = (
            store.get_usage("a", 0).unwrap(),
            store.get_usage("z", 0).unwrap(),
        );
        assert_eq!(
            snapshot.0,
            ledger(if matches!(writer, Writer::Accrual) {
                45
            } else {
                40
            })
        );
        assert_eq!(
            snapshot.1,
            ledger(if matches!(writer, Writer::AbsentInsert) {
                9
            } else {
                60
            })
        );
        assert_eq!(correct(&store, &candidate), Ok(Decision::Applied));
        assert_eq!(
            (
                store.get_usage("a", 0).unwrap(),
                store.get_usage("z", 0).unwrap()
            ),
            snapshot
        );
        assert_eq!(receipts(&store), std::mem::size_of::<Body>());
    }
}

#[test]
fn reserved_legacy_writes_refuse_whole_batches_but_an_applied_legacy_receipt_replays() {
    let store = store();
    for bucket in [ADJUST_UP_BUCKET, ADJUST_DOWN_BUCKET] {
        assert!(store.put_usage(bucket, 0, &ledger(1)).is_err());
        assert!(store.add_usage(bucket, 0, &delta(1)).is_err());
        let cells = [("plain", 0, delta(5)), (bucket, 0, delta(1))];
        assert!(matches!(
            batch(&store, op(1), &cells),
            Err(OpRefused::Refused(_))
        ));
        assert_eq!(store.get_usage("plain", 0).unwrap(), UsageLedger::default());
        assert_eq!(store.get_usage(bucket, 0).unwrap(), UsageLedger::default());
        assert_eq!(receipts(&store), 0);
    }
    let cells = [(ADJUST_DOWN_BUCKET, 0, delta(60))];

    store
        .deduped(op(2), Dedupe::usage(&cells), |s| {
            MemoryStore::apply_usage(&mut write(&s.usage), cells[0].0, cells[0].1, &cells[0].2)?;
            Ok(())
        })
        .unwrap();
    assert_eq!(batch(&store, op(2), &cells), Ok(()));
    assert_eq!(store.get_usage(ADJUST_DOWN_BUCKET, 0).unwrap(), ledger(60));
    let changed = [(ADJUST_DOWN_BUCKET, 0, delta(59))];
    assert_eq!(batch(&store, op(2), &changed), Err(OpRefused::Conflict));
    assert_eq!(receipts(&store), std::mem::size_of::<Body>());
    let candidate = request(2, vec![change("plain", UsageLedger::default(), ledger(1))]);
    assert_eq!(correct(&store, &candidate), Err(OpRefused::Conflict));
}

#[test]
fn an_invalid_stored_ledger_is_a_refusal_without_effects() {
    let store = store();
    let mut invalid = ledger(2);
    invalid.models.push(invalid.models[0].clone());
    store.put_usage("bad", 0, &invalid).unwrap();
    let candidate = request(900, vec![change("bad", ledger(2), ledger(1))]);
    assert!(
        matches!(correct(&store, &candidate), Err(OpRefused::Refused(reason)) if reason.contains("cannot be compared"))
    );
    assert_eq!(store.get_usage("bad", 0).unwrap(), invalid);
    assert_eq!(receipts(&store), 0);
}

#[test]
fn receipt_clock_uses_elapsed_process_time() {
    let store = MemoryStore::new();
    let before = store.v3.started.elapsed().as_secs();
    let receipt_time = store.receipt_now();
    let after = store.v3.started.elapsed().as_secs();
    assert!((before..=after).contains(&receipt_time));
}
