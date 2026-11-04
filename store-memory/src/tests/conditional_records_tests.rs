// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! New-API behavioral controls for the generic conditional record transaction.

use super::*;
use crate::v3::Stage;
use busbar_contract::abi::sdk::store::conditional::{
    self, Decision, Expected, Mutation, Predicate, Request,
};
use busbar_contract::abi::sdk::store::{Op, OpResult, Step, StoreSlots};
use busbar_contract::bounded::MAX_RECORD_BYTES;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc, Barrier};
use std::time::Duration;

type Observer = Box<dyn FnMut(Stage)>;
thread_local! { static OBSERVER: std::cell::RefCell<Option<Observer>> = const { std::cell::RefCell::new(None) }; }
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
            OBSERVER.with_borrow_mut(|o| {
                o.take();
            });
        }
    }
    OBSERVER.with_borrow_mut(|o| {
        assert!(o.replace(Box::new(observer)).is_none());
    });
    let _reset = Reset;
    run()
}
fn store() -> MemoryStore {
    let mut store = MemoryStore::new();
    store.observe_stages(at_stage);
    store.pin_clock(1);
    store
}
fn request(n: u64, predicates: Vec<Predicate>, mutations: Vec<Mutation>) -> Request {
    Request {
        version: conditional::VERSION,
        op_id: op(n),
        predicates,
        mutations,
    }
}
fn absent(schema: &str, key: &[u8]) -> Predicate {
    Predicate {
        schema: schema.into(),
        key: key.into(),
        expected: Expected::Absent,
    }
}
fn exact(schema: &str, key: &[u8], bytes: &[u8]) -> Predicate {
    Predicate {
        schema: schema.into(),
        key: key.into(),
        expected: Expected::Bytes(bytes.into()),
    }
}
fn put(schema: &str, key: &[u8], value: &[u8]) -> Mutation {
    Mutation::Put {
        schema: schema.into(),
        key: key.into(),
        value: value.into(),
    }
}
fn delete(schema: &str, key: &[u8]) -> Mutation {
    Mutation::Delete {
        schema: schema.into(),
        key: key.into(),
    }
}
fn range(schema: &str, prefix: &[u8], length: usize) -> Mutation {
    Mutation::DeleteRange {
        schema: schema.into(),
        prefix: prefix.into(),
        exact_key_length: length as u64,
    }
}
fn transact(store: &MemoryStore, request: &Request) -> OpResult<Decision> {
    match StoreSlots::record_transaction(store, &mut Op::detached(), request) {
        Step::Ready(result) => result,
        Step::Pending { .. } => panic!("memory conditional records must answer inline"),
    }
}
fn seed(store: &MemoryStore, schema: &str, key: &[u8], value: &[u8]) {
    match StoreSlots::record_put(store, &mut Op::detached(), schema, key, value) {
        Step::Ready(result) => result.unwrap(),
        Step::Pending { .. } => panic!("memory record_put must answer inline"),
    }
}
fn rows(store: &MemoryStore, schema: &str) -> BTreeMap<Vec<u8>, Vec<u8>> {
    match StoreSlots::record_scan(store, &mut Op::detached(), schema, b"", u32::MAX) {
        Step::Ready(result) => result
            .unwrap()
            .into_iter()
            .map(|(key, value)| (key, value.as_slice().to_vec()))
            .collect(),
        Step::Pending { .. } => panic!("memory record_scan must answer inline"),
    }
}
fn receipts(store: &MemoryStore) -> usize {
    store.v3.op_bytes()
}
fn receipt_size() -> usize {
    std::mem::size_of::<busbar_contract::abi::sdk::store::dedupe::Body>()
}

#[test]
fn supporting_memory_opts_in_and_uses_the_public_store_method() {
    const { assert!(<MemoryStore as StoreSlots>::RECORD_TRANSACTION) };
    assert_eq!(
        conditional::capability::<MemoryStore>(),
        Some((conditional::CAPABILITY, b"1".as_slice()))
    );
    let store = store();
    let request = request(
        1,
        vec![absent("arbitrary/schema", b"key")],
        vec![put("arbitrary/schema", b"key", b"value")],
    );
    assert_eq!(transact(&store, &request), Ok(Decision::Applied));
    assert_eq!(
        rows(&store, "arbitrary/schema").get(b"key".as_slice()),
        Some(&b"value".to_vec())
    );
}

#[test]
fn all_predicates_compare_the_prestate_before_any_mutation_including_overlaps() {
    let store = store();
    let request = request(
        1,
        vec![absent("schema", b"a"), absent("schema", b"b")],
        vec![
            put("schema", b"b", b"second"),
            put("schema", b"a", b"first"),
        ],
    );
    assert_eq!(transact(&store, &request), Ok(Decision::Applied));
    assert_eq!(
        rows(&store, "schema"),
        [
            (b"a".to_vec(), b"first".to_vec()),
            (b"b".to_vec(), b"second".to_vec())
        ]
        .into()
    );
    assert_eq!(receipts(&store), receipt_size());
}

#[test]
fn condition_conflict_changes_no_rows_revision_or_receipt_and_does_not_consume_the_id() {
    let store = store();
    seed(&store, "schema", b"guard", b"current");
    seed(&store, "schema", b"keep", b"unchanged");
    let before = rows(&store, "schema");
    let revision = store.revision.load(Ordering::Relaxed);
    let mut candidate = request(
        1,
        vec![exact("schema", b"guard", b"stale")],
        vec![delete("schema", b"keep"), put("schema", b"created", b"new")],
    );
    assert_eq!(
        transact(&store, &candidate),
        Ok(Decision::ConditionConflict)
    );
    assert_eq!(rows(&store, "schema"), before);
    assert_eq!(store.revision.load(Ordering::Relaxed), revision);
    assert_eq!(receipts(&store), 0);
    candidate.predicates = vec![exact("schema", b"guard", b"current")];
    assert_eq!(
        transact(&store, &candidate),
        Ok(Decision::Applied),
        "definitive condition conflict recorded no applied body"
    );
    assert!(!rows(&store, "schema").contains_key(b"keep".as_slice()));
    assert_eq!(receipts(&store), receipt_size());
}

#[test]
fn a_lost_success_replays_its_receipt_before_rechecking_rows_and_changed_body_conflicts() {
    let store = store();
    let candidate = request(
        1,
        vec![absent("schema", b"task")],
        vec![
            put("schema", b"task", b"committed"),
            put("schema", b"event", b"once"),
        ],
    );
    let _lost_answer = transact(&store, &candidate).unwrap();
    seed(&store, "schema", b"task", b"later-writer");
    seed(&store, "schema", b"event", b"later-event");
    let before = rows(&store, "schema");
    assert_eq!(transact(&store, &candidate), Ok(Decision::Applied));
    assert_eq!(rows(&store, "schema"), before);
    let mut changed = candidate.clone();
    changed.mutations[1] = put("schema", b"event", b"different-body");
    assert_eq!(transact(&store, &changed), Err(OpRefused::Conflict));
    assert_eq!(rows(&store, "schema"), before);
    assert_eq!(receipts(&store), receipt_size());
}

#[test]
fn conditional_receipts_share_the_existing_op_id_namespace() {
    let store = store();
    store.v3_add_usage_batch(op(1), &[]).unwrap();
    let candidate = request(
        1,
        vec![absent("schema", b"task")],
        vec![put("schema", b"task", b"new")],
    );
    assert_eq!(transact(&store, &candidate), Err(OpRefused::Conflict));
    assert!(rows(&store, "schema").is_empty());
    assert_eq!(receipts(&store), receipt_size());
}

#[test]
fn two_writers_of_the_same_absent_snapshot_apply_exactly_one_candidate() {
    let store = Arc::new(store());
    let gate = Arc::new(Barrier::new(3));
    let workers: Vec<_> = [(1, b"one".as_slice()), (2, b"two".as_slice())]
        .into_iter()
        .map(|(id, value)| {
            let store = Arc::clone(&store);
            let gate = Arc::clone(&gate);
            std::thread::spawn(move || {
                let candidate = request(
                    id,
                    vec![absent("schema", b"row")],
                    vec![put("schema", b"row", value), put("schema", b"event", value)],
                );
                gate.wait();
                (value.to_vec(), transact(&store, &candidate).unwrap())
            })
        })
        .collect();
    gate.wait();
    let results: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(
        results
            .iter()
            .filter(|(_, d)| *d == Decision::Applied)
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|(_, d)| *d == Decision::ConditionConflict)
            .count(),
        1
    );
    let winner = &results
        .iter()
        .find(|(_, d)| *d == Decision::Applied)
        .unwrap()
        .0;
    let snapshot = rows(&store, "schema");
    assert_eq!(snapshot.get(b"row".as_slice()), Some(winner));
    assert_eq!(snapshot.get(b"event".as_slice()), Some(winner));
    assert_eq!(receipts(&store), receipt_size());
}

#[test]
fn concurrent_same_id_body_replays_and_keeps_one_applied_receipt() {
    let store = Arc::new(store());
    let gate = Arc::new(Barrier::new(3));
    let candidate = request(
        1,
        vec![absent("schema", b"row")],
        vec![put("schema", b"row", b"new")],
    );
    let workers: Vec<_> = (0..2)
        .map(|_| {
            let store = Arc::clone(&store);
            let gate = Arc::clone(&gate);
            let candidate = candidate.clone();
            std::thread::spawn(move || {
                gate.wait();
                transact(&store, &candidate)
            })
        })
        .collect();
    gate.wait();
    for worker in workers {
        assert_eq!(worker.join().unwrap(), Ok(Decision::Applied));
    }
    assert_eq!(receipts(&store), receipt_size());
    assert_eq!(
        rows(&store, "schema").get(b"row".as_slice()),
        Some(&b"new".to_vec())
    );
}

#[test]
fn a_late_invalid_mutation_or_selector_rolls_back_every_earlier_candidate_effect() {
    let invalids = vec![
        vec![
            put("schema", b"first", b"changed"),
            put("schema", b"last", b""),
        ],
        vec![
            delete("schema", b"first"),
            put("schema", b"last", &vec![7; MAX_RECORD_BYTES + 1]),
        ],
        vec![
            put("schema", b"first", b"changed"),
            delete("schema", b"first"),
        ],
        vec![
            put("schema", b"first", b"changed"),
            range("schema", b"too-long", 1),
        ],
        vec![delete("schema", b"keep"), range("schema", b"kee", 4)],
        vec![range("schema", b"keep", 4), range("schema", b"kee", 4)],
    ];
    for mutations in invalids {
        let store = store();
        seed(&store, "schema", b"first", b"original");
        seed(&store, "schema", b"keep", b"retained");
        let before = rows(&store, "schema");
        let revision = store.revision.load(Ordering::Relaxed);
        let mut candidate = request(1, vec![exact("schema", b"first", b"original")], mutations);
        assert!(matches!(
            transact(&store, &candidate),
            Err(OpRefused::Refused(_))
        ));
        assert_eq!(rows(&store, "schema"), before);
        assert_eq!(receipts(&store), 0);
        assert_eq!(store.revision.load(Ordering::Relaxed), revision);
        candidate.mutations = vec![put("schema", b"first", b"valid-retry")];
        assert_eq!(transact(&store, &candidate), Ok(Decision::Applied));
        assert_eq!(receipts(&store), receipt_size());
    }
}

#[test]
fn malformed_versions_empty_lists_and_duplicate_predicates_leave_no_receipt() {
    let store = store();
    let valid = request(
        1,
        vec![absent("schema", b"row")],
        vec![put("schema", b"row", b"value")],
    );
    let mut cases = Vec::new();
    let mut bad = valid.clone();
    bad.version += 1;
    cases.push(bad);
    let mut bad = valid.clone();
    bad.predicates.clear();
    cases.push(bad);
    let mut bad = valid.clone();
    bad.mutations.clear();
    cases.push(bad);
    let mut bad = valid.clone();
    bad.predicates.push(bad.predicates[0].clone());
    cases.push(bad);
    let mut bad = valid.clone();
    bad.predicates[0].expected = Expected::Bytes(vec![3; MAX_RECORD_BYTES + 1]);
    cases.push(bad);
    for candidate in cases {
        assert!(matches!(
            transact(&store, &candidate),
            Err(OpRefused::Refused(_))
        ));
        assert!(rows(&store, "schema").is_empty());
        assert_eq!(receipts(&store), 0);
    }
    assert_eq!(transact(&store, &valid), Ok(Decision::Applied));
}

#[test]
fn delete_range_uses_raw_prefix_schema_and_exact_length_preserving_nested_and_sibling_rows() {
    let store = store();
    let prefix = b"scope/task/";
    let malformed = [prefix.as_slice(), &[0xff]].concat();
    let normal = [prefix.as_slice(), &[0x01]].concat();
    let nested = [prefix.as_slice(), b"child/", &[0x01]].concat();
    let different_width = prefix.to_vec();
    let sibling = b"scope/taskish/\x01".to_vec();
    let same_width_sibling = b"scope/tasx/\x01".to_vec();
    for key in [
        &malformed,
        &normal,
        &nested,
        &different_width,
        &sibling,
        &same_width_sibling,
    ] {
        seed(&store, "schema", key, b"held");
    }
    seed(&store, "other-schema", &malformed, b"foreign");
    seed(&store, "schema", b"guard", b"snapshot");
    let candidate = request(
        1,
        vec![
            exact("schema", b"guard", b"snapshot"),
            exact("schema", &malformed, b"held"),
        ],
        vec![
            range("schema", prefix, prefix.len() + 1),
            put("schema", b"marker", b"committed"),
        ],
    );
    assert_eq!(transact(&store, &candidate), Ok(Decision::Applied));
    let snapshot = rows(&store, "schema");
    assert!(!snapshot.contains_key(&malformed));
    assert!(!snapshot.contains_key(&normal));
    for key in [&nested, &different_width, &sibling, &same_width_sibling] {
        assert_eq!(snapshot.get(key), Some(&b"held".to_vec()));
    }
    assert_eq!(
        rows(&store, "other-schema").get(&malformed),
        Some(&b"foreign".to_vec())
    );
    seed(&store, "schema", &normal, b"created-later");
    assert_eq!(transact(&store, &candidate), Ok(Decision::Applied));
    assert_eq!(
        rows(&store, "schema").get(&normal),
        Some(&b"created-later".to_vec()),
        "receipt replay never re-runs the range"
    );
    let mut changed = candidate.clone();
    changed.mutations[0] = range("schema", prefix, prefix.len() + 2);
    assert_eq!(transact(&store, &changed), Err(OpRefused::Conflict));
}

fn lifecycle_candidates() -> (Request, Request) {
    (
        request(
            1,
            vec![exact("schema", b"row", b"old")],
            vec![
                put("schema", b"row", b"next"),
                put("schema", b"event/\x02", b"next-event"),
            ],
        ),
        request(
            2,
            vec![exact("schema", b"row", b"old")],
            vec![delete("schema", b"row"), range("schema", b"event/", 7)],
        ),
    )
}
fn lifecycle_store() -> MemoryStore {
    let store = store();
    seed(&store, "schema", b"row", b"old");
    seed(&store, "schema", b"event/\x01", b"old-event");
    seed(&store, "schema", b"event/nested/\x01", b"nested");
    store
}
fn assert_lifecycle(store: &MemoryStore, writer_won: bool) {
    let snapshot = rows(store, "schema");
    let expected: BTreeMap<Vec<u8>, Vec<u8>> = if writer_won {
        [
            (b"event/\x01".to_vec(), b"old-event".to_vec()),
            (b"event/\x02".to_vec(), b"next-event".to_vec()),
            (b"event/nested/\x01".to_vec(), b"nested".to_vec()),
            (b"row".to_vec(), b"next".to_vec()),
        ]
        .into()
    } else {
        [(b"event/nested/\x01".to_vec(), b"nested".to_vec())].into()
    };
    assert_eq!(snapshot, expected);
    assert_eq!(receipts(store), receipt_size());
}

#[test]
fn writer_and_retention_in_both_orders_refuse_the_stale_whole_snapshot() {
    for writer_first in [true, false] {
        let store = lifecycle_store();
        let (writer, retention) = lifecycle_candidates();
        let (first, second) = if writer_first {
            (&writer, &retention)
        } else {
            (&retention, &writer)
        };
        assert_eq!(transact(&store, first), Ok(Decision::Applied));
        assert_eq!(transact(&store, second), Ok(Decision::ConditionConflict));
        assert_lifecycle(&store, writer_first);
    }
}
#[test]
fn racing_writer_and_retention_publish_one_complete_outcome_and_one_receipt() {
    let store = Arc::new(lifecycle_store());
    let gate = Arc::new(Barrier::new(3));
    let (writer, retention) = lifecycle_candidates();
    let workers: Vec<_> = [writer, retention]
        .into_iter()
        .map(|candidate| {
            let store = Arc::clone(&store);
            let gate = Arc::clone(&gate);
            std::thread::spawn(move || {
                gate.wait();
                transact(&store, &candidate).unwrap()
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
    assert_lifecycle(&store, results[0] == Decision::Applied);
}

#[test]
fn large_binary_keys_and_full_width_values_round_trip_with_fixed_size_receipts() {
    let store = store();
    let mut key = vec![0xff; MAX_RECORD_BYTES / 2 + 1];
    key.extend_from_slice(&[0, 0x80, b'/']);
    let value = vec![0xfe; MAX_RECORD_BYTES];
    let candidate = request(
        1,
        vec![absent("unicode/λ", &key)],
        vec![put("unicode/λ", &key, &value)],
    );
    assert_eq!(transact(&store, &candidate), Ok(Decision::Applied));
    assert_eq!(rows(&store, "unicode/λ").get(&key), Some(&value));
    assert_eq!(transact(&store, &candidate), Ok(Decision::Applied));
    assert_eq!(receipts(&store), receipt_size());
    let mut changed = candidate.clone();
    if let Mutation::Put { value, .. } = &mut changed.mutations[0] {
        let last = value.len() - 1;
        value[last] ^= 1;
    }
    assert_eq!(transact(&store, &changed), Err(OpRefused::Conflict));
    assert_eq!(rows(&store, "unicode/λ").get(&key), Some(&value));
}

#[test]
fn ordinary_snapshots_and_receipt_replay_wait_until_the_complete_batch_commits() {
    use std::{cell::RefCell, rc::Rc};
    let store = Arc::new(store());
    seed(&store, "schema", b"gate", b"old");
    seed(&store, "schema", b"event/\x01", b"old-event");
    seed(&store, "schema", b"event/nested/\x01", b"nested");
    let candidate = request(
        1,
        vec![exact("schema", b"gate", b"old")],
        vec![
            put("schema", b"gate", b"new"),
            range("schema", b"event/", 7),
            put("schema", b"marker", b"done"),
        ],
    );
    let (read_start_tx, read_start_rx) = mpsc::channel();
    let (read_attempt_tx, read_attempt_rx) = mpsc::channel();
    let (read_done_tx, read_done_rx) = mpsc::channel();
    let reader_store = Arc::clone(&store);
    let reader = std::thread::spawn(move || {
        if read_start_rx.recv().is_err() {
            return;
        }
        read_attempt_tx.send(()).unwrap();
        let snapshot = rows(&reader_store, "schema");
        let _ = read_done_tx.send(snapshot);
    });
    let (replay_start_tx, replay_start_rx) = mpsc::channel();
    let (replay_attempt_tx, replay_attempt_rx) = mpsc::channel();
    let (replay_done_tx, replay_done_rx) = mpsc::channel();
    let replay_store = Arc::clone(&store);
    let replay_request = candidate.clone();
    let replay = std::thread::spawn(move || {
        if replay_start_rx.recv().is_err() {
            return;
        }
        replay_attempt_tx.send(()).unwrap();
        let _ = replay_done_tx.send(transact(&replay_store, &replay_request));
    });
    let read_done = Rc::new(RefCell::new(read_done_rx));
    let replay_done = Rc::new(RefCell::new(replay_done_rx));
    let observing_read = Rc::clone(&read_done);
    let observing_replay = Rc::clone(&replay_done);
    let mut started = false;
    let result = observe(
        move |stage| {
            if stage == Stage::Mutation(0) && !started {
                started = true;
                read_start_tx.send(()).unwrap();
                replay_start_tx.send(()).unwrap();
                read_attempt_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("ordinary reader attempted the real API");
                replay_attempt_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("receipt replay attempted the real API");
                assert!(
                    matches!(
                        observing_read
                            .borrow_mut()
                            .recv_timeout(Duration::from_millis(40)),
                        Err(mpsc::RecvTimeoutError::Timeout)
                    ),
                    "ordinary reader saw part of the batch"
                );
                assert!(
                    matches!(
                        observing_replay
                            .borrow_mut()
                            .recv_timeout(Duration::from_millis(40)),
                        Err(mpsc::RecvTimeoutError::Timeout)
                    ),
                    "receipt escaped before the batch completed"
                );
            }
            if stage == Stage::BeforeReceipt {
                assert!(started);
                assert!(matches!(
                    observing_read.borrow_mut().try_recv(),
                    Err(mpsc::TryRecvError::Empty)
                ));
                assert!(matches!(
                    observing_replay.borrow_mut().try_recv(),
                    Err(mpsc::TryRecvError::Empty)
                ));
            }
        },
        || transact(&store, &candidate),
    );
    assert_eq!(result, Ok(Decision::Applied));
    let snapshot = read_done
        .borrow_mut()
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    assert_eq!(
        snapshot,
        [
            (b"event/nested/\x01".to_vec(), b"nested".to_vec()),
            (b"gate".to_vec(), b"new".to_vec()),
            (b"marker".to_vec(), b"done".to_vec())
        ]
        .into()
    );
    assert_eq!(
        replay_done
            .borrow_mut()
            .recv_timeout(Duration::from_secs(5))
            .unwrap(),
        Ok(Decision::Applied)
    );
    reader.join().unwrap();
    replay.join().unwrap();
    assert_eq!(receipts(&store), receipt_size());
}

#[test]
fn an_ordinary_writer_cannot_interleave_before_mutations_and_receipt_are_together() {
    use std::{cell::RefCell, rc::Rc};
    let store = Arc::new(store());
    seed(&store, "schema", b"gate", b"old");
    seed(&store, "schema", b"companion", b"old");
    let candidate = request(
        1,
        vec![exact("schema", b"gate", b"old")],
        vec![
            put("schema", b"gate", b"transaction"),
            put("schema", b"companion", b"transaction"),
        ],
    );
    let (start_tx, start_rx) = mpsc::channel();
    let (attempt_tx, attempt_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let writer_store = Arc::clone(&store);
    let writer = std::thread::spawn(move || {
        if start_rx.recv().is_err() {
            return;
        }
        attempt_tx.send(()).unwrap();
        seed(&writer_store, "schema", b"gate", b"ordinary-after");
        let _ = done_tx.send(());
    });
    let done = Rc::new(RefCell::new(done_rx));
    let observing = Rc::clone(&done);
    let mut started = false;
    assert_eq!(
        observe(
            move |stage| {
                if stage == Stage::Mutation(0) && !started {
                    started = true;
                    start_tx.send(()).unwrap();
                    attempt_rx
                        .recv_timeout(Duration::from_secs(5))
                        .expect("ordinary write really attempted");
                    assert!(matches!(
                        observing
                            .borrow_mut()
                            .recv_timeout(Duration::from_millis(40)),
                        Err(mpsc::RecvTimeoutError::Timeout)
                    ));
                }
                if stage == Stage::BeforeReceipt {
                    assert!(started);
                    assert!(matches!(
                        observing.borrow_mut().try_recv(),
                        Err(mpsc::TryRecvError::Empty)
                    ));
                }
            },
            || transact(&store, &candidate)
        ),
        Ok(Decision::Applied)
    );
    done.borrow_mut()
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    writer.join().unwrap();
    let before = rows(&store, "schema");
    assert_eq!(
        before.get(b"gate".as_slice()),
        Some(&b"ordinary-after".to_vec())
    );
    assert_eq!(
        before.get(b"companion".as_slice()),
        Some(&b"transaction".to_vec())
    );
    assert_eq!(transact(&store, &candidate), Ok(Decision::Applied));
    assert_eq!(rows(&store, "schema"), before);
    assert_eq!(receipts(&store), receipt_size());
    let stale = request(
        2,
        vec![exact("schema", b"gate", b"transaction")],
        vec![delete("schema", b"companion")],
    );
    assert_eq!(transact(&store, &stale), Ok(Decision::ConditionConflict));
    assert_eq!(rows(&store, "schema"), before);
    assert_eq!(receipts(&store), receipt_size());
}

#[test]
fn a_condition_conflict_does_not_sweep_even_an_old_applied_receipt() {
    let store = store();
    let applied = request(
        1,
        vec![absent("schema", b"row")],
        vec![put("schema", b"row", b"current")],
    );
    assert_eq!(transact(&store, &applied), Ok(Decision::Applied));
    let held = receipts(&store);
    store.pin_clock(OP_ID_RETENTION_SECS + 100);
    let conflict = request(
        2,
        vec![exact("schema", b"row", b"stale")],
        vec![delete("schema", b"row")],
    );
    assert_eq!(transact(&store, &conflict), Ok(Decision::ConditionConflict));
    assert_eq!(receipts(&store), held);
    assert_eq!(
        transact(&store, &applied),
        Ok(Decision::Applied),
        "condition conflicts must not alter applied-only receipt state"
    );
    assert_eq!(
        rows(&store, "schema").get(b"row".as_slice()),
        Some(&b"current".to_vec())
    );
}

#[test]
fn range_and_point_mutations_at_different_lengths_or_schemas_are_not_falsely_rejected() {
    let store = store();
    seed(&store, "schema", b"p/x", b"three");
    seed(&store, "schema", b"p/xx", b"four");
    seed(&store, "other", b"p/x", b"foreign");
    let candidate = request(
        1,
        vec![exact("schema", b"p/x", b"three")],
        vec![
            range("schema", b"p/", 3),
            range("schema", b"p/", 4),
            put("schema", b"p/xxx", b"five"),
            put("other", b"p/x", b"foreign-updated"),
        ],
    );
    assert_eq!(transact(&store, &candidate), Ok(Decision::Applied));
    let snapshot = rows(&store, "schema");
    assert!(!snapshot.contains_key(b"p/x".as_slice()));
    assert!(!snapshot.contains_key(b"p/xx".as_slice()));
    assert_eq!(snapshot.get(b"p/xxx".as_slice()), Some(&b"five".to_vec()));
    assert_eq!(
        rows(&store, "other").get(b"p/x".as_slice()),
        Some(&b"foreign-updated".to_vec())
    );
}

#[test]
fn absent_and_empty_expected_bytes_are_distinct_and_delete_replay_does_not_resurrect() {
    let store = store();
    let mut candidate = request(
        1,
        vec![exact("schema", b"missing", b"")],
        vec![put("schema", b"missing", b"new")],
    );
    assert_eq!(
        transact(&store, &candidate),
        Ok(Decision::ConditionConflict)
    );
    assert_eq!(receipts(&store), 0);
    candidate.predicates = vec![absent("schema", b"missing")];
    assert_eq!(transact(&store, &candidate), Ok(Decision::Applied));
    seed(&store, "schema", b"missing", b"");
    assert!(rows(&store, "schema").is_empty());
    assert_eq!(transact(&store, &candidate), Ok(Decision::Applied));
    assert!(
        rows(&store, "schema").is_empty(),
        "lost-answer replay never resurrects a later deletion"
    );
    assert_eq!(receipts(&store), receipt_size());
}

#[test]
fn selective_range_deletion_keeps_unrelated_history_and_handles_binary_and_empty_prefixes() {
    let store = store();
    // A journal-like address is selected among a substantial unrelated retained history.
    for n in 0..4096u32 {
        seed(&store, "history", &n.to_be_bytes(), b"retained");
    }
    seed(&store, "history", b"\xff\xff", b"short");
    seed(&store, "history", b"\xff\xff\x00", b"remove");
    seed(&store, "history", b"\xff\xff\x01", b"remove");
    seed(&store, "history", b"\xff\xff\x00\x00", b"long");
    seed(&store, "history-next", b"\xff\xff\x00", b"other-schema");
    seed(&store, "guard", b"owner", b"current");
    let deletion = request(
        40_001,
        vec![exact("guard", b"owner", b"current")],
        vec![range("history", b"\xff\xff", 3)],
    );
    assert_eq!(transact(&store, &deletion), Ok(Decision::Applied));
    let selected = rows(&store, "history");
    assert_eq!(selected.len(), 4098);
    assert_eq!(
        selected.get(b"\xff\xff".as_slice()),
        Some(&b"short".to_vec())
    );
    assert_eq!(
        selected.get(b"\xff\xff\x00\x00".as_slice()),
        Some(&b"long".to_vec())
    );
    assert!(!selected.contains_key(b"\xff\xff\x00".as_slice()));
    assert!(!selected.contains_key(b"\xff\xff\x01".as_slice()));
    assert_eq!(rows(&store, "history-next").len(), 1);
    // A recorded replay must not delete a replacement installed after the first decision.
    seed(&store, "history", b"\xff\xff\x00", b"newer");
    assert_eq!(transact(&store, &deletion), Ok(Decision::Applied));
    assert_eq!(
        rows(&store, "history").get(b"\xff\xff\x00".as_slice()),
        Some(&b"newer".to_vec())
    );
    let empty_prefix = request(
        40_002,
        vec![exact("guard", b"owner", b"current")],
        vec![range("history", b"", 3)],
    );
    assert_eq!(transact(&store, &empty_prefix), Ok(Decision::Applied));
    assert_eq!(rows(&store, "history"), selected);
    let absent = request(
        40_003,
        vec![exact("guard", b"owner", b"current")],
        vec![range("history", b"absent", 6)],
    );
    assert_eq!(transact(&store, &absent), Ok(Decision::Applied));
    assert_eq!(rows(&store, "history"), selected);
    let conflict = request(
        40_004,
        vec![exact("guard", b"owner", b"wrong")],
        vec![range("history", b"", 4)],
    );
    assert_eq!(transact(&store, &conflict), Ok(Decision::ConditionConflict));
    assert_eq!(rows(&store, "history"), selected);
}
