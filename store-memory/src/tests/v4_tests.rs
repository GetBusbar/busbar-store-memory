// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors
use super::*;
const NS: [u8; 16] = [9; 16];
const WRITER: [u8; 16] = [2; 16];
fn address(key: &[u8]) -> Address {
    Address {
        namespace: NS,
        key: key.to_vec(),
    }
}
fn put(key: &[u8], value: &[u8]) -> Mutation<Own> {
    Mutation::Put {
        address: address(key),
        value: Secret::held(value.to_vec()),
    }
}
fn store() -> MemoryV4 {
    let s = MemoryV4::open(b"", &[], None).unwrap();
    s.state.lock().unwrap().now = Some(100);
    s
}
fn candidate(s: &MemoryV4, op: u128, mutations: Vec<Mutation<Own>>) -> Candidate<Own> {
    Candidate {
        id: AttemptId {
            dataset: s.info.dataset,
            writer_epoch: 1,
            writer: WRITER,
            execution_epoch: 1,
            operation: op.to_le_bytes(),
        },
        codec: CANDIDATE_VERSION,
        predicates: vec![],
        mutations,
        time: None,
        result: Secret::held(b"result".to_vec()),
    }
}
fn prepared(c: Candidate<Own>) -> PreparedCandidate<Own> {
    PreparedCandidate::new(c).unwrap()
}
fn initialization(s: &MemoryV4) -> Candidate<Own> {
    let root = Address {
        namespace: control::NAMESPACE.0,
        key: control::DATASET_KEY.to_vec(),
    };
    let writer = Address {
        namespace: control::NAMESPACE.0,
        key: control::writer_key(WRITER).to_vec(),
    };
    Candidate {
        id: AttemptId {
            dataset: s.info.dataset,
            writer_epoch: 1,
            writer: WRITER,
            execution_epoch: 1,
            operation: 1u128.to_le_bytes(),
        },
        codec: CANDIDATE_VERSION,
        predicates: vec![
            Predicate {
                address: root.clone(),
                expected: ExpectedValue::Absent,
            },
            Predicate {
                address: writer.clone(),
                expected: ExpectedValue::Absent,
            },
        ],
        mutations: vec![
            Mutation::Put {
                address: root,
                value: Secret::held(
                    control::Dataset {
                        id: s.info.dataset,
                        writer_epoch: 1,
                    }
                    .encode()
                    .unwrap()
                    .to_vec(),
                ),
            },
            Mutation::Put {
                address: writer,
                value: Secret::held(
                    control::Writer {
                        retired_through: 0,
                        admitted_through: 1,
                    }
                    .encode()
                    .unwrap()
                    .to_vec(),
                ),
            },
        ],
        time: None,
        result: Secret::held(vec![]),
    }
}
fn bootstrap(s: &MemoryV4) {
    assert!(matches!(
        s.commit_now(&prepared(initialization(s))).unwrap().decision,
        Decision::Applied(_)
    ));
}
fn get(s: &MemoryV4, key: &[u8]) -> Option<Vec<u8>> {
    match s
        .read_now(&Read {
            dataset: s.info.dataset,
            addresses: vec![address(key)],
            time: None,
        })
        .unwrap()
    {
        ReadResult::Values { values, .. } => {
            values[0].as_ref().map(|v| v.expose_own(&s.owner).clone())
        }
        _ => panic!(),
    }
}
fn scan(s: &MemoryV4) -> Scan {
    Scan {
        dataset: s.info.dataset,
        namespace: NamespaceId(NS),
        prefix: vec![],
        after: None,
        generation: None,
        allow_change: false,
        max_items: 2,
        max_bytes: 1024 * 1024,
    }
}
fn replace_writer(s: &MemoryV4, op: u128, next: control::Writer) {
    let at = Address {
        namespace: control::NAMESPACE.0,
        key: control::writer_key(WRITER).to_vec(),
    };
    let prior = s
        .guard()
        .unwrap()
        .get(at.namespace, &at.key)
        .unwrap()
        .expose_own(&s.owner)
        .clone();
    let mut c = candidate(
        s,
        op,
        vec![Mutation::Put {
            address: at.clone(),
            value: Secret::held(next.encode().unwrap().to_vec()),
        }],
    );
    c.predicates.push(Predicate {
        address: at,
        expected: ExpectedValue::Exact(Secret::held(prior)),
    });
    assert!(matches!(
        s.commit_now(&prepared(c)).unwrap().decision,
        Decision::Applied(_)
    ));
}
#[test]
fn effects_replay_original_observation_and_conflicting_body_and_epochs() {
    let s = store();
    bootstrap(&s);
    let p = prepared(candidate(&s, 2, vec![put(b"k", b"first")]));
    let first = s.commit_now(&p).unwrap();
    assert_eq!(first.observed_at.unix_seconds, 100);
    assert_eq!(get(&s, b"k"), Some(b"first".to_vec()));
    s.state.lock().unwrap().now = Some(300);
    let replay = s.commit_now(&p).unwrap();
    assert_eq!(replay.observed_at, first.observed_at);
    let Decision::Applied(result) = replay.decision else {
        panic!()
    };
    assert_eq!(result.expose_own(&s.owner), b"result");
    let conflict = prepared(candidate(&s, 2, vec![put(b"k", b"changed")]));
    assert!(matches!(
        s.commit_now(&conflict).unwrap().decision,
        Decision::OpIdConflict
    ));
    let mut c = candidate(&s, 2, vec![put(b"k", b"first")]);
    c.id.execution_epoch = 2;
    assert!(matches!(
        s.commit_now(&prepared(c)).unwrap().decision,
        Decision::OpIdConflict
    ));
    assert_eq!(get(&s, b"k"), Some(b"first".to_vec()));
}
#[test]
fn predicates_are_prestate_and_time_is_authoritative() {
    let s = store();
    bootstrap(&s);
    let mut c = candidate(&s, 2, vec![put(b"a", b"value"), put(b"b", b"value")]);
    c.predicates.push(Predicate {
        address: address(b"a"),
        expected: ExpectedValue::Exact(Secret::held(b"value".to_vec())),
    });
    assert!(matches!(
        s.commit_now(&prepared(c)).unwrap().decision,
        Decision::ConditionConflict
    ));
    assert_eq!(get(&s, b"b"), None);
    let mut c = candidate(&s, 3, vec![put(b"a", b"value")]);
    c.time = Some(TimeGuard {
        not_before: Some(90),
        before: Some(100),
    });
    assert!(matches!(
        s.commit_now(&prepared(c)).unwrap().decision,
        Decision::TimeGuardFailed {
            observed_at: ClockObservation {
                unix_seconds: 100,
                ..
            }
        }
    ));
    assert_eq!(get(&s, b"a"), None);
    assert!(matches!(
        s.read_now(&Read {
            dataset: s.info.dataset,
            addresses: vec![address(b"a")],
            time: Some(TimeGuard {
                not_before: Some(101),
                before: None
            })
        })
        .unwrap(),
        ReadResult::TimeGuardFailed { .. }
    ));
    let mut c = candidate(&s, 4, vec![put(b"a", b"value")]);
    c.time = Some(TimeGuard {
        not_before: Some(100),
        before: Some(101),
    });
    let p = prepared(c);
    assert!(matches!(
        s.commit_now(&p).unwrap().decision,
        Decision::Applied(_)
    ));
    s.state.lock().unwrap().now = Some(1000);
    assert!(matches!(
        s.commit_now(&p).unwrap().decision,
        Decision::Applied(_)
    ));
}
#[test]
fn scan_orders_raw_bytes_with_exclusive_cursor_and_generation() {
    let s = store();
    bootstrap(&s);
    s.commit_now(&prepared(candidate(
        &s,
        2,
        vec![
            put(&[255], b"z"),
            put(&[0, 255], b"b"),
            put(&[0], b"a"),
            put(&[0, 1], b"c"),
        ],
    )))
    .unwrap();
    let mut r = scan(&s);
    r.prefix = vec![0];
    let p = s.scan_now(&r).unwrap();
    assert_eq!(p.generation, 1);
    assert_eq!(
        p.rows
            .iter()
            .map(|(a, _)| a.key.clone())
            .collect::<Vec<_>>(),
        vec![vec![0], vec![0, 1]]
    );
    assert_eq!(p.next, Some(vec![0, 1]));
    r.after = p.next;
    r.generation = Some(p.generation);
    let p = s.scan_now(&r).unwrap();
    assert_eq!(p.rows[0].0.key, vec![0, 255]);
    assert!(p.next.is_none());
    s.commit_now(&prepared(candidate(&s, 3, vec![put(&[0, 2], b"d")])))
        .unwrap();
    assert!(matches!(s.scan_now(&r), Err(Failure::StaleScan)));
    r.allow_change = true;
    let p = s.scan_now(&r).unwrap();
    assert_eq!(p.generation, 2);
    assert_eq!(p.rows.len(), 2);
    assert!(p.next.is_none());
    r.max_items = 0;
    assert!(matches!(s.scan_now(&r), Err(Failure::Refused)));
    r.max_items = 1;
    r.max_bytes = 0;
    assert!(matches!(s.scan_now(&r), Err(Failure::Refused)));
}
#[test]
fn atomic_ranges_select_exact_length_without_hidden_caps() {
    let s = store();
    bootstrap(&s);
    let payload = vec![7; 2048];
    let mut mutations = vec![];
    for n in 0..150u16 {
        let k = n.to_be_bytes();
        mutations.push(put(&k, &payload));
    }
    mutations.push(put(&[0], b"short"));
    mutations.push(put(&[0, 1, 2], b"long"));
    assert!(matches!(
        s.commit_now(&prepared(candidate(&s, 2, mutations)))
            .unwrap()
            .decision,
        Decision::Applied(_)
    ));
    assert_eq!(get(&s, &[0, 149]).unwrap().len(), 2048);
    let c = candidate(
        &s,
        3,
        vec![
            Mutation::DeleteRange {
                namespace: NamespaceId(NS),
                prefix: vec![0],
                exact_key_length: 2,
            },
            put(b"other", b"present"),
        ],
    );
    assert!(matches!(
        s.commit_now(&prepared(c)).unwrap().decision,
        Decision::Applied(_)
    ));
    for n in 0..150u16 {
        assert_eq!(get(&s, &n.to_be_bytes()), None);
    }
    assert_eq!(get(&s, &[0]), Some(b"short".to_vec()));
    assert_eq!(get(&s, &[0, 1, 2]), Some(b"long".to_vec()));
    assert_eq!(get(&s, b"other"), Some(b"present".to_vec()));
    assert_eq!(s.scan_now(&scan(&s)).unwrap().generation, 2);
}
#[test]
fn authority_never_grants_unknown_or_retired_execution_after_gc() {
    let s = store();
    let unknown = prepared(candidate(&s, 2, vec![put(b"k", b"v")]));
    assert!(matches!(
        s.commit_now(&unknown).unwrap().decision,
        Decision::ExpiredUnknown
    ));
    bootstrap(&s);
    s.commit_now(&unknown).unwrap();
    s.state.lock().unwrap().elapsed = Some(Duration::from_secs(90000));
    {
        let mut state = s.guard().unwrap();
        s.collect_receipts(&mut state);
        assert!(state.receipts.contains_key(&(WRITER, 2u128.to_le_bytes())));
    }
    replace_writer(
        &s,
        3,
        control::Writer {
            retired_through: 0,
            admitted_through: 2,
        },
    );
    let mut future = candidate(&s, 4, vec![put(b"k", b"future")]);
    future.id.execution_epoch = 3;
    assert!(matches!(
        s.commit_now(&prepared(future)).unwrap().decision,
        Decision::ExpiredUnknown
    ));
    replace_writer(
        &s,
        5,
        control::Writer {
            retired_through: 1,
            admitted_through: 2,
        },
    );
    assert!(!s
        .guard()
        .unwrap()
        .receipts
        .contains_key(&(WRITER, 2u128.to_le_bytes())));
    assert!(matches!(
        s.commit_now(&unknown).unwrap().decision,
        Decision::ExpiredUnknown
    ));
    assert_eq!(get(&s, b"k"), Some(b"v".to_vec()));
    let mut epoch2 = candidate(&s, 6, vec![put(b"k", b"new")]);
    epoch2.id.execution_epoch = 2;
    assert!(matches!(
        s.commit_now(&prepared(epoch2)).unwrap().decision,
        Decision::Applied(_)
    ));
}
#[test]
fn control_generation_and_overflow_are_refused_before_any_effect() {
    let s = store();
    bootstrap(&s);
    let mut c = candidate(&s, 2, vec![put(b"k", b"v")]);
    c.mutations.push(Mutation::Delete(Address {
        namespace: control::NAMESPACE.0,
        key: control::DATASET_KEY.to_vec(),
    }));
    assert!(matches!(s.commit_now(&prepared(c)), Err(Failure::Refused)));
    assert_eq!(get(&s, b"k"), None);
    let c = candidate(
        &s,
        3,
        vec![Mutation::Put {
            address: Address {
                namespace: control::GENERATION_NAMESPACE.0,
                key: NS.to_vec(),
            },
            value: Secret::held(7u64.to_le_bytes().to_vec()),
        }],
    );
    assert!(matches!(s.commit_now(&prepared(c)), Err(Failure::Refused)));
    s.guard()
        .unwrap()
        .records
        .entry(control::GENERATION_NAMESPACE.0)
        .or_default()
        .insert(NS.to_vec(), Secret::held(u64::MAX.to_le_bytes().to_vec()));
    assert!(matches!(
        s.commit_now(&prepared(candidate(&s, 4, vec![put(b"k", b"v")]))),
        Err(Failure::Refused)
    ));
    assert_eq!(get(&s, b"k"), None);
}
#[test]
fn fresh_requires_completely_empty_records_and_validates_native_settings() {
    assert!(MemoryV4::validate(br#"{"input_budget_bytes":1048576}"#).is_ok());
    assert!(MemoryV4::validate(br#"{"clock":5}"#).is_err());
    assert!(MemoryV4::validate(br#"{"input_budget_bytes":0}"#).is_err());
    let a = store();
    let b = store();
    assert_ne!(a.info.dataset, b.info.dataset);
    assert_ne!(a.info.clock_authority, b.info.clock_authority);
    assert_ne!(a.info.atomic_domain, b.info.atomic_domain);
    assert_eq!(a.info.durability, Durability::Ephemeral);
    a.guard()
        .unwrap()
        .records
        .entry(NS)
        .or_default()
        .insert(b"legacy".to_vec(), Secret::held(b"v".to_vec()));
    let root = Address {
        namespace: control::NAMESPACE.0,
        key: control::DATASET_KEY.to_vec(),
    };
    let mut c = candidate(
        &a,
        2,
        vec![Mutation::Put {
            address: root.clone(),
            value: Secret::held(
                control::Dataset {
                    id: a.info.dataset,
                    writer_epoch: 1,
                }
                .encode()
                .unwrap()
                .to_vec(),
            ),
        }],
    );
    c.predicates.push(Predicate {
        address: root,
        expected: ExpectedValue::Absent,
    });
    assert!(matches!(
        a.commit_now(&prepared(c)).unwrap().decision,
        Decision::ExpiredUnknown
    ));
}

#[test]
fn initialization_cannot_regrant_legacy_partial_or_receipt_only_datasets() {
    let s = store();
    s.guard()
        .unwrap()
        .records
        .entry(NS)
        .or_default()
        .insert(b"legacy".to_vec(), Secret::held(vec![1]));
    let init = prepared(initialization(&s));
    assert!(control::initialization(init.candidate(), s.info.dataset));
    assert!(matches!(
        s.commit_now(&init).unwrap().decision,
        Decision::ExpiredUnknown
    ));
    s.guard().unwrap().records.clear();
    bootstrap(&s);
    s.guard().unwrap().records.clear();
    let mut init = initialization(&s);
    init.id.operation = 100u128.to_le_bytes();
    assert!(matches!(
        s.commit_now(&prepared(init)).unwrap().decision,
        Decision::ExpiredUnknown
    ));
}

#[test]
fn retired_receipt_replays_for_at_least_one_day_then_cannot_execute() {
    let s = store();
    s.guard().unwrap().elapsed = Some(Duration::ZERO);
    bootstrap(&s);
    let original = prepared(candidate(&s, 2, vec![put(b"a", b"v")]));
    s.commit_now(&original).unwrap();
    replace_writer(
        &s,
        3,
        control::Writer {
            retired_through: 1,
            admitted_through: 1,
        },
    );
    s.guard().unwrap().elapsed = Some(Duration::from_secs(86399));
    {
        let mut state = s.guard().unwrap();
        s.collect_receipts(&mut state);
    }
    assert!(matches!(
        s.commit_now(&original).unwrap().decision,
        Decision::Applied(_)
    ));
    s.guard().unwrap().elapsed = Some(Duration::from_secs(86400));
    {
        let mut state = s.guard().unwrap();
        s.collect_receipts(&mut state);
    }
    assert!(matches!(
        s.commit_now(&original).unwrap().decision,
        Decision::ExpiredUnknown
    ));
}

#[test]
fn generation_records_are_readable_predicates_and_point_delete_is_atomic() {
    let s = store();
    bootstrap(&s);
    let at = Address {
        namespace: control::GENERATION_NAMESPACE.0,
        key: NS.to_vec(),
    };
    let mut c = candidate(&s, 2, vec![put(b"a", b"v"), put(b"b", b"w")]);
    c.predicates.push(Predicate {
        address: at.clone(),
        expected: ExpectedValue::Absent,
    });
    s.commit_now(&prepared(c)).unwrap();
    let r = s
        .read_now(&Read {
            dataset: s.info.dataset,
            addresses: vec![
                address(b"b"),
                at.clone(),
                address(b"missing"),
                address(b"a"),
            ],
            time: None,
        })
        .unwrap();
    let ReadResult::Values { values, .. } = r else {
        panic!()
    };
    assert_eq!(values[0].as_ref().unwrap().expose_own(&s.owner), b"w");
    assert_eq!(
        values[1].as_ref().unwrap().expose_own(&s.owner),
        &1u64.to_le_bytes()
    );
    assert!(values[2].is_none());
    assert_eq!(values[3].as_ref().unwrap().expose_own(&s.owner), b"v");
    let mut c = candidate(
        &s,
        3,
        vec![
            Mutation::Delete(address(b"a")),
            Mutation::Delete(address(b"b")),
        ],
    );
    c.predicates.push(Predicate {
        address: at,
        expected: ExpectedValue::Exact(Secret::held(1u64.to_le_bytes().to_vec())),
    });
    s.commit_now(&prepared(c)).unwrap();
    assert!(get(&s, b"a").is_none());
    assert!(get(&s, b"b").is_none());
    assert_eq!(s.scan_now(&scan(&s)).unwrap().generation, 2);
}

#[test]
fn primitive_trait_entrypoints_and_normal_door_are_live() {
    let s = store();
    let mut op = Op::detached();
    assert!(matches!(s.connect(&mut op), Step::Ready(Ok(()))));
    assert_eq!(Backend::info(&s), s.info);
    assert!(matches!(
        s.clock(&mut op, s.info.dataset),
        Step::Ready(Ok(ClockObservation {
            unix_seconds: 100,
            ..
        }))
    ));
    assert!(matches!(
        s.commit(&mut op, &prepared(initialization(&s))),
        Step::Ready(Ok(Committed {
            decision: Decision::Applied(_),
            ..
        }))
    ));
    assert!(matches!(
        s.read(
            &mut op,
            &Read {
                dataset: s.info.dataset,
                addresses: vec![],
                time: None
            }
        ),
        Step::Ready(Ok(ReadResult::Values { .. }))
    ));
    assert!(matches!(s.scan(&mut op, &scan(&s)), Step::Ready(Ok(_))));
    assert_eq!(
        door().kind_abi(),
        busbar_contract::abi::store::v4::ABI_VERSION
    );
    assert_eq!(
        crate::door().kind_abi(),
        busbar_contract::abi::store::v4::ABI_VERSION
    );
    assert!(std::ptr::eq(door(), crate::door()));
}

#[test]
fn scan_byte_budget_never_returns_a_false_exhaustion_or_oversized_page() {
    let s = store();
    bootstrap(&s);
    s.commit_now(&prepared(candidate(
        &s,
        2,
        vec![put(b"a", b"12"), put(b"b", b"34")],
    )))
    .unwrap();
    let mut r = scan(&s);
    r.max_bytes = 3;
    let p = s.scan_now(&r).unwrap();
    assert_eq!(p.rows.len(), 1);
    assert_eq!(p.next, Some(b"a".to_vec()));
    r.after = p.next;
    r.generation = Some(p.generation);
    let p = s.scan_now(&r).unwrap();
    assert_eq!(p.rows[0].0.key, b"b");
    assert!(p.next.is_none());
    r.after = None;
    r.max_bytes = 2;
    assert!(matches!(s.scan_now(&r), Err(Failure::Refused)));
    s.guard()
        .unwrap()
        .records
        .entry(control::GENERATION_NAMESPACE.0)
        .or_default()
        .insert(NS.to_vec(), Secret::held(0u64.to_le_bytes().to_vec()));
    assert!(matches!(s.scan_now(&scan(&s)), Err(Failure::Corrupt)));
}

#[test]
fn control_puts_require_known_key_exact_predicate_and_monotonic_successor() {
    let s = store();
    bootstrap(&s);
    let at = Address {
        namespace: control::NAMESPACE.0,
        key: control::writer_key(WRITER).to_vec(),
    };
    let initial = control::Writer {
        retired_through: 0,
        admitted_through: 1,
    }
    .encode()
    .unwrap();
    let mut c = candidate(
        &s,
        2,
        vec![Mutation::Put {
            address: at.clone(),
            value: Secret::held(initial.to_vec()),
        }],
    );
    assert!(matches!(s.commit_now(&prepared(c)), Err(Failure::Refused)));
    c = candidate(
        &s,
        3,
        vec![Mutation::Put {
            address: Address {
                namespace: control::NAMESPACE.0,
                key: b"unknown".to_vec(),
            },
            value: Secret::held(initial.to_vec()),
        }],
    );
    c.predicates.push(Predicate {
        address: Address {
            namespace: control::NAMESPACE.0,
            key: b"unknown".to_vec(),
        },
        expected: ExpectedValue::Absent,
    });
    assert!(matches!(s.commit_now(&prepared(c)), Err(Failure::Refused)));
    replace_writer(
        &s,
        4,
        control::Writer {
            retired_through: 0,
            admitted_through: 2,
        },
    );
    let mut c = candidate(
        &s,
        5,
        vec![
            Mutation::Put {
                address: at.clone(),
                value: Secret::held(initial.to_vec()),
            },
            put(b"unapplied", b"v"),
        ],
    );
    c.predicates.push(Predicate {
        address: at,
        expected: ExpectedValue::Exact(Secret::held(
            control::Writer {
                retired_through: 0,
                admitted_through: 2,
            }
            .encode()
            .unwrap()
            .to_vec(),
        )),
    });
    assert!(matches!(s.commit_now(&prepared(c)), Err(Failure::Refused)));
    assert!(get(&s, b"unapplied").is_none());
    let c = candidate(
        &s,
        6,
        vec![Mutation::DeleteRange {
            namespace: control::NAMESPACE,
            prefix: vec![],
            exact_key_length: 17,
        }],
    );
    assert!(matches!(s.commit_now(&prepared(c)), Err(Failure::Refused)));
}

#[test]
fn present_corrupt_authority_is_distinct_from_missing_or_closed_authority() {
    let s = store();
    bootstrap(&s);
    let p = prepared(candidate(&s, 2, vec![put(b"a", b"v")]));
    let corrupt = |key: &[u8], value: Vec<u8>| {
        s.guard()
            .unwrap()
            .records
            .get_mut(&control::NAMESPACE.0)
            .unwrap()
            .insert(key.to_vec(), Secret::held(value));
    };
    corrupt(control::DATASET_KEY, b"broken".to_vec());
    assert!(matches!(s.commit_now(&p), Err(Failure::Corrupt)));
    corrupt(
        control::DATASET_KEY,
        control::Dataset {
            id: DatasetId([8; 16]),
            writer_epoch: 1,
        }
        .encode()
        .unwrap()
        .to_vec(),
    );
    assert!(matches!(s.commit_now(&p), Err(Failure::Corrupt)));
    corrupt(
        control::DATASET_KEY,
        control::Dataset {
            id: s.info.dataset,
            writer_epoch: 1,
        }
        .encode()
        .unwrap()
        .to_vec(),
    );
    corrupt(&control::writer_key(WRITER), b"broken".to_vec());
    assert!(matches!(s.commit_now(&p), Err(Failure::Corrupt)));
    s.guard()
        .unwrap()
        .records
        .get_mut(&control::NAMESPACE.0)
        .unwrap()
        .remove(control::writer_key(WRITER).as_slice());
    assert!(matches!(
        s.commit_now(&p).unwrap().decision,
        Decision::ExpiredUnknown
    ));
    assert!(get(&s, b"a").is_none());
}

#[test]
fn time_precedes_predicate_and_stale_control_predicate_precedes_effect_validation() {
    let s = store();
    bootstrap(&s);
    let mut c = candidate(&s, 2, vec![put(b"a", b"v")]);
    c.predicates.push(Predicate {
        address: address(b"missing"),
        expected: ExpectedValue::Exact(Secret::held(b"v".to_vec())),
    });
    c.time = Some(TimeGuard {
        not_before: None,
        before: Some(100),
    });
    assert!(matches!(
        s.commit_now(&prepared(c)).unwrap().decision,
        Decision::TimeGuardFailed { .. }
    ));
    let at = Address {
        namespace: control::NAMESPACE.0,
        key: control::writer_key(WRITER).to_vec(),
    };
    let old = control::Writer {
        retired_through: 0,
        admitted_through: 1,
    }
    .encode()
    .unwrap();
    replace_writer(
        &s,
        3,
        control::Writer {
            retired_through: 0,
            admitted_through: 2,
        },
    );
    let mut c = candidate(
        &s,
        4,
        vec![
            Mutation::Put {
                address: at.clone(),
                value: Secret::held(old.to_vec()),
            },
            put(b"a", b"v"),
        ],
    );
    c.predicates.push(Predicate {
        address: at,
        expected: ExpectedValue::Exact(Secret::held(old.to_vec())),
    });
    assert!(matches!(
        s.commit_now(&prepared(c)).unwrap().decision,
        Decision::ConditionConflict
    ));
    assert!(get(&s, b"a").is_none());
}

#[test]
fn lifecycle_inspection_reads_actual_state_and_never_calls_nonempty_empty() {
    let store = store();
    assert_eq!(store.inspect_now().unwrap(), InspectedState::Empty);
    bootstrap(&store);
    assert_eq!(store.inspect_now().unwrap(), InspectedState::Current);
    store
        .guard()
        .unwrap()
        .records
        .get_mut(&control::NAMESPACE.0)
        .unwrap()
        .insert(control::DATASET_KEY.to_vec(), Secret::held(vec![0]));
    assert_eq!(store.inspect_now(), Err(Failure::Corrupt));
    store
        .guard()
        .unwrap()
        .records
        .get_mut(&control::NAMESPACE.0)
        .unwrap()
        .remove(control::DATASET_KEY);
    assert_eq!(store.inspect_now(), Err(Failure::Refused));
    let fresh = MemoryV4::open(b"", &[], None).unwrap();
    assert_eq!(fresh.inspect_now().unwrap(), InspectedState::Empty);
    assert_ne!(fresh.info.dataset, store.info.dataset);
    let request = lifecycle::Request::Inspect {
        bounds: lifecycle::Bounds {
            items: 1,
            bytes: 4096,
        },
    };
    assert!(matches!(
        fresh.bootstrap_now(&request).unwrap(),
        lifecycle::Response::Inspected(lifecycle::Inspection {
            state: lifecycle::InspectionState::Empty,
            source: None,
            ..
        })
    ));
    assert!(fresh
        .connect_binding(Some(&lifecycle::OpenBinding::Empty))
        .is_ok());
    let held = fresh.guard().unwrap();
    assert!(matches!(
        fresh.bootstrap_now(&request),
        Err(lifecycle::Error::Busy)
    ));
    drop(held);
    let unsupported = lifecycle::Request::Target {
        upgrade: lifecycle::UpgradeRef {
            id: [1; 16],
            authority: Secret::held(vec![1]),
        },
    };
    assert!(matches!(
        fresh.bootstrap_now(&unsupported),
        Err(lifecycle::Error::MissingCapability)
    ));
    bootstrap(&fresh);
    fresh
        .guard()
        .unwrap()
        .records
        .entry(lifecycle::ADMISSION_NAMESPACE)
        .or_default()
        .insert(
            lifecycle::ADMISSION_KEY.to_vec(),
            Secret::held(vec![0, 255, 128]),
        );
    let lifecycle::Response::Inspected(current) = fresh.bootstrap_now(&request).unwrap() else {
        panic!("inspection")
    };
    assert_eq!(current.state, lifecycle::InspectionState::Current);
    assert_eq!(current.source.as_ref().unwrap().dataset, fresh.info.dataset);
    assert_eq!(
        current
            .admission_marker
            .as_ref()
            .unwrap()
            .expose_own(&fresh.owner),
        &[0, 255, 128]
    );
    assert_eq!(
        current.capabilities,
        vec![lifecycle::NativeCapability::CurrentOpen]
    );
    let binding = lifecycle::OpenBinding::Current {
        source: current.source.unwrap(),
        admission_marker_digest: archive::digest(
            current.admission_marker.unwrap().expose_own(&fresh.owner),
        ),
    };
    assert!(fresh.connect_binding(Some(&binding)).is_ok());
    assert!(fresh
        .connect_binding(Some(&lifecycle::OpenBinding::Empty))
        .is_err());
    fresh
        .guard()
        .unwrap()
        .records
        .get_mut(&lifecycle::ADMISSION_NAMESPACE)
        .unwrap()
        .insert(lifecycle::ADMISSION_KEY.to_vec(), Secret::held(vec![9]));
    assert!(fresh.connect_binding(Some(&binding)).is_err());
}
