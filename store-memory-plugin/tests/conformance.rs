// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The actual linked and dropped product answer neutral primitives identically.
mod support;
use busbar_contract::{persistence::*, Secret};
use std::sync::Arc;
const NS: NamespaceId = NamespaceId([9; 16]);
const WRITER: [u8; 16] = [7; 16];
fn address(key: &[u8]) -> Address {
    Address {
        namespace: NS.0,
        key: key.to_vec(),
    }
}
fn candidate(
    info: BackendInfo,
    operation: u128,
    predicates: Vec<Predicate>,
    mutations: Vec<Mutation>,
) -> Candidate {
    Candidate {
        id: AttemptId {
            dataset: info.dataset,
            writer_epoch: 1,
            writer: WRITER,
            execution_epoch: 1,
            operation: operation.to_le_bytes(),
        },
        codec: CANDIDATE_VERSION,
        predicates,
        mutations,
        time: None,
        result: Secret::new(b"applied".to_vec()),
    }
}
fn prepared(
    info: BackendInfo,
    operation: u128,
    predicates: Vec<Predicate>,
    mutations: Vec<Mutation>,
) -> PreparedCandidate {
    PreparedCandidate::new(candidate(info, operation, predicates, mutations)).unwrap()
}
pub async fn bootstrap(store: &dyn PersistenceBackend) {
    let info = store.info();
    let root = Address {
        namespace: control::NAMESPACE.0,
        key: control::DATASET_KEY.to_vec(),
    };
    let writer = Address {
        namespace: control::NAMESPACE.0,
        key: control::writer_key(WRITER).to_vec(),
    };
    let mut candidate = candidate(
        info,
        1,
        vec![
            Predicate {
                address: root.clone(),
                expected: ExpectedValue::Absent,
            },
            Predicate {
                address: writer.clone(),
                expected: ExpectedValue::Absent,
            },
        ],
        vec![
            Mutation::Put {
                address: root,
                value: Secret::new(
                    control::Dataset {
                        id: info.dataset,
                        writer_epoch: 1,
                    }
                    .encode()
                    .unwrap()
                    .to_vec(),
                ),
            },
            Mutation::Put {
                address: writer,
                value: Secret::new(
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
    );
    candidate.result = Secret::new(Vec::new());
    assert!(matches!(
        store
            .commit(PreparedCandidate::new(candidate).unwrap())
            .await
            .unwrap(),
        Decision::Applied(_)
    ));
}
#[test]
fn linked_and_dropped_primitives_preserve_replay_empty_values_ranges_and_cursor_guards() {
    support::block(async {
        for dropped in [false, true] {
            let store = support::open(dropped).await;
            let info = store.info();
            assert_eq!(info.durability, Durability::Ephemeral);
            assert_eq!(info.physical_version, 1);
            assert_ne!(info.dataset.0, [0; 16]);
            assert_ne!(info.clock_authority, [0; 16]);
            assert_ne!(info.atomic_domain, [0; 16]);
            bootstrap(&store).await;
            let candidate = prepared(
                info,
                2,
                vec![Predicate {
                    address: address(b"a"),
                    expected: ExpectedValue::Absent,
                }],
                vec![
                    Mutation::Put {
                        address: address(b"a"),
                        value: Secret::new(Vec::new()),
                    },
                    Mutation::Put {
                        address: address(b"a0"),
                        value: Secret::new(vec![0, 255, 128]),
                    },
                    Mutation::Put {
                        address: address(b"b"),
                        value: Secret::new(vec![8; 2048]),
                    },
                ],
            );
            let fingerprint = candidate.fingerprint();
            for original in [candidate.clone(), candidate.clone()] {
                assert!(
                    matches!(store.commit(original).await.unwrap(),Decision::Applied(result) if result==Secret::new(b"applied".to_vec()))
                );
            }
            assert_eq!(candidate.fingerprint(), fingerprint);
            let changed = prepared(info, 2, vec![], vec![Mutation::Delete(address(b"b"))]);
            assert!(matches!(
                store.commit(changed).await.unwrap(),
                Decision::OpIdConflict
            ));
            let ReadResult::Values {
                observed_at,
                values,
            } = store
                .read(Arc::new(Read {
                    dataset: info.dataset,
                    addresses: vec![
                        address(b"missing"),
                        address(b"a"),
                        address(b"a0"),
                        address(b"b"),
                    ],
                    time: None,
                }))
                .await
                .unwrap()
            else {
                panic!("coherent values")
            };
            assert_eq!(observed_at.authority, info.clock_authority);
            assert_eq!(
                values,
                vec![
                    None,
                    Some(Secret::new(Vec::new())),
                    Some(Secret::new(vec![0, 255, 128])),
                    Some(Secret::new(vec![8; 2048]))
                ]
            );
            let scan = |after, generation| Scan {
                dataset: info.dataset,
                namespace: NS,
                prefix: vec![],
                after,
                generation,
                allow_change: false,
                max_items: 1,
                max_bytes: 4096,
            };
            let page = store.scan(Arc::new(scan(None, None))).await.unwrap();
            assert_eq!(page.rows.len(), 1);
            assert_eq!(page.rows[0].0, address(b"a"));
            assert_eq!(page.next.as_deref(), Some(b"a".as_slice()));
            let next = store
                .scan(Arc::new(scan(page.next.clone(), Some(page.generation))))
                .await
                .unwrap();
            assert_eq!(next.rows[0].0, address(b"a0"));
            let range = prepared(
                info,
                3,
                vec![Predicate {
                    address: address(b"a"),
                    expected: ExpectedValue::Exact(Secret::new(Vec::new())),
                }],
                vec![Mutation::DeleteRange {
                    namespace: NS,
                    prefix: b"a".to_vec(),
                    exact_key_length: 1,
                }],
            );
            assert!(matches!(
                store.commit(range).await.unwrap(),
                Decision::Applied(_)
            ));
            assert_eq!(
                store
                    .scan(Arc::new(scan(None, Some(page.generation))))
                    .await
                    .unwrap_err(),
                Failure::StaleScan
            );
            let ReadResult::Values { values, .. } = store
                .read(Arc::new(Read {
                    dataset: info.dataset,
                    addresses: vec![address(b"a"), address(b"a0")],
                    time: None,
                }))
                .await
                .unwrap()
            else {
                panic!("values")
            };
            assert_eq!(values, vec![None, Some(Secret::new(vec![0, 255, 128]))]);
        }
    });
}

#[test]
fn linked_and_dropped_preopen_inspection_adopts_the_retained_empty_session() {
    use busbar_contract::persistence::lifecycle::{self, PreparedStore};
    support::block(async {
        for dropped in [false, true] {
            let prepared = support::prepare(dropped);
            assert_eq!(prepared.phase(), lifecycle::Phase::Bound);
            assert!(matches!(prepared.serving(), Err(lifecycle::Error::Pending)));
            assert!(matches!(
                prepared.open(lifecycle::OpenBinding::Empty).await,
                Err(lifecycle::Error::Refused)
            ));
            let inspection = prepared
                .bootstrap(Arc::new(lifecycle::Request::Inspect {
                    bounds: lifecycle::Bounds {
                        items: 16,
                        bytes: 4096,
                    },
                }))
                .await
                .unwrap();
            let lifecycle::Response::Inspected(inspection) = inspection.as_ref() else {
                panic!("native inspection")
            };
            assert_eq!(inspection.state, lifecycle::InspectionState::Empty);
            assert!(inspection.source.is_none());
            assert!(inspection.admission_marker.is_none());
            assert_eq!(
                inspection.capabilities,
                vec![lifecycle::NativeCapability::CurrentOpen]
            );
            assert_eq!(prepared.phase(), lifecycle::Phase::Prepared);
            let backend = prepared.open(lifecycle::OpenBinding::Empty).await.unwrap();
            assert_eq!(prepared.phase(), lifecycle::Phase::Open);
            assert!(Arc::ptr_eq(&backend, &prepared.serving().unwrap()));
            assert_eq!(backend.info().durability, Durability::Ephemeral);
            bootstrap(backend.as_ref()).await;
        }
    });
}
