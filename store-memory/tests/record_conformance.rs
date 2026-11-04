// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The record half of the store protocol, held to what `busbar_contract::abi::sdk::store::StoreSlots` declares.

use busbar_contract::ids::RecordSchemaId;
use busbar_contract::kinds::RecordBytes;
use busbar_store_memory::MemoryStore;

const TASKS: RecordSchemaId = RecordSchemaId::new("task");
const EVENTS: RecordSchemaId = RecordSchemaId::new("task_event");

fn body(bytes: &[u8]) -> RecordBytes {
    RecordBytes::new(bytes.to_vec()).expect("inside the record ceiling")
}

/// What was written is what comes back.
#[test]
fn a_record_reads_back_exactly_as_it_was_written() {
    let store = MemoryStore::new();
    store
        .record_put(TASKS, b"t-1", &body(b"submitted"))
        .expect("the put");
    assert_eq!(
        store.record_get(TASKS, b"t-1").expect("the get"),
        Some(body(b"submitted"))
    );
}

/// An absent record is an ANSWER, not a fault.
#[test]
fn an_absent_record_reads_back_as_none() {
    let store = MemoryStore::new();
    assert_eq!(
        store.record_get(TASKS, b"never-written").expect("the get"),
        None
    );
}

/// A PUT REPLACES.
#[test]
fn a_put_replaces_rather_than_accumulating() {
    let store = MemoryStore::new();
    store
        .record_put(TASKS, b"t-1", &body(b"working"))
        .expect("first");
    store
        .record_put(TASKS, b"t-1", &body(b"completed"))
        .expect("second");
    assert_eq!(
        store.record_get(TASKS, b"t-1").expect("the get"),
        Some(body(b"completed"))
    );
    assert_eq!(
        store.record_scan(TASKS, b"", 16).expect("the scan").len(),
        1
    );
}

/// TWO SCHEMAS ARE TWO NAMESPACES.
#[test]
fn schemas_are_separate_namespaces() {
    let store = MemoryStore::new();
    store
        .record_put(TASKS, b"k", &body(b"a task"))
        .expect("task");
    store
        .record_put(EVENTS, b"k", &body(b"an event"))
        .expect("event");
    assert_eq!(
        store.record_get(TASKS, b"k").expect("the get"),
        Some(body(b"a task"))
    );
    assert_eq!(
        store.record_get(EVENTS, b"k").expect("the get"),
        Some(body(b"an event"))
    );
    assert_eq!(
        store.record_scan(TASKS, b"", 16).expect("the scan").len(),
        1
    );
}

/// A KEY IS OPAQUE BYTES.
#[test]
fn a_key_is_opaque_bytes() {
    let store = MemoryStore::new();
    let key: &[u8] = &[0x00, 0xff, 0x41, 0x00];
    store
        .record_put(TASKS, key, &body(b"row"))
        .expect("the put");
    assert_eq!(
        store.record_get(TASKS, key).expect("the get"),
        Some(body(b"row"))
    );
    let scanned = store.record_scan(TASKS, &[0x00], 16).expect("the scan");
    assert_eq!(scanned, vec![(key.to_vec(), body(b"row"))]);
}

/// A SCAN IS A PREFIX WALK, IN KEY ORDER, and it answers only what shares the prefix.
#[test]
fn a_scan_walks_one_prefix_in_key_order() {
    let store = MemoryStore::new();
    store
        .record_put(EVENTS, b"t-1/0002", &body(b"second"))
        .expect("put");
    store
        .record_put(EVENTS, b"t-1/0001", &body(b"first"))
        .expect("put");
    store
        .record_put(EVENTS, b"t-2/0001", &body(b"other task"))
        .expect("put");

    let walked = store.record_scan(EVENTS, b"t-1/", 16).expect("the scan");
    assert_eq!(
        walked,
        vec![
            (b"t-1/0001".to_vec(), body(b"first")),
            (b"t-1/0002".to_vec(), body(b"second")),
        ],
        "one parent's chain, oldest key first, and nothing of the other parent's"
    );
}

/// An empty prefix walks the whole schema, and still only that schema.
#[test]
fn an_empty_prefix_walks_the_whole_schema() {
    let store = MemoryStore::new();
    store.record_put(EVENTS, b"a", &body(b"1")).expect("put");
    store.record_put(EVENTS, b"b", &body(b"2")).expect("put");
    store.record_put(TASKS, b"c", &body(b"3")).expect("put");
    assert_eq!(
        store.record_scan(EVENTS, b"", 16).expect("the scan").len(),
        2
    );
}

/// THE LIMIT BINDS, and it takes the first rows in key order rather than an arbitrary subset.
#[test]
fn the_limit_binds_and_takes_the_first_rows_in_key_order() {
    let store = MemoryStore::new();
    for (key, value) in [
        (&b"t-1/0001"[..], &b"first"[..]),
        (&b"t-1/0002"[..], &b"second"[..]),
        (&b"t-1/0003"[..], &b"third"[..]),
    ] {
        store.record_put(EVENTS, key, &body(value)).expect("put");
    }
    let walked = store.record_scan(EVENTS, b"t-1/", 2).expect("the scan");
    assert_eq!(
        walked,
        vec![
            (b"t-1/0001".to_vec(), body(b"first")),
            (b"t-1/0002".to_vec(), body(b"second")),
        ]
    );
}

/// A LIMIT OF ZERO MEANS NOTHING, not everything.
#[test]
fn a_limit_of_zero_answers_nothing() {
    let store = MemoryStore::new();
    store
        .record_put(EVENTS, b"t-1/0001", &body(b"first"))
        .expect("put");
    assert!(store
        .record_scan(EVENTS, b"t-1/", 0)
        .expect("the scan")
        .is_empty());
}

/// A scan of a schema nothing was ever written under is EMPTY, not an error.
#[test]
fn a_scan_of_an_untouched_schema_is_empty() {
    let store = MemoryStore::new();
    store.record_put(TASKS, b"t-1", &body(b"row")).expect("put");
    assert!(store
        .record_scan(EVENTS, b"", 16)
        .expect("the scan")
        .is_empty());
}

/// The record verbs and the PUBLISHED kind-tagged verbs are two protocols over two maps, and neither can see the other's rows.
#[test]
fn the_record_verbs_do_not_disturb_the_published_kind_tagged_rows() {
    use busbar_contract::records::{
        PlaneDisposition, PlaneRecord, PlaneSelector, RecordStore as Store,
    };

    let store = MemoryStore::new();
    store
        .upsert_plane_record(
            PlaneRecord {
                kind: "task".to_string(),
                id: "t-1".to_string(),
                parent: None,
                seq: 0,
                ts: 1,
                disposition: PlaneDisposition::Active,
                body: b"published".to_vec(),
            }
            .view(),
        )
        .expect("the published upsert");
    store
        .record_put(TASKS, b"t-1", &body(b"record leg"))
        .expect("the record put");

    assert_eq!(
        store
            .list_plane_records("task", &PlaneSelector::All)
            .expect("the published list"),
        vec![b"published".to_vec()],
        "the published path answers exactly what the published path wrote"
    );
    assert_eq!(
        store.record_get(TASKS, b"t-1").expect("the record get"),
        Some(body(b"record leg"))
    );
    assert_eq!(
        store
            .get_plane_record("task", "t-1")
            .expect("the published get"),
        Some(b"published".to_vec())
    );
}
