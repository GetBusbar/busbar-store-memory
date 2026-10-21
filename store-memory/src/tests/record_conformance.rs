// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The record half of the store protocol, held to what `busbar_contract::abi::sdk::store::StoreSlots` declares.
//!
//! `record_put`, `record_get` and `record_scan` are the three verbs a plane's kernel-held durable
//! records are reached through, and until they were written here nothing in the workspace
//! implemented them — the contract declared three signatures and every caller went round them. So
//! these are the first cells that say what they MEAN, and each one pins a behaviour a second
//! backend would otherwise be free to disagree with:
//!
//! - a key is opaque bytes, not a string, so a key with a NUL or an invalid UTF-8 byte in it round
//!   trips;
//! - schemas are separate namespaces — the same key under two schemas is two records;
//! - a put REPLACES, so a schema's row has one current value and never a history;
//! - a scan is a PREFIX walk in key order, bounded by its own limit, and `limit` 0 means nothing;
//! - an absent record reads back as `None` and never as an error.
//!
//! Its own file rather than an inline module, per the repo's test-locality rule.

use crate::MemoryStore;
use busbar_contract::ids::RecordSchemaId;
use busbar_contract::kinds::RecordBytes;
use busbar_contract::kinds::StoreError as ContractStoreError;

/// The typed record verbs the contract spells (`RecordSchemaId`), over the schema-by-name ones the
/// store v3 table uses; only these tests call them.
impl MemoryStore {
    /// Write one of a plane's kernel-held durable records.
    ///
    /// The verb is `busbar_contract::abi::sdk::store::StoreSlots::record_put`'s, and the three
    /// below are its siblings. They are INHERENT rather than a trait implementation for one reason,
    /// and it is a rule rather than a preference: the manifest allow-list refuses a store-kind crate
    /// that names `busbar-kernel`, so the kernel's own record sink is not a trait this crate may
    /// implement; and the contract's `StoreSlots` is the whole store table, of which this
    /// backend answers the published half through [`Store`] above. What is here is the record half,
    /// at the contract's own spelling, so the adapter that binds a loaded store to the kernel's sink
    /// has one shape to forward to rather than two.
    ///
    /// The value arrives as a [`RecordBytes`], which is where the record ceiling is enforced: a body
    /// over it cannot be constructed, so this method cannot be handed one.
    ///
    /// # Errors
    ///
    /// Never, for a RAM backend: there is nothing under it to be unavailable. The result is the
    /// contract's shape so a durable backend can answer in the same place.
    pub fn record_put(
        &self,
        schema: RecordSchemaId,
        key: &[u8],
        value: &RecordBytes,
    ) -> Result<(), ContractStoreError> {
        self.record_put_at(schema.as_str(), key, value);
        Ok(())
    }

    /// Read one of a plane's kernel-held durable records.
    ///
    /// # Errors
    ///
    /// Never, for a RAM backend. An absent record is `Ok(None)` and not an error: a plane asking
    /// for a row it has not written yet is an ordinary answer, not a fault.
    pub fn record_get(
        &self,
        schema: RecordSchemaId,
        key: &[u8],
    ) -> Result<Option<RecordBytes>, ContractStoreError> {
        Ok(self.record_get_at(schema.as_str(), key))
    }

    /// Walk a plane's records under a prefix, in key order, at most `limit` of them.
    ///
    /// `limit` 0 means NOTHING, not everything. A caller that wants the whole prefix names a number;
    /// reading zero as unbounded would make a miscomputed bound the one case that returns the entire
    /// schema.
    ///
    /// # Errors
    ///
    /// Never, for a RAM backend.
    pub fn record_scan(
        &self,
        schema: RecordSchemaId,
        prefix: &[u8],
        limit: u32,
    ) -> Result<Vec<(Vec<u8>, RecordBytes)>, ContractStoreError> {
        Ok(self.record_scan_at(schema.as_str(), prefix, limit))
    }
}

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

/// An absent record is an ANSWER, not a fault. A plane asking for a row it has not written yet is
/// the ordinary shape of a first read.
#[test]
fn an_absent_record_reads_back_as_none() {
    let store = MemoryStore::new();
    assert_eq!(
        store.record_get(TASKS, b"never-written").expect("the get"),
        None
    );
}

/// A PUT REPLACES. The schema's row has one current value; a caller wanting a history appends to a
/// schema that keeps one, which is a different schema and a different key.
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

/// TWO SCHEMAS ARE TWO NAMESPACES. The same key under each is two records, and neither read sees
/// the other. A backend that keyed on the key alone would hand one plane's row to another's leg.
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

/// A KEY IS OPAQUE BYTES. A NUL byte and a byte that is not valid UTF-8 both round trip, because a
/// key that had to be a string is a key a caller has to encode around.
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
///
/// The reading matters more than it looks: a caller whose bound is computed and comes out zero has
/// asked for no rows, and a backend that read it as unbounded would answer that caller with the
/// entire schema — the one case where a miscomputed bound returns the most data instead of the
/// least.
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

/// The record verbs and the PUBLISHED kind-tagged verbs are two protocols over two maps, and
/// neither can see the other's rows.
///
/// This is the cell that protects the published surface's byte-identity (the kind-tagged verbs are
/// 1.6.0's published store surface): if a record leg's write showed up in `list_plane_records`,
/// every reader of a plane's rows would start seeing rows it never wrote, and a record leg would
/// have become a change to the published path rather than an addition beside it.
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
