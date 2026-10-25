// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! Sibling-alignment checks: the memory store's settings contract (it takes none) and
//! `purge_metering_before` really purging the billing ledger, like sqlite/postgres/mysql.

use busbar_contract::records::{MeteringDelta, RecordStore as Store};
use busbar_store_memory::MemoryStore;

fn delta(key: &str, bucket: u64, priced_from_ms: u64) -> MeteringDelta {
    MeteringDelta {
        key_id: key.into(),
        bucket,
        model: "m".into(),
        provider: "p".into(),
        tokens_input: 1,
        tokens_output: 1,
        tokens_cache_read: 0,
        tokens_cache_write: 0,
        requests: 1,
        billable_requests: 1,
        key_group_at_use: "g".into(),
        pricing_version: "v".into(),
        priced_from_ms,
        usage_units: Default::default(),
    }
}

#[test]
fn purge_metering_before_removes_only_the_named_bucket() {
    let s = MemoryStore::new();
    s.add_metering(&delta("k1", 100, 0)).unwrap();
    s.add_metering(&delta("k2", 100, 5)).unwrap();
    s.add_metering(&delta("k1", 200, 0)).unwrap();
    assert_eq!(s.purge_metering_before("100").unwrap(), 2);
    assert!(s.list_metering(100).unwrap().is_empty());
    assert_eq!(s.list_metering(200).unwrap().len(), 1);
    // Purging again, or a bucket that holds nothing or is not an integer, removes nothing.
    assert_eq!(s.purge_metering_before("100").unwrap(), 0);
    assert_eq!(s.purge_metering_before("not-a-bucket").unwrap(), 0);
    assert_eq!(s.list_metering(200).unwrap().len(), 1);
}

#[test]
fn settings_accept_empty_whitespace_null_and_empty_object() {
    for ok in ["", "  \n", "{}", " { } ", "null"] {
        assert_eq!(
            <MemoryStore as busbar_contract::abi::sdk::store::StoreSlots>::validate(ok.as_bytes()),
            Ok(()),
            "{ok:?}"
        );
        assert!(
            <MemoryStore as busbar_contract::abi::sdk::store::StoreSlots>::open(
                ok.as_bytes(),
                None
            )
            .is_ok(),
            "{ok:?}"
        );
        assert!(busbar_store_memory::open(ok).is_ok(), "{ok:?}");
    }
}

#[test]
fn settings_refuse_malformed_json_and_any_key() {
    let e = <MemoryStore as busbar_contract::abi::sdk::store::StoreSlots>::validate(b"{not json")
        .unwrap_err();
    assert!(e.starts_with("invalid memory plugin config: "), "{e}");
    let e = <MemoryStore as busbar_contract::abi::sdk::store::StoreSlots>::validate(b"{\"x\": 1}")
        .unwrap_err();
    assert_eq!(
        e,
        "invalid memory plugin config: unknown setting `x` (the memory store takes no settings)"
    );
    assert!(
        <MemoryStore as busbar_contract::abi::sdk::store::StoreSlots>::open(b"{\"x\": 1}", None)
            .is_err()
    );
    assert!(busbar_store_memory::open("{\"x\": 1}").is_err());
    assert!(busbar_store_memory::open("{oops").is_err());
    let e = <MemoryStore as busbar_contract::abi::sdk::store::StoreSlots>::validate(b"[1]")
        .unwrap_err();
    assert!(e.starts_with("invalid memory plugin config: "), "{e}");
}
