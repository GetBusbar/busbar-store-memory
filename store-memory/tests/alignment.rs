// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! Sibling-alignment checks: the memory store's settings contract (it takes none).

use busbar_store_memory::MemoryStore;

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
        "invalid memory plugin config: unknown setting `x` (accepted: token)"
    );
    assert!(
        <MemoryStore as busbar_contract::abi::sdk::store::StoreSlots>::open(b"{\"x\": 1}", None)
            .is_err()
    );
    let e = <MemoryStore as busbar_contract::abi::sdk::store::StoreSlots>::validate(b"[1]")
        .unwrap_err();
    assert!(e.starts_with("invalid memory plugin config: "), "{e}");
}

/// A 1.5.5 memory config carried `token` (the door declares it in `secret_refs`): accepted, any
/// JSON value, and ignored. Every other key stays refused.
#[test]
fn settings_accept_and_ignore_a_token() {
    for ok in [
        r#"{"token":"x"}"#,
        r#"{"token":{"env":"X"}}"#,
        r#"{"token":null}"#,
    ] {
        assert_eq!(
            <MemoryStore as busbar_contract::abi::sdk::store::StoreSlots>::validate(ok.as_bytes()),
            Ok(()),
            "{ok}"
        );
        assert!(
            <MemoryStore as busbar_contract::abi::sdk::store::StoreSlots>::open(
                ok.as_bytes(),
                None
            )
            .is_ok(),
            "{ok}"
        );
    }
    let e = <MemoryStore as busbar_contract::abi::sdk::store::StoreSlots>::validate(
        br#"{"token":"x","x":1}"#,
    )
    .unwrap_err();
    assert!(e.contains("unknown setting `x`"), "{e}");
}

/// The non-object refusal names the JSON type, never the value (it may be a mis-nested secret).
#[test]
fn a_non_object_refusal_does_not_echo_the_value() {
    let e = <MemoryStore as busbar_contract::abi::sdk::store::StoreSlots>::validate(b"\"s3cr3t\"")
        .unwrap_err();
    assert!(e.starts_with("invalid memory plugin config: "), "{e}");
    assert!(!e.contains("s3cr3t"), "{e}");
}
