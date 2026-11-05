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
            <MemoryStore as busbar_contract::abi::sdk::store::StoreSlots>::open_with_secrets(
                ok.as_bytes(),
                &[],
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
        "invalid memory plugin config: unknown setting `x` (accepted: none)"
    );
    assert!(
        <MemoryStore as busbar_contract::abi::sdk::store::StoreSlots>::open_with_secrets(
            b"{\"x\": 1}",
            &[],
            None
        )
        .is_err()
    );
    let e = <MemoryStore as busbar_contract::abi::sdk::store::StoreSlots>::validate(b"[1]")
        .unwrap_err();
    assert!(e.starts_with("invalid memory plugin config: "), "{e}");
}

/// Migration removes the formerly ignored token field; the runtime accepts no settings.
#[test]
fn settings_refuse_the_retired_token() {
    for rejected in [
        r#"{"token":"x"}"#,
        r#"{"token":{"env":"X"}}"#,
        r#"{"token":null}"#,
    ] {
        assert!(
            <MemoryStore as busbar_contract::abi::sdk::store::StoreSlots>::validate(
                rejected.as_bytes()
            )
            .is_err(),
            "{rejected}"
        );
        assert!(
            <MemoryStore as busbar_contract::abi::sdk::store::StoreSlots>::open_with_secrets(
                rejected.as_bytes(),
                &[],
                None
            )
            .is_err(),
            "{rejected}"
        );
    }
}

/// The non-object refusal names the JSON type, never the value (it may be a mis-nested secret).
#[test]
fn a_non_object_refusal_does_not_echo_the_value() {
    for (input, kind) in [
        (r#""s3cr3t""#, "a string"),
        ("[1]", "an array"),
        ("true", "a boolean"),
        ("42", "a number"),
        ("1", "a number"),
    ] {
        let e = <MemoryStore as busbar_contract::abi::sdk::store::StoreSlots>::validate(
            input.as_bytes(),
        )
        .unwrap_err();
        assert_eq!(
            e,
            format!("invalid memory plugin config: expected a JSON object, got {kind}")
        );
        assert!(!e.contains("s3cr3t"));
    }
}
