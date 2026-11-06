// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

use busbar_contract::abi::sdk::store::StoreSlots;

#[test]
fn settings_schema_matches_lifecycle_shapes() {
    let schema: serde_json::Value =
        serde_json::from_slice(include_bytes!("../settings.schema.json")).expect("schema JSON");
    let validator = jsonschema::validator_for(&schema).expect("valid settings schema");
    let cases: &[(&str, bool)] = &[
        (r#"{}"#, true),
        (r#"null"#, true),
        (r#"{"token":null}"#, false),
        (r#"{"token":{"env":"EXAMPLE_TOKEN"}}"#, false),
        (r#"{"token":[]}"#, false),
        (r#"{"unknown":0}"#, false),
        (r#"[]"#, false),
        (r#""""#, false),
    ];
    for &(text, accepted) in cases {
        let value: serde_json::Value = serde_json::from_str(text).expect("case JSON");
        assert_eq!(validator.is_valid(&value), accepted, "schema: {text}");
        assert_eq!(
            <crate::MemoryStore as StoreSlots>::validate(text.as_bytes()).is_ok(),
            accepted,
            "lifecycle: {text}"
        );
    }
    assert!(<crate::MemoryStore as StoreSlots>::validate(b"").is_ok());
}

#[test]
fn settings_schema_declares_no_settings() {
    let schema: serde_json::Value =
        serde_json::from_slice(include_bytes!("../settings.schema.json")).unwrap();
    assert!(schema["properties"].as_object().unwrap().is_empty());
    assert_eq!(schema["additionalProperties"], false);
}

/// Blank settings are "none"; malformed JSON and every non-object JSON value are refused, both by
/// `validate` and by `open_with_secrets`.
#[test]
fn lifecycle_accepts_blank_settings_and_refuses_malformed_or_non_object_values() {
    for blank in [&b""[..], b"   ", b"\n\t "] {
        assert!(<crate::MemoryStore as StoreSlots>::validate(blank).is_ok());
        assert!(<crate::MemoryStore as StoreSlots>::open_with_secrets(blank, &[], None).is_ok());
    }
    for (text, reason) in [
        (&b"{"[..], "invalid memory plugin config"),
        (b"true", "got a boolean"),
        (b"7", "got a number"),
        (b"\"x\"", "got a string"),
        (b"[1]", "got an array"),
        (b"{\"x\":1}", "unknown setting `x` (accepted: none)"),
    ] {
        let validated = <crate::MemoryStore as StoreSlots>::validate(text).unwrap_err();
        assert!(validated.contains(reason), "{validated}");
        let opened = <crate::MemoryStore as StoreSlots>::open_with_secrets(text, &[], None)
            .err()
            .expect("open refuses what validate refuses");
        assert!(opened.contains(reason), "{opened}");
    }
}
