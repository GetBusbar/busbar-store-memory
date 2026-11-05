// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

use busbar_contract::abi::sdk::store::StoreSlots;

#[test]
fn settings_schema_matches_lifecycle_shapes() {
    let schema: serde_json::Value =
        serde_json::from_slice(include_bytes!("settings.schema.json")).expect("schema JSON");
    let validator = jsonschema::validator_for(&schema).expect("valid settings schema");
    let cases: &[(&str, bool)] = &[
        (r#"{}"#, true),
        (r#"null"#, true),
        (r#"{"token":null}"#, true),
        (r#"{"token":{"env":"EXAMPLE_TOKEN"}}"#, true),
        (r#"{"token":[]}"#, true),
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
fn settings_schema_marks_secret_fields_without_defaults() {
    let schema: serde_json::Value =
        serde_json::from_slice(include_bytes!("settings.schema.json")).unwrap();
    let field = &schema["properties"]["token"];
    assert_eq!(field["writeOnly"], true);
    assert_eq!(field["x-busbar-secret"], true);
    assert!(field.get("default").is_none());
}
