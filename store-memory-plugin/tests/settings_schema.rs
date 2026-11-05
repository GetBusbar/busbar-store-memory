// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

#[test]
fn settings_schema_is_carried_by_statement() {
    let bytes = busbar_plugin_loader::dispatch::rendering_of(busbar_store_memory::door)
        .expect("actual linked door renders");
    let statement = busbar_contract::abi::mechanism::rendering::read(&bytes)
        .expect("Statement rendering reads");
    assert_eq!(
        statement.settings_schema.fmt,
        busbar_contract::abi::mechanism::call::BLOB_JSON
    );
    assert_eq!(
        statement.settings_schema.bytes,
        include_bytes!("../../store-memory/src/settings.schema.json")
    );
}

mod without_schema {
    busbar_contract::store_door!(
        busbar_store_memory::MemoryStore,
        "schema-default-fixture",
        "1.6.0",
        1
    );
}

#[test]
fn settings_schema_omitted_keeps_existing_macro_callers_absent() {
    let bytes = busbar_plugin_loader::dispatch::rendering_of(without_schema::door).unwrap();
    let statement = busbar_contract::abi::mechanism::rendering::read(&bytes).unwrap();
    assert_eq!(
        statement.settings_schema.fmt,
        busbar_contract::abi::mechanism::call::BLOB_ABSENT
    );
    assert!(statement.settings_schema.bytes.is_empty());
}
