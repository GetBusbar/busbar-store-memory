// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

mod support;

use busbar_contract::abi::sdk::migrate::Migrated;
use busbar_plugin_loader::dispatch::{load::raw, migrate};
use serde_json::{json, Value};

fn convert(dropped: bool, settings: &Value) -> Migrated {
    let answer = if dropped {
        let path = support::artifact();
        migrate::migrate_library(
            &std::fs::read(&path).expect("fresh actual plugin image"),
            &path.display().to_string(),
            settings,
            "1.5.5",
        )
    } else {
        migrate::migrate_door(raw(busbar_store_memory::door), settings, "1.5.5")
    };
    let answer = answer
        .expect("actual door migrates")
        .expect("migration is declared");
    let seal = busbar_contract::caps::KernelSeal::acquire_for_kernel();
    Migrated {
        settings: serde_json::to_value(answer.authoring(&seal)).unwrap(),
        report: answer.report,
        needs: answer.needs,
    }
}

#[test]
fn actual_linked_and_dropped_doors_remove_ignored_token_before_open() {
    for dropped in [false, true] {
        let answer = convert(
            dropped,
            &json!({"token": {"env": "UNAVAILABLE_MEMORY_TOKEN_SENTINEL"}}),
        );
        assert_eq!(answer.settings, json!({}));
        assert_eq!(
            answer.report,
            ["removed settings.token: the memory store never used this setting"]
        );
        assert!(answer.needs.is_empty());
        assert_eq!(
            convert(dropped, &answer.settings),
            Migrated::unchanged(json!({}))
        );
        // Both actual lifecycle doors accept the migrated empty settings.
        let _opened = support::block(support::open(dropped));
    }
}

#[test]
fn both_config_names_register_the_migrating_store_door() {
    let (alias, ephemeral, door, canonical) = busbar_store_memory::linked::STORE;
    assert_eq!((alias, canonical), ("memory", "busbar-store-memory"));
    assert!(ephemeral);
    let row = busbar_plugin_loader::dispatch::LinkedRow::of_macro(door)
        .expect("registered Store Statement");
    let actual = busbar_plugin_loader::dispatch::LinkedRow::of(busbar_store_memory::door)
        .expect("migrating Store Statement");
    assert_eq!(row.statement, actual.statement);
    let answer = convert(false, &json!({"token": null}));
    assert_eq!(answer.settings, json!({}));
    assert_eq!(answer.report.len(), 1);
}
