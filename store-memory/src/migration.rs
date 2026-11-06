// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

use busbar_contract::abi::sdk::migrate::{Migrate, Migrated};

/// One-shot configuration conversion, called before any Store instance or secret resolution.
pub(crate) struct MemoryMigration;

impl Migrate for MemoryMigration {
    fn migrate(mut settings: serde_json::Value, _from: &str) -> Migrated {
        let removed = settings
            .as_object_mut()
            .is_some_and(|object| object.remove("token").is_some());
        let mut answer = Migrated::unchanged(settings);
        if removed {
            answer
                .report
                .push("removed settings.token: the memory store never used this setting".into());
        }
        answer
    }
}

#[cfg(test)]
#[path = "tests/migration_tests.rs"]
mod tests;
