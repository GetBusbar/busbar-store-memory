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
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn ignored_token_is_removed_without_resolving_or_disclosing_its_value() {
        for token in [
            json!("private-token-sentinel"),
            json!({"env": "UNAVAILABLE_MEMORY_TOKEN_SENTINEL"}),
            json!(null),
            json!(["arbitrary-old-value"]),
        ] {
            let answer = MemoryMigration::migrate(json!({"token": token}), "1.5.5");
            assert_eq!(answer.settings, json!({}));
            assert_eq!(
                answer.report,
                ["removed settings.token: the memory store never used this setting"]
            );
            assert!(answer.needs.is_empty());
            let repeated = MemoryMigration::migrate(answer.settings.clone(), "1.5.5");
            assert_eq!(repeated, Migrated::unchanged(answer.settings));
        }
    }

    #[test]
    fn migration_preserves_every_other_setting_and_does_not_echo_it() {
        let answer = MemoryMigration::migrate(
            json!({"token": "discarded-secret", "other": {"token": "retained-secret"}}),
            "1.5.5",
        );
        assert_eq!(
            answer.settings,
            json!({"other": {"token": "retained-secret"}})
        );
        assert_eq!(answer.report.len(), 1);
        assert!(!answer.report[0].contains("secret"));
        assert!(answer.needs.is_empty());
    }

    #[test]
    fn settings_without_token_are_unchanged() {
        for settings in [json!({}), json!(null), json!({"other": 1}), json!([])] {
            assert_eq!(
                MemoryMigration::migrate(settings.clone(), "1.6.0"),
                Migrated::unchanged(settings)
            );
        }
    }
}
