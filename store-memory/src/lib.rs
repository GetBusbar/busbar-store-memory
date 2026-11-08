// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! Ephemeral neutral Store primitives. Every connection owns a fresh dataset;
//! domain records, arithmetic, claims, credentials and retention belong to core.

#![forbid(unsafe_code)]

mod migration;

/// The native primitive backend and its actual lifecycle door.
mod v4;

/// Actual primitive door used by linked registration and the plugin export.
pub use v4::{door, MACRO_DOOR};

/// The stable linked selection names and ephemeral durability declaration.
pub mod linked {
    /// Alias, durability, native door and canonical Statement name.
    pub const STORE: (&str, bool, busbar_contract::abi::sdk::door::MacroDoor, &str) =
        ("memory", true, super::MACRO_DOOR, "busbar-store-memory");
}

#[cfg(test)]
#[path = "tests/settings_schema_tests.rs"]
mod settings_schema_tests;
