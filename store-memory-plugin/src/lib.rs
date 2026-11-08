// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The in-memory store packaged as a droppable `cdylib` exporting `busbar_plugin_door`.

#![deny(unsafe_code)]

pub use busbar_store_memory::{door, linked, MACRO_DOOR};

/// The exported door: the macro's `#[unsafe(no_mangle)]` symbol is an exemption.
#[allow(unsafe_code)]
mod exported {
    busbar_contract::export_door!(busbar_store_memory::door);
}
