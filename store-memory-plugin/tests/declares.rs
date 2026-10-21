// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The door's Statement states the whole `declares` document under `EXT_DECLARES` (BUSBAR-1.6.0.md
//! l.246), read back the way the host reads it.

use busbar_plugin_loader::dispatch::{LinkedRow, LoadedDoor};

/// This crate's own door, as the loader names a compiled-in one.
#[allow(unused_unsafe)]
fn loaded() -> LoadedDoor {
    // SAFETY: `door` is this crate's own `store_door!` entry.
    unsafe { LoadedDoor::from_raw(busbar_store_memory::door) }
}

#[test]
fn the_statement_carries_the_declares_document() {
    let row = LinkedRow::of_loaded(loaded()).unwrap();
    let read = busbar_contract::abi::mechanism::rendering::read(&row.statement).unwrap();
    assert_eq!(
        busbar_contract::abi::mechanism::extensions::declares(&read.extensions.bytes),
        Some(&include_bytes!("../../store-memory/declares.json")[..]),
    );
}
