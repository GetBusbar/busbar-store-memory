// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The **in-memory store as a droppable busbar plugin**: the `cdylib` a signed tarball of the store
//! carries (`kind: store`, key `memory`). The logic crate is re-exported whole, and its door
//! (`busbar_store_memory::door`) is this image's ONE symbol, `busbar_plugin_door`, exported here by
//! the contract's `export_door!`, unconditionally. The library therefore carries exactly the door a
//! busbar build links, and the logic crate (which a busbar build links, with no door symbol) stays
//! `forbid(unsafe_code)`.
//!
//! This crate is `deny`, not `forbid`: the export macro's `#[unsafe(no_mangle)]` item is the one
//! reviewed exemption (a `forbid` cannot be lifted for it), allowed in the one module below. No
//! other `unsafe` exists here.

#![deny(unsafe_code)]

pub use busbar_store_memory::*;

/// The exported door: the macro's `#[no_mangle]` symbol is an exemption.
#[allow(unsafe_code)]
mod exported {
    busbar_contract::export_door!(busbar_store_memory::door);
}
