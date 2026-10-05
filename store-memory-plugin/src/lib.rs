// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The **in-memory store as a droppable busbar plugin**: the `cdylib` a signed tarball of the store
//! carries (`kind: store`, key `memory`). The logic crate is re-exported whole, and its door
//! (`busbar_store_memory::door`) is this image's ONE symbol, `busbar_plugin_door`, under this
//! crate's `dropped-in` feature, which switches on the logic crate's own `export_door!`
//! (`busbar_store_memory::door_export`). The door is exported in ONE place, so the library carries
//! exactly the door a busbar build links, and no build defines the symbol twice.
//!
//! `cold-dropped-in` (M6: the legacy cold door) is forwarded the same way: the logic crate's
//! `exports` module and its `BUSBAR_COLD_ENTRY`, reachable here as
//! `busbar_store_memory_plugin::BUSBAR_COLD_ENTRY`.
//!
//! No `unsafe` code lives in this crate: the export macros' `#[unsafe(no_mangle)]` items are the
//! logic crate's, in the two modules it allows them in.

#![forbid(unsafe_code)]

pub use busbar_store_memory::*;
