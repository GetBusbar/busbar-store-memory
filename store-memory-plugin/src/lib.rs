// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The **in-memory store as a droppable busbar plugin**: the `cdylib` a signed tarball of the store
//! carries (`kind: store`, key `memory`). The logic crate is re-exported whole, and its door
//! (`busbar_store_memory::door`) is exported as this image's ONE symbol, `busbar_plugin_door`
//! (`export_door!`), so the library carries exactly the door a busbar build links.
//!
//! This crate is `deny`, not `forbid`: the export macro's `#[unsafe(no_mangle)]` is the one
//! reviewed exemption (a `forbid` cannot be lifted for it). No other `unsafe` exists here.

#![deny(unsafe_code)]

pub use busbar_store_memory::*;

/// The exported door, behind `dropped-in` (the cdylib build only): the macro's `#[no_mangle]` symbol is
/// the one exemption.
#[cfg(feature = "dropped-in")]
#[allow(unsafe_code)]
mod exported {
    busbar_contract::export_door!(busbar_store_memory::door);
}

// M6: the legacy cold export below goes with the cold ABI (TODO M6 COLD-DELETE).
/// THE LEGACY COLD DOOR (feature `cold-dropped-in`): [`open`] exported through the contract's cold
/// store export macro. Unsafe code is allowed here because the C-ABI boundary functions the macro
/// generates are `unsafe extern "C-unwind"` by the cold ABI's own definition.
#[cfg(feature = "cold-dropped-in")]
#[allow(unsafe_code)]
pub mod cold_exports {
    busbar_contract::abi::sdk::export_store_plugin!(busbar_store_memory::open);
}

/// The legacy cold door's boundary as a LINKED entry (busbar's plugin-loader's cold-lane proof).
#[cfg(feature = "cold-dropped-in")]
pub use cold_exports::BUSBAR_COLD_ENTRY;
