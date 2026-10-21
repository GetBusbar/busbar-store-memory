// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! THE BOTH-WAYS CONFORMANCE of `busbar-store-memory`: busbar's PUBLISHED suite
//! (`busbar_plugin_loader::conformance_suite!`, at the busbar commit this repo pins in
//! `.busbar-ref`), run by this repo. It loads the LINKED door (the logic crate's `door`, the one a
//! busbar build names) and the BUILT cdylib (`busbar-store-memory-plugin`, its `dropped-in` door on)
//! through busbar's real plugin loader, drives both with the store kind's script over
//! `conformance.json`, and requires every step's crossings exactly at the script's pin and the two
//! folds equal. The suite's RED arms are kept beside it. `plugin-ci.yml` runs it under `--release`.

use busbar_plugin_loader::dispatch::LoadedDoor;

/// This crate's own door, as the loader names a compiled-in one.
#[allow(unused_unsafe)]
fn loaded() -> LoadedDoor {
    // SAFETY: `door` is this crate's own `plugin_door!` entry.
    unsafe { LoadedDoor::from_raw(busbar_store_memory::door) }
}

busbar_plugin_loader::conformance_suite! {
    loaded: loaded(),
    cdylib: "busbar_store_memory_plugin",
    inputs: include_str!("conformance.json"),
}
