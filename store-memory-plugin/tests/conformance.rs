// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! THE BOTH-WAYS CONFORMANCE of `busbar-store-memory`: busbar's PUBLISHED suite
//! (`busbar_test_kit::conformance_suite!`, at the busbar commit this repo pins in
//! `.busbar-ref`), run by this repo. It loads the LINKED door (the logic crate's `door`, the one a
//! busbar build names) and the BUILT cdylib (`busbar-store-memory-plugin`)
//! through busbar's real plugin loader, drives both with the store kind's script over
//! `conformance.json`, and requires every step's crossings exactly at the script's pin and the two
//! folds equal. The suite's RED arms are kept beside it. `plugin-ci.yml` runs it under `--release`.

busbar_test_kit::conformance_suite! {
    door: busbar_store_memory::door,
    cdylib: "busbar_store_memory_plugin",
    inputs: include_str!("conformance.json"),
}

/// busbar 1.5.5 resolved a secret reference (`{env:}`, `{file:}`, `{module:}`) at ANY top-level
/// store setting at boot, and when one did not resolve it WARNED and booted the memory store, which
/// reads no settings (busbar oracle BOOT-W15). busbar now resolves a reference only at a position
/// the plugin's Statement declares secret (`secret_refs`) and refuses one anywhere else (busbar
/// #1699, ORACLE-SIGNOFF G14), so the store declares every top-level setting: `*`, any key.
#[test]
fn the_statement_declares_every_top_level_setting_may_hold_a_secret_reference() {
    let stated = busbar_contract::abi::mechanism::rendering::read(
        &busbar_plugin_loader::dispatch::rendering_of(busbar_store_memory::door)
            .expect("the door renders"),
    )
    .expect("the rendering reads back");
    assert_eq!(stated.secret_refs, ["*".to_string()]);
}
