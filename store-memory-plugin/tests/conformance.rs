// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! THE BOTH-WAYS CONFORMANCE of `busbar-store-memory`: busbar's PUBLISHED suite
//! (`busbar_plugin_loader::conformance_suite!`, at the busbar commit this repo pins in
//! `.busbar-ref`), run by this repo. It loads the LINKED door (the logic crate's `door`, the one a
//! busbar build names) and the BUILT cdylib (`busbar-store-memory-plugin`, its `dropped-in` door on)
//! through busbar's real plugin loader, drives both with the store kind's script over
//! `conformance.json`, and requires every step's crossings exactly at the script's pin and the two
//! folds equal. The suite's RED arms are kept beside it. `plugin-ci.yml` runs it under `--release`.

busbar_plugin_loader::conformance_suite! {
    door: busbar_store_memory::door,
    cdylib: "busbar_store_memory_plugin",
    inputs: include_str!("conformance.json"),
}

/// The memory store's Statement declares exactly its secret fields (`secret_refs`), never `*`: busbar
/// resolves a secret reference only at a position the Statement declares secret and refuses one
/// anywhere else (busbar #1699, ORACLE-SIGNOFF G14: each plugin declares its secret fields). The
/// store reads no settings; `token` is the one key a 1.5.5 store config held a reference at (busbar
/// oracle BOOT-W15: an unresolved `token` reference in the memory store's settings warns and boots).
#[test]
fn the_statement_declares_exactly_its_secret_fields() {
    let stated = busbar_contract::abi::mechanism::rendering::read(
        &busbar_plugin_loader::dispatch::rendering_of(busbar_store_memory::door)
            .expect("the door renders"),
    )
    .expect("the rendering reads back");
    assert_eq!(stated.secret_refs, ["token".to_string()]);
}
