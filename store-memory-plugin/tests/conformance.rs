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
