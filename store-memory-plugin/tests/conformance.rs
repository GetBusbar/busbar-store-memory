// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! **THE PUBLISHED CONFORMANCE SUITE, RUN BY THIS PLUGIN** (busbar TODO ABI-b4; OWNER 2026-10-03:
//! plugins test themselves against busbar). busbar's suite, at the commit this repo pins
//! (`.busbar-ref`), drives the `memory` store two ways through the one loader: LINKED (the logic
//! crate's `door`) and DROPPED IN (this crate's built cdylib, its `dropped-in` door on), over the
//! store kind's script with the inputs in `conformance.json`; every step's crossings exactly at the
//! script's pin, the two folds equal, and the suite's RED arms kept. `plugin-ci.yml` runs it under
//! `--release`, then again with `BUSBAR_CONFORMANCE_RED=count`, which must fail on the comparator.

busbar_plugin_loader::conformance_suite! {
    door: busbar_store_memory::door,
    cdylib: "busbar_store_memory_plugin",
    inputs: include_str!("conformance.json"),
}
