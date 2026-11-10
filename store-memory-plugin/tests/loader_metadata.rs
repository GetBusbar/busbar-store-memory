// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! Memory product identity and native facts through the ordinary loader.
mod support;
use busbar_contract::persistence::{Durability, PersistenceBackend};

#[test]
fn actual_memory_open_reports_ephemeral_native_metadata() {
    let statement = busbar_plugin_loader::dispatch::rendering_of(busbar_store_memory::door)
        .expect("Memory Statement renders");
    let stated = busbar_contract::abi::mechanism::rendering::read(&statement)
        .expect("Memory Statement reads");
    assert_eq!(stated.name, "busbar-store-memory");
    support::block(async {
        for dropped in [false, true] {
            let opened = support::open(dropped).await;
            let info = opened.info();
            assert_eq!(info.durability, Durability::Ephemeral);
            let observed = opened.clock(info.dataset).await.expect("native clock");
            assert_eq!(observed.authority, info.clock_authority);
        }
    });
}
