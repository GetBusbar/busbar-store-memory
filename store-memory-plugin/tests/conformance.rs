// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! **ONE MEMORY STORE, BOTH DOORS, ONE TABLE**: the linked door (`busbar_store_memory::door`) and
//! this crate's built cdylib (the same door behind the one `export_door!`), each admitted through
//! the loader's ONE door validation, opened as the host opens a store (`LoadedStore`) and driven
//! through one script over the ONE dispatcher, give the same transcript. Run against the busbar rev
//! this repo pins (`.busbar-ref`).
//!
//! THE RED ARMS, same file: (a) the door asked for as another kind is refused, linked and dropped
//! in; (b) a store that holds a foreign row compares UNEQUAL, so the equality is not vacuous. A
//! missing cdylib PANICS: this test IS the dropped-in door's proof, and never skips.

use std::sync::Arc;

use busbar_contract::abi::sdk::store::{Cell, CellKey, Dimension};
use busbar_contract::abi::store::OpId;
use busbar_contract::kinds::RecordBytes;
use busbar_contract::records::{RecordStore, VirtualKey};
use busbar_contract::store_calls::StoreCalls;
use busbar_plugin_loader::dispatch::kinds::hook::Hook;
use busbar_plugin_loader::dispatch::kinds::store::Store;
use busbar_plugin_loader::dispatch::{
    load_dropped, load_linked, rendering_of, rendering_of_library, Bind, DispatchConfig,
    Dispatcher, LinkedRow, LoadError, NoSink,
};
use busbar_plugin_loader::store_v3::LoadedStore;
use busbar_store_memory::door;

#[derive(Clone, Copy)]
enum Door {
    Linked,
    Dropped,
}

/// This crate's built cdylib (uplifted or under `deps`, newest wins). A missing artifact is a
/// failure, never a skip.
fn cdylib() -> std::path::PathBuf {
    let exe = std::env::current_exe().expect("the test binary has a path");
    let profile = exe
        .parent()
        .and_then(|d| d.parent())
        .expect("target/<profile>");
    let file = busbar_plugin_loader::plugin_library_filename("busbar_store_memory_plugin");
    [profile.join(&file), profile.join("deps").join(&file)]
        .into_iter()
        .filter_map(|p| Some((std::fs::metadata(&p).ok()?.modified().ok()?, p)))
        .max()
        .map(|(_, p)| p)
        .unwrap_or_else(|| panic!("the busbar-store-memory-plugin cdylib ({file}) is not built"))
}

fn bind(d: &Dispatcher) -> Bind {
    Bind {
        instance: Arc::from("the-instance"),
        max_inflight_cap: 1024,
        sink: Arc::new(NoSink),
        dispatcher: d.adopter(),
        conns: None,
    }
}

fn open(by: Door) -> LoadedStore {
    let d = Arc::new(Dispatcher::new(DispatchConfig::default()));
    let plugin = match by {
        Door::Linked => load_linked::<Store>(&LinkedRow::of(door).expect("states"), bind(&d)),
        Door::Dropped => {
            load_dropped::<Store>(&cdylib(), &rendering_of(door).expect("renders"), bind(&d))
        }
    }
    .expect("the door loads");
    LoadedStore::open(plugin, d, b"{}", mint).expect("the store opens")
}

/// This test's `op_id` allocator: one counter, as the kernel's `door::op_id` is.
fn mint() -> OpId {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    OpId::from_parts(
        0x5703,
        N.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1,
    )
}

fn block<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(f)
}

fn key(id: &str) -> VirtualKey {
    VirtualKey {
        id: id.into(),
        generation_hash: format!("binding:{id}:1"),
        name: id.into(),
        enabled: true,
        created_at: 100,
        ..Default::default()
    }
}

const SLOT: CellKey<'static> = CellKey {
    bucket: "k",
    pool: None,
    dimension: Dimension::Requests,
    window_start: 60,
};

/// One scripted exchange, one line per answer. `foreign` seeds a key the other arm lacks.
fn script(s: &LoadedStore, foreign: Option<&str>) -> Vec<String> {
    let r = |v: &[u8]| RecordBytes::new(v.to_vec()).expect("a record");
    let show =
        |a: Option<RecordBytes>| a.map(|r| String::from_utf8_lossy(r.as_slice()).into_owned());
    if let Some(id) = foreign {
        s.put_key(&key(id)).expect("seed");
    }
    let mut t = vec![
        format!("facts = {:?}", s.facts()),
        format!("name = {}", s.name()),
        format!("put key = {:?}", s.put_key(&key("key-1"))),
        format!("get key = {:?}", s.get_key("key-1")),
        format!("get no key = {:?}", s.get_key("key-0")),
        format!("list keys = {}", s.list_keys().expect("list").len()),
    ];
    let cell = [Cell {
        key: SLOT,
        amount: 4,
    }];
    block(async {
        t.push(format!(
            "put a = {:?}",
            s.record_put("c", b"a", &r(b"one")).await
        ));
        t.push(format!(
            "get a = {:?}",
            StoreCalls::record_get(s, "c", b"a").await.map(show)
        ));
        t.push(format!(
            "get missing = {:?}",
            StoreCalls::record_get(s, "c", b"zz").await.map(show)
        ));
        t.push(format!(
            "reserve, no cap = {:?}",
            StoreCalls::reserve(s, OpId::from_parts(3, 1), 1, &cell).await
        ));
    });
    t
}

#[test]
fn the_linked_and_the_dropped_in_door_are_one_store() {
    assert_eq!(
        rendering_of_library(&cdylib()).expect("the cdylib loads"),
        Some(rendering_of(door).expect("the door renders")),
        "the dropped-in library must state exactly the linked door's Statement"
    );
    let linked = script(&open(Door::Linked), None);
    let dropped = script(&open(Door::Dropped), None);
    assert_eq!(linked, dropped, "both doors answer alike");
    assert!(linked[0].contains("ephemeral: true"), "{linked:?}");
    assert!(linked[3].contains("key-1"), "{linked:?}");
    assert!(linked[4].ends_with("Ok(None)"), "{linked:?}");
    assert!(linked[7].contains("one"), "{linked:?}");
    assert!(linked[8].ends_with("Ok(None)"), "{linked:?}");
    assert!(linked[9].contains("NoCap"), "{linked:?}");

    // RED (b): a store holding a foreign row is a different transcript.
    let red = script(&open(Door::Dropped), Some("vk_foreign"));
    assert_ne!(red, linked, "the comparison must see a real difference");
}

#[test]
fn the_door_asked_for_as_another_kind_is_refused_both_ways() {
    let d = Dispatcher::new(DispatchConfig::default());
    match load_linked::<Hook>(&LinkedRow::of(door).expect("states"), bind(&d)) {
        Err(LoadError::WrongKind { .. }) => {}
        other => panic!("the linked store door loaded as a hook: {:?}", other.err()),
    }
    match load_dropped::<Hook>(&cdylib(), &rendering_of(door).expect("renders"), bind(&d)) {
        Err(LoadError::ManifestKind { .. }) => {}
        other => panic!(
            "the dropped-in store door loaded as a hook: {:?}",
            other.err()
        ),
    }
}
