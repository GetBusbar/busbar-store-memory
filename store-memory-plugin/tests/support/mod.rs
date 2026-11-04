// SPDX-License-Identifier: Apache-2.0
#![allow(dead_code)]
use busbar_contract::abi::store::OpId;
use busbar_plugin_loader::dispatch::kinds::store::Store;
use busbar_plugin_loader::dispatch::{
    self, Bind, ConnTable, DispatchConfig, Dispatcher, LinkedRow, NoSink,
};
use busbar_plugin_loader::store_v3::LoadedStore;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

pub fn artifact() -> std::path::PathBuf {
    let path = std::env::var_os("BUSBAR_PLUGIN_CDYLIB")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            let exe = std::env::current_exe().expect("test executable");
            exe.parent()
                .and_then(|p| p.parent())
                .expect("profile directory")
                .join(format!(
                    "{}busbar_store_memory_plugin{}",
                    std::env::consts::DLL_PREFIX,
                    std::env::consts::DLL_SUFFIX
                ))
        });
    assert!(path.is_absolute(), "BUSBAR_PLUGIN_CDYLIB must be absolute");
    assert!(
        path.is_file(),
        "actual cdylib missing: {}; run cargo build -p busbar-store-memory-plugin --lib first",
        path.display()
    );
    path
}
fn mint() -> OpId {
    static N: AtomicU64 = AtomicU64::new(1);
    OpId::from_parts(0x3a11, N.fetch_add(1, Ordering::Relaxed))
}
pub fn open(dropped: bool) -> LoadedStore {
    let d = Arc::new(Dispatcher::new(DispatchConfig::default()));
    let bind = Bind {
        instance: Arc::from("memory-product"),
        max_inflight_cap: 1024,
        sink: Arc::new(NoSink),
        dispatcher: d.adopter(),
        conns: ConnTable::NoNeeds,
    };
    let row = LinkedRow::of(busbar_store_memory::door).expect("actual Statement");
    let p = if dropped {
        dispatch::load_dropped::<Store>(&artifact(), &row.statement, bind)
    } else {
        dispatch::load_linked::<Store>(&row, bind)
    }
    .expect("actual door loads");
    LoadedStore::open(p, d, b"{}", mint).expect("actual memory opens")
}
