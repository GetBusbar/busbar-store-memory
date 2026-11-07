// SPDX-License-Identifier: Apache-2.0
#![allow(dead_code)]
use busbar_contract::persistence::{
    axis::{PersistenceAxis, StoreDoor},
    PersistenceBackend,
};
use busbar_plugin_loader::{
    dispatch::{self, Bind, ConnTable, DispatchConfig, Dispatcher, LinkedRow, NoSink},
    store_v4::{DoorPersistenceAxis, Limits, LoadedPersistence},
};
use std::sync::Arc;

pub fn artifact() -> std::path::PathBuf {
    let path = std::env::var_os("BUSBAR_PLUGIN_CDYLIB")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            let executable = std::env::current_exe().expect("test executable");
            executable
                .parent()
                .and_then(|path| path.parent())
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
        "actual cdylib missing: {}; build the actual memory plugin first",
        path.display()
    );
    path
}
pub fn block<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("native fixture runtime")
        .block_on(future)
}
pub fn limits() -> Limits {
    Limits {
        descriptor_bytes: 32 * 1024 * 1024,
        output_items: 4096,
        output_bytes: 32 * 1024 * 1024,
        decode_bytes: 32 * 1024 * 1024,
    }
}
fn bound(
    dropped: bool,
) -> (
    dispatch::Plugin<dispatch::kinds::store_v4::StoreV4>,
    Arc<Dispatcher>,
) {
    let dispatcher = Arc::new(Dispatcher::new(DispatchConfig::default()));
    let bind = Bind {
        instance: Arc::from("memory-product"),
        max_inflight_cap: 1024,
        sink: Arc::new(NoSink),
        dispatcher: dispatcher.adopter(),
        conns: ConnTable::NoNeeds,
    };
    let row = LinkedRow::of(busbar_store_memory::door).expect("actual Statement");
    let plugin = if dropped {
        dispatch::load_dropped::<dispatch::kinds::store_v4::StoreV4>(
            &artifact(),
            &row.statement,
            bind,
        )
    } else {
        dispatch::load_linked::<dispatch::kinds::store_v4::StoreV4>(&row, bind)
    }
    .expect("actual primitive door loads");
    (plugin, dispatcher)
}
pub fn prepare(dropped: bool) -> busbar_plugin_loader::store_v4::LoadedPrepared {
    let (plugin, dispatcher) = bound(dropped);
    busbar_plugin_loader::store_v4::LoadedPrepared::new(plugin, dispatcher, b"{}", limits())
        .expect("actual primitive memory prepares without OPEN")
}
pub async fn open(dropped: bool) -> LoadedPersistence {
    let (plugin, dispatcher) = bound(dropped);
    LoadedPersistence::open(plugin, dispatcher, b"{}", limits())
        .await
        .expect("actual primitive memory opens")
}
pub async fn open_axis() -> Arc<dyn PersistenceBackend> {
    let axis = DoorPersistenceAxis {
        dispatcher: Arc::new(Dispatcher::new(DispatchConfig::default())),
        logs: dispatch::PluginLogConfig::from_words(None, None, &Default::default(), None, None)
            .expect("default logs"),
        conns: ConnTable::NoNeeds,
        limits: limits(),
    };
    axis.open(
        StoreDoor::Linked(busbar_store_memory::door),
        "memory-product-axis",
        b"{}",
    )
    .await
    .expect("normal primitive persistence axis opens")
}
