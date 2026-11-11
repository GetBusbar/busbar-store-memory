// SPDX-License-Identifier: Apache-2.0
//! Malformed admissions made by changing only metadata on this real memory product.
mod support;
use busbar_store_memory::MACRO_DOOR;
use std::sync::Arc;
#[derive(Clone, Copy)]
enum Door {
    Linked,
    Dropped,
}
fn cdylib() -> std::path::PathBuf {
    support::artifact()
}
fn bind(d: &Dispatcher) -> busbar_plugin_loader::dispatch::Bind {
    busbar_plugin_loader::dispatch::Bind {
        instance: Arc::from("memory-refusal"),
        max_inflight_cap: 64,
        sink: Arc::new(busbar_plugin_loader::dispatch::NoSink),
        dispatcher: d.adopter(),
        conns: busbar_plugin_loader::dispatch::ConnTable::NoNeeds,
    }
}
fn load<K: busbar_plugin_loader::dispatch::Kind>(
    door: Door,
    d: &Dispatcher,
) -> Result<busbar_plugin_loader::dispatch::Plugin<K>, LoadError> {
    let row = LinkedRow::of_macro(MACRO_DOOR)?;
    match door {
        Door::Linked => load_linked::<K>(&row, bind(d)),
        Door::Dropped => load_dropped::<K>(&cdylib(), &row.statement, bind(d)),
    }
}
use busbar_contract::abi::mechanism::call::AbiStr;
use busbar_contract::abi::mechanism::door::{Door as AbiDoor, DoorParts, Statement};
use busbar_contract::abi::mechanism::KindCode;
use busbar_plugin_loader::dispatch::kinds::{
    auth::Auth, export::Export, hook::Hook, plane::Plane, secret::Secret,
    store_v4::StoreV4 as Store, transport::Transport,
};
use busbar_plugin_loader::dispatch::{
    load_dropped, load_linked, rendering_of_macro, DispatchConfig, Dispatcher, LinkedRow, LoadError,
};
use std::sync::atomic::{AtomicPtr, Ordering};

static METADATA: std::sync::Mutex<()> = std::sync::Mutex::new(());
static RESTATED: AtomicPtr<AbiDoor> = AtomicPtr::new(std::ptr::null_mut());
extern "C" fn malformed() -> *const AbiDoor {
    RESTATED.load(Ordering::SeqCst)
}
fn real() -> DoorParts {
    // SAFETY: the shipped macro door owns a complete immutable static descriptor.
    busbar_store_memory::door().parts()
}
fn restate(parts: DoorParts) {
    // SAFETY: all pointers remain those of the actual product or leaked immutable test metadata.
    let door = unsafe { AbiDoor::__from_parts(parts) };
    RESTATED.store(Box::into_raw(Box::new(door)), Ordering::SeqCst);
}

#[test]
fn incompatible_abi_layout_and_statement_metadata_are_refused() {
    let _lock = METADATA.lock().unwrap_or_else(|poison| poison.into_inner());
    let d = Dispatcher::new(
        DispatchConfig::default(),
        busbar_contract::services::test_caller_binding(),
    );
    let honest = rendering_of_macro(MACRO_DOOR).unwrap();
    for by in [Door::Linked, Door::Dropped] {
        load::<Store>(by, &d).expect("honest memory loads");
    }
    let host = KindCode::Store.abi_version();
    assert_eq!(real().kind_abi, host);
    for abi in [host + 1, host.wrapping_sub(1)] {
        restate(DoorParts {
            kind_abi: abi,
            ..real()
        });
        let want = LoadError::KindAbi {
            kind: KindCode::Store,
            door: abi,
            host,
        };
        let err = LinkedRow::of_raw(malformed).unwrap_err();
        assert_eq!(err, want);
        assert!(err.to_string().contains("rebuild"));
        let mut stated = honest.clone();
        let at = busbar_contract::abi::mechanism::rendering::RENDERING_MAGIC.len() + 8;
        stated[at..at + 4].copy_from_slice(&abi.to_le_bytes());
        assert_eq!(
            load_dropped::<Store>(&cdylib(), &stated, bind(&d))
                .map(|_| ())
                .unwrap_err(),
            LoadError::ManifestKindAbi { stated: abi, host }
        );
    }
    let host = KindCode::Store.layout_fingerprint();
    assert_eq!(real().layout, host);
    for layout in [host ^ 1, 0] {
        restate(DoorParts { layout, ..real() });
        let err = LinkedRow::of_raw(malformed).unwrap_err();
        let LoadError::KindLayout {
            kind,
            stated,
            host: read,
            ..
        } = &err
        else {
            panic!("{err:?}")
        };
        assert_eq!((*kind, *stated, *read), (KindCode::Store, layout, host));
        assert!(err
            .to_string()
            .contains("rebuild against this busbar-contract"));
        let row = LinkedRow {
            statement: honest.clone(),
            door: malformed,
        };
        assert_eq!(
            load_linked::<Store>(&row, bind(&d))
                .map(|_| ())
                .unwrap_err(),
            err
        );
    }
    let original = real();
    // SAFETY: read the real descriptor's static Statement without changing any backing data.
    let st = unsafe { original.statement.read_unaligned() };
    const VERSION: &str = "0.0.0-conformance-restated";
    let st = Box::leak(Box::new(Statement {
        version: AbiStr {
            ptr: VERSION.as_ptr(),
            len: VERSION.len(),
        },
        ..st
    }));
    restate(DoorParts {
        statement: st,
        ..original
    });
    let lying = LinkedRow::of_raw(malformed).unwrap().statement;
    busbar_contract::abi::mechanism::rendering::read(&lying).unwrap();
    assert_ne!(lying, honest);
    let mut row = LinkedRow::of_macro(MACRO_DOOR).unwrap();
    row.statement = lying.clone();
    assert_eq!(
        load_linked::<Store>(&row, bind(&d))
            .map(|_| ())
            .unwrap_err(),
        LoadError::StatementMismatch
    );
    assert_eq!(
        load_dropped::<Store>(&cdylib(), &lying, bind(&d))
            .map(|_| ())
            .unwrap_err(),
        LoadError::StatementMismatch
    );
    let row = LinkedRow {
        statement: honest,
        door: malformed,
    };
    assert_eq!(
        load_linked::<Store>(&row, bind(&d))
            .map(|_| ())
            .unwrap_err(),
        LoadError::StatementMismatch
    );
}

fn wrong_kind<K: busbar_plugin_loader::dispatch::Kind>() {
    let d = Dispatcher::new(
        DispatchConfig::default(),
        busbar_contract::services::test_caller_binding(),
    );
    for by in [Door::Linked, Door::Dropped] {
        assert_eq!(
            load::<K>(by, &d).map(|_| ()).unwrap_err(),
            LoadError::ManifestKind {
                stated: KindCode::Store,
                want: K::CODE
            }
        );
    }
}
#[test]
fn the_real_store_refuses_every_other_kind_through_both_doors() {
    wrong_kind::<Auth>();
    wrong_kind::<Secret>();
    wrong_kind::<Hook>();
    wrong_kind::<Export>();
    wrong_kind::<Plane>();
    wrong_kind::<Transport>();
}

#[derive(Default)]
struct ReadyWitness {
    ready_calls: u64,
    close: Option<busbar_contract::abi::mechanism::call::Op>,
    close_calls: u64,
    close_answer: Option<busbar_contract::abi::mechanism::call::Outcome>,
}
static READY_WITNESS: std::sync::Mutex<ReadyWitness> = std::sync::Mutex::new(ReadyWitness {
    ready_calls: 0,
    close: None,
    close_calls: 0,
    close_answer: None,
});
static READY_CLOSED: std::sync::Condvar = std::sync::Condvar::new();
const READY_FAILURE: &str = "conformance: discovery refused";
extern "C" fn ready_close(
    instance: *mut std::ffi::c_void,
    input: *const std::ffi::c_void,
    out: *mut std::ffi::c_void,
) -> busbar_contract::abi::mechanism::call::RawOutcome {
    let close = {
        let mut w = READY_WITNESS.lock().unwrap();
        w.close_calls += 1;
        w.close.expect("real CLOSE")
    };
    // SAFETY: forwards the real live instance and complete frames to its original CLOSE exactly once.
    let answer = unsafe { close(instance, input, out) };
    READY_WITNESS.lock().unwrap().close_answer = Some(answer.outcome());
    READY_CLOSED.notify_all();
    answer
}
extern "C" fn ready_fails(
    _: *mut std::ffi::c_void,
    _: *const std::ffi::c_void,
    out: *mut std::ffi::c_void,
) -> busbar_contract::abi::mechanism::call::RawOutcome {
    use busbar_contract::abi::mechanism::call::{OutHead, Outcome, RawOutcome};
    READY_WITNESS.lock().unwrap().ready_calls += 1;
    let failed = RawOutcome::of(Outcome::Failed);
    // SAFETY: the host gives READY a complete writable OutHead; immutable text remains static.
    unsafe {
        let head = &mut *out.cast::<OutHead>();
        head.outcome = failed;
        head.error = AbiStr {
            ptr: READY_FAILURE.as_ptr(),
            len: READY_FAILURE.len(),
        };
    }
    failed
}
#[test]
fn failed_ready_closes_the_real_opened_store_once() {
    let _lock = METADATA.lock().unwrap_or_else(|poison| poison.into_inner());
    let original = real();
    // SAFETY: the actual memory door owns the complete current store operation table.
    let mut ops = unsafe {
        original
            .ops
            .cast::<busbar_contract::abi::store::v4::Ops>()
            .read_unaligned()
    };
    *READY_WITNESS.lock().unwrap() = ReadyWitness {
        close: ops.head.close,
        ..Default::default()
    };
    ops.head.close = Some(ready_close);
    let ops = Box::leak(Box::new(ops));
    restate(DoorParts {
        size: std::mem::size_of::<AbiDoor>() as u32,
        ready: Some(ready_fails),
        ops: std::ptr::from_ref(ops).cast(),
        ..original
    });
    let d = std::sync::Arc::new(Dispatcher::new(
        DispatchConfig::default(),
        busbar_contract::services::test_caller_binding(),
    ));
    let p = load_linked::<Store>(&LinkedRow::of_raw(malformed).unwrap(), bind(&d)).unwrap();
    assert!(p.has_ready());
    let probe = p.test_invocation_probe();
    let first = || {
        let (crossings, resumes) = probe();
        crossings - resumes
    };
    let before = first();
    let err = support::block(busbar_plugin_loader::store_v4::LoadedPersistence::open(
        p,
        d,
        b"{}",
        support::limits(),
    ))
    .map(|_| ())
    .unwrap_err();
    assert_eq!(err, "primitive store ready failed");
    // READY publishes its answer before the worker releases the retained session
    // outside its state lock. Observe the real CLOSE completion before teardown
    // assertions; returning from OPEN alone does not synchronize that release.
    let (w, _) = READY_CLOSED
        .wait_timeout_while(
            READY_WITNESS.lock().unwrap(),
            std::time::Duration::from_secs(5),
            |w| w.close_answer.is_none(),
        )
        .unwrap();
    assert!(
        w.close_answer.is_some(),
        "failed READY must finish cleanup CLOSE"
    );
    assert_eq!(
        first() - before,
        4,
        "OPEN, metadata answer release, failed READY, cleanup CLOSE"
    );
    assert_eq!(w.ready_calls, 1, "failing READY is not retried");
    assert_eq!(w.close_calls, 1, "successful OPEN is closed exactly once");
    assert_eq!(
        w.close_answer,
        Some(busbar_contract::abi::mechanism::call::Outcome::Ready)
    );
}

#[path = "support/host_connections.rs"]
mod host_connections;
