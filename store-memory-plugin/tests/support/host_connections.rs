// SPDX-License-Identifier: Apache-2.0
//! Assertion-specific host values: no network, framing or store implementation.
use super::*;
use busbar_contract::abi::mechanism::rendering::ReadNeed;
use busbar_contract::conn::{
    ConnId, ConnSlab, Conns, DeclaredConns, InstanceId, NeedId, OpenDesc, Piece,
    Ticket as ConnTicket,
};
use busbar_contract::transport::ConnFacts;
#[derive(Default)]
struct OwnershipValues(ConnSlab<()>);
impl Conns for OwnershipValues {
    fn open(
        &self,
        caller: InstanceId,
        need: NeedId,
        _: &OpenDesc<'_>,
    ) -> Result<ConnId, ConnError> {
        self.0.insert(caller, need, ())
    }
    fn write(
        &self,
        caller: InstanceId,
        conn: ConnId,
        bytes: &[u8],
        _: bool,
        _: bool,
    ) -> Result<usize, ConnError> {
        self.0.get(caller, conn)?;
        Ok(bytes.len())
    }
    fn read(
        &self,
        caller: InstanceId,
        conn: ConnId,
        _: ConnTicket,
        _: &mut [u8],
    ) -> Result<Piece, ConnError> {
        self.0.get(caller, conn)?;
        Err(ConnError::Refused)
    }
    fn wait(&self, caller: InstanceId, set: &[ConnId], _: ConnTicket) -> Result<usize, ConnError> {
        for conn in set {
            self.0.get(caller, *conn)?;
        }
        Err(ConnError::Refused)
    }
    fn facts(&self, caller: InstanceId, conn: ConnId) -> Result<ConnFacts, ConnError> {
        self.0.get(caller, conn)?;
        Err(ConnError::Refused)
    }
    fn close(&self, caller: InstanceId, conn: ConnId) -> Result<(), ConnError> {
        self.0.remove(caller, conn).map(|_| ())
    }
}
impl DeclaredConns for OwnershipValues {
    fn open_request(
        &self,
        caller: InstanceId,
        need: NeedId,
        desc: &OpenDesc<'_>,
        require_secure: bool,
        _dial_timeout_ms: u64,
    ) -> Result<ConnId, ConnError> {
        // These values do no I/O: the independent dial bound needs no timer. Preserve
        // security refusal and route the actual host's request to the ownership book.
        self.open_with_security(caller, need, desc, require_secure)
    }
    fn declare(
        &self,
        owner: InstanceId,
        need: NeedId,
        _: &ReadNeed,
        _: Option<&str>,
        _: Option<&str>,
    ) -> Result<(), ConnError> {
        self.0.declare(owner, need);
        Ok(())
    }
    fn declared(&self, owner: InstanceId, need: NeedId) -> Option<Result<(), ConnError>> {
        self.0.check_need(owner, need).ok().map(|_| Ok(()))
    }
    fn serves_scheme(&self, transport: &str) -> bool {
        transport == "ownership-values"
    }
}
fn with_an_outbound_need(real: DoorParts) -> DoorParts {
    use busbar_contract::abi::host::conn::connector::{Need, DIRECTION_OUTBOUND, KEEP_NAMED};
    use busbar_contract::abi::mechanism::door::Statement;
    const NONE: AbiStr = AbiStr {
        ptr: std::ptr::null(),
        len: 0,
    };
    let stream = "ownership-values";
    let needs: &'static [Need] = Box::leak(Box::new([Need {
        direction: DIRECTION_OUTBOUND,
        egress_class: 0,
        transport: AbiStr {
            ptr: stream.as_ptr(),
            len: stream.len(),
        },
        auth: NONE,
        target_from: NONE,
        trust_from: NONE,
        details: busbar_plugin_loader::dispatch::NO_BLOB,
        keep_response_headers: std::ptr::null(),
        keep_response_headers_len: 0,
        timeout_ms: 0,
        keep_mode: KEEP_NAMED,
        _reserved: 0,
        deny_response_headers: std::ptr::null(),
        deny_response_headers_len: 0,
    }]));
    // SAFETY: a door's Statement is `'static`; only its needs are restated.
    let st: Statement = unsafe { real.statement.read_unaligned() };
    let st: &'static Statement = Box::leak(Box::new(Statement {
        needs: needs.as_ptr(),
        needs_len: needs.len(),
        ..st
    }));
    // The fixture requests connector work during a real loader crossing.
    let mut ops = unsafe { *real.ops.cast::<busbar_contract::abi::store::v4::Ops>() };
    ops.head.tick = Some(probe_tick);
    let ops = Box::leak(Box::new(ops));
    DoorParts {
        statement: st,
        ops: std::ptr::from_ref(ops).cast(),
        ..real
    }
}

use busbar_contract::abi::host::conn::connector::{service, EstablishIn, IoIn, StreamIn};
use busbar_contract::abi::host::service::{ServiceHead, ServiceOut};
use busbar_contract::abi::mechanism::call::{Outcome, RawOutcome};
use busbar_contract::abi::mechanism::ticket::{CompletionHandle, Ticket};
use busbar_contract::conn::ConnError;
use busbar_plugin_loader::dispatch::conn_services::CONN_SLOTS;
use std::ffi::c_void;
fn head(op: u32, size: usize, ticket: Ticket) -> ServiceHead {
    ServiceHead {
        size: u32::try_from(size).unwrap_or(u32::MAX),
        op,
        handle: CompletionHandle {
            ticket,
            seq: 0,
            _reserved: 0,
        },
    }
}

fn out() -> ServiceOut {
    // SAFETY: a plain repr(C) value of integers and a NULL/0 string: all-zero is valid.
    let mut out: ServiceOut = unsafe { std::mem::zeroed() };
    out.size = std::mem::size_of::<ServiceOut>() as u32;
    out
}

fn answered(raw: RawOutcome, o: &ServiceOut) -> (Outcome, u64, String) {
    let text = if o.error.ptr.is_null() {
        String::new()
    } else {
        // SAFETY: the host's text, `'static` (interned) or held for the call.
        String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(o.error.ptr, o.error.len) })
            .into_owned()
    };
    (raw.outcome(), o.value, text)
}

fn establish(
    p: &busbar_plugin_loader::dispatch::Plugin<Store>,
    need: u32,
    target: &str,
) -> (Outcome, u64, String) {
    let target = target.to_owned();
    crossing(p, move |ctx, ticket| {
        let none = AbiStr {
            ptr: std::ptr::null(),
            len: 0,
        };
        let i = EstablishIn {
            head: head(
                service::ESTABLISH,
                std::mem::size_of::<EstablishIn>(),
                ticket,
            ),
            need,
            timeout_ms: 5_000,
            target: AbiStr {
                ptr: target.as_ptr(),
                len: target.len(),
            },
            within: none,
            member: none,
        };
        let mut o = out();
        let f = CONN_SLOTS.establish.expect("the host offers ESTABLISH");
        // SAFETY: the local input, exclusive output, nested buffers and strings live through this crossing.
        let raw = unsafe { f(ctx, std::ptr::from_ref(&i).cast::<c_void>(), &mut o) };
        answered(raw, &o)
    })
}

fn io(
    p: &busbar_plugin_loader::dispatch::Plugin<Store>,
    stream: u64,
    write: bool,
) -> (Outcome, u64, String) {
    crossing(p, move |ctx, ticket| {
        let mut buf = *b"x";
        let (op, f) = if write {
            (service::WRITE, CONN_SLOTS.write)
        } else {
            (service::READ, CONN_SLOTS.read)
        };
        let i = IoIn {
            head: head(op, std::mem::size_of::<IoIn>(), ticket),
            stream,
            buf: buf.as_mut_ptr(),
            len: buf.len(),
        };
        let mut o = out();
        // SAFETY: the local input, exclusive output, nested buffers and strings live through this crossing.
        let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let raw = unsafe {
                f.expect("the host offers READ and WRITE")(
                    ctx,
                    std::ptr::from_ref(&i).cast::<c_void>(),
                    &mut o,
                )
            };
            let answer = answered(raw, &o);
            if answer.0 != Outcome::Pending || std::time::Instant::now() >= until {
                return answer;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    })
}

fn close_stream(
    p: &busbar_plugin_loader::dispatch::Plugin<Store>,
    stream: u64,
) -> (Outcome, u64, String) {
    crossing(p, move |ctx, ticket| {
        let i = StreamIn {
            head: head(service::CLOSE, std::mem::size_of::<StreamIn>(), ticket),
            stream,
        };
        let mut o = out();
        // SAFETY: the local input, exclusive output, nested buffers and strings live through this crossing.
        let raw = unsafe {
            CONN_SLOTS.close.expect("the host offers CLOSE")(
                ctx,
                std::ptr::from_ref(&i).cast::<c_void>(),
                &mut o,
            )
        };
        answered(raw, &o)
    })
}

#[test]
fn actual_memory_contexts_refuse_undeclared_needs_and_foreign_connections() {
    let _lock = METADATA.lock().unwrap_or_else(|poison| poison.into_inner());
    restate(with_an_outbound_need(real()));
    let d = Dispatcher::new(
        DispatchConfig::default(),
        busbar_contract::services::test_caller_binding(),
    );
    let table: Arc<dyn DeclaredConns> = Arc::new(OwnershipValues::default());
    let row = LinkedRow::of_raw(malformed).expect("actual door with only one restated need");
    let owner = load_linked::<Store>(
        &row,
        busbar_plugin_loader::dispatch::Bind {
            conns: busbar_plugin_loader::dispatch::ConnTable::Host(Arc::clone(&table)),
            ..bind(&d)
        },
    )
    .unwrap();
    let other = load_linked::<Store>(
        &row,
        busbar_plugin_loader::dispatch::Bind {
            conns: busbar_plugin_loader::dispatch::ConnTable::Host(table),
            ..bind(&d)
        },
    )
    .unwrap();
    open(&owner);
    open(&other);
    let (answer, stream, text) = establish(&owner, 0, "ownership-value");
    assert_eq!(answer, Outcome::Ready, "declared need opens: {text}");
    assert_eq!(close_stream(&owner, stream).0, Outcome::Ready);
    for need in [1, u32::MAX] {
        let (answer, _, text) = establish(&owner, need, "ownership-value");
        assert_eq!(
            (answer, text.as_str()),
            (Outcome::Refused, ConnError::UndeclaredNeed.text()),
            "undeclared need {need}"
        );
    }
    let (answer, stream, text) = establish(&owner, 0, "ownership-value");
    assert_eq!(answer, Outcome::Ready, "owner opens declared need: {text}");
    assert_eq!(io(&owner, stream, true).0, Outcome::Ready, "owner writes");
    for (what, (answer, _, text)) in [
        ("write", io(&other, stream, true)),
        ("read", io(&other, stream, false)),
        ("close", close_stream(&other, stream)),
    ] {
        assert_eq!(
            (answer, text.as_str()),
            (Outcome::Refused, ConnError::NotOwner.text()),
            "foreign {what}"
        );
    }
    assert_eq!(
        io(&owner, stream, true).0,
        Outcome::Ready,
        "owner connection untouched"
    );
    assert_eq!(close_stream(&owner, stream).0, Outcome::Ready);
    close(&owner);
    close(&other);
}

// The fixture runs inside the ordinary loader's TICK, so production phase and
// ticket checks stay enabled. No fixture can enter the authority scope directly.
type Answer = (Outcome, u64, String);
type Probe = Box<dyn FnOnce(Ticket) -> Answer + Send>;
static PROBE: std::sync::Mutex<Option<Probe>> = std::sync::Mutex::new(None);
static ANSWER: std::sync::Mutex<Option<Answer>> = std::sync::Mutex::new(None);
unsafe extern "C" fn probe_tick(
    _: *mut c_void,
    input: *const c_void,
    output: *mut c_void,
) -> RawOutcome {
    // SAFETY: the loader lends the lifecycle frame for the synchronous callback.
    let ticket = unsafe {
        (*input.cast::<busbar_contract::abi::mechanism::lifecycle::TickIn>())
            .head
            .ticket
    };
    let probe = PROBE.lock().unwrap().take().unwrap();
    *ANSWER.lock().unwrap() = Some(probe(ticket));
    unsafe {
        (*output.cast::<busbar_contract::abi::mechanism::lifecycle::TickOut>())
            .head
            .outcome = RawOutcome::of(Outcome::Ready);
    }
    RawOutcome::of(Outcome::Ready)
}
fn crossing(
    p: &busbar_plugin_loader::dispatch::Plugin<Store>,
    f: impl FnOnce(busbar_contract::abi::mechanism::ticket::HostCtx, Ticket) -> Answer + Send + 'static,
) -> Answer {
    use busbar_contract::abi::mechanism::lifecycle::{slot, TickIn, TickOut};
    let ctx = p.test_host_context();
    assert!(PROBE
        .lock()
        .unwrap()
        .replace(Box::new(move |ticket| f(ctx, ticket)))
        .is_none());
    let mut frame = busbar_plugin_loader::dispatch::Frame::new(
        TickIn {
            head: busbar_plugin_loader::dispatch::in_head(),
            now_ns: 0,
        },
        TickOut {
            head: busbar_plugin_loader::dispatch::out_head(),
            next_tick_ns: 0,
        },
    );
    assert_eq!(p.call(slot::TICK, &mut frame).outcome, Outcome::Ready);
    ANSWER.lock().unwrap().take().unwrap()
}

fn open(p: &busbar_plugin_loader::dispatch::Plugin<Store>) {
    use busbar_contract::abi::mechanism::{
        call::Blob,
        lifecycle::{slot, OpenIn, OpenOut},
    };
    use busbar_plugin_loader::dispatch::{in_head, out_head, Frame};
    // SAFETY: lifecycle frames are plain C data with nullable pointer fields.
    let mut input: OpenIn = unsafe { std::mem::zeroed() };
    let mut output: OpenOut = unsafe { std::mem::zeroed() };
    input.head = in_head();
    output.head = out_head();
    input.generation = 1;
    input.settings = Blob {
        ptr: b"{}".as_ptr(),
        len: 2,
        fmt: busbar_contract::abi::mechanism::call::BLOB_JSON,
        flags: 0,
    };
    let called = p.call(slot::OPEN, &mut Frame::new(input, output));
    assert_eq!(called.outcome, Outcome::Ready, "{called:?}");
}
fn close(p: &busbar_plugin_loader::dispatch::Plugin<Store>) {
    use busbar_plugin_loader::dispatch::{in_head, out_head, Frame};
    assert_eq!(
        p.call(
            busbar_contract::abi::mechanism::lifecycle::slot::CLOSE,
            &mut Frame::new(in_head(), out_head())
        )
        .outcome,
        Outcome::Ready
    );
}
