// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The store v3 slots this backend answers beyond the 1.5.5 op set ([`StoreSlots`]): the
//! `op_id`-carrying writes, the ledger ops, the money slots and `window_caps`.
//!
//! DEDUPE (`abi::store` S1-S4): every `op_id` this store APPLIED is remembered, with a SHA-256
//! digest of the op's value fields and its answer, for [`OP_ID_RETENTION_SECS`]. A replay with the
//! same value fields applies nothing and answers the original; different value fields are a
//! conflict. The dedupe log, caps, drawn totals and slices live under ONE lock, and an op's check,
//! effect and record happen under it, so two racing calls with one `op_id` apply once. The log is
//! lost on restart together with everything it guards.
//!
//! EPOCH AND SLICE LIFE (`abi::store::SLICE_TTL_MS`): a NODE-LOCAL store. It holds one constant
//! epoch, accepts the `epoch` a caller states, and grants slices that never expire
//! (`valid_until_ms = u64::MAX`). A release returns at most what the slice has left; a release
//! with nothing unspent closes the slice.
//!
//! GRANT SIZE: a cell grants its whole `amount` or the reserve fails; the per-dimension test is
//! 1.5.5's, cited on `abi::store::ReserveIn`.

use std::collections::{HashMap, VecDeque};
use std::fmt::Write as _;
use std::sync::{Mutex, MutexGuard};

use busbar_contract::abi::sdk::conn::Host;
use busbar_contract::abi::sdk::store::{
    Cap, CapsRefused, Cell, CellKey, Dimension, Grant, Op, OpRefused, OpResult, ReserveRefused,
    Scanned, Step, StoreSlots, Tail,
};
use busbar_contract::abi::store::{OpId, OP_ID_RETENTION_SECS};
use busbar_contract::kinds::{Head, RecordBytes};
use busbar_contract::records::{
    AuditRecord, CredentialMeta, CredentialSecret, MeteringDelta, MeteringRow, PlaneRecordRef,
    PlaneSelector, RecordStore, RecordStoreError, RecordStoreResult, Retention, UsageDelta,
    UsageLedger, VirtualKey,
};

use crate::MemoryStore;

busbar_contract::store_door!(MemoryStore, "memory", env!("CARGO_PKG_VERSION"), 1024);

/// A slot key owned by the store: `(bucket, pool, dimension, class_key, window_start)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Slot {
    bucket: String,
    pool: Option<String>,
    dimension: u32,
    class_key: String,
    window_start: u64,
}

impl Slot {
    fn of(k: &CellKey<'_>) -> Self {
        let (dimension, class_key) = match k.dimension {
            Dimension::NanoUnits => (0, ""),
            Dimension::Requests => (1, ""),
            Dimension::Concurrency => (2, ""),
            Dimension::Class(c) => (3, c),
        };
        Self {
            bucket: k.bucket.to_string(),
            pool: k.pool.map(str::to_string),
            dimension,
            class_key: class_key.to_string(),
            window_start: k.window_start,
        }
    }
}

/// What an applied op answered, replayed verbatim.
#[derive(Debug, Clone)]
enum Answer {
    Done,
    Head(Head),
    Grants(Vec<Grant>),
    Released(Vec<u64>),
}

/// The SHA-256 digest of an op's value fields, as their text renders them.
type Body = [u8; 32];

/// A [`std::fmt::Write`] that feeds what is written into a digest, so the text is never held.
struct Feed(sha2::Sha256);

impl std::fmt::Write for Feed {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        sha2::Digest::update(&mut self.0, s.as_bytes());
        Ok(())
    }
}

/// The [`Body`] of the value fields `args` render.
fn body(args: std::fmt::Arguments<'_>) -> Body {
    let mut feed = Feed(<sha2::Sha256 as sha2::Digest>::new());
    // Writing into a digest cannot fail.
    let _ = feed.write_fmt(args);
    sha2::Digest::finalize(feed.0).into()
}

/// One remembered op: the digest of its value fields and its answer.
struct Recorded {
    body: Body,
    answer: Answer,
}

/// A drawn slice: the slot it drew from and what it still holds.
struct Drawn {
    slot: Slot,
    left: u64,
}

#[derive(Default)]
struct Inner {
    ops: HashMap<OpId, Recorded>,
    /// `(recorded_at, op_id)`, oldest first: the retention sweep's order.
    aged: VecDeque<(u64, OpId)>,
    /// `slot -> (cap, config_gen)`.
    caps: HashMap<Slot, (u64, u64)>,
    /// `slot -> drawn and not released`.
    used: HashMap<Slot, u64>,
    slices: HashMap<u64, Drawn>,
    /// The slices whose unreturned unspent reached `0`, with when: a later release of one returns
    /// `0`. Kept as long as an `op_id` is (`OP_ID_RETENTION_SECS`), then forgotten.
    closed: HashMap<u64, u64>,
    next_slice: u64,
    /// `stream -> its head's seq`, the count of records appended to it. `heads` is the one read of a
    /// stream, so its records are not kept.
    journals: HashMap<String, u64>,
    /// `session -> (node, principal)`.
    sessions: HashMap<u64, (String, String)>,
}

/// The v3 state [`MemoryStore`] carries.
#[derive(Default)]
pub(crate) struct State {
    inner: Mutex<Inner>,
}

/// Whether an op is new, a replay, or a conflict.
enum Seen {
    New,
    Replay(Answer),
    Conflict,
}

impl Inner {
    fn seen(&self, op: OpId, body: &Body) -> Seen {
        match self.ops.get(&op) {
            None => Seen::New,
            Some(r) if r.body == *body => Seen::Replay(r.answer.clone()),
            Some(_) => Seen::Conflict,
        }
    }

    /// Remember an APPLIED op, and forget every op past its retention.
    fn record(&mut self, now: u64, op: OpId, body: Body, answer: Answer) {
        while let Some(&(at, old)) = self.aged.front() {
            if at.saturating_add(OP_ID_RETENTION_SECS) > now {
                break;
            }
            self.aged.pop_front();
            self.ops.remove(&old);
        }
        self.ops.insert(op, Recorded { body, answer });
        self.aged.push_back((now, op));
    }
}

impl Inner {
    /// ROLLING MONEY WINDOWS ARE BOUNDED where they roll: per slot family (bucket, pool, dimension,
    /// class), every rolling window (`window_start > 0`) older than the one before the family's
    /// newest is retired, its cap and its counter, unless an open slice still draws on it. Window 0
    /// never rolls (the gauges and the caps that never reset) and is never retired; the window
    /// before the newest stays, so a unit drawing across the roll still finds its cap.
    fn retire_rolled_windows(&mut self) {
        type Family = (String, Option<String>, u32, String);
        let family = |s: &Slot| -> Family {
            (
                s.bucket.clone(),
                s.pool.clone(),
                s.dimension,
                s.class_key.clone(),
            )
        };
        let mut windows: HashMap<Family, Vec<u64>> = HashMap::new();
        for slot in self.caps.keys().filter(|s| s.window_start > 0) {
            windows
                .entry(family(slot))
                .or_default()
                .push(slot.window_start);
        }
        // The window before each family's newest: anything older retires.
        let keep_from: HashMap<Family, u64> = windows
            .into_iter()
            .filter_map(|(f, mut w)| {
                w.sort_unstable();
                w.dedup();
                (w.len() > 2).then(|| (f, w[w.len() - 2]))
            })
            .collect();
        if keep_from.is_empty() {
            return;
        }
        let drawn: std::collections::HashSet<Slot> =
            self.slices.values().map(|d| d.slot.clone()).collect();
        let retired = |s: &Slot| {
            s.window_start > 0
                && !drawn.contains(s)
                && keep_from
                    .get(&family(s))
                    .is_some_and(|&from| s.window_start < from)
        };
        self.caps.retain(|s, _| !retired(s));
        self.used.retain(|s, _| !retired(s));
    }
}

impl State {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }
}

/// What the v3 state holds, as a test measures it.
#[cfg(test)]
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Held {
    /// The bytes the dedupe log keeps per remembered op, summed.
    pub(crate) op_bytes: usize,
    /// The record bytes the ledger streams keep, summed.
    pub(crate) journal_bytes: usize,
    /// How many caps, slot counters and open slices are kept.
    pub(crate) caps: usize,
    pub(crate) used: usize,
    pub(crate) slices: usize,
    /// How many closed slices are remembered (so a late release of one answers 0).
    pub(crate) closed: usize,
}

#[cfg(test)]
impl State {
    /// Test-only: what this state holds ([`Held`]).
    pub(crate) fn held(&self) -> Held {
        let inner = self.lock();
        Held {
            op_bytes: inner.ops.values().map(|r| r.body.len()).sum(),
            // A stream keeps its count alone.
            journal_bytes: 0,
            caps: inner.caps.len(),
            used: inner.used.len(),
            slices: inner.slices.len(),
            closed: inner.closed.len(),
        }
    }
}

/// The 1.5.5 admission test for one cell (`abi::store::ReserveIn`, GRANT SIZE): whether drawing
/// `amount` onto `used` under `cap` is refused. `used + amount` is checked: an overflow refuses.
fn exhausted(dimension: u32, used: u64, amount: u64, cap: u64) -> bool {
    let Some(after) = used.checked_add(amount) else {
        return true;
    };
    match dimension {
        // DIM_CLASS: `tokens >= cap` — the draw that crosses the cap is granted whole.
        3 => used >= cap,
        // DIM_NANO_UNITS: `derived >= cap || derived + fee > cap`.
        0 => used >= cap || after > cap,
        // DIM_REQUESTS / DIM_CONCURRENCY: `used + amount > cap`.
        _ => after > cap,
    }
}

fn failed(e: busbar_contract::records::RecordStoreError) -> OpRefused {
    OpRefused::Failed(e.0)
}

impl MemoryStore {
    /// Run one `op_id`-carrying write under the v3 lock: a replay answers the original, a
    /// conflict applies nothing, and a new op runs `apply` and is remembered only if it applied.
    fn deduped(
        &self,
        op: OpId,
        body: Body,
        apply: impl FnOnce(&Self, &mut Inner) -> OpResult<Answer>,
    ) -> OpResult<Answer> {
        let mut inner = self.v3.lock();
        match inner.seen(op, &body) {
            Seen::Replay(a) => Ok(a),
            Seen::Conflict => Err(OpRefused::Conflict),
            Seen::New => {
                let answer = apply(self, &mut inner)?;
                inner.record(self.now(), op, body, answer.clone());
                Ok(answer)
            }
        }
    }

    /// [`Self::deduped`] for an op whose answer is just `Done`.
    fn applied(
        &self,
        op: OpId,
        body: Body,
        apply: impl FnOnce(&Self) -> RecordStoreResult<()>,
    ) -> OpResult<()> {
        self.deduped(op, body, |s, _| {
            apply(s).map_err(failed)?;
            Ok(Answer::Done)
        })
        .map(drop)
    }

    /// Every audit record in `entries` would append (none forks a stored `seq`, nor another in the
    /// batch), checked before any is applied.
    fn audit_fits(&self, entries: &[AuditRecord]) -> RecordStoreResult<()> {
        let audit = crate::read(&self.audit);
        let mut batch: HashMap<u64, &AuditRecord> = HashMap::new();
        for e in entries {
            let prior = audit.get(&e.seq).or_else(|| batch.get(&e.seq).copied());
            if prior.is_some_and(|p| p != e) {
                return Err(RecordStoreError(format!(
                    "append_audit: seq {} already holds a DIFFERENT record — the audit chain has forked",
                    e.seq
                )));
            }
            batch.insert(e.seq, e);
        }
        Ok(())
    }
}

/// The store v3 slots' own bodies: each op's ONE body, which the table's slot answers inline (the
/// memory store never pends).
impl MemoryStore {
    pub(crate) fn v3_add_usage_op(
        &self,
        op: OpId,
        bucket: &str,
        window_start: u64,
        delta: &UsageDelta,
    ) -> OpResult<()> {
        let body = body(format_args!(
            "add_usage:{bucket:?}:{window_start}:{delta:?}"
        ));
        self.applied(op, body, |s| s.add_usage(bucket, window_start, delta))
    }

    pub(crate) fn v3_add_metering_op(&self, op: OpId, delta: &MeteringDelta) -> OpResult<()> {
        let body = body(format_args!("add_metering:{delta:?}"));
        self.applied(op, body, |s| s.add_metering(delta))
    }

    pub(crate) fn v3_append_audit_op(&self, op: OpId, entry: &AuditRecord) -> OpResult<()> {
        let body = body(format_args!("append_audit:{entry:?}"));
        self.applied(op, body, |s| s.append_audit(entry))
    }

    pub(crate) fn v3_append_plane_record_op(
        &self,
        op: OpId,
        record: PlaneRecordRef<'_>,
    ) -> OpResult<()> {
        let body = body(format_args!("append_plane_record:{:?}", record.to_record()));
        self.applied(op, body, |s| s.append_plane_record(record))
    }

    pub(crate) fn v3_append_batch(
        &self,
        op: OpId,
        stream: &str,
        records: &[RecordBytes],
    ) -> OpResult<Head> {
        let body = body(format_args!("append_batch:{stream:?}:{records:?}"));
        let answer = self.deduped(op, body, |_, inner| {
            let seq = inner.journals.entry(stream.to_string()).or_default();
            *seq = seq.saturating_add(records.len() as u64);
            Ok(Answer::Head(Head {
                seq: *seq,
                epoch: 0,
            }))
        })?;
        match answer {
            Answer::Head(h) => Ok(h),
            _ => Err(OpRefused::Conflict),
        }
    }

    pub(crate) fn v3_heads(&self) -> Result<Vec<(String, Head)>, String> {
        let inner = self.v3.lock();
        let mut heads: Vec<(String, Head)> = inner
            .journals
            .iter()
            .map(|(s, seq)| {
                (
                    s.clone(),
                    Head {
                        seq: *seq,
                        epoch: 0,
                    },
                )
            })
            .collect();
        heads.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(heads)
    }

    pub(crate) fn v3_session_put(
        &self,
        session: u64,
        node: &str,
        principal: &str,
    ) -> Result<(), String> {
        self.v3
            .lock()
            .sessions
            .insert(session, (node.to_string(), principal.to_string()));
        Ok(())
    }

    pub(crate) fn v3_session_remove(&self, session: u64) -> Result<(), String> {
        self.v3.lock().sessions.remove(&session);
        Ok(())
    }

    pub(crate) fn v3_sessions_for(&self, principal: &str) -> Result<Vec<(u64, String)>, String> {
        let inner = self.v3.lock();
        let mut rows: Vec<(u64, String)> = inner
            .sessions
            .iter()
            .filter(|(_, (_, p))| p == principal)
            .map(|(s, (n, _))| (*s, n.clone()))
            .collect();
        rows.sort_unstable();
        Ok(rows)
    }

    pub(crate) fn v3_record_put(
        &self,
        schema: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), String> {
        // The store keeps the record, so it copies it here (the door lends the host's bytes).
        let value = RecordBytes::new(value.to_vec())
            .map_err(|n| format!("a record of {n} bytes is over the ceiling"))?;
        self.record_put_at(schema, key, &value);
        Ok(())
    }

    pub(crate) fn v3_record_get(
        &self,
        schema: &str,
        key: &[u8],
    ) -> Result<Option<RecordBytes>, String> {
        Ok(self.record_get_at(schema, key))
    }

    pub(crate) fn v3_record_scan(
        &self,
        schema: &str,
        prefix: &[u8],
        limit: u32,
    ) -> Result<Vec<(Vec<u8>, RecordBytes)>, String> {
        Ok(self.record_scan_at(schema, prefix, limit))
    }

    pub(crate) fn v3_reserve<'c>(
        &self,
        op: OpId,
        epoch: u64,
        cells: impl Iterator<Item = Cell<'c>> + Clone,
        grants: &mut impl Extend<Grant>,
    ) -> Result<(), ReserveRefused> {
        let cells: Vec<Cell<'_>> = cells.collect();
        let body = body(format_args!("reserve:{epoch}:{cells:?}"));
        let mut inner = self.v3.lock();
        match inner.seen(op, &body) {
            Seen::Replay(Answer::Grants(g)) => {
                grants.extend(g);
                return Ok(());
            }
            Seen::Replay(_) | Seen::Conflict => return Err(ReserveRefused::Conflict),
            Seen::New => {}
        }
        // The chain draw is all or nothing: test every cell against what the cells before it in
        // THIS draw add, and apply only when every cell passes.
        let mut drawn: HashMap<Slot, u64> = HashMap::new();
        let mut slots = Vec::with_capacity(cells.len());
        for (i, c) in cells.iter().enumerate() {
            let slot = Slot::of(&c.key);
            let Some(&(cap, _)) = inner.caps.get(&slot) else {
                return Err(ReserveRefused::NoCap { cell: i as u32 });
            };
            let used = inner.used.get(&slot).copied().unwrap_or(0);
            let used = used.saturating_add(drawn.get(&slot).copied().unwrap_or(0));
            if exhausted(slot.dimension, used, c.amount, cap) {
                return Err(ReserveRefused::Exhausted { cell: i as u32 });
            }
            *drawn.entry(slot.clone()).or_default() += c.amount;
            slots.push(slot);
        }
        let mut granted = Vec::with_capacity(cells.len());
        for (c, slot) in cells.iter().zip(slots) {
            inner.next_slice += 1;
            let slice_id = inner.next_slice;
            let used = inner.used.entry(slot.clone()).or_default();
            *used = used.saturating_add(c.amount);
            inner.slices.insert(
                slice_id,
                Drawn {
                    slot,
                    left: c.amount,
                },
            );
            granted.push(Grant {
                slice_id,
                granted: c.amount,
                // Single node: nothing expires a slice no other node can draw against.
                valid_until_ms: u64::MAX,
            });
        }
        grants.extend(granted.iter().copied());
        inner.record(self.now(), op, body, Answer::Grants(granted));
        Ok(())
    }

    pub(crate) fn v3_slice_release(
        &self,
        op: OpId,
        epoch: u64,
        items: impl Iterator<Item = (u64, u64)> + Clone,
        released: &mut impl Extend<u64>,
    ) -> OpResult<()> {
        let items: Vec<(u64, u64)> = items.collect();
        let body = body(format_args!("slice_release:{epoch}:{items:?}"));
        let now = self.now();
        let answer = self.deduped(op, body, |_, inner| {
            if inner.closed.len() > 1024 {
                inner
                    .closed
                    .retain(|_, at| at.saturating_add(OP_ID_RETENTION_SECS) > now);
            }
            let known = |id: &u64| inner.slices.contains_key(id) || inner.closed.contains_key(id);
            if let Some((id, _)) = items.iter().find(|(id, _)| !known(id)) {
                return Err(OpRefused::Failed(format!(
                    "slice_release: slice {id} was never granted"
                )));
            }
            let mut back_all = Vec::with_capacity(items.len());
            for &(id, unspent) in &items {
                // A slice closed once its unreturned unspent reached 0 (by an earlier release, or an
                // earlier item of this call) returns nothing more.
                let Some(d) = inner.slices.get_mut(&id) else {
                    back_all.push(0);
                    continue;
                };
                let back = unspent.min(d.left);
                d.left -= back;
                let slot = d.slot.clone();
                // Nothing left to return, or nothing unspent to return it from: the slice closes
                // (its unit spent the rest; "it closes when nothing is left"). A slice on window 0
                // closes at its first release whatever it still holds (`abi::store` slice life (e),
                // a release on window 0 is final): window 0 never rolls and a node-local slice
                // never expires, so left open it would outlive its holder.
                if d.left == 0 || unspent == 0 || slot.window_start == 0 {
                    inner.slices.remove(&id);
                    inner.closed.insert(id, now);
                }
                if let Some(u) = inner.used.get_mut(&slot) {
                    *u = u.saturating_sub(back);
                }
                back_all.push(back);
            }
            Ok(Answer::Released(back_all))
        })?;
        match answer {
            Answer::Released(r) => {
                released.extend(r);
                Ok(())
            }
            _ => Err(OpRefused::Conflict),
        }
    }

    pub(crate) fn v3_add_usage_batch(
        &self,
        op: OpId,
        cells: &[(&str, u64, UsageDelta)],
    ) -> OpResult<()> {
        let body = body(format_args!("add_usage_batch:{cells:?}"));
        // A RAM ledger cannot fail an add, so applying in order is atomic.
        self.applied(op, body, |s| {
            cells
                .iter()
                .try_for_each(|(bucket, window, delta)| s.add_usage(bucket, *window, delta))
        })
    }

    pub(crate) fn v3_add_metering_batch(&self, op: OpId, deltas: &[MeteringDelta]) -> OpResult<()> {
        let body = body(format_args!("add_metering_batch:{deltas:?}"));
        self.applied(op, body, |s| {
            deltas.iter().try_for_each(|d| s.add_metering(d))
        })
    }

    pub(crate) fn v3_append_audit_batch(&self, op: OpId, entries: &[AuditRecord]) -> OpResult<()> {
        let body = body(format_args!("append_audit_batch:{entries:?}"));
        self.applied(op, body, |s| {
            s.audit_fits(entries)?;
            entries.iter().try_for_each(|e| s.append_audit(e))
        })
    }

    pub(crate) fn v3_window_caps(&self, op: OpId, caps: &[Cap<'_>]) -> Result<(), CapsRefused> {
        let body = body(format_args!("window_caps:{caps:?}"));
        let mut inner = self.v3.lock();
        match inner.seen(op, &body) {
            Seen::Replay(_) => return Ok(()),
            Seen::Conflict => return Err(CapsRefused::Conflict),
            Seen::New => {}
        }
        // Atomic per push: find the first conflict before applying any cap.
        let mut pushed: HashMap<Slot, (u64, u64)> = HashMap::new();
        for (index, c) in caps.iter().enumerate() {
            let slot = Slot::of(&c.key);
            let stored = pushed.get(&slot).or_else(|| inner.caps.get(&slot)).copied();
            match stored {
                Some((cap, gen)) if gen == c.config_gen && cap != c.cap => {
                    return Err(CapsRefused::CapConflict { index });
                }
                Some((_, gen)) if gen >= c.config_gen => {}
                _ => {
                    pushed.insert(slot, (c.cap, c.config_gen));
                }
            }
        }
        inner.caps.extend(pushed);
        inner.retire_rolled_windows();
        inner.record(self.now(), op, body, Answer::Done);
        Ok(())
    }
}

/// Each listed slot answers `Step::Ready` over the body named before its argument list.
macro_rules! ready {
    ($($name:ident => $via:ident::$body:ident ($($arg:ident: $ty:ty),*) -> $ret:ty;)*) => {$(
        fn $name(&self, _: &mut Op<'_>, $($arg: $ty),*) -> Step<$ret> {
            Step::Ready($via::$body(self, $($arg),*))
        }
    )*};
}

impl StoreSlots for MemoryStore {
    const TAIL: Tail = Tail {
        ephemeral: true,
        durable_plane: false,
        fork_refusal: true,
    };

    fn validate(settings: &[u8]) -> Result<(), String> {
        crate::check_settings(settings)
    }

    fn open(settings: &[u8], _host: Option<Host>) -> Result<Self, String> {
        crate::check_settings(settings)?;
        Ok(Self::new())
    }

    fn reserve<'c>(
        &self,
        _: &mut Op<'_>,
        op: OpId,
        epoch: u64,
        cells: impl Iterator<Item = Cell<'c>> + Clone,
        grants: &mut impl Extend<Grant>,
    ) -> Step<Result<(), ReserveRefused>> {
        Step::Ready(self.v3_reserve(op, epoch, cells, grants))
    }

    fn slice_release(
        &self,
        _: &mut Op<'_>,
        op: OpId,
        epoch: u64,
        items: impl Iterator<Item = (u64, u64)> + Clone,
        released: &mut impl Extend<u64>,
    ) -> Step<OpResult<()>> {
        Step::Ready(self.v3_slice_release(op, epoch, items, released))
    }

    ready! {
        add_usage_op => Self::v3_add_usage_op(op: OpId, bucket: &str, window_start: u64, delta: &UsageDelta) -> OpResult<()>;
        add_metering_op => Self::v3_add_metering_op(op: OpId, delta: &MeteringDelta) -> OpResult<()>;
        append_audit_op => Self::v3_append_audit_op(op: OpId, entry: &AuditRecord) -> OpResult<()>;
        append_plane_record_op => Self::v3_append_plane_record_op(op: OpId, record: PlaneRecordRef<'_>) -> OpResult<()>;
        append_batch => Self::v3_append_batch(op: OpId, stream: &str, records: &[RecordBytes]) -> OpResult<Head>;
        heads => Self::v3_heads() -> Result<Vec<(String, Head)>, String>;
        session_put => Self::v3_session_put(session: u64, node: &str, principal: &str) -> Result<(), String>;
        session_remove => Self::v3_session_remove(session: u64) -> Result<(), String>;
        sessions_for => Self::v3_sessions_for(principal: &str) -> Result<Vec<(u64, String)>, String>;
        record_put => Self::v3_record_put(schema: &str, key: &[u8], value: &[u8]) -> Result<(), String>;
        record_get => Self::v3_record_get(schema: &str, key: &[u8]) -> Result<Option<RecordBytes>, String>;
        record_scan => Self::v3_record_scan(schema: &str, prefix: &[u8], limit: u32) -> Result<Scanned, String>;
        add_usage_batch => Self::v3_add_usage_batch(op: OpId, cells: &[(&str, u64, UsageDelta)]) -> OpResult<()>;
        add_metering_batch => Self::v3_add_metering_batch(op: OpId, deltas: &[MeteringDelta]) -> OpResult<()>;
        append_audit_batch => Self::v3_append_audit_batch(op: OpId, entries: &[AuditRecord]) -> OpResult<()>;
        window_caps => Self::v3_window_caps(op: OpId, caps: &[Cap<'_>]) -> Result<(), CapsRefused>;
        put_key => RecordStore::put_key(key: &VirtualKey) -> RecordStoreResult<()>;
        get_key => RecordStore::get_key(id: &str) -> RecordStoreResult<Option<VirtualKey>>;
        list_keys => RecordStore::list_keys() -> RecordStoreResult<Vec<VirtualKey>>;
        delete_key => RecordStore::delete_key(id: &str) -> RecordStoreResult<()>;
        scrub_key => RecordStore::scrub_key(id: &str) -> RecordStoreResult<()>;
        list_keys_since => RecordStore::list_keys_since(since: u64) -> RecordStoreResult<Vec<VirtualKey>>;
        get_usage => RecordStore::get_usage(bucket_id: &str, window_start: u64) -> RecordStoreResult<UsageLedger>;
        put_usage => RecordStore::put_usage(bucket_id: &str, window_start: u64, ledger: &UsageLedger) -> RecordStoreResult<()>;
        list_metering => RecordStore::list_metering(bucket: u64) -> RecordStoreResult<Vec<MeteringRow>>;
        purge_windows_before => RecordStore::purge_windows_before(before: u64) -> RecordStoreResult<u64>;
        purge_metering_before => RecordStore::purge_metering_before(bucket: &str) -> RecordStoreResult<u64>;
        put_credential => RecordStore::put_credential(secret: &CredentialSecret) -> RecordStoreResult<()>;
        put_key_with_credential => RecordStore::put_key_with_credential(key: &VirtualKey, secret: &CredentialSecret) -> RecordStoreResult<()>;
        list_credentials => RecordStore::list_credentials(key_id: &str) -> RecordStoreResult<Vec<CredentialMeta>>;
        lookup_credential_secret => RecordStore::lookup_credential_secret(kind: &str, public_id: &str) -> RecordStoreResult<Option<CredentialSecret>>;
        revoke_credential => RecordStore::revoke_credential(id: &str, reason: &str) -> RecordStoreResult<()>;
        list_credentials_since => RecordStore::list_credentials_since(since: u64) -> RecordStoreResult<Vec<CredentialSecret>>;
        list_audit => RecordStore::list_audit() -> RecordStoreResult<Vec<AuditRecord>>;
        add_denylist => RecordStore::add_denylist(sub: &str, reason: &str) -> RecordStoreResult<()>;
        list_denylist => RecordStore::list_denylist() -> RecordStoreResult<Vec<String>>;
        list_audit_tail => RecordStore::list_audit_tail(limit: u64) -> RecordStoreResult<Vec<AuditRecord>>;
        upsert_plane_record => RecordStore::upsert_plane_record(record: PlaneRecordRef<'_>) -> RecordStoreResult<()>;
        get_plane_record => RecordStore::get_plane_record(kind: &str, id: &str) -> RecordStoreResult<Option<Vec<u8>>>;
        list_plane_records => RecordStore::list_plane_records(kind: &str, selector: &PlaneSelector<'_>) -> RecordStoreResult<Vec<Vec<u8>>>;
        list_plane_record_parents => RecordStore::list_plane_record_parents(kind: &str) -> RecordStoreResult<Vec<String>>;
        purge_plane_records_before => RecordStore::purge_plane_records_before(kind: &str, before: u64) -> RecordStoreResult<u64>;
        purge_plane_records => RecordStore::purge_plane_records(kind: &str, before: u64, retention: Retention) -> RecordStoreResult<u64>;
        delete_plane_record => RecordStore::delete_plane_record(kind: &str, id: &str) -> RecordStoreResult<()>;
        redeem_plane_token => RecordStore::redeem_plane_token(kind: &str, token: &str, expires_at: u64, now: u64) -> RecordStoreResult<bool>;
        plane_token_live => RecordStore::plane_token_live(kind: &str, token: &str, expires_at: u64, now: u64) -> RecordStoreResult<bool>;
    }
}
