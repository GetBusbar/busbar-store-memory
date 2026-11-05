// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The store v3 slots this backend answers beyond the 1.5.5 op set ([`StoreSlots`]): the
//! `op_id`-carrying count batches, plane records and the kernel-held records.

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, MutexGuard};

use busbar_contract::abi::sdk::conn::Host;
use busbar_contract::abi::sdk::store::conditional::{self, Decision, Expected, Mutation, Request};
use busbar_contract::abi::sdk::store::correction;
use busbar_contract::abi::sdk::store::dedupe::{Body, Dedupe, Seen};
use busbar_contract::abi::sdk::store::{Op, OpRefused, OpResult, Scanned, Step, StoreSlots, Tail};
use busbar_contract::abi::store::{OpId, OP_ID_RETENTION_SECS};
use busbar_contract::kinds::RecordBytes;
use busbar_contract::records::{
    AuditRecord, CredentialMeta, CredentialSecret, MeteringDelta, MeteringRow, PlaneRecordRef,
    PlaneSelector, RecordStore, RecordStoreResult, Retention, UsageDelta, UsageLedger, VirtualKey,
};

use crate::{read, write, MemoryStore, MeteringKey};

/// The settings paths this store declares may hold a secret reference (`secret_refs`): `token`, named
/// rather than `*` because busbar resolves a reference only where the Statement declares one.
const SECRET_REFS: &[busbar_contract::abi::mechanism::call::AbiStr] =
    &[busbar_contract::abi::sdk::door::abi_str("token")];

/// The most ops the host may hold in flight: the memory store answers every op inline and never
/// pends, so this only bounds a flood.
const MAX_INFLIGHT: u32 = 1024;

busbar_contract::statement_extensions!(EXTENSIONS: [
    (busbar_contract::abi::sdk::store::rewrap::INTENT, b"1"),
    (conditional::CAPABILITY, b"1"),
    ("busbar.store.count-scale", b"6"),
    (correction::CAPABILITY, correction::CAPABILITY_VALUE)
]);

/// The settings accepted by this plugin's lifecycle parser.
const SETTINGS_SCHEMA: busbar_contract::abi::mechanism::call::Blob =
    busbar_contract::abi::mechanism::call::Blob::json(include_bytes!("settings.schema.json"));

busbar_contract::store_door!(
    MemoryStore,
    "memory",
    env!("CARGO_PKG_VERSION"),
    MAX_INFLIGHT,
    extensions: EXTENSIONS,
    secret_refs: SECRET_REFS,
    settings_schema: SETTINGS_SCHEMA
);

#[derive(Default)]
struct Inner {
    ops: HashMap<OpId, Body>,
    /// `(recorded_at, op_id)`, oldest first: the retention sweep's order.
    aged: VecDeque<(u64, OpId)>,
}

/// A point inside an atomic v3 op at which every guard the op takes is still held.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stage {
    /// A usage correction has compared its whole pre-state and written nothing yet.
    Compared,
    /// Mutation `n` (from 0) of a correction or record transaction is applied.
    Mutation(usize),
    /// Every mutation is applied and the op's receipt is not yet recorded.
    BeforeReceipt,
    /// One cell or delta of a count batch is applied.
    BatchItem,
}

/// The v3 state [`MemoryStore`] carries: the dedupe log, under ONE lock that an op's check, effect
/// and receipt all hold.
pub(crate) struct State {
    inner: Mutex<Inner>,
    started: std::time::Instant,
    /// Called at each [`Stage`]; a no-op unless a test observes the op's critical section.
    probe: fn(&MemoryStore, Stage),
}

impl Default for State {
    fn default() -> Self {
        Self {
            inner: Mutex::default(),
            started: std::time::Instant::now(),
            probe: |_, _| {},
        }
    }
}

impl Inner {
    /// Remember an APPLIED op, and forget every op past its retention.
    fn record(&mut self, now: u64, op: OpId, body: Body) {
        while let Some(&(at, old)) = self.aged.front() {
            if at.saturating_add(OP_ID_RETENTION_SECS) > now {
                break;
            }
            self.aged.pop_front();
            self.ops.remove(&old);
        }
        self.ops.insert(op, body);
        let position = self.aged.partition_point(|(at, _)| *at <= now);
        self.aged.insert(position, (now, op));
    }
}

impl State {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }
}

fn refused(e: busbar_contract::records::RecordStoreError) -> OpRefused {
    OpRefused::Refused(e.0)
}

fn failed(e: busbar_contract::records::RecordStoreError) -> OpRefused {
    OpRefused::Failed(e.0)
}

impl MemoryStore {
    /// Receipt age uses elapsed process time so wall-clock corrections cannot expire dedupe early.
    fn receipt_now(&self) -> u64 {
        match self.clock.load(std::sync::atomic::Ordering::Relaxed) {
            0 => self.v3.started.elapsed().as_secs(),
            pinned => pinned,
        }
    }

    /// Run one `op_id`-carrying write [`Dedupe::run`] under the v3 lock; a new op is remembered
    /// only if it applied.
    fn deduped(
        &self,
        op: OpId,
        d: Dedupe<'_>,
        apply: impl FnOnce(&Self) -> RecordStoreResult<()>,
    ) -> OpResult<()> {
        d.run(
            &mut *self.v3.lock(),
            |i| Ok(i.ops.get(&op).copied()),
            |i, body| {
                apply(self).map_err(failed)?;
                i.record(self.receipt_now(), op, *body);
                Ok(())
            },
        )
    }
}

/// Validated, owned mutation inputs: no recoverable validation remains under the records guard.
enum RecordMutation {
    Put((String, Vec<u8>), RecordBytes),
    Delete((String, Vec<u8>)),
    DeleteRange {
        schema: String,
        prefix: Vec<u8>,
        exact_key_length: usize,
    },
}

/// The store v3 slots' own bodies: each op's ONE body, which the table's slot answers inline (the
/// memory store never pends).
impl MemoryStore {
    /// Compare the complete pre-state while holding the guards every ordinary writer uses.
    pub(crate) fn v3_apply_usage_correction(
        &self,
        request: &correction::Request,
    ) -> OpResult<correction::Decision> {
        use busbar_contract::records::correction::{canonical_ledger, canonical_metering};

        request
            .validate()
            .map_err(|reason| OpRefused::Refused(reason.into()))?;
        let dedupe = Dedupe::usage_correction(request);
        let mut normalized_request = request.clone();
        for row in &mut normalized_request.metering {
            row.normalize_scale6()
                .map_err(|e| OpRefused::Refused(e.to_string()))?;
        }
        for change in &mut normalized_request.usage {
            change
                .expected
                .normalize_scale6()
                .map_err(|e| OpRefused::Refused(e.to_string()))?;
            change
                .replacement
                .normalize_scale6()
                .map_err(|e| OpRefused::Refused(e.to_string()))?;
        }
        let request = &normalized_request;
        let mut inner = self.v3.lock();
        match dedupe.seen(inner.ops.get(&request.op_id).map(|body| body.as_slice())) {
            Seen::Replay => return Ok(correction::Decision::Applied),
            Seen::Conflict => return Err(OpRefused::Conflict),
            Seen::New => {}
        }
        let keys = read(&self.keys);
        if !keys
            .get(&request.key_id)
            .is_some_and(|key| key.deleted_at.is_none())
        {
            return Ok(correction::Decision::ConditionConflict);
        }
        let metering = read(&self.metering);
        let first: MeteringKey = (
            request.day_start,
            request.key_id.clone(),
            String::new(),
            String::new(),
            0,
            String::new(),
            String::new(),
        );
        let mut current_metering: Vec<_> = metering
            .range(first..)
            .take_while(|((day, key, ..), _)| *day == request.day_start && key == &request.key_id)
            .map(|(_, row)| row.clone())
            .collect();
        canonical_metering(&mut current_metering);
        if current_metering != request.metering {
            return Ok(correction::Decision::ConditionConflict);
        }
        let mut usage = write(&self.usage);
        for change in &request.usage {
            let mut current = usage
                .get(&(
                    change.address.bucket_id.clone(),
                    change.address.window_start,
                ))
                .cloned()
                .unwrap_or_default();

            canonical_ledger(&mut current).map_err(|reason| {
                OpRefused::Refused(format!(
                    "usage correction: the stored usage row {:?} at window {} cannot be compared: {reason}",
                    change.address.bucket_id, change.address.window_start
                ))
            })?;
            if current != change.expected {
                return Ok(correction::Decision::ConditionConflict);
            }
        }
        (self.v3.probe)(self, Stage::Compared);
        for (index, change) in request.usage.iter().enumerate() {
            usage.insert(
                (
                    change.address.bucket_id.clone(),
                    change.address.window_start,
                ),
                change.replacement.clone(),
            );
            (self.v3.probe)(self, Stage::Mutation(index));
        }
        (self.v3.probe)(self, Stage::BeforeReceipt);
        inner.record(self.receipt_now(), request.op_id, *dedupe.body());
        drop(usage);
        drop(metering);
        drop(keys);
        Ok(correction::Decision::Applied)
    }

    pub(crate) fn v3_append_plane_record_op(
        &self,
        op: OpId,
        record: PlaneRecordRef<'_>,
    ) -> OpResult<()> {
        crate::check_plane("append_plane_record", record).map_err(refused)?;
        self.deduped(op, Dedupe::append(record), |s| {
            s.append_plane_record(record)
        })
    }

    /// Applied-only conditional records over the SAME map and guard as ordinary record access.
    pub(crate) fn v3_record_transaction(&self, request: &Request) -> OpResult<Decision> {
        request
            .validate()
            .map_err(|reason| OpRefused::Refused(reason.into()))?;
        let dedupe = Dedupe::record_transaction(request);
        let mut inner = self.v3.lock();
        match dedupe.seen(inner.ops.get(&request.op_id).map(|body| body.as_slice())) {
            Seen::Replay => return Ok(Decision::Applied),
            Seen::Conflict => return Err(OpRefused::Conflict),
            Seen::New => {}
        }

        let mut prepared = Vec::with_capacity(request.mutations.len());
        for mutation in &request.mutations {
            prepared.push(match mutation {
                Mutation::Put { schema, key, value } => RecordMutation::Put(
                    (schema.clone(), key.clone()),
                    RecordBytes::new(value.clone()).map_err(|_| {
                        OpRefused::Refused(
                            "record transaction value exceeds record byte bound".into(),
                        )
                    })?,
                ),
                Mutation::Delete { schema, key } => {
                    RecordMutation::Delete((schema.clone(), key.clone()))
                }
                Mutation::DeleteRange {
                    schema,
                    prefix,
                    exact_key_length,
                } => RecordMutation::DeleteRange {
                    schema: schema.clone(),
                    prefix: prefix.clone(),
                    exact_key_length: usize::try_from(*exact_key_length).map_err(|_| {
                        OpRefused::Refused(
                            "record transaction range length cannot be represented".into(),
                        )
                    })?,
                },
            });
        }
        let predicates: Vec<_> = request
            .predicates
            .iter()
            .map(|predicate| {
                (
                    (predicate.schema.clone(), predicate.key.clone()),
                    &predicate.expected,
                )
            })
            .collect();
        let mut records = write(&self.records);
        for (address, expected) in predicates {
            let current = records.get(&address);
            let matches = match expected {
                Expected::Absent => current.is_none(),
                Expected::Bytes(bytes) => {
                    current.is_some_and(|value| value.as_slice() == bytes.as_slice())
                }
            };
            if !matches {
                return Ok(Decision::ConditionConflict);
            }
        }
        for (index, mutation) in prepared.into_iter().enumerate() {
            match mutation {
                RecordMutation::Put(address, value) => {
                    records.insert(address, value);
                }
                RecordMutation::Delete(address) => {
                    records.remove(&address);
                }
                RecordMutation::DeleteRange {
                    schema,
                    prefix,
                    exact_key_length,
                } => {
                    // Seek only this schema/prefix interval. Journal appends delete the
                    // parts of one address first; walking every retained record here made
                    // a growing journal require quadratic work, even for an absent address.
                    // Advance an exclusive cursor before removing each key so memory stays
                    // bounded even for an empty prefix selecting a whole schema.
                    use std::ops::Bound::{Excluded, Included, Unbounded};
                    let mut cursor = Included((schema.clone(), prefix.clone()));
                    while let Some((address, _)) = records.range((cursor, Unbounded)).next() {
                        if address.0 != schema || !address.1.starts_with(&prefix) {
                            break;
                        }
                        let address = address.clone();
                        if address.1.len() == exact_key_length {
                            records.remove(&address);
                        }
                        cursor = Excluded(address);
                    }
                }
            }
            (self.v3.probe)(self, Stage::Mutation(index));
        }
        (self.v3.probe)(self, Stage::BeforeReceipt);
        inner.record(self.receipt_now(), request.op_id, *dedupe.body());

        drop(records);
        Ok(Decision::Applied)
    }

    pub(crate) fn v3_purge_plane_records_before(
        &self,
        kind: &str,
        before: u64,
    ) -> RecordStoreResult<u64> {
        RecordStore::purge_plane_records(self, kind, before, Retention::Age)
    }

    pub(crate) fn v3_record_put(
        &self,
        schema: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), String> {
        use busbar_contract::bounded::MAX_RECORD_BYTES;
        let over_limit =
            |n| format!("record_put: value is {n} bytes, over the {MAX_RECORD_BYTES}-byte limit");
        if value.len() > MAX_RECORD_BYTES {
            return Err(over_limit(value.len()));
        }
        let value = RecordBytes::new(value.to_vec()).map_err(over_limit)?;
        let mut records = write(&self.records);
        let at = (schema.to_string(), key.to_vec());
        if value.as_slice().is_empty() {
            records.remove(&at);
        } else {
            records.insert(at, value);
        }
        Ok(())
    }

    pub(crate) fn v3_record_get(
        &self,
        schema: &str,
        key: &[u8],
    ) -> Result<Option<RecordBytes>, String> {
        Ok(read(&self.records)
            .get(&(schema.to_string(), key.to_vec()))
            .cloned())
    }

    pub(crate) fn v3_record_scan(
        &self,
        schema: &str,
        prefix: &[u8],
        limit: u32,
    ) -> Result<Vec<(Vec<u8>, RecordBytes)>, String> {
        let from = (schema.to_string(), prefix.to_vec());
        Ok(read(&self.records)
            .range(from..)
            .take_while(|((s, k), _)| s == schema && k.starts_with(prefix))
            .take(limit as usize)
            .map(|((_, k), value)| (k.clone(), value.clone()))
            .collect())
    }

    pub(crate) fn v3_add_usage_batch(
        &self,
        op: OpId,
        cells: &[(&str, u64, UsageDelta)],
    ) -> OpResult<()> {
        let dedupe = Dedupe::usage(cells);
        let mut inner = self.v3.lock();

        match dedupe.seen(inner.ops.get(&op).map(|body| body.as_slice())) {
            Seen::Replay => return Ok(()),
            Seen::Conflict => return Err(OpRefused::Conflict),
            Seen::New => {}
        }
        for (bucket, _, _) in cells {
            Self::check_legacy_usage_bucket(bucket).map_err(refused)?;
        }
        let mut normalized = cells.to_vec();
        for (_, _, delta) in &mut normalized {
            delta
                .normalize_scale6()
                .map_err(|e| OpRefused::Refused(e.to_string()))?;
        }
        let cells = &normalized;
        let mut usage = write(&self.usage);
        let mut staged = std::collections::HashMap::new();
        for (bucket, window, _) in cells {
            let key = (bucket.to_string(), *window);
            if let Some(row) = usage.get(&key) {
                staged.insert(key, row.clone());
            }
        }
        for (bucket, window, delta) in cells {
            Self::apply_usage(&mut staged, bucket, *window, delta).map_err(refused)?;
        }
        let mut sweep = false;
        for (key, row) in staged {
            usage.insert(key, row);
            sweep |= crate::sweep_due(&self.usage_sweep_ticker);
            (self.v3.probe)(self, Stage::BatchItem);
        }

        if sweep {
            self.sweep_usage(&mut usage);
        }
        inner.record(self.receipt_now(), op, *dedupe.body());
        drop(usage);
        Ok(())
    }

    pub(crate) fn v3_purge_usage_before(&self, before: u64) -> RecordStoreResult<u64> {
        let mut usage = write(&self.usage);
        let held = usage.len();
        usage.retain(|(_, window), _| *window == 0 || *window >= before);
        Ok((held - usage.len()) as u64)
    }

    pub(crate) fn v3_add_metering_batch(&self, op: OpId, deltas: &[MeteringDelta]) -> OpResult<()> {
        deltas
            .iter()
            .try_for_each(|d| crate::check_metering("add_metering_batch", d))
            .map_err(refused)?;
        let dedupe = Dedupe::metering(deltas);
        let mut normalized = deltas.to_vec();
        for delta in &mut normalized {
            delta
                .normalize_scale6()
                .map_err(|e| OpRefused::Refused(e.to_string()))?;
        }
        let deltas = &normalized;
        self.deduped(op, dedupe, |s| {
            let mut metering = write(&s.metering);
            let mut staged = std::collections::BTreeMap::new();
            for d in deltas {
                let key = (
                    d.bucket,
                    d.key_id.clone(),
                    d.model.clone(),
                    d.provider.clone(),
                    d.priced_from_ms,
                    d.key_group_at_use.clone(),
                    d.pricing_version.clone(),
                );
                if let Some(row) = metering.get(&key) {
                    staged.insert(key, row.clone());
                }
            }
            for d in deltas {
                Self::apply_metering(&mut staged, d)?;
            }
            for (key, row) in staged {
                metering.insert(key, row);
                (s.v3.probe)(s, Stage::BatchItem);
            }
            Ok(())
        })
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
    const RECORD_TRANSACTION: bool = true;
    const USAGE_CORRECTION: bool = true;

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

    ready! {
        append_plane_record_op => Self::v3_append_plane_record_op(op: OpId, record: PlaneRecordRef<'_>) -> OpResult<()>;
        record_transaction => Self::v3_record_transaction(request: &Request) -> OpResult<Decision>;
        apply_usage_correction => Self::v3_apply_usage_correction(request: &correction::Request) -> OpResult<correction::Decision>;
        record_put => Self::v3_record_put(schema: &str, key: &[u8], value: &[u8]) -> Result<(), String>;
        record_get => Self::v3_record_get(schema: &str, key: &[u8]) -> Result<Option<RecordBytes>, String>;
        record_scan => Self::v3_record_scan(schema: &str, prefix: &[u8], limit: u32) -> Result<Scanned, String>;
        add_usage_batch => Self::v3_add_usage_batch(op: OpId, cells: &[(&str, u64, UsageDelta)]) -> OpResult<()>;
        add_metering_batch => Self::v3_add_metering_batch(op: OpId, deltas: &[MeteringDelta]) -> OpResult<()>;
        put_key => RecordStore::put_key(key: &VirtualKey) -> RecordStoreResult<()>;
        get_key => RecordStore::get_key(id: &str) -> RecordStoreResult<Option<VirtualKey>>;
        list_keys => RecordStore::list_keys() -> RecordStoreResult<Vec<VirtualKey>>;
        delete_key => RecordStore::delete_key(id: &str) -> RecordStoreResult<()>;
        list_keys_since => RecordStore::list_keys_since(since: u64) -> RecordStoreResult<Vec<VirtualKey>>;
        get_usage => RecordStore::get_usage(bucket_id: &str, window_start: u64) -> RecordStoreResult<UsageLedger>;
        put_usage => RecordStore::put_usage(bucket_id: &str, window_start: u64, ledger: &UsageLedger) -> RecordStoreResult<()>;
        purge_usage_before => Self::v3_purge_usage_before(before: u64) -> RecordStoreResult<u64>;
        list_metering => RecordStore::list_metering(bucket: u64) -> RecordStoreResult<Vec<MeteringRow>>;
        put_credential => RecordStore::put_credential(secret: &CredentialSecret) -> RecordStoreResult<()>;
        rewrap_credential => RecordStore::rewrap_credential(expected: &CredentialSecret, replacement: &str) -> RecordStoreResult<bool>;
        put_key_with_credential => RecordStore::put_key_with_credential(key: &VirtualKey, secret: &CredentialSecret) -> RecordStoreResult<()>;
        list_credentials => RecordStore::list_credentials(key_id: &str) -> RecordStoreResult<Vec<CredentialMeta>>;
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
        purge_plane_records_before => Self::v3_purge_plane_records_before(kind: &str, before: u64) -> RecordStoreResult<u64>;
        purge_plane_records => RecordStore::purge_plane_records(kind: &str, before: u64, retention: Retention) -> RecordStoreResult<u64>;
        delete_plane_record => RecordStore::delete_plane_record(kind: &str, id: &str) -> RecordStoreResult<()>;
        redeem_plane_token => RecordStore::redeem_plane_token(kind: &str, token: &str, expires_at: u64, now: u64) -> RecordStoreResult<bool>;
        plane_token_live => RecordStore::plane_token_live(kind: &str, token: &str, expires_at: u64, now: u64) -> RecordStoreResult<bool>;
    }
}

#[cfg(test)]
#[path = "tests/usage_correction_tests.rs"]
mod usage_correction_tests;

#[cfg(test)]
#[path = "settings_schema_tests.rs"]
mod settings_schema_tests;
