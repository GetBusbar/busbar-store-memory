// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The store v3 slots this backend answers beyond the 1.5.5 op set ([`StoreSlots`]): the
//! `op_id`-carrying count batches, plane records and the kernel-held records. It holds no money:
//! money is counts x the rate card, derived in busbar's core.
//!
//! DEDUPE (`abi::store` S1-S4, [`Dedupe`]): every `op_id` this store APPLIED is remembered with
//! its [`Body`] for [`OP_ID_RETENTION_SECS`]. The dedupe log lives under ONE lock, and an op's
//! check, effect and record happen under it, so two racing calls with one `op_id` apply once. The
//! log is lost on restart together with everything it guards.

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, MutexGuard};

use busbar_contract::abi::sdk::conn::Host;
use busbar_contract::abi::sdk::store::dedupe::{Body, Dedupe};
use busbar_contract::abi::sdk::store::{Op, OpRefused, OpResult, Scanned, Step, StoreSlots, Tail};
use busbar_contract::abi::store::{OpId, OP_ID_RETENTION_SECS};
use busbar_contract::kinds::RecordBytes;
use busbar_contract::records::{
    AuditRecord, CredentialMeta, CredentialSecret, MeteringDelta, MeteringRow, PlaneRecordRef,
    PlaneSelector, RecordStore, RecordStoreResult, Retention, UsageDelta, UsageLedger, VirtualKey,
};

use crate::{read, write, MemoryStore};

/// The settings paths this store declares may hold a secret reference (`secret_refs`): `token`. The
/// store reads no settings; `token` is the key a 1.5.5 store config held a reference at (busbar oracle
/// BOOT-W15: an unresolved reference there warns and boots). busbar resolves a reference only where
/// the Statement declares one and refuses it anywhere else (G14: each plugin declares its secret
/// fields), so the store names its field rather than `*`.
const SECRET_REFS: &[busbar_contract::abi::mechanism::call::AbiStr] =
    &[busbar_contract::abi::sdk::door::abi_str("token")];

busbar_contract::store_door!(
    MemoryStore,
    "memory",
    env!("CARGO_PKG_VERSION"),
    1024,
    secret_refs: SECRET_REFS
);

#[derive(Default)]
struct Inner {
    ops: HashMap<OpId, Body>,
    /// `(recorded_at, op_id)`, oldest first: the retention sweep's order.
    aged: VecDeque<(u64, OpId)>,
}

/// The v3 state [`MemoryStore`] carries.
#[derive(Default)]
pub(crate) struct State {
    inner: Mutex<Inner>,
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
        self.aged.push_back((now, op));
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
}

#[cfg(test)]
impl State {
    /// Test-only: what this state holds ([`Held`]).
    pub(crate) fn held(&self) -> Held {
        let inner = self.lock();
        Held {
            op_bytes: inner.ops.values().map(|b| b.len()).sum(),
        }
    }
}

fn refused(e: busbar_contract::records::RecordStoreError) -> OpRefused {
    OpRefused::Refused(e.0)
}

fn failed(e: busbar_contract::records::RecordStoreError) -> OpRefused {
    OpRefused::Failed(e.0)
}

impl MemoryStore {
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
                i.record(self.now(), op, *body);
                Ok(())
            },
        )
    }
}

/// The store v3 slots' own bodies: each op's ONE body, which the table's slot answers inline (the
/// memory store never pends).
impl MemoryStore {
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

    pub(crate) fn v3_record_put(
        &self,
        schema: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), String> {
        // The store keeps the record, so it copies it here (the door lends the host's bytes).
        let value = RecordBytes::new(value.to_vec())
            .map_err(|n| format!("a record of {n} bytes is over the ceiling"))?;
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
        // Seek to the first key at or after `(schema, prefix)` and stop at the first past the prefix.
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
        // A RAM ledger cannot fail an add, so applying in order is atomic.
        self.deduped(op, Dedupe::usage(cells), |s| {
            cells
                .iter()
                .try_for_each(|(bucket, window, delta)| s.add_usage(bucket, *window, delta))
        })
    }

    pub(crate) fn v3_add_metering_batch(&self, op: OpId, deltas: &[MeteringDelta]) -> OpResult<()> {
        deltas
            .iter()
            .try_for_each(|d| crate::check_metering("add_metering_batch", d))
            .map_err(refused)?;
        self.deduped(op, Dedupe::metering(deltas), |s| {
            deltas.iter().try_for_each(|d| s.add_metering(d))
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
        list_metering => RecordStore::list_metering(bucket: u64) -> RecordStoreResult<Vec<MeteringRow>>;
        put_credential => RecordStore::put_credential(secret: &CredentialSecret) -> RecordStoreResult<()>;
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
        purge_plane_records_before => RecordStore::purge_plane_records_before(kind: &str, before: u64) -> RecordStoreResult<u64>;
        purge_plane_records => RecordStore::purge_plane_records(kind: &str, before: u64, retention: Retention) -> RecordStoreResult<u64>;
        delete_plane_record => RecordStore::delete_plane_record(kind: &str, id: &str) -> RecordStoreResult<()>;
        redeem_plane_token => RecordStore::redeem_plane_token(kind: &str, token: &str, expires_at: u64, now: u64) -> RecordStoreResult<bool>;
        plane_token_live => RecordStore::plane_token_live(kind: &str, token: &str, expires_at: u64, now: u64) -> RecordStoreResult<bool>;
    }
}
