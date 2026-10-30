// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The in-memory (RAM) `kind: store` backend, key `memory`: zero setup, nothing to run outside the
//! process. A config names it; it is not a default. EPHEMERAL: every counter, key, and
//! credential is lost on restart; configure a durable backend for persistence. Locks recover from
//! poison (the governance surface must never panic on a request).
//!
//! [`door`] is what a build that links this crate registers ([`linked::STORE`]) and what the
//! dropped-in `cdylib` exports. The export itself (`export_door!`, whose C-ABI symbol needs
//! `unsafe`) lives in the sibling `busbar-store-memory-plugin` crate, so this crate carries none.

#![forbid(unsafe_code)]

use busbar_contract::abi::sdk::store::fork;
use busbar_contract::ids::RecordSchemaId;
use busbar_contract::kinds::{RecordBytes, StoreError as ContractStoreError};
use busbar_contract::records::{
    AuditRecord, CredentialMeta, CredentialSecret, MeteringDelta, MeteringRow, PlaneDisposition,
    PlaneRecord, PlaneRecordRef, PlaneSelector, RecordStore as Store,
    RecordStoreError as StoreError, RecordStoreResult as StoreResult, Retention, UsageDelta,
    UsageLedger, VirtualKey,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{SystemTime, UNIX_EPOCH};

/// Retention ceiling for `usage`/`metering` rows, keyed by their epoch-second period-start field
/// (`window_start` / `bucket`): 31 days, the governance engine's own `max_window` for its in-memory
/// rate cells, which this store's ledgers shadow.
const MAX_RETENTION_SECS: u64 = 31 * 86_400;

/// One `retain()` sweep per this many writes to a map.
const SWEEP_INTERVAL: u64 = 256;

/// The `records` map: `(schema, key) -> body`, in KEY ORDER because `record_scan` promises a
/// prefix walk answers in it.
type RecordMap = BTreeMap<(String, Vec<u8>), RecordBytes>;

/// A metering row's key: (key_id, bucket, model, provider, priced_from_ms).
type MeteringKey = (String, u64, String, String, u64);

/// A plane record's key: `(kind, identity, seq)`.
type PlaneKey = (String, String, u64);

/// The `seq` an upserted plane record is stored at (it is keyed by `(kind, id)`).
const UPSERT_SEQ: u64 = 0;

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|e| e.into_inner())
}

fn write<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(|e| e.into_inner())
}

/// Count one write on `ticker`; true on every [`SWEEP_INTERVAL`]th.
fn sweep_due(ticker: &AtomicU64) -> bool {
    ticker
        .fetch_add(1, Ordering::Relaxed)
        .wrapping_add(1)
        .is_multiple_of(SWEEP_INTERVAL)
}

/// In-memory `Store`. Credential lookups by `(kind, public_id)` and the per-key listing and
/// cascade are scans over the one `creds` map, not secondary indexes: the RAM backend holds a
/// fixture-sized table, and a second map to keep in step is more failure surface than the scan
/// costs.
#[derive(Default)]
pub struct MemoryStore {
    keys: RwLock<HashMap<String, VirtualKey>>,
    creds: RwLock<HashMap<String, CredentialSecret>>,
    usage: RwLock<HashMap<(String, u64), UsageLedger>>,
    metering: RwLock<HashMap<MeteringKey, MeteringRow>>,
    /// Denied subject ids (the reason is audit-only, not needed for the enforcement read).
    denylist: RwLock<HashSet<String>>,
    /// The spent-token ledger behind `redeem_plane_token`: `(kind, token) -> expires_at`. A live
    /// row means "already redeemed"; a lapsed row is vacant to its own key.
    plane_tokens: RwLock<HashMap<(String, String), u64>>,
    /// Plane records by `(kind, identity, seq)`. The identity is the record's `parent` for appended
    /// child kinds and its `id` for upserted ones, which take `seq` 0.
    plane_records: RwLock<HashMap<PlaneKey, PlaneRecord>>,
    /// A plane's kernel-held durable records, by `(schema, key)`: a second map beside
    /// `plane_records` because these verbs key on opaque bytes, not `(kind, id, seq)`.
    ///
    /// NEVER AGED OUT: this backend cannot tell a settled row from an active work handle, and a row
    /// it cannot prove settled is one it may not drop. Only the host removes a row, by a
    /// `record_put` of an EMPTY value.
    records: RwLock<RecordMap>,
    /// The admin audit log, by `seq`.
    audit: RwLock<BTreeMap<u64, AuditRecord>>,
    /// Write counters driving each map's amortized sweep (`records` has none: never aged out).
    usage_sweep_ticker: AtomicU64,
    metering_sweep_ticker: AtomicU64,
    tokens_sweep_ticker: AtomicU64,
    /// The store-global monotonic revision counter (see `VirtualKey::revision`), bumped on every
    /// `keys`/`creds` mutation.
    revision: AtomicU64,
    /// Test-only pinned clock: `0` means the wall clock, otherwise `self.now()` answers this.
    #[cfg(test)]
    clock: AtomicU64,
    /// The store v3 slots' own state ([`v3`]).
    v3: v3::State,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Write one of a plane's kernel-held durable records; an EMPTY value removes the row.
    ///
    /// These are inherent rather than a trait implementation: the contract's `StoreSlots` is the
    /// whole store table, and what is here is the record half at the contract's own spelling.
    ///
    /// # Errors
    ///
    /// Never, for a RAM backend; the result is the contract's shape so a durable one can answer.
    pub fn record_put(
        &self,
        schema: RecordSchemaId,
        key: &[u8],
        value: &RecordBytes,
    ) -> Result<(), ContractStoreError> {
        self.v3_record_put(schema.as_str(), key, value.as_slice())
            .map_err(ContractStoreError::Rejected)
    }

    /// Read one of a plane's kernel-held durable records (`Ok(None)` when absent).
    ///
    /// # Errors
    ///
    /// Never, for a RAM backend.
    pub fn record_get(
        &self,
        schema: RecordSchemaId,
        key: &[u8],
    ) -> Result<Option<RecordBytes>, ContractStoreError> {
        self.v3_record_get(schema.as_str(), key)
            .map_err(ContractStoreError::Rejected)
    }

    /// Walk a plane's records under a prefix, in key order, at most `limit` of them. `limit` 0
    /// means NOTHING, not everything: a miscomputed bound must not return the entire schema.
    ///
    /// # Errors
    ///
    /// Never, for a RAM backend.
    pub fn record_scan(
        &self,
        schema: RecordSchemaId,
        prefix: &[u8],
        limit: u32,
    ) -> Result<Vec<(Vec<u8>, RecordBytes)>, ContractStoreError> {
        self.v3_record_scan(schema.as_str(), prefix, limit)
            .map_err(ContractStoreError::Rejected)
    }

    fn next_revision(&self) -> u64 {
        self.revision.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// "Now" as the retention sweep sees it: the pinned test clock if set, else the wall clock.
    fn now(&self) -> u64 {
        #[cfg(test)]
        if let pinned @ 1.. = self.clock.load(Ordering::Relaxed) {
            return pinned;
        }
        now()
    }

    /// An APPENDED record's position in the one map: its chain (its `parent`, else its own `id`)
    /// at its `seq`.
    fn chain_key(record: PlaneRecordRef<'_>) -> PlaneKey {
        let identity = record.parent.unwrap_or(record.id).to_string();
        (record.kind.to_string(), identity, record.seq)
    }

    /// An UPSERTED record's key in the one map: its own `id`, parent or not (the contract upserts
    /// by `(kind, id)`), so `get_plane_record`/`delete_plane_record` reach it by that id.
    fn upsert_key(record: PlaneRecordRef<'_>) -> PlaneKey {
        (record.kind.to_string(), record.id.to_string(), UPSERT_SEQ)
    }

    /// The two credential-table preconditions, shared by `put_credential` and the atomic
    /// `put_key_with_credential`.
    fn credential_preconditions(
        creds: &HashMap<String, CredentialSecret>,
        secret: &CredentialSecret,
    ) -> StoreResult<()> {
        let meta = &secret.meta;
        // An explicit slot pointed at a LIVE credential of the same (key_id, kind) would silently
        // clobber a working credential mid-overlap-window.
        let occupied = creds.values().any(|c| {
            c.meta.id != meta.id
                && c.meta.key_id == meta.key_id
                && c.meta.kind == meta.kind
                && c.meta.slot == meta.slot
                && c.meta.revoked_at.is_none()
        });
        if occupied {
            return Err(StoreError(format!(
                "put_credential: slot {} for key '{}' kind '{}' holds a live credential; revoke it first",
                meta.slot, meta.key_id, meta.kind
            )));
        }
        // UNIQUE(kind, public_id), even across keys: a public_id is a global lookup handle.
        let public_id_taken = creds.values().any(|c| {
            c.meta.id != meta.id && c.meta.kind == meta.kind && c.meta.public_id == meta.public_id
        });
        if public_id_taken {
            return Err(StoreError(format!(
                "put_credential: public_id '{}' is already in use for kind '{}'",
                meta.public_id, meta.kind
            )));
        }
        Ok(())
    }

    /// Insert `secret`, replacing a REVOKED row in the same `(key_id, kind, slot)` (the contract
    /// upserts on that triple; a live occupant was refused by the preconditions).
    fn insert_credential(creds: &mut HashMap<String, CredentialSecret>, secret: CredentialSecret) {
        let meta = &secret.meta;
        creds.retain(|_, c| {
            !(c.meta.id != meta.id
                && c.meta.key_id == meta.key_id
                && c.meta.kind == meta.kind
                && c.meta.slot == meta.slot)
        });
        creds.insert(secret.meta.id.clone(), secret);
    }

    /// Test-only: pin `self.now()` to `t`.
    #[cfg(test)]
    fn pin_clock(&self, t: u64) {
        self.clock.store(t, Ordering::Relaxed);
    }
}

impl Store for MemoryStore {
    fn put_key(&self, key: &VirtualKey) -> StoreResult<()> {
        let mut key = key.clone();
        let mut keys = write(&self.keys);
        // Tested and applied under one guard: a live-shaped write over a tombstoned row would
        // resurrect a revoked key, and a read-then-write check in the caller cannot close that.
        if key.deleted_at.is_none() && keys.get(&key.id).is_some_and(|k| k.deleted_at.is_some()) {
            return Err(StoreError(format!(
                "put_key: '{}' is tombstoned and its id is never reissued; refusing to \
                 clear the tombstone",
                key.id
            )));
        }
        key.revision = self.next_revision();
        keys.insert(key.id.clone(), key);
        Ok(())
    }

    fn get_key(&self, id: &str) -> StoreResult<Option<VirtualKey>> {
        Ok(read(&self.keys).get(id).cloned())
    }

    /// Unfiltered: tombstones are included, since the default `list_keys_since` needs them.
    fn list_keys(&self) -> StoreResult<Vec<VirtualKey>> {
        let mut v: Vec<VirtualKey> = read(&self.keys).values().cloned().collect();
        v.sort_by_key(|k| k.created_at);
        Ok(v)
    }

    fn delete_key(&self, id: &str) -> StoreResult<()> {
        // TOMBSTONE: the key row survives (billing and audit attribution keep resolving, the id is
        // never reissued); only its credentials and `usage` ledger go. All three write guards are
        // held for the whole cascade, in a fixed order (keys, usage, creds), so a concurrent
        // `add_usage` cannot resurrect a ledger row in the gap.
        let mut keys = write(&self.keys);
        let mut usage = write(&self.usage);
        let mut creds = write(&self.creds);
        let Some(key) = keys.get_mut(id) else {
            return Err(StoreError(format!("delete_key: unknown id '{id}'")));
        };
        if key.deleted_at.is_some() {
            return Ok(());
        }
        let rev = self.next_revision();
        key.enabled = false;
        key.deleted_at = Some(self.now());
        key.revision = rev;
        usage.retain(|(k, _), _| k != id);
        creds.retain(|_, c| c.meta.key_id != id);
        Ok(())
    }

    fn list_keys_since(&self, since: u64) -> StoreResult<Vec<VirtualKey>> {
        Ok(read(&self.keys)
            .values()
            .filter(|k| k.revision > since)
            .cloned()
            .collect())
    }

    fn get_usage(&self, bucket_id: &str, window_start: u64) -> StoreResult<UsageLedger> {
        Ok(read(&self.usage)
            .get(&(bucket_id.to_string(), window_start))
            .cloned()
            .unwrap_or_default())
    }

    fn put_usage(
        &self,
        bucket_id: &str,
        window_start: u64,
        ledger: &UsageLedger,
    ) -> StoreResult<()> {
        write(&self.usage).insert((bucket_id.to_string(), window_start), ledger.clone());
        Ok(())
    }

    fn add_usage(&self, bucket_id: &str, window_start: u64, delta: &UsageDelta) -> StoreResult<()> {
        let mut usage = write(&self.usage);
        usage
            .entry((bucket_id.to_string(), window_start))
            .or_default()
            .apply_delta(delta);

        // Only this hot path sweeps; `put_usage` (absolute set) does not.
        if sweep_due(&self.usage_sweep_ticker) {
            let n = self.now();
            // Window 0 is the all-time ledger, never aged.
            usage.retain(|(_, window_start), _| {
                *window_start == 0 || window_start.saturating_add(MAX_RETENTION_SECS) > n
            });
        }
        Ok(())
    }

    fn add_metering(&self, d: &MeteringDelta) -> StoreResult<()> {
        let mut m = write(&self.metering);
        // `priced_from_ms` is part of the key: a rate-card edit inside a UTC day opens a second row
        // so each half keeps the card it was earned under.
        let e = m
            .entry((
                d.key_id.clone(),
                d.bucket,
                d.model.clone(),
                d.provider.clone(),
                d.priced_from_ms,
            ))
            .or_insert_with(|| MeteringRow {
                key_id: d.key_id.clone(),
                model: d.model.clone(),
                provider: d.provider.clone(),
                tokens_input: 0,
                tokens_output: 0,
                tokens_cache_read: 0,
                tokens_cache_write: 0,
                requests: 0,
                billable_requests: 0,
                key_group_at_use: d.key_group_at_use.clone(),
                pricing_version: d.pricing_version.clone(),
                priced_from_ms: d.priced_from_ms,
                usage_units: BTreeMap::new(),
            });
        e.tokens_input = e.tokens_input.saturating_add(d.tokens_input);
        e.tokens_output = e.tokens_output.saturating_add(d.tokens_output);
        e.tokens_cache_read = e.tokens_cache_read.saturating_add(d.tokens_cache_read);
        e.tokens_cache_write = e.tokens_cache_write.saturating_add(d.tokens_cache_write);
        e.requests = e.requests.saturating_add(d.requests);
        e.billable_requests = e.billable_requests.saturating_add(d.billable_requests);
        for (class, n) in &d.usage_units {
            let cur = e.usage_units.entry(class.clone()).or_insert(0);
            *cur = cur.saturating_add(*n);
        }

        if sweep_due(&self.metering_sweep_ticker) {
            let n = self.now();
            m.retain(|(_, bucket, _, _, _), _| bucket.saturating_add(MAX_RETENTION_SECS) > n);
        }
        Ok(())
    }

    fn list_metering(&self, bucket: u64) -> StoreResult<Vec<MeteringRow>> {
        Ok(read(&self.metering)
            .iter()
            .filter(|((_, b, _, _, _), _)| *b == bucket)
            .map(|(_, row)| row.clone())
            .collect())
    }

    fn put_credential(&self, secret: &CredentialSecret) -> StoreResult<()> {
        // The owning key is read under the SAME critical section as the credential write, in
        // `delete_key`'s lock order (keys, creds): a `delete_key` committing between a check and a
        // write would leave secret material that outlives the tombstone.
        let keys = read(&self.keys);
        let mut creds = write(&self.creds);
        let Some(owner) = keys.get(&secret.meta.key_id) else {
            return Err(StoreError(format!(
                "put_credential: key '{}' does not exist; a credential must hang off a real key",
                secret.meta.key_id
            )));
        };
        if owner.deleted_at.is_some() {
            return Err(StoreError(format!(
                "put_credential: key '{}' is tombstoned; its credentials were revoked with it and \
                 are never reissued",
                secret.meta.key_id
            )));
        }
        Self::credential_preconditions(&creds, secret)?;
        let mut secret = secret.clone();
        secret.meta.revision = self.next_revision();
        Self::insert_credential(&mut creds, secret);
        Ok(())
    }

    fn put_key_with_credential(
        &self,
        key: &VirtualKey,
        secret: &CredentialSecret,
    ) -> StoreResult<()> {
        // Both rows go in under ONE acquisition of the guards `delete_key` takes, with every
        // precondition tested first: `put_key` then `put_credential` would leave a bare live key
        // behind when the credential leg fails.
        if key.deleted_at.is_some() {
            return Err(StoreError(format!(
                "put_key_with_credential: key '{}' is tombstoned; a credential is never minted onto it",
                key.id
            )));
        }
        let mut keys = write(&self.keys);
        let mut creds = write(&self.creds);
        if keys.get(&key.id).is_some_and(|k| k.deleted_at.is_some()) {
            return Err(StoreError(format!(
                "put_key_with_credential: '{}' is tombstoned and its id is never reissued",
                key.id
            )));
        }
        if secret.meta.key_id != key.id {
            return Err(StoreError(format!(
                "put_key_with_credential: the credential names key '{}', not the key '{}' being \
                 minted with it",
                secret.meta.key_id, key.id
            )));
        }
        Self::credential_preconditions(&creds, secret)?;

        let mut key = key.clone();
        key.revision = self.next_revision();
        keys.insert(key.id.clone(), key);
        let mut secret = secret.clone();
        secret.meta.revision = self.next_revision();
        Self::insert_credential(&mut creds, secret);
        Ok(())
    }

    fn list_credentials(&self, key_id: &str) -> StoreResult<Vec<CredentialMeta>> {
        Ok(read(&self.creds)
            .values()
            .filter(|c| c.meta.key_id == key_id)
            .map(|c| c.meta.clone())
            .collect())
    }

    fn revoke_credential(&self, id: &str, reason: &str) -> StoreResult<()> {
        let mut creds = write(&self.creds);
        let Some(c) = creds.get_mut(id) else {
            return Err(StoreError(format!("revoke_credential: unknown id '{id}'")));
        };
        // Idempotent: a repeat keeps the first `revoked_at` and reason.
        if c.meta.revoked_at.is_none() {
            c.meta.revoked_at = Some(self.now());
            c.meta.revoke_reason = Some(reason.to_string());
            c.meta.revision = self.next_revision();
        }
        Ok(())
    }

    fn list_credentials_since(&self, since: u64) -> StoreResult<Vec<CredentialSecret>> {
        Ok(read(&self.creds)
            .values()
            .filter(|c| c.meta.revision > since)
            .cloned()
            .collect())
    }

    fn add_denylist(&self, sub: &str, _reason: &str) -> StoreResult<()> {
        write(&self.denylist).insert(sub.to_string());
        Ok(())
    }

    fn list_denylist(&self) -> StoreResult<Vec<String>> {
        Ok(read(&self.denylist).iter().cloned().collect())
    }

    fn append_audit(&self, entry: &AuditRecord) -> StoreResult<()> {
        // A second record on an occupied `seq` is EITHER the write-through retrying (identical: Ok)
        // or a forked chain (different: error). An audit store must not collapse the two.
        let mut audit = write(&self.audit);
        match audit.get(&entry.seq) {
            Some(stored) if stored == entry => Ok(()),
            Some(_) => Err(fork::audit_fork(entry.seq)),
            None => {
                audit.insert(entry.seq, entry.clone());
                Ok(())
            }
        }
    }

    fn list_audit(&self) -> StoreResult<Vec<AuditRecord>> {
        Ok(read(&self.audit).values().cloned().collect())
    }

    fn upsert_plane_record(&self, record: PlaneRecordRef<'_>) -> StoreResult<()> {
        write(&self.plane_records).insert(Self::upsert_key(record), record.to_record());
        Ok(())
    }

    fn get_plane_record(&self, kind: &str, id: &str) -> StoreResult<Option<Vec<u8>>> {
        Ok(read(&self.plane_records)
            .get(&(kind.to_string(), id.to_string(), UPSERT_SEQ))
            .map(|r| r.body.clone()))
    }

    fn append_plane_record(&self, record: PlaneRecordRef<'_>) -> StoreResult<()> {
        // Append-only with the same fork rule as `append_audit`: identical is a retry, different
        // at an occupied `(parent, seq)` is a second writer forking the chain.
        let mut records = write(&self.plane_records);
        let key = Self::chain_key(record);
        match records.get(&key) {
            Some(existing) if existing.view() == record => Ok(()),
            Some(_) => Err(fork::chain_fork(
                record.kind,
                record.parent.unwrap_or(record.id),
                record.seq,
            )),
            None => {
                records.insert(key, record.to_record());
                Ok(())
            }
        }
    }

    fn list_plane_records(
        &self,
        kind: &str,
        selector: &PlaneSelector<'_>,
    ) -> StoreResult<Vec<Vec<u8>>> {
        let records = read(&self.plane_records);
        // `seq`, then the record's identity and id: a total order, independent of the map's.
        let mut rows: Vec<(&PlaneKey, &PlaneRecord)> = records
            .iter()
            .filter(|((k, _, _), r)| {
                k == kind
                    && match selector {
                        PlaneSelector::All => true,
                        PlaneSelector::Parent(p) => r.parent.as_deref() == Some(&**p),
                    }
            })
            .collect();
        rows.sort_by_key(|(key, r)| (r.seq, &key.1, &r.id));
        Ok(rows.into_iter().map(|(_, r)| r.body.clone()).collect())
    }

    fn list_plane_record_parents(&self, kind: &str) -> StoreResult<Vec<String>> {
        let mut parents: Vec<String> = read(&self.plane_records)
            .iter()
            .filter(|((k, _, _), _)| k == kind)
            .filter_map(|(_, r)| r.parent.clone())
            .collect();
        parents.sort();
        parents.dedup();
        Ok(parents)
    }

    /// `task` drops only TERMINAL rows (an interrupted task waiting on a human sits still longest),
    /// every other kind drops any row older than `before`.
    fn purge_plane_records_before(&self, kind: &str, before: u64) -> StoreResult<u64> {
        let retention = if kind == "task" {
            Retention::TerminalOnly
        } else {
            Retention::Age
        };
        self.purge_plane_records(kind, before, retention)
    }

    fn purge_plane_records(
        &self,
        kind: &str,
        before: u64,
        retention: Retention,
    ) -> StoreResult<u64> {
        let mut records = write(&self.plane_records);
        let before_len = records.len();
        records.retain(|(k, _, _), r| {
            let goes = k == kind
                && r.ts < before
                && match retention {
                    Retention::Age => true,
                    Retention::TerminalOnly => r.disposition == PlaneDisposition::Terminal,
                };
            !goes
        });
        Ok((before_len - records.len()) as u64)
    }

    /// Absent is a no-op; every `seq` under the identity goes.
    fn delete_plane_record(&self, kind: &str, id: &str) -> StoreResult<()> {
        write(&self.plane_records).retain(|(k, i, _), _| !(k == kind && i == id));
        Ok(())
    }

    fn redeem_plane_token(
        &self,
        kind: &str,
        token: &str,
        expires_at: u64,
        now: u64,
    ) -> StoreResult<bool> {
        // Put-if-absent under one guard (the trait default would make every approval replayable): a
        // live row answers `false` and is left as it is; a lapsed row is a new claim.
        let mut spent = write(&self.plane_tokens);
        let first = match spent.entry((kind.to_string(), token.to_string())) {
            std::collections::hash_map::Entry::Occupied(mut row) => {
                let lapsed = *row.get() <= now;
                if lapsed {
                    row.insert(expires_at);
                }
                lapsed
            }
            std::collections::hash_map::Entry::Vacant(row) => {
                row.insert(expires_at);
                true
            }
        };
        if sweep_due(&self.tokens_sweep_ticker) {
            spent.retain(|_, exp| *exp > now);
        }
        Ok(first)
    }

    /// Multi-use and SPENDS NOTHING: a plain read of the `(kind, token)` upsert record, live while
    /// it is present, `Active`, and `now` has not reached `expires_at`.
    fn plane_token_live(
        &self,
        kind: &str,
        token: &str,
        expires_at: u64,
        now: u64,
    ) -> StoreResult<bool> {
        Ok(read(&self.plane_records)
            .get(&(kind.to_string(), token.to_string(), UPSERT_SEQ))
            .is_some_and(|r| r.disposition == PlaneDisposition::Active && now < expires_at))
    }
}

/// The memory store takes no settings: empty, whitespace, `null` and `{}` are all "none"; malformed
/// JSON, a non-object, or any key but `token` is refused. `token` (any value) is a 1.5.5 config
/// key, accepted and ignored. Shared by `validate` and `open`.
pub(crate) fn check_settings(settings: &[u8]) -> Result<(), String> {
    if settings.trim_ascii().is_empty() {
        return Ok(());
    }
    let v: serde_json::Value = serde_json::from_slice(settings)
        .map_err(|e| format!("invalid memory plugin config: {e}"))?;
    match v {
        serde_json::Value::Null => Ok(()),
        serde_json::Value::Object(m) => match m.keys().find(|k| *k != "token") {
            None => Ok(()),
            Some(k) => Err(format!(
                "invalid memory plugin config: unknown setting `{k}` (the memory store takes no settings)"
            )),
        },
        other => {
            let got = match other {
                serde_json::Value::Bool(_) => "a boolean",
                serde_json::Value::Number(_) => "a number",
                serde_json::Value::String(_) => "a string",
                _ => "an array",
            };
            Err(format!(
                "invalid memory plugin config: expected a JSON object, got {got}"
            ))
        }
    }
}

/// The store door (store v3): every slot of the table over [`MemoryStore`], through the contract's
/// store SDK. A build that links this crate registers it as its compiled-in row; the dropped-in
/// `cdylib` exports it. The memory store never pends, so `max_inflight` only bounds a flood.
pub use v3::{door, MACRO_DOOR};

/// What a build that links this store registers onto the store axis: `STORE` is
/// `(key, ephemeral, door, canonical)`, the key `store.module` selects it by, its statement that
/// what it holds is lost on restart, its door, and the manifest name config may also name it by.
/// It claims no default: a config names its store.
pub mod linked {
    pub const STORE: (&str, bool, busbar_contract::abi::sdk::door::MacroDoor, &str) =
        ("memory", true, super::MACRO_DOOR, "busbar-store-memory");
}

mod v3;

#[cfg(test)]
#[path = "tests/lib_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/v3_tests.rs"]
mod v3_tests;
