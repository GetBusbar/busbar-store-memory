// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The in-memory (RAM) `kind: store` backend, key `memory`: zero setup, nothing to run outside the
//! process, and everything it holds is lost on restart.

#![forbid(unsafe_code)]

use busbar_contract::abi::sdk::store::column::storable_i64;
use busbar_contract::abi::sdk::store::fork;
use busbar_contract::ids::RecordSchemaId;
use busbar_contract::kinds::{RecordBytes, StoreError as ContractStoreError};
use busbar_contract::records::{
    AuditRecord, CredentialMeta, CredentialSecret, MeteringDelta, MeteringRow, PlaneDisposition,
    PlaneRecord, PlaneRecordRef, PlaneSelector, RecordStore as Store,
    RecordStoreError as StoreError, RecordStoreResult as StoreResult, Retention, SecretForm,
    UsageDelta, UsageLedger, VirtualKey,
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{SystemTime, UNIX_EPOCH};

/// Retention ceiling for usage windows: 31 days, matching the governance rate cells.
const MAX_RETENTION_SECS: u64 = 31 * 86_400;

/// One retention sweep per this many `add_usage` cells (batch cells included) or token redemptions.
const SWEEP_INTERVAL: u64 = 256;

/// The `records` map: `(schema, key) -> body`, in KEY ORDER because `record_scan` promises a
/// prefix walk answers in it.
type RecordMap = BTreeMap<(String, Vec<u8>), RecordBytes>;

/// A metering row's key retains the group and pricing version that owned its counts; the bucket
/// leads so a day's rows are one ordered range.
type MeteringKey = (u64, String, String, String, u64, String, String);

/// The [`MeteringKey`] a delta's counts accrue under.
fn metering_key(d: &MeteringDelta) -> MeteringKey {
    (
        d.bucket,
        d.key_id.clone(),
        d.model.clone(),
        d.provider.clone(),
        d.priced_from_ms,
        d.key_group_at_use.clone(),
        d.pricing_version.clone(),
    )
}

/// A plane record's key: `(kind, identity, seq)`.
type PlaneKey = (String, String, u64);

/// The plane kind whose rows own a chain of [`TASK_EVENT_KIND`] rows (parent = the task id).
const TASK_KIND: &str = "task";

/// A task's event chain: deleted or purged with its task.
const TASK_EVENT_KIND: &str = "task_event";

/// The highest credential slot: two slots per `(key_id, kind)` allow overlap-window rotation.
const MAX_CREDENTIAL_SLOT: u8 = 1;

/// Reserved host ticket records carry their absolute expiry in `ts`.
const TICKET_RECORD_KIND: &str = "ticket";

/// The `seq` an upserted plane record is stored at (it is keyed by `(kind, id)`).
const UPSERT_SEQ: u64 = 0;

/// Claim rows and their expiry order share the SAME guard. The index contains exactly one
/// entry per row; replacing a lapsed claim removes its former expiry before indexing the new one.
#[derive(Default)]
struct SpentTokens {
    rows: HashMap<(String, String), u64>,
    expiries: BTreeSet<(u64, String, String)>,
}

impl SpentTokens {
    fn redeem(&mut self, kind: &str, token: &str, expires_at: u64, now: u64) -> bool {
        let address = (kind.to_owned(), token.to_owned());
        if let Some(&previous) = self.rows.get(&address) {
            if previous >= now {
                return false;
            }
            self.expiries
                .remove(&(previous, address.0.clone(), address.1.clone()));
        }
        self.expiries
            .insert((expires_at, address.0.clone(), address.1.clone()));
        self.rows.insert(address, expires_at);
        true
    }

    fn sweep(&mut self, now: u64) {
        while self
            .expiries
            .first()
            .is_some_and(|(expiry, ..)| *expiry < now)
        {
            if let Some((_, kind, token)) = self.expiries.pop_first() {
                self.rows.remove(&(kind, token));
            }
        }
    }
}

/// Remove one identity without walking unrelated kinds or identities.
fn remove_plane_identity(records: &mut BTreeMap<PlaneKey, PlaneRecord>, kind: &str, id: &str) {
    let first = (kind.to_owned(), id.to_owned(), 0);
    let last = (kind.to_owned(), id.to_owned(), u64::MAX);
    let keys: Vec<_> = records
        .range(first..=last)
        .map(|(key, _)| key.clone())
        .collect();
    for key in keys {
        records.remove(&key);
    }
}

/// The wall clock in Unix seconds; `None` before the epoch.
fn wall_clock() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs())
}

/// Remove every credential row `doomed` selects, wiping each removed secret before it is freed;
/// the wiped rows are returned so a caller can drop them (or a test can inspect them).
fn discard_credentials(
    creds: &mut HashMap<String, CredentialSecret>,
    mut doomed: impl FnMut(&CredentialSecret) -> bool,
) -> Vec<CredentialSecret> {
    creds
        .extract_if(|_, c| doomed(c))
        .map(|(_, mut c)| {
            zeroize::Zeroize::zeroize(&mut c.secret);
            c
        })
        .collect()
}

/// The refusal a slot holding a live credential answers with.
fn slot_occupied(op: &str, meta: &CredentialMeta) -> StoreError {
    StoreError(format!(
        "{op}: slot {} for key '{}' kind '{}' holds a live credential; revoke it first",
        meta.slot, meta.key_id, meta.kind
    ))
}

/// A refused store v3 op as the record-store error the trait methods answer.
fn op_error(error: busbar_contract::abi::sdk::store::OpRefused) -> StoreError {
    use busbar_contract::abi::sdk::store::OpRefused;
    match error {
        OpRefused::Conflict => StoreError(busbar_contract::abi::store::DIAG_OPID_CONFLICT.into()),
        OpRefused::Failed(reason) | OpRefused::Refused(reason) => StoreError(reason),
    }
}

/// The plane record's `seq` and `ts` must fit the signed column a durable store keeps them in.
pub(crate) fn check_plane(method: &str, record: PlaneRecordRef<'_>) -> StoreResult<()> {
    storable_i64(method, "seq", record.seq)?;
    storable_i64(method, "ts", record.ts)?;
    Ok(())
}

/// A metering delta's `priced_from_ms` is part of its key and must fit the signed column.
pub(crate) fn check_metering(method: &str, d: &MeteringDelta) -> StoreResult<()> {
    storable_i64(method, "priced_from_ms", d.priced_from_ms).map(drop)
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
    metering: RwLock<BTreeMap<MeteringKey, MeteringRow>>,
    /// Denied subject ids (the reason is audit-only, not needed for the enforcement read).
    denylist: RwLock<HashSet<String>>,
    /// The spent-token ledger behind `redeem_plane_token`: `(kind, token) -> expires_at`. A live
    /// row means "already redeemed"; a lapsed row is vacant to its own key.
    plane_tokens: RwLock<SpentTokens>,
    /// Plane records by `(kind, identity, seq)`. The identity is the record's `parent` for appended
    /// child kinds and its `id` for upserted ones, which take `seq` 0.
    plane_records: RwLock<BTreeMap<PlaneKey, PlaneRecord>>,
    /// A plane's kernel-held durable records, by `(schema, key)`: a second map beside
    /// `plane_records` because these verbs key on opaque bytes, not `(kind, id, seq)`.
    ///
    /// NEVER AGED OUT: this backend cannot tell a settled row from an active work handle, and a row
    /// it cannot prove settled is one it may not drop. Only the host removes a row, by a
    /// `record_put` of an EMPTY value.
    records: RwLock<RecordMap>,
    /// The admin audit log, by `seq`.
    audit: RwLock<BTreeMap<u64, AuditRecord>>,
    /// Counters for `add_usage` cells and token redemptions; ordinary puts do not sweep.
    usage_sweep_ticker: AtomicU64,
    tokens_sweep_ticker: AtomicU64,
    /// The store-global monotonic revision counter (see `VirtualKey::revision`), bumped on every
    /// `keys`/`creds` mutation.
    revision: AtomicU64,
    /// A pinned clock: `0` (the default, and always in production, where nothing writes it) means
    /// the wall clock; any other value is what `self.now()` and the token clock answer.
    clock: AtomicU64,
    /// The store v3 slots' own state ([`v3`]).
    v3: v3::State,
}

impl MemoryStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Write one of a plane's kernel-held durable records; an EMPTY value removes the row.
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
    pub fn record_get(
        &self,
        schema: RecordSchemaId,
        key: &[u8],
    ) -> Result<Option<RecordBytes>, ContractStoreError> {
        self.v3_record_get(schema.as_str(), key)
            .map_err(ContractStoreError::Rejected)
    }

    /// Walk a plane's records under a prefix, in key order, at most `limit` of them. `limit` 0
    /// means nothing, not everything.
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

    /// The pinned clock if set, else the wall clock (`None` before the Unix epoch).
    fn clock_secs(&self) -> Option<u64> {
        match self.clock.load(Ordering::Relaxed) {
            0 => wall_clock(),
            pinned => Some(pinned),
        }
    }

    /// Clock for revocation, deletion and usage retention (0 before the Unix epoch); dedupe
    /// receipts age by elapsed process time instead (`receipt_now`).
    fn now(&self) -> u64 {
        self.clock_secs().unwrap_or(0)
    }

    /// An APPENDED record's position in the one map: its chain (its `parent`, else its own `id`)
    /// at its `seq`.
    fn chain_key(record: PlaneRecordRef<'_>) -> PlaneKey {
        let identity = record.parent.unwrap_or(record.id).to_string();
        (record.kind.to_string(), identity, record.seq)
    }

    /// An UPSERTED record's key in the one map: its own `id`, parent or not (the contract upserts
    /// by `(kind, id)`).
    fn upsert_key(record: PlaneRecordRef<'_>) -> PlaneKey {
        (record.kind.to_string(), record.id.to_string(), UPSERT_SEQ)
    }

    /// The credential-table preconditions, shared by `put_credential` and the atomic
    /// `put_key_with_credential`; `Ok(true)` means the identical live credential is already stored
    /// (revision aside), so the caller writes nothing.
    fn credential_preconditions(
        op: &str,
        creds: &HashMap<String, CredentialSecret>,
        secret: &CredentialSecret,
    ) -> StoreResult<bool> {
        let meta = &secret.meta;
        if let Some(stored) = creds.get(&meta.id) {
            if stored.meta.revoked_at.is_some() {
                return Err(StoreError(format!(
                    "{op}: credential '{}' is revoked; a revoked credential is never re-minted",
                    meta.id
                )));
            }
            let offered = CredentialMeta {
                revision: stored.meta.revision,
                ..meta.clone()
            };
            if offered == stored.meta && secret.secret == stored.secret {
                return Ok(true);
            }
            return Err(slot_occupied(op, &stored.meta));
        }

        if meta.slot > MAX_CREDENTIAL_SLOT {
            return Err(StoreError(format!(
                "{op}: credential '{}' names slot {}; a slot is 0 or 1",
                meta.id, meta.slot
            )));
        }

        let occupied = creds.values().any(|c| {
            c.meta.id != meta.id
                && c.meta.key_id == meta.key_id
                && c.meta.kind == meta.kind
                && c.meta.slot == meta.slot
                && c.meta.revoked_at.is_none()
        });
        if occupied {
            return Err(slot_occupied(op, meta));
        }

        let public_id_taken = creds.values().any(|c| {
            c.meta.id != meta.id
                && c.meta.kind == meta.kind
                && c.meta.public_id == meta.public_id
                && !(c.meta.key_id == meta.key_id
                    && c.meta.slot == meta.slot
                    && c.meta.revoked_at.is_some())
        });
        if public_id_taken {
            return Err(StoreError(format!(
                "{op}: public_id '{}' is already in use for kind '{}'",
                meta.public_id, meta.kind
            )));
        }
        Ok(false)
    }

    /// Insert `secret`, replacing (and wiping) a REVOKED row in the same `(key_id, kind, slot)` (the
    /// contract upserts on that triple; the preconditions already refused a live occupant).
    fn insert_credential(creds: &mut HashMap<String, CredentialSecret>, secret: CredentialSecret) {
        let meta = &secret.meta;
        discard_credentials(creds, |c| {
            c.meta.id != meta.id
                && c.meta.key_id == meta.key_id
                && c.meta.kind == meta.kind
                && c.meta.slot == meta.slot
        });
        creds.insert(secret.meta.id.clone(), secret);
    }

    /// The clock token claims and tickets are judged by; a clock before the Unix epoch is an error.
    fn token_now(&self) -> StoreResult<u64> {
        self.clock_secs()
            .ok_or_else(|| StoreError("memory store token clock is before Unix epoch".into()))
    }

    fn apply_usage(
        usage: &mut HashMap<(String, u64), UsageLedger>,
        bucket_id: &str,
        window_start: u64,
        delta: &UsageDelta,
    ) -> StoreResult<()> {
        // `checked_apply_delta` is atomic: a refused delta leaves the stored row untouched, and a
        // refused first delta creates no row.
        let key = (bucket_id.to_string(), window_start);
        if let Some(ledger) = usage.get_mut(&key) {
            return ledger
                .checked_apply_delta(delta)
                .map_err(|e| StoreError(e.to_string()));
        }
        let mut ledger = UsageLedger::default();
        ledger
            .checked_apply_delta(delta)
            .map_err(|e| StoreError(e.to_string()))?;
        usage.insert(key, ledger);
        Ok(())
    }

    fn check_legacy_usage_bucket(bucket: &str) -> StoreResult<()> {
        if busbar_contract::records::correction::reserved_bucket(bucket) {
            return Err(StoreError(
                "reserved adjustment rows require atomic usage correction".into(),
            ));
        }
        Ok(())
    }

    fn sweep_usage(&self, usage: &mut HashMap<(String, u64), UsageLedger>) {
        let n = self.now();

        usage.retain(|(_, window_start), _| {
            *window_start == 0 || window_start.saturating_add(MAX_RETENTION_SECS) > n
        });
    }

    fn apply_metering(
        m: &mut BTreeMap<MeteringKey, MeteringRow>,
        d: &MeteringDelta,
    ) -> StoreResult<()> {
        let key = metering_key(d);
        // `checked_add_delta` is atomic: a refused delta leaves the stored row untouched, and a
        // refused first delta creates no row.
        if let Some(row) = m.get_mut(&key) {
            return row
                .checked_add_delta(d)
                .map_err(|e| StoreError(e.to_string()));
        }
        let mut row = MeteringRow {
            scale: busbar_contract::COUNT_SCALE,
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
        };
        row.checked_add_delta(d)
            .map_err(|e| StoreError(e.to_string()))?;
        m.insert(key, row);
        Ok(())
    }
}

impl Store for MemoryStore {
    fn put_key(&self, key: &VirtualKey) -> StoreResult<()> {
        let mut key = key.clone();
        let mut keys = write(&self.keys);

        if key.deleted_at.is_none() && keys.get(&key.id).is_some_and(|k| k.deleted_at.is_some()) {
            return Err(StoreError(format!(
                "put_key: '{}' is tombstoned and its id is never reissued; refusing to \
                 clear the tombstone",
                key.id
            )));
        }
        key.revision = self.next_revision();
        // A tombstone offered over an absent or live row takes `delete_key`'s cascade, in its lock
        // order (keys, creds), in the same critical section as the insert.
        if key.deleted_at.is_some() && keys.get(&key.id).is_none_or(|k| k.deleted_at.is_none()) {
            discard_credentials(&mut write(&self.creds), |c| c.meta.key_id == key.id);
        }
        keys.insert(key.id.clone(), key);
        Ok(())
    }

    fn get_key(&self, id: &str) -> StoreResult<Option<VirtualKey>> {
        Ok(read(&self.keys).get(id).cloned())
    }

    /// Unfiltered: tombstones are included, since the default `list_keys_since` needs them.
    fn list_keys(&self) -> StoreResult<Vec<VirtualKey>> {
        let mut v: Vec<VirtualKey> = read(&self.keys).values().cloned().collect();
        v.sort_by(|a, b| (a.created_at, &a.id).cmp(&(b.created_at, &b.id)));
        Ok(v)
    }

    fn delete_key(&self, id: &str) -> StoreResult<()> {
        let mut keys = write(&self.keys);
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
        discard_credentials(&mut creds, |c| c.meta.key_id == id);
        Ok(())
    }

    fn list_keys_since(&self, since: u64) -> StoreResult<Vec<VirtualKey>> {
        let mut v: Vec<VirtualKey> = read(&self.keys)
            .values()
            .filter(|k| k.revision > since)
            .cloned()
            .collect();
        v.sort_by_key(|k| k.revision);
        Ok(v)
    }

    fn get_usage(&self, bucket_id: &str, window_start: u64) -> StoreResult<UsageLedger> {
        Ok(read(&self.usage)
            .get(&(bucket_id.to_string(), window_start))
            .cloned()
            .unwrap_or_default())
    }

    fn supports_usage_correction(&self) -> bool {
        true
    }

    fn add_usage_batch_deduped(
        &self,
        op: busbar_contract::abi::store::OpId,
        cells: &[(&str, u64, UsageDelta)],
    ) -> StoreResult<()> {
        self.v3_add_usage_batch(op, cells).map_err(op_error)
    }

    fn add_metering_batch_deduped(
        &self,
        op: busbar_contract::abi::store::OpId,
        deltas: &[MeteringDelta],
    ) -> StoreResult<()> {
        self.v3_add_metering_batch(op, deltas).map_err(op_error)
    }

    fn supports_metering_batch_deduped(&self) -> bool {
        true
    }

    fn supports_usage_batch_deduped(&self) -> bool {
        true
    }

    fn apply_usage_correction(
        &self,
        request: &busbar_contract::records::correction::Request,
    ) -> StoreResult<busbar_contract::records::correction::Decision> {
        self.v3_apply_usage_correction(request).map_err(op_error)
    }

    fn put_usage(
        &self,
        bucket_id: &str,
        window_start: u64,
        ledger: &UsageLedger,
    ) -> StoreResult<()> {
        let mut ledger = ledger.clone();
        ledger
            .normalize_scale6()
            .map_err(|e| StoreError(e.to_string()))?;
        let ledger = &ledger;

        Self::check_legacy_usage_bucket(bucket_id)?;
        write(&self.usage).insert((bucket_id.to_string(), window_start), ledger.clone());
        Ok(())
    }

    fn add_usage(&self, bucket_id: &str, window_start: u64, delta: &UsageDelta) -> StoreResult<()> {
        let mut delta = delta.clone();
        delta
            .normalize_scale6()
            .map_err(|e| StoreError(e.to_string()))?;
        let delta = &delta;

        Self::check_legacy_usage_bucket(bucket_id)?;
        let mut usage = write(&self.usage);
        Self::apply_usage(&mut usage, bucket_id, window_start, delta)?;

        if sweep_due(&self.usage_sweep_ticker) {
            self.sweep_usage(&mut usage);
        }
        Ok(())
    }

    fn add_metering(&self, d: &MeteringDelta) -> StoreResult<()> {
        let mut d = d.clone();
        d.normalize_scale6()
            .map_err(|e| StoreError(e.to_string()))?;
        let d = &d;

        check_metering("add_metering", d)?;
        let mut m = write(&self.metering);
        Self::apply_metering(&mut m, d)?;
        Ok(())
    }

    fn list_metering(&self, bucket: u64) -> StoreResult<Vec<MeteringRow>> {
        let first: MeteringKey = (
            bucket,
            String::new(),
            String::new(),
            String::new(),
            0,
            String::new(),
            String::new(),
        );
        Ok(read(&self.metering)
            .range(first..)
            .take_while(|((b, ..), _)| *b == bucket)
            .map(|(_, row)| row.clone())
            .collect())
    }

    fn put_credential(&self, secret: &CredentialSecret) -> StoreResult<()> {
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
        if Self::credential_preconditions("put_credential", &creds, secret)? {
            return Ok(());
        }
        let mut secret = secret.clone();
        secret.meta.revision = self.next_revision();
        Self::insert_credential(&mut creds, secret);
        Ok(())
    }

    fn rewrap_credential(
        &self,
        expected: &CredentialSecret,
        replacement: &str,
    ) -> StoreResult<bool> {
        let keys = read(&self.keys);
        let mut creds = write(&self.creds);
        if !keys
            .get(&expected.meta.key_id)
            .is_some_and(|key| key.deleted_at.is_none())
            || expected.meta.revoked_at.is_some()
            || expected.meta.secret_form != SecretForm::Recoverable
            || expected.secret == replacement
        {
            return Ok(false);
        }
        let Some(current) = creds.get_mut(&expected.meta.id) else {
            return Ok(false);
        };
        if &*current != expected {
            return Ok(false);
        }
        zeroize::Zeroize::zeroize(&mut current.secret);
        current.secret = replacement.to_owned();
        current.meta.revision = self.next_revision();
        Ok(true)
    }

    fn put_key_with_credential(
        &self,
        key: &VirtualKey,
        secret: &CredentialSecret,
    ) -> StoreResult<()> {
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
        let unchanged = Self::credential_preconditions("put_key_with_credential", &creds, secret)?;

        let mut key = key.clone();
        key.revision = self.next_revision();
        keys.insert(key.id.clone(), key);
        if unchanged {
            return Ok(());
        }
        let mut secret = secret.clone();
        secret.meta.revision = self.next_revision();
        Self::insert_credential(&mut creds, secret);
        Ok(())
    }

    fn list_credentials(&self, key_id: &str) -> StoreResult<Vec<CredentialMeta>> {
        let mut v: Vec<CredentialMeta> = read(&self.creds)
            .values()
            .filter(|c| c.meta.key_id == key_id)
            .map(|c| c.meta.clone())
            .collect();
        v.sort_by(|a, b| (&a.kind, a.slot).cmp(&(&b.kind, b.slot)));
        Ok(v)
    }

    fn revoke_credential(&self, id: &str, reason: &str) -> StoreResult<()> {
        let mut creds = write(&self.creds);
        let Some(c) = creds.get_mut(id) else {
            return Err(StoreError(format!("revoke_credential: unknown id '{id}'")));
        };

        if c.meta.revoked_at.is_none() {
            c.meta.revoked_at = Some(self.now());
            c.meta.revoke_reason = Some(reason.to_string());
            c.meta.secret_form = SecretForm::None;
            let mut retired = std::mem::take(&mut c.secret);
            zeroize::Zeroize::zeroize(&mut retired);
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

    fn list_audit_tail(&self, limit: u64) -> StoreResult<Vec<AuditRecord>> {
        let audit = read(&self.audit);
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        let mut rows: Vec<_> = audit.values().rev().take(limit).cloned().collect();
        rows.reverse();
        Ok(rows)
    }

    fn upsert_plane_record(&self, record: PlaneRecordRef<'_>) -> StoreResult<()> {
        check_plane("upsert_plane_record", record)?;
        write(&self.plane_records).insert(Self::upsert_key(record), record.to_record());
        Ok(())
    }

    fn get_plane_record(&self, kind: &str, id: &str) -> StoreResult<Option<Vec<u8>>> {
        Ok(read(&self.plane_records)
            .get(&(kind.to_string(), id.to_string(), UPSERT_SEQ))
            .map(|r| r.body.clone()))
    }

    fn append_plane_record(&self, record: PlaneRecordRef<'_>) -> StoreResult<()> {
        check_plane("append_plane_record", record)?;

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

        let mut rows: Vec<(&PlaneKey, &PlaneRecord)> = records
            .range((kind.to_owned(), String::new(), 0)..)
            .take_while(|((k, _, _), _)| k == kind)
            .filter(|(_, r)| match selector {
                PlaneSelector::All => true,
                PlaneSelector::Parent(p) => r.parent.as_deref() == Some(&**p),
            })
            .collect();
        rows.sort_by_key(|(key, r)| (r.seq, *key));
        Ok(rows.into_iter().map(|(_, r)| r.body.clone()).collect())
    }

    fn list_plane_record_parents(&self, kind: &str) -> StoreResult<Vec<String>> {
        let records = read(&self.plane_records);
        let parents: BTreeSet<&str> = records
            .range((kind.to_owned(), String::new(), 0)..)
            .take_while(|((k, _, _), _)| k == kind)
            .filter_map(|(_, r)| r.parent.as_deref())
            .collect();
        Ok(parents.into_iter().map(str::to_owned).collect())
    }

    /// `task` drops only TERMINAL rows (an interrupted task waiting on a human sits still longest),
    /// every other kind drops any row older than `before`.
    fn purge_plane_records_before(&self, kind: &str, before: u64) -> StoreResult<u64> {
        let retention = if kind == TASK_KIND {
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
        let before = if kind == TICKET_RECORD_KIND {
            before.min(self.token_now()?)
        } else {
            before
        };
        let first = (kind.to_owned(), String::new(), 0);
        let removed: Vec<_> = records
            .range(first..)
            .take_while(|((stored_kind, _, _), _)| stored_kind == kind)
            .filter(|(_, row)| {
                row.ts < before
                    && match retention {
                        Retention::Age => true,
                        Retention::TerminalOnly => row.disposition == PlaneDisposition::Terminal,
                    }
            })
            .map(|(key, _)| key.clone())
            .collect();
        let purged = removed.len() as u64;
        let mut tasks = HashSet::new();
        for key in removed {
            if kind == TASK_KIND {
                tasks.insert(key.1.clone());
            }
            records.remove(&key);
        }
        for task in tasks {
            remove_plane_identity(&mut records, TASK_EVENT_KIND, &task);
        }
        Ok(purged)
    }

    /// Absent is a no-op; every `seq` under the identity goes, and a task takes its `task_event`
    /// chain with it.
    fn delete_plane_record(&self, kind: &str, id: &str) -> StoreResult<()> {
        let mut records = write(&self.plane_records);
        remove_plane_identity(&mut records, kind, id);
        if kind == TASK_KIND {
            remove_plane_identity(&mut records, TASK_EVENT_KIND, id);
        }
        Ok(())
    }

    fn redeem_plane_token(
        &self,
        kind: &str,
        token: &str,
        expires_at: u64,
        _now: u64,
    ) -> StoreResult<bool> {
        let mut spent = write(&self.plane_tokens);
        let now = self.token_now()?;
        if expires_at < now {
            return Ok(false);
        }
        let first = spent.redeem(kind, token, expires_at, now);
        if sweep_due(&self.tokens_sweep_ticker) {
            spent.sweep(now);
        }
        Ok(first)
    }

    /// Multi-use and SPENDS NOTHING: a plain read of the `(kind, token)` upsert record, live while
    /// it is present, `Active` and `now` has not passed `expires_at`, except that a `ticket` must be
    /// `Terminal` with `ts` equal to `expires_at` and is judged by the store's own clock.
    fn plane_token_live(
        &self,
        kind: &str,
        token: &str,
        expires_at: u64,
        now: u64,
    ) -> StoreResult<bool> {
        let records = read(&self.plane_records);
        let now = if kind == TICKET_RECORD_KIND {
            self.token_now()?
        } else {
            now
        };
        Ok(records
            .get(&(kind.to_string(), token.to_string(), UPSERT_SEQ))
            .is_some_and(|r| {
                let standing = if kind == TICKET_RECORD_KIND {
                    r.disposition == PlaneDisposition::Terminal && r.ts == expires_at
                } else {
                    r.disposition == PlaneDisposition::Active
                };
                standing && now <= expires_at
            }))
    }
}

/// The memory store takes no settings: empty, whitespace, `null` and `{}` are all "none"; malformed
/// JSON, a non-object, or any setting key is refused.
pub(crate) fn check_settings(settings: &[u8]) -> Result<(), String> {
    if settings.trim_ascii().is_empty() {
        return Ok(());
    }
    let v: serde_json::Value = serde_json::from_slice(settings)
        .map_err(|e| format!("invalid memory plugin config: {e}"))?;
    match v {
        serde_json::Value::Null => Ok(()),
        serde_json::Value::Object(m) => match m.keys().next() {
            None => Ok(()),
            Some(k) => Err(format!(
                "invalid memory plugin config: unknown setting `{k}` (accepted: none)"
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
/// store SDK.
pub use v3::{door, MACRO_DOOR};

/// What a build that links this store registers onto the store axis: `STORE` is
/// `(key, ephemeral, door, canonical)`, the key `store.module` selects it by, its statement that what
/// it holds is lost on restart, its door, and the manifest name config may also name it by.
pub mod linked {
    pub const STORE: (&str, bool, busbar_contract::abi::sdk::door::MacroDoor, &str) =
        ("memory", true, super::MACRO_DOOR, "busbar-store-memory");
}

mod migration;
mod v3;

#[cfg(test)]
#[path = "tests/lib_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/v3_tests.rs"]
mod v3_tests;
