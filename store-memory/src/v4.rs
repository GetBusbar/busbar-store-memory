// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! The normal neutral Store primitive implementation.
//! With no configured budget the host supplies admission; allocations here are fallible.
use busbar_contract::abi::sdk::conn::Host;
use busbar_contract::abi::sdk::store::v4::backend::{Backend, Committed};
use busbar_contract::abi::sdk::store::{Op, Step};
use busbar_contract::persistence::{control, *};
use busbar_contract::Secret;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

busbar_contract::own_seal!(Own);
// Both linked registration and the plugin export use this primitive door.
busbar_contract::store_v4_door!(
    MemoryV4,
    "busbar-store-memory",
    env!("CARGO_PKG_VERSION"),
    1024,
    settings_schema: busbar_contract::abi::mechanism::call::Blob::json(include_bytes!("settings.schema.json")),
    migrate: busbar_contract::abi::sdk::migrate::Migrator<crate::migration::MemoryMigration>,
);
type Value = Secret<Vec<u8>, Own>;
type NamespaceRecords = HashMap<Vec<u8>, Value>;
type ReceiptKey = ([u8; 16], [u8; 16]);
struct Receipt {
    fingerprint: [u8; 32],
    result: Value,
    observed: ClockObservation,
    execution_epoch: u64,
    applied: Duration,
}
#[derive(Default)]
struct State {
    records: HashMap<[u8; 16], NamespaceRecords>,
    receipts: HashMap<ReceiptKey, Receipt>,
    #[cfg(test)]
    now: Option<u64>,
    #[cfg(test)]
    elapsed: Option<Duration>,
}
/// Ephemeral neutral primitive backend, with one serialization guard.
pub(crate) struct MemoryV4 {
    owner: Own,
    info: BackendInfo,
    budget: usize,
    started: Instant,
    state: Mutex<State>,
}
fn reserve<T>(n: usize) -> Result<Vec<T>, Failure> {
    let mut v = Vec::new();
    v.try_reserve_exact(n).map_err(|_| Failure::Overloaded)?;
    Ok(v)
}
fn bytes(source: &[u8]) -> Result<Vec<u8>, Failure> {
    let mut v = reserve(source.len())?;
    v.extend_from_slice(source);
    Ok(v)
}
fn protected(source: &[u8], _own: &Own) -> Result<Value, Failure> {
    let mut value = zeroize::Zeroizing::new(Vec::new());
    let v = &mut *value;
    v.try_reserve_exact(source.len())
        .map_err(|_| Failure::Overloaded)?;
    v.extend_from_slice(source);
    Ok(Secret::held(std::mem::take(&mut *value)))
}
impl State {
    fn get(&self, namespace: [u8; 16], key: &[u8]) -> Option<&Value> {
        self.records
            .get(&namespace)
            .and_then(|records| records.get(key))
    }
    fn generation(&self, namespace: [u8; 16], own: &Own) -> Result<u64, Failure> {
        match self.get(control::GENERATION_NAMESPACE.0, &namespace) {
            None => Ok(0),
            Some(v) => {
                let value = u64::from_le_bytes(
                    v.expose_own(own)
                        .as_slice()
                        .try_into()
                        .map_err(|_| Failure::Corrupt)?,
                );
                if value == 0 {
                    Err(Failure::Corrupt)
                } else {
                    Ok(value)
                }
            }
        }
    }
}
/// Physical state discovered while holding the same lock as every primitive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InspectedState {
    Empty,
    Current,
}

impl MemoryV4 {
    fn guard(&self) -> Result<std::sync::MutexGuard<'_, State>, Failure> {
        self.state.lock().map_err(|_| Failure::Unavailable)
    }
    #[cfg(test)]
    fn inspect_now(&self) -> Result<InspectedState, Failure> {
        let state = self.guard()?;
        self.inspect_state(&state)
    }
    fn inspect_state(&self, state: &State) -> Result<InspectedState, Failure> {
        let empty = state.records.values().all(HashMap::is_empty) && state.receipts.is_empty();
        if empty {
            return Ok(InspectedState::Empty);
        }
        let root = state
            .get(control::NAMESPACE.0, control::DATASET_KEY)
            .ok_or(Failure::Refused)?;
        let root =
            control::Dataset::decode(root.expose_own(&self.owner)).ok_or(Failure::Corrupt)?;
        if root.id != self.info.dataset || self.info.physical_version != 1 {
            return Err(Failure::Corrupt);
        }
        Ok(InspectedState::Current)
    }
    fn source_identity(&self) -> lifecycle::SourceIdentity {
        let mut identity = [0u8; 64];
        identity[..16].copy_from_slice(b"busbar.memory.01");
        identity[16..32].copy_from_slice(&self.info.dataset.0);
        identity[32..48].copy_from_slice(&self.info.atomic_domain);
        identity[48..].copy_from_slice(&self.info.clock_authority);
        let layout = archive::digest(b"memory/physical1:records(namespace16,keybytes,valuebytes);receipts(writer16,operation16,fingerprint32,resultbytes,observation,execution_epoch,applied)");
        lifecycle::SourceIdentity {
            native_anchor: archive::digest(&identity),
            dataset: self.info.dataset,
            format: None,
            catalogue_digest: layout,
            layout_digest: layout,
        }
    }
    fn connect_binding(&self, binding: Option<&lifecycle::OpenBinding>) -> Result<(), String> {
        let state = self.guard().map_err(|_| "memory state unavailable")?;
        let inspected = self
            .inspect_state(&state)
            .map_err(|_| "memory current selector invalid")?;
        let marker = || {
            state
                .get(lifecycle::ADMISSION_NAMESPACE, lifecycle::ADMISSION_KEY)
                .map(|value| value.expose_own(&self.owner))
        };
        let admitted = match (binding, inspected) {
            (None, InspectedState::Empty)
            | (Some(lifecycle::OpenBinding::Empty), InspectedState::Empty) => true,
            (None, InspectedState::Current) => marker().is_some(),
            (
                Some(lifecycle::OpenBinding::Current {
                    source,
                    admission_marker_digest,
                }),
                InspectedState::Current,
            ) => {
                source == &self.source_identity()
                    && marker()
                        .is_some_and(|value| archive::digest(value) == *admission_marker_digest)
            }
            _ => false,
        };
        admitted
            .then_some(())
            .ok_or_else(|| "memory prepared source changed or unsupported recovery".into())
    }
    fn bootstrap_now(
        &self,
        request: &lifecycle::Request<Own>,
    ) -> Result<lifecycle::Response<Own>, lifecycle::Error> {
        let lifecycle::Request::Inspect { bounds } = request else {
            return Err(lifecycle::Error::MissingCapability);
        };
        if bounds.items == 0
            || (bounds.bytes as usize) < std::mem::size_of::<lifecycle::Inspection<Own>>()
        {
            return Err(lifecycle::Error::Bounds);
        }
        let state = match self.state.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::WouldBlock) => return Err(lifecycle::Error::Busy),
            Err(std::sync::TryLockError::Poisoned(_)) => return Err(lifecycle::Error::Unavailable),
        };
        let inspected = self.inspect_state(&state).map_err(|error| match error {
            Failure::Refused => lifecycle::Error::UnsupportedFormat,
            Failure::Corrupt => lifecycle::Error::Corrupt,
            _ => lifecycle::Error::Unavailable,
        })?;
        let (state, source, admission_marker) = match inspected {
            InspectedState::Empty => (lifecycle::InspectionState::Empty, None, None),
            InspectedState::Current => {
                // Opaque core admission bytes: native inspection does not interpret their policy.
                let marker = state
                    .get(lifecycle::ADMISSION_NAMESPACE, lifecycle::ADMISSION_KEY)
                    .ok_or(lifecycle::Error::Corrupt)?;
                let marker = marker.expose_own(&self.owner);
                let owned = std::mem::size_of::<lifecycle::Inspection<Own>>()
                    .checked_add(std::mem::size_of::<lifecycle::SourceIdentity>())
                    .and_then(|size| size.checked_add(marker.len()))
                    .ok_or(lifecycle::Error::Bounds)?;
                if owned > bounds.bytes as usize {
                    return Err(lifecycle::Error::Bounds);
                }
                (
                    lifecycle::InspectionState::Current,
                    Some(self.source_identity()),
                    Some(protected(marker, &self.owner).map_err(|_| lifecycle::Error::Bounds)?),
                )
            }
        };
        let mut capabilities = reserve(1).map_err(|_| lifecycle::Error::Bounds)?;
        capabilities.push(lifecycle::NativeCapability::CurrentOpen);
        Ok(lifecycle::Response::Inspected(lifecycle::Inspection {
            state,
            source,
            admission_marker,
            catalogue: Vec::new(),
            capabilities,
        }))
    }
    fn observation(&self, s: &State) -> Result<ClockObservation, Failure> {
        #[cfg(test)]
        if let Some(now) = s.now {
            return Ok(ClockObservation {
                authority: self.info.clock_authority,
                unix_seconds: now,
            });
        }
        let _ = s;
        Ok(ClockObservation {
            authority: self.info.clock_authority,
            unix_seconds: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| Failure::Unavailable)?
                .as_secs(),
        })
    }
    fn elapsed(&self, s: &State) -> Duration {
        #[cfg(test)]
        if let Some(elapsed) = s.elapsed {
            return elapsed;
        }
        let _ = s;
        self.started.elapsed()
    }
    fn dataset(&self, id: DatasetId) -> Result<(), Failure> {
        if id == self.info.dataset {
            Ok(())
        } else {
            Err(Failure::Refused)
        }
    }
    fn read_now(&self, r: &Read) -> Result<ReadResult<Own>, Failure> {
        self.dataset(r.dataset)?;
        let s = self.guard()?;
        let observed_at = self.observation(&s)?;
        if r.time.is_some_and(|t| !t.permits(observed_at.unix_seconds)) {
            return Ok(ReadResult::TimeGuardFailed { observed_at });
        }
        let mut values = reserve(r.addresses.len())?;
        for a in &r.addresses {
            values.push(
                s.get(a.namespace, &a.key)
                    .map(|v| protected(v.expose_own(&self.owner), &self.owner))
                    .transpose()?,
            );
        }
        Ok(ReadResult::Values {
            observed_at,
            values,
        })
    }
    fn scan_now(&self, r: &Scan) -> Result<Page<Own>, Failure> {
        self.dataset(r.dataset)?;
        if r.max_items == 0
            || r.max_bytes == 0
            || (!r.allow_change && r.after.is_some() && r.generation.is_none())
        {
            return Err(Failure::Refused);
        }
        let s = self.guard()?;
        let generation = s.generation(r.namespace.0, &self.owner)?;
        if !r.allow_change && r.generation.is_some_and(|g| g != generation) {
            return Err(Failure::StaleScan);
        }
        let records = s.records.get(&r.namespace.0);
        let mut matches = reserve(records.map_or(0, HashMap::len))?;
        if let Some(records) = records {
            for (key, value) in records {
                if key.starts_with(&r.prefix) && r.after.as_ref().is_none_or(|a| key > a) {
                    matches.push((key, value));
                }
            }
        }
        matches.sort_unstable_by(|a, b| a.0.cmp(b.0));
        let mut rows = reserve(matches.len().min(r.max_items))?;
        let mut used = 0usize;
        let mut more = false;
        for (k, v) in matches {
            let size = k
                .len()
                .checked_add(v.expose_own(&self.owner).len())
                .ok_or(Failure::Overloaded)?;
            if rows.len() == r.max_items || size > r.max_bytes - used {
                more = true;
                break;
            }
            used += size;
            rows.push((
                Address {
                    namespace: r.namespace.0,
                    key: bytes(k)?,
                },
                protected(v.expose_own(&self.owner), &self.owner)?,
            ));
        }
        if more && rows.is_empty() {
            return Err(Failure::Refused);
        }
        let next = if more {
            Some(bytes(&rows.last().ok_or(Failure::Corrupt)?.0.key)?)
        } else {
            None
        };
        Ok(Page {
            generation,
            rows,
            next,
        })
    }
    fn commit_now(&self, p: &PreparedCandidate<Own>) -> Result<Committed<Own>, Failure> {
        let c = p.candidate();
        let mut s = self.guard()?;
        // Receipts bind writer + operation regardless of changed epochs or dataset.
        let receipt_key = (c.id.writer, c.id.operation);
        if let Some(r) = s.receipts.get(&receipt_key) {
            return Ok(Committed {
                observed_at: r.observed,
                decision: if r.fingerprint == p.fingerprint() {
                    Decision::Applied(protected(r.result.expose_own(&self.owner), &self.owner)?)
                } else {
                    Decision::OpIdConflict
                },
            });
        }
        self.dataset(c.id.dataset)?;
        let observed_at = self.observation(&s)?;
        let answer = |decision| {
            Ok(Committed {
                observed_at,
                decision,
            })
        };
        let fresh = s.records.is_empty()
            && s.receipts.is_empty()
            && control::initialization(c, self.info.dataset);
        if !fresh {
            let Some(root) = s.get(control::NAMESPACE.0, control::DATASET_KEY) else {
                return answer(Decision::ExpiredUnknown);
            };
            let root =
                control::Dataset::decode(root.expose_own(&self.owner)).ok_or(Failure::Corrupt)?;
            if root.id != self.info.dataset {
                return Err(Failure::Corrupt);
            }
            let Some(writer) = s.get(control::NAMESPACE.0, &control::writer_key(c.id.writer))
            else {
                return answer(Decision::ExpiredUnknown);
            };
            let writer =
                control::Writer::decode(writer.expose_own(&self.owner)).ok_or(Failure::Corrupt)?;
            if root.writer_epoch != c.id.writer_epoch || !writer.admits(c.id.execution_epoch) {
                return answer(Decision::ExpiredUnknown);
            }
        }
        if c.time.is_some_and(|t| !t.permits(observed_at.unix_seconds)) {
            return answer(Decision::TimeGuardFailed { observed_at });
        }
        for pred in &c.predicates {
            let old = s.get(pred.address.namespace, &pred.address.key);
            if !match (&pred.expected, old) {
                (ExpectedValue::Absent, None) => true,
                (ExpectedValue::Exact(a), Some(b)) => a == b,
                _ => false,
            } {
                return answer(Decision::ConditionConflict);
            }
        }
        for m in &c.mutations {
            let (ns, key, value) = match m {
                Mutation::Put { address, value } => {
                    (address.namespace, Some(&address.key), Some(value))
                }
                Mutation::Delete(a) => (a.namespace, Some(&a.key), None),
                Mutation::DeleteRange { namespace, .. } => (namespace.0, None, None),
            };
            if ns == control::GENERATION_NAMESPACE.0 {
                return Err(Failure::Refused);
            }
            if ns == control::NAMESPACE.0 && !fresh {
                let key = key.ok_or(Failure::Refused)?;
                let value = value.ok_or(Failure::Refused)?;
                let known = control::Key::decode(key).ok_or(Failure::Refused)?;
                let old = s.get(ns, key);
                let explicit = c.predicates.iter().any(|p| {
                    p.address.namespace == ns
                        && p.address.key == *key
                        && match (&p.expected, old) {
                            (ExpectedValue::Absent, None) => true,
                            (ExpectedValue::Exact(a), Some(b)) => a == b,
                            _ => false,
                        }
                });
                if !explicit {
                    return Err(Failure::Refused);
                }
                control::replacement(
                    known,
                    self.info.dataset,
                    old.map(|v| v.expose_own(&self.owner).as_slice()),
                    value.expose_own(&self.owner),
                )?;
            }
        }
        // Everything that can allocate/fail is staged before the first effect.
        let mut effects = reserve(c.mutations.len())?;
        let mut namespaces = reserve(c.mutations.len())?;
        for m in &c.mutations {
            let ns = match m {
                Mutation::Put { address, value } => {
                    effects.try_reserve(1).map_err(|_| Failure::Overloaded)?;
                    effects.push((
                        (address.namespace, bytes(&address.key)?),
                        Some(protected(value.expose_own(&self.owner), &self.owner)?),
                    ));
                    address.namespace
                }
                Mutation::Delete(a) => {
                    effects.try_reserve(1).map_err(|_| Failure::Overloaded)?;
                    effects.push(((a.namespace, bytes(&a.key)?), None));
                    a.namespace
                }
                Mutation::DeleteRange {
                    namespace,
                    prefix,
                    exact_key_length,
                } => {
                    if let Some(records) = s.records.get(&namespace.0) {
                        for key in records.keys() {
                            if key.starts_with(prefix) && key.len() as u64 == *exact_key_length {
                                effects.try_reserve(1).map_err(|_| Failure::Overloaded)?;
                                effects.push(((namespace.0, bytes(key)?), None));
                            }
                        }
                    }
                    namespace.0
                }
            };
            namespaces.push(ns);
        }
        namespaces.sort_unstable();
        namespaces.dedup();
        effects
            .try_reserve(namespaces.len())
            .map_err(|_| Failure::Overloaded)?;
        for ns in namespaces {
            let next = s
                .generation(ns, &self.owner)?
                .checked_add(1)
                .ok_or(Failure::Refused)?;
            effects.push((
                (control::GENERATION_NAMESPACE.0, bytes(&ns)?),
                Some(protected(&next.to_le_bytes(), &self.owner)?),
            ));
        }
        let result = protected(c.result.expose_own(&self.owner), &self.owner)?;
        let returned = protected(c.result.expose_own(&self.owner), &self.owner)?;
        let mut receipt = Receipt {
            fingerprint: p.fingerprint(),
            result,
            observed: observed_at,
            execution_epoch: c.id.execution_epoch,
            applied: self.elapsed(&s),
        };
        // Count puts by namespace; reserve existing tables and prepare new tables
        // without publishing any record, receipt, or new namespace yet.
        let mut put_namespaces = reserve(effects.len())?;
        for ((ns, _), value) in &effects {
            if value.is_some() {
                put_namespaces.push(*ns);
            }
        }
        put_namespaces.sort_unstable();
        let mut new_namespaces = reserve(put_namespaces.len())?;
        let mut offset = 0;
        while offset < put_namespaces.len() {
            let ns = put_namespaces[offset];
            let count = put_namespaces[offset..].partition_point(|n| *n == ns);
            if let Some(records) = s.records.get_mut(&ns) {
                records
                    .try_reserve(count)
                    .map_err(|_| Failure::Overloaded)?;
            } else {
                let mut records = HashMap::new();
                records
                    .try_reserve(count)
                    .map_err(|_| Failure::Overloaded)?;
                new_namespaces.push((ns, records));
            }
            offset += count;
        }
        s.records
            .try_reserve(new_namespaces.len())
            .map_err(|_| Failure::Overloaded)?;
        s.receipts.try_reserve(1).map_err(|_| Failure::Overloaded)?;
        // From here onward all operations are infallible and hold the same guard.
        for (ns, records) in new_namespaces {
            s.records.insert(ns, records);
        }
        for ((ns, key), value) in effects {
            if let Some(value) = value {
                s.records
                    .get_mut(&ns)
                    .expect("reserved mutation namespace")
                    .insert(key, value);
            } else if let Some(records) = s.records.get_mut(&ns) {
                records.remove(&key);
            }
        }
        // Age from publication, after preparation and effects, never from a caller clock.
        receipt.applied = self.elapsed(&s);
        s.receipts.insert(receipt_key, receipt);
        self.collect_receipts(&mut s);
        answer(Decision::Applied(returned))
    }
    fn collect_receipts(&self, s: &mut State) {
        let elapsed = self.elapsed(s);
        let records = &s.records;
        s.receipts.retain(|(writer, _), r| {
            if elapsed.saturating_sub(r.applied) < Duration::from_secs(86400) {
                return true;
            }
            let key = control::writer_key(*writer);
            let retired = records
                .get(&control::NAMESPACE.0)
                .and_then(|rows| rows.get(key.as_slice()))
                .and_then(|v| control::Writer::decode(v.expose_own(&self.owner)))
                .is_some_and(|w| r.execution_epoch <= w.retired_through);
            !retired
        });
    }
}
fn settings(input: &[u8]) -> Result<usize, String> {
    if input.trim_ascii().is_empty() {
        return Ok(usize::MAX);
    }
    let value: serde_json::Value =
        serde_json::from_slice(input).map_err(|e| format!("invalid memory plugin config: {e}"))?;
    if value.is_null() {
        return Ok(usize::MAX);
    }
    let map = value.as_object().ok_or_else(|| {
        let got = match &value {
            serde_json::Value::Bool(_) => "a boolean",
            serde_json::Value::Number(_) => "a number",
            serde_json::Value::String(_) => "a string",
            _ => "an array",
        };
        format!("invalid memory plugin config: expected a JSON object, got {got}")
    })?;
    if let Some(key) = map.keys().find(|key| key.as_str() != "input_budget_bytes") {
        return Err(format!(
            "invalid memory plugin config: unknown setting `{key}` (accepted: input_budget_bytes)"
        ));
    }
    match map.get("input_budget_bytes") {
        None => Ok(usize::MAX),
        Some(v) => v
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .filter(|n| *n > 0)
            .ok_or_else(|| "input_budget_bytes must be a positive usize".into()),
    }
}
fn identity() -> Result<[u8; 16], String> {
    loop {
        let mut id = [0; 16];
        getrandom::fill(&mut id).map_err(|_| "OS entropy unavailable")?;
        if id != [0; 16] {
            return Ok(id);
        }
    }
}
impl Backend for MemoryV4 {
    type Owner = Own;
    fn owner(&self) -> &Own {
        &self.owner
    }
    fn input_budget(&self) -> usize {
        self.budget
    }
    fn validate(input: &[u8]) -> Result<(), String> {
        settings(input).map(drop)
    }
    fn open(
        input: &[u8],
        _: &[&[u8]],
        _: Option<Host<busbar_contract::plugin::markers::StoreKind>>,
    ) -> Result<Self, String> {
        Ok(Self {
            owner: Own::here(),
            budget: settings(input)?,
            info: BackendInfo {
                dataset: DatasetId(identity()?),
                atomic_domain: identity()?,
                clock_authority: identity()?,
                physical_version: 1,
                durability: Durability::Ephemeral,
            },
            started: Instant::now(),
            state: Mutex::new(State::default()),
        })
    }
    fn bootstrap(
        &self,
        _: &mut Op<'_>,
        request: &lifecycle::Request<Own>,
    ) -> Step<Result<lifecycle::Response<Own>, lifecycle::Error>> {
        Step::Ready(self.bootstrap_now(request))
    }
    fn connect(&self, _: &mut Op<'_>) -> Step<Result<(), String>> {
        Step::Ready(self.connect_binding(None))
    }
    fn connect_prepared(
        &self,
        _: &mut Op<'_>,
        binding: &lifecycle::OpenBinding,
    ) -> Step<Result<(), String>> {
        Step::Ready(self.connect_binding(Some(binding)))
    }
    fn info(&self) -> BackendInfo {
        self.info
    }
    fn read(&self, _: &mut Op<'_>, r: &Read) -> Step<Result<ReadResult<Own>, Failure>> {
        Step::Ready(self.read_now(r))
    }
    fn scan(&self, _: &mut Op<'_>, r: &Scan) -> Step<Result<Page<Own>, Failure>> {
        Step::Ready(self.scan_now(r))
    }
    fn commit(
        &self,
        _: &mut Op<'_>,
        r: &PreparedCandidate<Own>,
    ) -> Step<Result<Committed<Own>, Failure>> {
        Step::Ready(self.commit_now(r))
    }
    fn clock(&self, _: &mut Op<'_>, dataset: DatasetId) -> Step<Result<ClockObservation, Failure>> {
        Step::Ready(
            self.dataset(dataset)
                .and_then(|()| self.guard().and_then(|s| self.observation(&s))),
        )
    }
}
#[cfg(test)]
#[path = "tests/v4_tests.rs"]
mod tests;
