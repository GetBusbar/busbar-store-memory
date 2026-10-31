// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! Tests for `crates/store-memory/src/lib.rs`.

use super::*;
use busbar_contract::records::SecretForm;

/// A credential resolved by `(kind, public_id)`, read off the store's own rows: what the tests below
/// check a cascade, a refusal or the retention sweep left behind.
trait Lookup {
    fn lookup_credential_secret(
        &self,
        kind: &str,
        public_id: &str,
    ) -> StoreResult<Option<CredentialSecret>>;
}

impl Lookup for MemoryStore {
    fn lookup_credential_secret(
        &self,
        kind: &str,
        public_id: &str,
    ) -> StoreResult<Option<CredentialSecret>> {
        Ok(read(&self.creds)
            .values()
            .find(|c| c.meta.kind == kind && c.meta.public_id == public_id)
            .cloned())
    }
}

fn key(id: &str) -> VirtualKey {
    VirtualKey {
        id: id.to_string(),
        generation_hash: format!("h_{id}"),
        name: "t".to_string(),
        allowed_scopes: None,
        enabled: true,
        created_at: 0,
        group: None,
        labels: std::collections::BTreeMap::new(),
        expires_at: None,
        deleted_at: None,
        revision: 0,
        ..Default::default()
    }
}

fn credential(id: &str, key_id: &str, public_id: &str) -> CredentialSecret {
    CredentialSecret {
        meta: CredentialMeta {
            id: id.to_string(),
            key_id: key_id.to_string(),
            kind: "generic".to_string(),
            slot: 0,
            public_id: public_id.to_string(),
            secret_form: SecretForm::Recoverable,
            created_at: 0,
            updated_at: 0,
            expires_at: None,
            revoked_at: None,
            revoke_reason: None,
            revision: 0,
        },
        secret: "v1:plain:sek".to_string(),
    }
}

fn ledger(requests: u64, model: &str, input: u64, output: u64) -> UsageLedger {
    UsageLedger {
        requests,
        billable_requests: requests,
        models: vec![busbar_contract::records::ModelTokens {
            model: model.to_string(),
            usage_units: [
                (busbar_contract::records::UNIT_INPUT, input),
                (busbar_contract::records::UNIT_OUTPUT, output),
            ]
            .into_iter()
            .filter(|(_, v)| *v != 0)
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
        }],
    }
}

#[test]
fn key_crud_and_ledger_roundtrip() {
    let s = MemoryStore::new();
    s.put_key(&key("a")).unwrap();
    assert_eq!(s.get_key("a").unwrap().unwrap().id, "a");
    assert_eq!(s.list_keys().unwrap().len(), 1);
    // absolute put_usage then read back
    s.put_usage("a", 0, &ledger(3, "m", 100, 40)).unwrap();
    let u = s.get_usage("a", 0).unwrap();
    assert_eq!(u.requests, 3);
    assert_eq!(u.total_input(), 100);
    // absolute overwrite (not additive)
    s.put_usage("a", 0, &ledger(1, "m", 20, 0)).unwrap();
    assert_eq!(s.get_usage("a", 0).unwrap().total_input(), 20);
    // unknown window is default-empty
    assert_eq!(s.get_usage("a", 999).unwrap(), UsageLedger::default());
}

/// Additive per-model delta accumulate: two adds sum, a second model materializes its own row,
/// and negative deltas floor at 0 (parity contract with sqlite/postgres/valkey).
#[test]
fn add_usage_accumulates_per_model() {
    let s = MemoryStore::new();
    let d = UsageDelta {
        requests: 1,
        billable_requests: 1,
        models: vec![busbar_contract::records::ModelTokensDelta {
            model: "gpt-5".to_string(),
            usage_units: std::collections::BTreeMap::from([
                (busbar_contract::records::UNIT_INPUT.to_string(), 10i64),
                (busbar_contract::records::UNIT_OUTPUT.to_string(), 5),
                (busbar_contract::records::UNIT_CACHE_READ.to_string(), 1),
            ]),
        }],
    };
    s.add_usage("bucket", 100, &d).unwrap();
    s.add_usage("bucket", 100, &d).unwrap();
    let u = s.get_usage("bucket", 100).unwrap();
    assert_eq!(u.requests, 2);
    assert_eq!(
        (u.total_input(), u.total_output(), u.total_cache_read()),
        (20, 10, 2)
    );
    // Refund floors at zero.
    s.add_usage(
        "bucket",
        100,
        &UsageDelta {
            requests: -5,
            billable_requests: -5,
            models: vec![],
        },
    )
    .unwrap();
    assert_eq!(s.get_usage("bucket", 100).unwrap().requests, 0);
}

#[test]
fn delete_key_tombstones_and_cascades_creds_but_keeps_usage() {
    let s = MemoryStore::new();
    s.put_key(&key("a")).unwrap();
    s.put_usage("a", 0, &ledger(1, "m", 5, 0)).unwrap();
    s.put_credential(&credential("c1", "a", "AKIA1")).unwrap();
    s.delete_key("a").unwrap();
    // TOMBSTONE, not removed: the row survives, disabled, with deleted_at set.
    let tombstone = s.get_key("a").unwrap().unwrap();
    assert!(!tombstone.enabled);
    assert!(tombstone.deleted_at.is_some());
    // Usage is billing evidence: it outlives the key.
    assert_eq!(s.get_usage("a", 0).unwrap().requests, 1);
    assert!(s.list_credentials("a").unwrap().is_empty());
    // Idempotent: a second delete of an already-tombstoned key is a no-op, not an error.
    s.delete_key("a").unwrap();
}

#[test]
fn put_credential_rejects_a_slot_already_holding_a_live_credential() {
    let s = MemoryStore::new();
    s.put_key(&key("a")).unwrap();
    s.put_credential(&credential("c1", "a", "AKIA1")).unwrap();
    // Same (key_id, kind, slot), different id/public_id: must fail, not silently clobber.
    let clobber = credential("c2", "a", "AKIA2");
    assert!(s.put_credential(&clobber).is_err());
    // Revoking the occupant frees the slot for a fresh mint.
    s.revoke_credential("c1", "rotated").unwrap();
    assert!(s.put_credential(&clobber).is_ok());
}

#[test]
fn put_credential_rejects_a_public_id_reused_under_a_different_key() {
    let s = MemoryStore::new();
    s.put_key(&key("a")).unwrap();
    s.put_key(&key("b")).unwrap();
    s.put_credential(&credential("c1", "a", "AKIA1")).unwrap();
    // Different key, different id, SAME (kind, public_id) — the global AccessKeyId->credential
    // lookup handle must resolve to exactly one credential.
    let mut dupe = credential("c2", "b", "AKIA1");
    dupe.meta.slot = 1; // different slot too, so only the public_id clash can reject it
    assert!(s.put_credential(&dupe).is_err());
    // A genuinely distinct public_id under the other key is fine.
    let mut ok = credential("c3", "b", "AKIA2");
    ok.meta.slot = 1;
    assert!(s.put_credential(&ok).is_ok());
}

#[test]
fn put_credential_public_id_check_excludes_its_own_row_on_reput() {
    // The uniqueness scan excludes the row with the SAME id (`c.meta.id != secret.meta.id`) —
    // otherwise a credential could never even be inserted once the id already existed. This
    // only matters once an id can legitimately be re-put; simulate it by inserting once, then
    // putting the identical secret again under the identical id/public_id/kind and confirming
    // it's accepted, not rejected as "colliding with itself".
    let s = MemoryStore::new();
    s.put_key(&key("a")).unwrap();
    let c = credential("c1", "a", "AKIA1");
    s.put_credential(&c).unwrap();
    assert!(
        s.put_credential(&c).is_ok(),
        "a row must not collide with itself"
    );
}

#[test]
fn list_credentials_filters_by_key_id_and_since_boundary_is_exclusive() {
    let s = MemoryStore::new();
    s.put_key(&key("a")).unwrap();
    s.put_key(&key("b")).unwrap();
    s.put_credential(&credential("c1", "a", "AKIA1")).unwrap();
    let mut c2 = credential("c2", "b", "AKIA2");
    c2.meta.slot = 1;
    s.put_credential(&c2).unwrap();

    let for_a = s.list_credentials("a").unwrap();
    assert_eq!(for_a.len(), 1, "must not also return key b's credential");
    assert_eq!(for_a[0].id, "c1");

    // revision boundary: `revision > since` is exclusive of `since` itself. c2 was put after
    // c1, so it holds the higher revision — use IT as the boundary reference, or c1 (the lower
    // revision) would still be `> since` and the assertion below would be vacuous. The store's
    // revision counter is global (shared with `put_key`), so c1's revision is NOT necessarily
    // `newest_rev - 1` — read it directly rather than assuming adjacency.
    let oldest_rev = s.list_credentials("a").unwrap()[0].revision;
    let newest_rev = s.list_credentials("b").unwrap()[0].revision;
    assert!(newest_rev > oldest_rev);
    assert_eq!(
        s.list_credentials_since(newest_rev).unwrap().len(),
        0,
        "since == the newest row's own revision must exclude it"
    );
    assert_eq!(
        s.list_credentials_since(oldest_rev - 1).unwrap().len(),
        2,
        "since one below the lowest revision must include everything"
    );
}

#[test]
fn list_keys_since_boundary_is_exclusive() {
    let s = MemoryStore::new();
    s.put_key(&key("a")).unwrap();
    s.put_key(&key("b")).unwrap();
    let rev_a = s.get_key("a").unwrap().unwrap().revision;
    let rev_b = s.get_key("b").unwrap().unwrap().revision;
    assert!(rev_b > rev_a);
    assert_eq!(
        s.list_keys_since(rev_b).unwrap().len(),
        0,
        "since == the newest row's own revision must exclude it"
    );
    assert_eq!(
        s.list_keys_since(rev_a).unwrap().len(),
        1,
        "must include only b"
    );
    assert_eq!(s.list_keys_since(rev_a - 1).unwrap().len(), 2);
}

#[test]
fn next_revision_is_strictly_monotonic_starting_above_zero() {
    let s = MemoryStore::new();
    s.put_key(&key("a")).unwrap();
    s.put_key(&key("b")).unwrap();
    let rev_a = s.get_key("a").unwrap().unwrap().revision;
    let rev_b = s.get_key("b").unwrap().unwrap().revision;
    assert!(
        rev_a > 0,
        "the counter must not hand out 0 as a real revision"
    );
    assert_eq!(rev_b, rev_a + 1, "each call must advance by exactly 1");
}

#[test]
fn metering_accumulates_per_bucket() {
    let s = MemoryStore::new();
    let d = MeteringDelta {
        usage_units: Default::default(),
        key_id: "a".to_string(),
        bucket: 7,
        model: "m".to_string(),
        provider: "p".to_string(),
        tokens_input: 10,
        tokens_output: 5,
        tokens_cache_read: 0,
        tokens_cache_write: 0,
        requests: 1,
        billable_requests: 1,
        key_group_at_use: String::new(),
        pricing_version: String::new(),
        priced_from_ms: 0,
    };
    s.add_metering(&d).unwrap();
    s.add_metering(&d).unwrap();
    let rows = s.list_metering(7).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].tokens_input, 20);
    assert_eq!(rows[0].requests, 2);
    assert!(s.list_metering(999).unwrap().is_empty());
}

/// Regression: `usage` must not grow unbounded forever. A window older than the 31-day
/// retention ceiling gets swept once `add_usage` has been called `SWEEP_INTERVAL` times
/// (the amortized sweep cadence), even though nothing ever explicitly deletes it.
#[test]
fn add_usage_sweeps_stale_windows() {
    let s = MemoryStore::new();
    let old_window = now().saturating_sub(40 * 86_400); // 40 days old > 31-day retention
    let d = UsageDelta {
        requests: 1,
        billable_requests: 1,
        models: vec![],
    };
    for _ in 0..SWEEP_INTERVAL {
        s.add_usage("old-bucket", old_window, &d).unwrap();
    }
    // The sweep fired on the SWEEP_INTERVAL-th write and evicted the stale row (including the
    // one just written in that same call, since it's aged by its window_start, not by
    // recency-of-write).
    assert_eq!(
        s.get_usage("old-bucket", old_window).unwrap(),
        UsageLedger::default()
    );

    // A fresh window written afterward is unaffected.
    let fresh_window = now();
    s.add_usage("fresh-bucket", fresh_window, &d).unwrap();
    assert_eq!(
        s.get_usage("fresh-bucket", fresh_window).unwrap().requests,
        1
    );
}

/// Regression: the sweep must not over-prune. A window well within the 31-day retention
/// ceiling survives a sweep triggered by writes to an unrelated, genuinely stale window.
#[test]
fn add_usage_sweep_preserves_fresh_windows() {
    let s = MemoryStore::new();
    let young_window = now().saturating_sub(5 * 86_400); // 5 days old, well within retention
    let old_window = now().saturating_sub(40 * 86_400); // 40 days old, past retention
    let d = UsageDelta {
        requests: 1,
        billable_requests: 1,
        models: vec![],
    };
    s.add_usage("young-bucket", young_window, &d).unwrap();
    for _ in 0..(SWEEP_INTERVAL - 1) {
        s.add_usage("old-bucket", old_window, &d).unwrap();
    }
    // That's SWEEP_INTERVAL total add_usage calls, so the sweep just fired.
    assert_eq!(
        s.get_usage("young-bucket", young_window).unwrap().requests,
        1
    );
    assert_eq!(
        s.get_usage("old-bucket", old_window).unwrap(),
        UsageLedger::default()
    );
}

/// The sweep boundary itself: a window exactly `MAX_RETENTION_SECS` old sits AT the ceiling
/// (`window_start + MAX_RETENTION_SECS == now`) and must be evicted (`>`, not `>=`, is the
/// retain condition — a row must be STRICTLY inside the window to survive), while one second
/// fresher survives.
#[test]
fn add_usage_sweep_boundary_is_exact() {
    let s = MemoryStore::new();
    let n = now();
    // Pin the sweep's clock to the SAME `n` the test derives its buckets from, so the retention
    // ceiling is exact and a wall-clock tick between here and the sweep can't shift it. Without
    // this, `one_inside` intermittently falls at/below an advanced ceiling and is wrongly evicted.
    s.pin_clock(n);
    let at_ceiling = n.saturating_sub(MAX_RETENTION_SECS);
    let one_inside = at_ceiling + 1;
    let d = UsageDelta {
        requests: 1,
        billable_requests: 1,
        models: vec![],
    };
    s.add_usage("at-ceiling", at_ceiling, &d).unwrap();
    s.add_usage("one-inside", one_inside, &d).unwrap();
    for _ in 0..(SWEEP_INTERVAL - 2) {
        s.add_usage("filler", one_inside, &d).unwrap();
    }
    assert_eq!(
        s.get_usage("at-ceiling", at_ceiling).unwrap(),
        UsageLedger::default(),
        "a window exactly at the retention ceiling must be evicted"
    );
    assert_eq!(
        s.get_usage("one-inside", one_inside).unwrap().requests,
        1,
        "a window one second inside the ceiling must survive"
    );
}

/// The all-time window (`window_start == 0`) is the ledger's own row, not an aged period: the
/// sweep never evicts it.
#[test]
fn add_usage_sweep_keeps_the_all_time_window() {
    let s = MemoryStore::new();
    s.pin_clock(100 * 86_400);
    let d = UsageDelta {
        requests: 1,
        billable_requests: 1,
        models: vec![],
    };
    for _ in 0..SWEEP_INTERVAL {
        s.add_usage("b", 0, &d).unwrap();
    }
    assert_eq!(
        s.get_usage("b", 0).unwrap().requests,
        SWEEP_INTERVAL,
        "the sweep evicted the all-time ledger row"
    );
}

/// Regression: `metering` must not grow unbounded forever either — same amortized sweep, keyed
/// by the (day) `bucket` field this time.
#[test]
fn add_metering_sweeps_stale_buckets() {
    let s = MemoryStore::new();
    let old_bucket = now().saturating_sub(40 * 86_400);
    let d = MeteringDelta {
        usage_units: Default::default(),
        key_id: "k".to_string(),
        bucket: old_bucket,
        model: "m".to_string(),
        provider: "p".to_string(),
        tokens_input: 1,
        tokens_output: 0,
        tokens_cache_read: 0,
        tokens_cache_write: 0,
        requests: 1,
        billable_requests: 1,
        key_group_at_use: String::new(),
        pricing_version: String::new(),
        priced_from_ms: 0,
    };
    for _ in 0..SWEEP_INTERVAL {
        s.add_metering(&d).unwrap();
    }
    assert!(s.list_metering(old_bucket).unwrap().is_empty());

    let fresh_bucket = now();
    let fresh = MeteringDelta {
        bucket: fresh_bucket,
        ..d.clone()
    };
    s.add_metering(&fresh).unwrap();
    assert_eq!(s.list_metering(fresh_bucket).unwrap().len(), 1);
}

/// Regression: metering sweep must not over-prune fresh buckets either.
#[test]
fn add_metering_sweep_preserves_fresh_buckets() {
    let s = MemoryStore::new();
    let young_bucket = now().saturating_sub(5 * 86_400);
    let old_bucket = now().saturating_sub(40 * 86_400);
    let young = MeteringDelta {
        usage_units: Default::default(),
        key_id: "k".to_string(),
        bucket: young_bucket,
        model: "m".to_string(),
        provider: "p".to_string(),
        tokens_input: 1,
        tokens_output: 0,
        tokens_cache_read: 0,
        tokens_cache_write: 0,
        requests: 1,
        billable_requests: 1,
        key_group_at_use: String::new(),
        pricing_version: String::new(),
        priced_from_ms: 0,
    };
    let old = MeteringDelta {
        bucket: old_bucket,
        ..young.clone()
    };
    s.add_metering(&young).unwrap();
    for _ in 0..(SWEEP_INTERVAL - 1) {
        s.add_metering(&old).unwrap();
    }
    assert_eq!(s.list_metering(young_bucket).unwrap().len(), 1);
    assert!(s.list_metering(old_bucket).unwrap().is_empty());
}

/// Same exact-boundary case as `add_usage_sweep_boundary_is_exact`, for metering's bucket
/// retention: a bucket exactly `MAX_RETENTION_SECS` old must be evicted, one second fresher
/// must survive.
#[test]
fn add_metering_sweep_boundary_is_exact() {
    let s = MemoryStore::new();
    let n = now();
    // Pin the sweep's clock to the SAME `n` the test derives its buckets from (see the usage
    // boundary test above) so the retention ceiling is exact and race-free.
    s.pin_clock(n);
    let at_ceiling = n.saturating_sub(MAX_RETENTION_SECS);
    let one_inside = at_ceiling + 1;
    let base = MeteringDelta {
        usage_units: Default::default(),
        key_id: "k".to_string(),
        bucket: at_ceiling,
        model: "m".to_string(),
        provider: "p".to_string(),
        tokens_input: 1,
        tokens_output: 0,
        tokens_cache_read: 0,
        tokens_cache_write: 0,
        requests: 1,
        billable_requests: 1,
        key_group_at_use: String::new(),
        pricing_version: String::new(),
        priced_from_ms: 0,
    };
    let inside = MeteringDelta {
        bucket: one_inside,
        ..base.clone()
    };
    s.add_metering(&base).unwrap();
    for _ in 0..(SWEEP_INTERVAL - 1) {
        s.add_metering(&inside).unwrap();
    }
    assert!(
        s.list_metering(at_ceiling).unwrap().is_empty(),
        "a bucket exactly at the retention ceiling must be evicted"
    );
    assert_eq!(
        s.list_metering(one_inside).unwrap().len(),
        1,
        "a bucket one second inside the ceiling must survive"
    );
}

/// A tombstone is permanent (the contract: the id is never reissued, `delete_key` stays
/// idempotent), so no sweep ages it out however old it is.
#[test]
fn a_tombstone_survives_every_sweep_and_its_id_stays_refused() {
    let s = MemoryStore::new();
    let n = now();
    let old_deleted_at = n.saturating_sub(40 * 86_400);

    s.put_key(&key("old-tombstone")).unwrap();
    s.pin_clock(old_deleted_at);
    s.delete_key("old-tombstone").unwrap();
    s.put_key(&key("live")).unwrap();
    s.pin_clock(n);

    for i in 0..2 * SWEEP_INTERVAL {
        s.put_key(&key(&format!("filler-{i}"))).unwrap();
    }

    let kept = s.get_key("old-tombstone").unwrap().expect("tombstone kept");
    assert_eq!(kept.deleted_at, Some(old_deleted_at));
    assert!(
        s.put_key(&key("old-tombstone")).is_err(),
        "a live write over an aged tombstone reissued the id"
    );
    s.delete_key("old-tombstone")
        .expect("delete stays idempotent");
    assert!(s.get_key("live").unwrap().is_some());
}

/// A revoked credential is kept, however old: revoking it again is `Ok` (the contract), and no
/// sweep may turn that retry into "unknown id".
#[test]
fn a_revoked_credential_survives_every_sweep_and_revoke_stays_idempotent() {
    let s = MemoryStore::new();
    let n = now();
    s.put_key(&key("k-old")).unwrap();
    s.put_credential(&credential("old-revoked", "k-old", "AKIA_OLD"))
        .unwrap();
    s.pin_clock(n.saturating_sub(40 * 86_400));
    s.revoke_credential("old-revoked", "rotated").unwrap();
    s.pin_clock(n);

    for i in 0..2 * SWEEP_INTERVAL {
        let kid = format!("k-filler-{i}");
        s.put_key(&key(&kid)).unwrap();
        s.put_credential(&credential(
            &format!("filler-{i}"),
            &kid,
            &format!("AKIA_F{i}"),
        ))
        .unwrap();
    }

    s.revoke_credential("old-revoked", "again")
        .expect("a retried revoke of an already-revoked credential is Ok");
    let kept = s
        .lookup_credential_secret("generic", "AKIA_OLD")
        .unwrap()
        .expect("revoked row kept");
    assert_eq!(kept.meta.revoke_reason.as_deref(), Some("rotated"));
}

/// The contract upserts a credential on `(key_id, kind, slot)`: minting into a slot a REVOKED
/// credential holds replaces that row, whether or not the id is the same. Both mint paths.
#[test]
fn a_mint_into_a_revoked_slot_replaces_the_revoked_row() {
    let s = MemoryStore::new();
    s.put_key(&key("k")).unwrap();
    s.put_credential(&credential("c1", "k", "P1")).unwrap();
    s.revoke_credential("c1", "rotated").unwrap();
    s.put_credential(&credential("c2", "k", "P2")).unwrap();
    let ids: Vec<String> = s
        .list_credentials("k")
        .unwrap()
        .into_iter()
        .map(|c| c.id)
        .collect();
    assert_eq!(ids, vec!["c2".to_string()], "slot 0 holds one row");

    s.revoke_credential("c2", "rotated").unwrap();
    s.put_key_with_credential(&key("k"), &credential("c3", "k", "P3"))
        .unwrap();
    let ids: Vec<String> = s
        .list_credentials("k")
        .unwrap()
        .into_iter()
        .map(|c| c.id)
        .collect();
    assert_eq!(ids, vec!["c3".to_string()], "the atomic mint upserts too");
}

/// The upsert on `(key_id, kind, slot)` replaces a revoked row, so re-minting that row's own
/// `public_id` into its slot is not a clash with the row it replaces.
#[test]
fn a_remint_into_a_revoked_slot_may_reuse_its_public_id() {
    let s = MemoryStore::new();
    s.put_key(&key("k")).unwrap();
    s.put_credential(&credential("c1", "k", "P1")).unwrap();
    s.revoke_credential("c1", "rotated").unwrap();
    s.put_credential(&credential("c2", "k", "P1"))
        .expect("the revoked row is replaced, its public_id with it");
    let ids: Vec<String> = s
        .list_credentials("k")
        .unwrap()
        .into_iter()
        .map(|c| c.id)
        .collect();
    assert_eq!(ids, vec!["c2".to_string()]);
}

/// A pure READ must not queue behind another pure read. `list_keys` over a large table clones every
/// row while holding the `keys` lock, and on the governance hot path a `get_key` arriving in that
/// window is an admit decision waiting: if both take the WRITE side of the `RwLock`, the point read
/// blocks for the whole listing pass, and a big enough table turns an admin listing into a latency
/// spike on every request.
///
/// The listing's guard is parked here for the whole check (a stand-in for "a very large table"), so
/// the assertion is deterministic rather than a timing race: with a shared read guard the concurrent
/// `get_key` completes immediately; with an exclusive one it cannot complete at all until the
/// listing lets go, and the bounded wait below fails rather than hanging the suite.
#[test]
fn a_parked_listing_read_does_not_block_a_concurrent_point_read() {
    use std::sync::mpsc;
    let store = std::sync::Arc::new(MemoryStore::new());
    store.put_key(&key("k1")).unwrap();

    // Exactly the guard `list_keys` holds while it clones the table.
    let parked = store.keys.read().unwrap_or_else(|e| e.into_inner());

    let (tx, rx) = mpsc::channel();
    let reader = std::sync::Arc::clone(&store);
    let handle = std::thread::spawn(move || {
        let found = reader.get_key("k1").expect("point read").is_some();
        // A second listing is another shared read, so it must not wait either.
        let listed = reader.list_keys().expect("second listing").len() == 1;
        let _ = tx.send(found && listed);
    });

    let answered = rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap_or(false);
    drop(parked);
    handle.join().expect("the point read thread");
    assert!(
        answered,
        "a concurrent get_key or list_keys did not complete while a listing read was in flight — \
         a read is serialized behind the whole listing pass"
    );
}

/// A credential may only hang off a LIVE key. `delete_key` tombstones the key and cascades away
/// every credential it owned precisely so the secret material stops resolving; a `put_credential`
/// arriving afterwards (an in-flight rotation, a retry, a hydrating replica) put the material
/// straight back under a key an operator had just revoked, and `lookup_credential_secret` resolved
/// it — the tombstone cascade undone through the other door.
#[test]
fn put_credential_refuses_a_tombstoned_or_absent_key() {
    let s = MemoryStore::new();

    // No such key at all: nothing to own the credential.
    assert!(
        s.put_credential(&credential("orphan", "ghost", "AKIA_GHOST"))
            .is_err(),
        "a credential whose owning key names no row must be refused"
    );

    s.put_key(&key("a")).unwrap();
    s.delete_key("a").unwrap();
    assert!(
        s.put_credential(&credential("c1", "a", "AKIA1")).is_err(),
        "a credential minted onto a TOMBSTONED key must be refused"
    );
    assert!(
        s.lookup_credential_secret("generic", "AKIA1")
            .unwrap()
            .is_none(),
        "the refused credential must not resolve — that is the whole point of the cascade"
    );
}

/// The key+credential mint is ATOMIC or it is nothing. The trait's default is the two-call sequence
/// (`put_key` then `put_credential`), and the credential leg fails on an ordinary operator mistake —
/// a `public_id` already in use. Under the default the key leg has already committed by then, so the
/// mint reports failure while leaving a live key with no credential behind it: a row the
/// caller does not know exists and will never clean up.
#[test]
fn put_key_with_credential_leaves_no_key_behind_when_the_credential_is_refused() {
    let s = MemoryStore::new();
    s.put_key(&key("incumbent")).unwrap();
    s.put_credential(&credential("c-incumbent", "incumbent", "AKIA_TAKEN"))
        .unwrap();

    // The mint the operator asks for, colliding on the global (kind, public_id) handle.
    let mut minted = credential("c-new", "fresh", "AKIA_TAKEN");
    minted.meta.key_id = "fresh".to_string();
    assert!(
        s.put_key_with_credential(&key("fresh"), &minted).is_err(),
        "a mint whose credential collides on public_id must fail"
    );
    assert!(
        s.get_key("fresh").unwrap().is_none(),
        "the failed mint left the key committed — the pair went in one leg at a time"
    );
    assert!(
        s.list_credentials("fresh").unwrap().is_empty(),
        "and no credential row may survive the refusal either"
    );

    // The success path still writes BOTH rows.
    let ok = credential("c-ok", "fresh", "AKIA_FREE");
    s.put_key_with_credential(&key("fresh"), &ok)
        .expect("a clean mint writes both rows");
    assert!(s.get_key("fresh").unwrap().is_some());
    assert_eq!(s.list_credentials("fresh").unwrap().len(), 1);
}

/// The spent-token ledger is bounded by the tokens' own expiry: a row past its `expires_at` can
/// never be presented again, so the amortised sweep drops lapsed rows, once every `SWEEP_INTERVAL`
/// redemptions. The ledger is keyed by `(kind, token)`, so the same nonce under a different kind is
/// its own single-use grant rather than a collision.
#[test]
fn redeem_plane_token_sweeps_lapsed_rows_and_separates_kinds() {
    let s = MemoryStore::new();
    assert!(s.redeem_plane_token("ask", "n1", 100, 50).unwrap());
    assert!(!s.redeem_plane_token("ask", "n1", 100, 50).unwrap());

    // Same nonce, different kind: a separate grant, not the same spent row.
    assert!(s.redeem_plane_token("other", "n1", 100, 50).unwrap());

    // Past its own expiry the row is dropped by the sweep — nothing upstream can present it again
    // anyway, and keeping it only grows the map.
    for i in 0..SWEEP_INTERVAL {
        assert!(s
            .redeem_plane_token("ask", &format!("m{i}"), 300, 200)
            .unwrap());
    }
    assert!(
        s.plane_tokens
            .read()
            .unwrap()
            .keys()
            .all(|(_, t)| t != "n1"),
        "the two lapsed rows must be swept within one sweep interval"
    );
}

/// A LOSING REDEMPTION CHANGES NOTHING (`records.claim` is "a one-time put-if-absent with a time to
/// live", the one path for approval redemption and replay refusal). The loser's own expiry must not
/// replace the winner's: a shorter one would free the key inside the winner's window, so the same
/// approval could be redeemed twice; a longer one would keep it blocked past it.
#[test]
fn a_losing_redemption_leaves_the_winners_expiry() {
    let s = MemoryStore::new();
    assert!(s.redeem_plane_token("ask", "n1", 1_000, 0).unwrap());
    // A replay with a short time to live loses, and must not shorten the winner's claim.
    assert!(!s.redeem_plane_token("ask", "n1", 10, 0).unwrap());
    assert!(
        !s.redeem_plane_token("ask", "n1", 2_000, 500).unwrap(),
        "the key was won a second time inside the first winner's window"
    );
    // A replay with a long time to live loses, and must not extend it.
    assert!(s.redeem_plane_token("ask", "n3", 100, 0).unwrap());
    assert!(!s.redeem_plane_token("ask", "n3", 5_000, 50).unwrap());
    assert!(
        s.redeem_plane_token("ask", "n3", 6_000, 101).unwrap(),
        "a lapsed claim stays blocked by a loser's expiry"
    );
}

/// A REDEMPTION DOES NOT WALK THE SPENT MAP. The inbound replay check redeems once per request that
/// carries a replay key, so a redemption that swept every live row under the map's exclusive guard
/// made each request O(live tokens). A lapsed row is vacant at its own lookup instead, and the sweep
/// is amortised.
#[test]
fn a_redemption_does_not_sweep_the_spent_map() {
    let s = MemoryStore::new();
    assert!(s.redeem_plane_token("ask", "a", 100, 0).unwrap());
    assert!(s.redeem_plane_token("ask", "b", 100, 0).unwrap());
    assert!(s.redeem_plane_token("ask", "c", 1_000, 200).unwrap());
    assert_eq!(
        s.plane_tokens.read().unwrap().len(),
        3,
        "one redemption swept the whole spent map"
    );
    // The lapsed row is vacant to its own key.
    assert!(s.redeem_plane_token("ask", "a", 1_000, 200).unwrap());
    assert!(!s.redeem_plane_token("ask", "a", 1_000, 300).unwrap());
}

/// AN ACTIVE WORK HANDLE IS NEVER EVICTED (`BUSBAR-1.6.0.md` THE DESIGN §1, "Admission bounds live
/// work; nothing evicts it"), and on the record leg nothing tells this backend a row is settled.
///
/// The conformance suite's definition of active is the published path's: a row is settled only when
/// its [`PlaneDisposition`] sidecar says `Terminal`, and age alone never drops a task
/// (`tests/store_conformance.rs`, `assert_plane_purge_task_keeps_active_rows`). A record-leg row
/// has no sidecar — an opaque key, an opaque body — so no row here can be proved settled, and every
/// row is kept however long ago it was written. This cell used to pin the opposite: a `task` record
/// written 40 days ago was swept on write age alone after `SWEEP_INTERVAL` unrelated writes.
///
/// THE RED ARM, kept in the cell: the SAME store, on the SAME pinned clock and the SAME cadence,
/// DOES age out a 40-day-old usage window. So the fixture really is past the 31-day ceiling and the
/// store's age sweep really did come round; the record survives because the record leg is exempt,
/// not because nothing was old enough or no sweep ran. The pre-fix store fails the first assertion
/// below (the `task` record read back `None`).
#[test]
fn record_put_never_evicts_a_record_by_age() {
    const SCHEMA: RecordSchemaId = RecordSchemaId::new("task");
    let body = |b: &[u8]| RecordBytes::new(b.to_vec()).expect("inside the record ceiling");

    let s = MemoryStore::new();
    let n = now();
    let forty_days_ago = n.saturating_sub(40 * 86_400);
    // An interrupted task, written 40 days ago and never rewritten since: the row that sits still
    // the longest is exactly the one an age rule would drop. (`record_put` reads no clock; the
    // age is in the usage window below.)
    s.record_put(SCHEMA, b"waiting-on-a-human", &body(b"input-required"))
        .unwrap();
    s.record_put(SCHEMA, b"at-the-old-ceiling", &body(b"working"))
        .unwrap();

    // Pin the sweep's clock, and write far more than any amortized cadence in this file needs to
    // come round (twice SWEEP_INTERVAL, so a ticker offset by the two writes above cannot dodge it).
    s.pin_clock(n);
    let usage = UsageDelta {
        requests: 1,
        billable_requests: 1,
        models: vec![],
    };
    for i in 0..2 * SWEEP_INTERVAL {
        s.record_put(SCHEMA, format!("filler-{i}").as_bytes(), &body(b"f"))
            .unwrap();
        s.add_usage("old-bucket", forty_days_ago, &usage).unwrap();
    }

    assert_eq!(
        s.record_get(SCHEMA, b"waiting-on-a-human").unwrap(),
        Some(body(b"input-required")),
        "a record-leg task written 40 days ago was EVICTED on write age alone — nothing on the record \
         leg says it is settled, so it is an active work handle and may not be evicted"
    );
    assert_eq!(
        s.record_get(SCHEMA, b"at-the-old-ceiling").unwrap(),
        Some(body(b"working")),
        "a record-leg row at the old 31-day ceiling was evicted by age"
    );
    assert_eq!(
        s.record_scan(SCHEMA, b"", u32::MAX).unwrap().len(),
        2 + 2 * SWEEP_INTERVAL as usize,
        "the record leg dropped rows: every record written must still be readable"
    );

    // THE RED ARM: the store's age sweep is live and the clock is past the ceiling — the 40-day-old
    // usage window written in the same loop IS gone.
    assert_eq!(
        s.get_usage("old-bucket", forty_days_ago).unwrap(),
        UsageLedger::default(),
        "the positive control did not fire: a 40-day-old usage window survived the same cadence, so \
         this cell could not tell a record leg that never evicts from a sweep that never ran"
    );
}

/// AN EMPTY PUT REMOVES THE ROW (slot 40's rule, ARCHITECT 2026-10-21): the host writes every
/// strike (a swept work handle, a dropped chain) as an empty `record_put` and reads empty as
/// absent, so the row LEAVES the map: `record_get` answers `None`, `record_scan` never returns it,
/// and the map shrinks. A store that kept the empty row would keep every strike forever. Removing
/// a row that is not there is `Ok` and changes nothing.
#[test]
fn an_empty_put_removes_the_row() {
    const SCHEMA: RecordSchemaId = RecordSchemaId::new("task");
    let body = |b: &[u8]| RecordBytes::new(b.to_vec()).expect("inside the record ceiling");
    let held = |s: &MemoryStore| s.records.read().unwrap_or_else(|e| e.into_inner()).len();

    let s = MemoryStore::new();
    s.record_put(SCHEMA, b"a/1", &body(b"one")).unwrap();
    s.record_put(SCHEMA, b"a/2", &body(b"two")).unwrap();
    assert_eq!(held(&s), 2);

    s.record_put(SCHEMA, b"a/1", &body(b"")).unwrap();
    assert_eq!(held(&s), 1, "the struck row left the map");
    assert_eq!(s.record_get(SCHEMA, b"a/1").unwrap(), None);
    assert_eq!(
        s.record_scan(SCHEMA, b"a/", 10).unwrap(),
        vec![(b"a/2".to_vec(), body(b"two"))],
        "a scan never returns a struck row"
    );

    // A strike of a row that is not there is Ok, and the map is unchanged.
    s.record_put(SCHEMA, b"a/9", &body(b"")).unwrap();
    assert_eq!(held(&s), 1);
    // Put again after the strike, the key is a fresh row.
    s.record_put(SCHEMA, b"a/1", &body(b"again")).unwrap();
    assert_eq!(s.record_get(SCHEMA, b"a/1").unwrap(), Some(body(b"again")));
}

/// A rewrite REPLACES: the record leg answers the newest body for a key, and a scan sees one row
/// per key, however long ago the key was first written.
#[test]
fn record_put_rewrite_replaces_the_row() {
    const SCHEMA: RecordSchemaId = RecordSchemaId::new("task");
    let body = |b: &[u8]| RecordBytes::new(b.to_vec()).expect("inside the record ceiling");

    let s = MemoryStore::new();
    let n = now();
    s.pin_clock(n.saturating_sub(40 * 86_400));
    s.record_put(SCHEMA, b"t1", &body(b"v1")).unwrap();
    s.pin_clock(n);
    s.record_put(SCHEMA, b"t1", &body(b"v2")).unwrap();

    assert_eq!(s.record_get(SCHEMA, b"t1").unwrap(), Some(body(b"v2")));
    assert_eq!(
        s.record_scan(SCHEMA, b"t", 10).unwrap(),
        vec![(b"t1".to_vec(), body(b"v2"))]
    );
}

/// An upsert-kind plane record (`task`): `parent` is `None` and `seq` is `0`, so `upsert_key`
/// resolves its identity to its own `id` and it lands where `get_plane_record`/`plane_token_live`
/// point-read.
fn plane_task(id: &str, disposition: PlaneDisposition) -> PlaneRecord {
    PlaneRecord {
        kind: "task".to_string(),
        id: id.to_string(),
        parent: None,
        seq: 0,
        ts: 0,
        disposition,
        body: b"opaque".to_vec(),
    }
}

/// `plane_token_live` is the MULTI-use, time-and-disposition-bounded capability check, NOT the
/// single-use `redeem_plane_token` test-and-set. On the default in-memory backend it answers `true`
/// while a matching `(kind, token)` record is present, still `Active`, and unexpired — and it must be
/// REPEATABLE, spending nothing, because the A2A push-callback verify-live leg calls back several
/// times against one long-running task. The trait default returns `Ok(false)`, which would refuse
/// every legitimate callback out of the box; this proves the override answers `true` for the live
/// case and stays fail-closed for every other.
#[test]
fn plane_token_live_is_true_while_active_and_unexpired_and_is_repeatable() {
    let s = MemoryStore::new();
    s.upsert_plane_record(plane_task("t1", PlaneDisposition::Active).view())
        .unwrap();

    // Present, Active, and now (100) is before expires_at (1000): live.
    assert!(s.plane_token_live("task", "t1", 1000, 100).unwrap());
    // MULTI-use: asking again answers the same, and spends nothing — the record is untouched.
    assert!(s.plane_token_live("task", "t1", 1000, 100).unwrap());
    assert!(
        s.get_plane_record("task", "t1").unwrap().is_some(),
        "a live-check must not consume or remove the record (unlike a redeem)"
    );
    assert!(
        s.plane_token_live("task", "t1", 1000, 100).unwrap(),
        "still live after repeated checks — the capability is 'work still in flight', not 'unused'"
    );
}

/// Fail-closed on every non-live case: unknown `(kind, token)`, a terminal disposition, and a lapsed
/// deadline (`now > expires_at`) each yield `Ok(false)`; `now == expires_at` is still live.
#[test]
fn plane_token_live_is_false_for_unknown_terminal_and_expired() {
    let s = MemoryStore::new();

    // Unknown (kind, token): fail-closed.
    assert!(!s.plane_token_live("task", "missing", 1000, 100).unwrap());
    // Wrong kind for an existing token is also unknown.
    s.upsert_plane_record(plane_task("t1", PlaneDisposition::Active).view())
        .unwrap();
    assert!(!s.plane_token_live("other", "t1", 1000, 100).unwrap());

    // Live while now <= expires_at; dead only once now > expires_at.
    assert!(
        s.plane_token_live("task", "t1", 1000, 1000).unwrap(),
        "now == expires_at is the last live second"
    );
    assert!(!s.plane_token_live("task", "t1", 1000, 1001).unwrap());

    // Terminal disposition: the task has finished, so the token is dead even before its deadline.
    s.upsert_plane_record(plane_task("t1", PlaneDisposition::Terminal).view())
        .unwrap();
    assert!(
        !s.plane_token_live("task", "t1", 1000, 100).unwrap(),
        "a terminal record refuses further callbacks even while unexpired"
    );
}

/// An APPEND-ONLY plane record (`task_event`): `parent`/`seq` identify its position in a durable
/// chain, exactly like the `task_event`/`call` rows the journal seam appends.
fn plane_event(parent: &str, seq: u64, body: &[u8]) -> PlaneRecord {
    PlaneRecord {
        kind: "task_event".to_string(),
        id: parent.to_string(),
        parent: Some(parent.to_string()),
        seq,
        ts: 0,
        disposition: PlaneDisposition::Active,
        body: body.to_vec(),
    }
}

/// AUDIT-INTEGRITY FIX: `append_plane_record` must detect a SECOND WRITER forking the chain, exactly
/// as the legacy `append_audit` already does above (`Some(stored) if stored == entry => Ok(()); Some(_)
/// => Err(...)`) — the two append-only paths must never disagree about what a fork is. Before this
/// fix `append_plane_record` was a blind `HashMap::insert`: two busbar processes pointed at one
/// durable store would silently overwrite each other's `task_event`/`call`/`audit` rows with no
/// trace.
#[test]
fn append_plane_record_refuses_a_second_writers_fork_at_an_occupied_seq() {
    let s = MemoryStore::new();
    let first = plane_event("t1", 1, b"first-writer");
    s.append_plane_record(first.view()).unwrap();

    // Re-appending the IDENTICAL record — the write-through retry path — must stay Ok.
    s.append_plane_record(first.view())
        .expect("re-appending the IDENTICAL record is the retry path and must be Ok");

    // A DIFFERENT record at the SAME seq is a second writer forking the chain: refused, not
    // silently applied.
    let forked = plane_event("t1", 1, b"second-writer");
    assert!(
        s.append_plane_record(forked.view()).is_err(),
        "a DIFFERENT record on an already-occupied seq was accepted — the chain has forked and the \
         store said nothing"
    );

    // The first writer's record must still be intact: neither overwritten nor corrupted.
    let rows = s
        .list_plane_records("task_event", &PlaneSelector::Parent("t1".into()))
        .unwrap();
    assert_eq!(
        rows,
        vec![b"first-writer".to_vec()],
        "the original record must survive the refused fork attempt untouched"
    );
}

/// Record blobs are opaque to the store (unit-map shapes plus the scale marker are
/// the KERNEL's encoding): whatever bytes go in come back exactly, by key and by prefix scan.
/// Moved here from plugin-loader's store money acceptance suite: the record half is this store's own
/// inherent API, so the store tests it itself.
#[test]
fn a_record_blob_comes_back_byte_exact() {
    const SCHEMA: RecordSchemaId = RecordSchemaId::new("money_acceptance");
    let blob = br#"{"scale":6,"units":{"input":27500000,"output":1}}"#.to_vec();
    let s = MemoryStore::new();
    let body = RecordBytes::new(blob.clone()).expect("inside the record ceiling");
    s.record_put(SCHEMA, b"cell/k/1", &body).expect("put");
    let got = s
        .record_get(SCHEMA, b"cell/k/1")
        .expect("get")
        .expect("present");
    assert_eq!(got.as_slice(), blob.as_slice());
    let scan = s.record_scan(SCHEMA, b"cell/k/", 10).expect("scan");
    assert_eq!(scan.len(), 1);
    assert_eq!(scan[0].1.as_slice(), blob.as_slice());
}

/// A RECORD SCAN SEEKS ITS OWN SCHEMA AND PREFIX. A plane's `records.list` scans its schema on every
/// call, beside the node journal's rows under another schema, which grow for the life of the
/// process. A scan that walked the whole map from its first key cost every call O(all rows of all
/// schemas) under the map's read guard. Twenty thousand scans of a one-row schema beside 200 000 rows
/// of another take a few milliseconds with a seek and many seconds with a walk.
#[test]
fn a_record_scan_seeks_its_schema_and_prefix() {
    // Time the same scans with and without 200 000 rows of another schema beside them. A seek
    // costs the same either way (a few x for tree depth); a walk costs thousands of times more, so
    // the ratio holds on any machine, unlike an absolute bound.
    let time_scans = |s: &MemoryStore| {
        let started = std::time::Instant::now();
        for _ in 0..300 {
            let rows = s.v3_record_scan("a.mine", b"k", u32::MAX).unwrap();
            assert_eq!(rows.len(), 1);
        }
        started.elapsed()
    };
    let s = MemoryStore::new();
    s.v3_record_put("a.mine", b"k1", &[1]).unwrap();
    s.v3_record_put("a.mine", b"x1", &[1]).unwrap();
    let alone = (0..3).map(|_| time_scans(&s)).min().unwrap();
    for i in 0u32..200_000 {
        s.v3_record_put("z.other", &i.to_be_bytes(), &[1]).unwrap();
    }
    let beside = (0..3).map(|_| time_scans(&s)).min().unwrap();
    let floor = std::time::Duration::from_millis(2);
    assert!(
        beside < 100 * alone.max(floor),
        "300 scans took {beside:?} beside 200000 other rows, {alone:?} alone"
    );
    // The seek answers exactly what the walk did, in key order, within the limit.
    s.v3_record_put("a.mine", b"k0", &[1]).unwrap();
    let keys: Vec<Vec<u8>> = s
        .v3_record_scan("a.mine", b"k", 10)
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert_eq!(keys, vec![b"k0".to_vec(), b"k1".to_vec()]);
    assert_eq!(s.v3_record_scan("a.mine", b"", 2).unwrap().len(), 2);
    assert_eq!(s.v3_record_scan("a.mine", b"k", 0).unwrap().len(), 0);
    assert_eq!(s.v3_record_scan("a.min", b"", 10).unwrap().len(), 0);
}

/// THE PURGE HONOURS THE RETENTION RULE IT IS HANDED (coordinator ruling (b)), never one inferred
/// from the kind's name: a `task` kind declared `Age` loses its old ACTIVE row, and another kind
/// declared `TerminalOnly` keeps its old ACTIVE row and loses only its old TERMINAL one. RED before
/// the override: the trait's default purged nothing under `TerminalOnly`, and `Age` kept a `task`
/// row that was not terminal.
#[test]
fn a_purge_honours_the_retention_it_is_handed_never_the_kinds_name() {
    let s = MemoryStore::new();
    let mut active = plane_task("t-active", PlaneDisposition::Active);
    active.ts = 5;
    s.upsert_plane_record(active.view()).unwrap();
    assert_eq!(
        s.purge_plane_records("task", 10, busbar_contract::records::Retention::Age)
            .unwrap(),
        1
    );
    assert!(
        s.get_plane_record("task", "t-active").unwrap().is_none(),
        "a task kind declared age loses its old active row"
    );

    for (id, disposition) in [
        ("o-active", PlaneDisposition::Active),
        ("o-terminal", PlaneDisposition::Terminal),
    ] {
        let mut r = plane_task(id, disposition);
        r.kind = "order".to_string();
        r.ts = 5;
        s.upsert_plane_record(r.view()).unwrap();
    }
    assert_eq!(
        s.purge_plane_records(
            "order",
            10,
            busbar_contract::records::Retention::TerminalOnly
        )
        .unwrap(),
        1
    );
    assert!(s.get_plane_record("order", "o-active").unwrap().is_some());
    assert!(s.get_plane_record("order", "o-terminal").unwrap().is_none());
}

#[test]
fn purge_plane_records_before_counts_what_it_drops_and_keeps_the_cutoff() {
    let s = MemoryStore::new();
    for (id, ts) in [("below", 4), ("at", 5), ("above", 6)] {
        let mut r = plane_task(id, PlaneDisposition::Active);
        r.kind = "call".to_string();
        r.ts = ts;
        s.upsert_plane_record(r.view()).unwrap();
    }
    // Only the row strictly older than the cutoff goes, and the count says so.
    assert_eq!(s.purge_plane_records_before("call", 5).unwrap(), 1);
    assert!(s.get_plane_record("call", "below").unwrap().is_none());
    assert!(s.get_plane_record("call", "at").unwrap().is_some());
    assert!(s.get_plane_record("call", "above").unwrap().is_some());
}

#[test]
fn a_mint_with_a_tombstoned_key_writes_nothing() {
    let s = MemoryStore::new();
    let mut dead = key("dead");
    dead.deleted_at = Some(5);
    assert!(
        s.put_key_with_credential(&dead, &credential("c", "dead", "pub"))
            .is_err(),
        "a credential is never minted onto a key that carries a tombstone"
    );
    assert!(s.get_key("dead").unwrap().is_none());
    assert!(s
        .lookup_credential_secret("generic", "pub")
        .unwrap()
        .is_none());
}

/// UPSERT IS BY `(kind, id)` (the contract's `RecordStore::upsert_plane_record` /
/// `get_plane_record`), with or without a parent: an upserted record that names a parent reads back
/// by its own id, a second id under the same parent is a second record (never an overwrite), and
/// the parent-scoped listing still returns both.
#[test]
fn a_parented_upserted_record_reads_back_by_its_own_id() {
    let s = MemoryStore::new();
    let child = |id: &str, body: &[u8]| PlaneRecord {
        kind: "task".to_string(),
        id: id.to_string(),
        parent: Some("ctx-1".to_string()),
        seq: 0,
        ts: 0,
        disposition: PlaneDisposition::Active,
        body: body.to_vec(),
    };
    s.upsert_plane_record(child("t1", b"one").view()).unwrap();
    s.upsert_plane_record(child("t2", b"two").view()).unwrap();

    assert_eq!(
        s.get_plane_record("task", "t1").unwrap(),
        Some(b"one".to_vec())
    );
    assert_eq!(
        s.get_plane_record("task", "t2").unwrap(),
        Some(b"two".to_vec())
    );
    let mut listed = s
        .list_plane_records("task", &PlaneSelector::Parent("ctx-1".into()))
        .unwrap();
    listed.sort();
    assert_eq!(listed, vec![b"one".to_vec(), b"two".to_vec()]);

    // A second upsert of one id replaces it; deleting it by id removes it alone.
    s.upsert_plane_record(child("t1", b"one'").view()).unwrap();
    assert_eq!(
        s.get_plane_record("task", "t1").unwrap(),
        Some(b"one'".to_vec())
    );
    s.delete_plane_record("task", "t1").unwrap();
    assert_eq!(s.get_plane_record("task", "t1").unwrap(), None);
    assert_eq!(
        s.get_plane_record("task", "t2").unwrap(),
        Some(b"two".to_vec())
    );
}

/// Upsert is by `(kind, id)` whatever `seq` the record carries: it reads back by id, and a second
/// upsert with another `seq` replaces it rather than adding a row.
#[test]
fn an_upsert_ignores_the_records_seq() {
    let s = MemoryStore::new();
    let rec = |seq: u64, body: &[u8]| PlaneRecord {
        kind: "k".to_string(),
        id: "x".to_string(),
        parent: None,
        seq,
        ts: 0,
        disposition: PlaneDisposition::Active,
        body: body.to_vec(),
    };
    s.upsert_plane_record(rec(5, b"one").view()).unwrap();
    assert_eq!(
        s.get_plane_record("k", "x").unwrap(),
        Some(b"one".to_vec()),
        "an upsert at seq 5 is not reachable by (kind, id)"
    );
    s.upsert_plane_record(rec(7, b"two").view()).unwrap();
    assert_eq!(
        s.list_plane_records("k", &PlaneSelector::All).unwrap(),
        vec![b"two".to_vec()]
    );
}

/// Equal-`seq` rows list in one stable order (by id), whatever the map's iteration order is.
#[test]
fn list_plane_records_orders_equal_seqs_by_id() {
    let ids: Vec<String> = (0..16).map(|i| format!("id-{i:02}")).collect();
    let expected: Vec<Vec<u8>> = ids.iter().map(|i| i.as_bytes().to_vec()).collect();
    for _ in 0..4 {
        let s = MemoryStore::new();
        for id in ids.iter().rev() {
            let r = PlaneRecord {
                kind: "k".to_string(),
                id: id.clone(),
                parent: None,
                seq: 0,
                ts: 0,
                disposition: PlaneDisposition::Active,
                body: id.as_bytes().to_vec(),
            };
            s.upsert_plane_record(r.view()).unwrap();
        }
        assert_eq!(
            s.list_plane_records("k", &PlaneSelector::All).unwrap(),
            expected
        );
    }
}

/// The atomic mint refuses, and writes nothing, when the stored row under the key's id is a
/// tombstone (even though the key it is handed is live) and when the credential names another key.
#[test]
fn put_key_with_credential_refusals_write_nothing() {
    let s = MemoryStore::new();
    s.put_key(&key("dead")).unwrap();
    s.delete_key("dead").unwrap();
    let tombstone = s.get_key("dead").unwrap().unwrap();
    assert!(s
        .put_key_with_credential(&key("dead"), &credential("c", "dead", "pub"))
        .is_err());
    assert_eq!(s.get_key("dead").unwrap(), Some(tombstone));
    assert!(s
        .lookup_credential_secret("generic", "pub")
        .unwrap()
        .is_none());

    assert!(s
        .put_key_with_credential(&key("k2"), &credential("c2", "other", "pub2"))
        .is_err());
    assert!(s.get_key("k2").unwrap().is_none());
    assert!(s
        .lookup_credential_secret("generic", "pub2")
        .unwrap()
        .is_none());
}

/// `revoke_credential` bumps the credential's revision once (visible to `list_credentials_since`);
/// a repeat keeps the first `revoked_at` and reason and does not bump again.
#[test]
fn revoke_credential_bumps_the_revision_once() {
    let s = MemoryStore::new();
    s.put_key(&key("k")).unwrap();
    s.put_credential(&credential("c", "k", "P")).unwrap();
    let minted = s.list_credentials("k").unwrap()[0].revision;

    s.pin_clock(1_000);
    s.revoke_credential("c", "first").unwrap();
    let after = s.list_credentials_since(minted).unwrap();
    assert_eq!(after.len(), 1);
    let revoked = after[0].meta.clone();
    assert!(revoked.revision > minted);
    assert_eq!(revoked.revoked_at, Some(1_000));

    s.pin_clock(2_000);
    s.revoke_credential("c", "second").unwrap();
    assert_eq!(s.list_credentials("k").unwrap(), vec![revoked.clone()]);
    assert!(s
        .list_credentials_since(revoked.revision)
        .unwrap()
        .is_empty());
}

/// `delete_key` bumps the key's revision (visible to `list_keys_since`); the idempotent second
/// delete does not.
#[test]
fn delete_key_bumps_the_revision_once() {
    let s = MemoryStore::new();
    s.put_key(&key("k")).unwrap();
    let live = s.get_key("k").unwrap().unwrap().revision;
    s.delete_key("k").unwrap();
    let since = s.list_keys_since(live).unwrap();
    assert_eq!(since.len(), 1);
    assert!(since[0].deleted_at.is_some());
    let tomb = since[0].revision;
    assert!(tomb > live);
    s.delete_key("k").unwrap();
    assert!(s.list_keys_since(tomb).unwrap().is_empty());
    assert_eq!(s.get_key("k").unwrap().unwrap().revision, tomb);
}

#[test]
fn the_denylist_round_trips_and_lists_an_id_once() {
    let s = MemoryStore::new();
    assert!(s.list_denylist().unwrap().is_empty());
    s.add_denylist("sub-1", "why").unwrap();
    s.add_denylist("sub-1", "again").unwrap();
    s.add_denylist("sub-2", "why").unwrap();
    let mut listed = s.list_denylist().unwrap();
    listed.sort();
    assert_eq!(listed, vec!["sub-1".to_string(), "sub-2".to_string()]);
}

/// A metering row's key includes `priced_from_ms` and `provider`: deltas differing only in one of
/// them make two rows; `usage_units` accumulate per unit.
#[test]
fn metering_rows_split_on_priced_from_and_provider_and_sum_units() {
    let s = MemoryStore::new();
    let base = MeteringDelta {
        key_id: "k".to_string(),
        bucket: 9,
        model: "m".to_string(),
        provider: "p".to_string(),
        tokens_input: 1,
        tokens_output: 0,
        tokens_cache_read: 0,
        tokens_cache_write: 0,
        requests: 1,
        billable_requests: 1,
        key_group_at_use: String::new(),
        pricing_version: String::new(),
        priced_from_ms: 100,
        usage_units: [("a".to_string(), 2u64), ("b".to_string(), 3)]
            .into_iter()
            .collect(),
    };
    s.add_metering(&base).unwrap();
    s.add_metering(&base).unwrap();
    assert_eq!(s.list_metering(9).unwrap().len(), 1);
    let units = &s.list_metering(9).unwrap()[0].usage_units;
    assert_eq!((units["a"], units["b"]), (4, 6));

    let mut repriced = base.clone();
    repriced.priced_from_ms = 200;
    s.add_metering(&repriced).unwrap();
    assert_eq!(s.list_metering(9).unwrap().len(), 2);

    let mut other_provider = base.clone();
    other_provider.provider = "q".to_string();
    s.add_metering(&other_provider).unwrap();
    assert_eq!(s.list_metering(9).unwrap().len(), 3);
}

/// `public_id` and slot occupancy are per `kind`: the same value under another kind is no clash.
#[test]
fn credential_preconditions_are_scoped_to_the_kind() {
    let s = MemoryStore::new();
    s.put_key(&key("k")).unwrap();
    s.put_credential(&credential("c1", "k", "P")).unwrap();
    let mut other = credential("c2", "k", "P");
    other.meta.kind = "other".to_string();
    s.put_credential(&other)
        .expect("same public_id and slot under another kind");
    assert_eq!(s.list_credentials("k").unwrap().len(), 2);
}

/// `list_plane_record_parents` lists a parent once however many records its chain holds, and
/// `delete_plane_record` on that identity removes every `seq` of the chain.
#[test]
fn plane_chains_list_one_parent_and_delete_every_seq() {
    let s = MemoryStore::new();
    for seq in 1..=3 {
        s.append_plane_record(plane_event("t1", seq, b"e").view())
            .unwrap();
    }
    s.append_plane_record(plane_event("t2", 1, b"e").view())
        .unwrap();
    assert_eq!(
        s.list_plane_record_parents("task_event").unwrap(),
        vec!["t1".to_string(), "t2".to_string()]
    );
    s.delete_plane_record("task_event", "t1").unwrap();
    assert!(s
        .list_plane_records("task_event", &PlaneSelector::Parent("t1".into()))
        .unwrap()
        .is_empty());
    assert_eq!(
        s.list_plane_records("task_event", &PlaneSelector::All)
            .unwrap()
            .len(),
        1
    );
}

/// One text per condition, the calling method's own name in front of it.
#[test]
fn credential_refusals_name_the_calling_method() {
    let s = MemoryStore::new();
    s.put_key(&key("k")).unwrap();
    s.put_key(&key("other")).unwrap();
    s.put_credential(&credential("c1", "k", "P1")).unwrap();

    let live_slot = |op: &str| {
        format!("{op}: slot 0 for key 'k' kind 'generic' holds a live credential; revoke it first")
    };
    let taken = |op: &str| format!("{op}: public_id 'P1' is already in use for kind 'generic'");

    let err = s.put_credential(&credential("c2", "k", "P2")).unwrap_err();
    assert_eq!(err.0, live_slot("put_credential"));
    let err = s
        .put_key_with_credential(&key("k"), &credential("c2", "k", "P2"))
        .unwrap_err();
    assert_eq!(err.0, live_slot("put_key_with_credential"));

    let err = s
        .put_credential(&credential("c3", "other", "P1"))
        .unwrap_err();
    assert_eq!(err.0, taken("put_credential"));
    let err = s
        .put_key_with_credential(&key("other"), &credential("c3", "other", "P1"))
        .unwrap_err();
    assert_eq!(err.0, taken("put_key_with_credential"));

    assert_eq!(
        s.revoke_credential("nope", "r").unwrap_err().0,
        "revoke_credential: unknown id 'nope'"
    );
    assert_eq!(
        s.delete_key("nope").unwrap_err().0,
        "delete_key: unknown id 'nope'"
    );
}

/// A revoked credential id is never re-minted, by either mint path, in any slot; a NEW id minted
/// into a revoked occupant's slot reclaims it.
#[test]
fn a_revoked_credential_id_is_never_reminted() {
    let s = MemoryStore::new();
    s.put_key(&key("k")).unwrap();
    s.put_credential(&credential("c1", "k", "P1")).unwrap();
    s.revoke_credential("c1", "rotated").unwrap();

    let text = |op: &str| {
        format!("{op}: credential 'c1' is revoked; a revoked credential is never re-minted")
    };
    let mut elsewhere = credential("c1", "k", "P1");
    elsewhere.meta.slot = 3;
    for again in [credential("c1", "k", "P1"), elsewhere] {
        let r = s.put_credential(&again);
        assert_eq!(
            r.expect_err("a revoked id was re-minted").0,
            text("put_credential")
        );
        let r = s.put_key_with_credential(&key("k"), &again);
        assert_eq!(
            r.expect_err("a revoked id was re-minted").0,
            text("put_key_with_credential")
        );
    }
    let kept = s.list_credentials("k").unwrap();
    assert_eq!(kept.len(), 1);
    assert!(
        kept[0].revoked_at.is_some(),
        "the revoked row was rewritten"
    );

    let mut fresh = credential("c2", "k", "P2");
    fresh.meta.created_at = 77;
    s.put_credential(&fresh).unwrap();
    let rows = s.list_credentials("k").unwrap();
    assert_eq!(rows.len(), 1, "the new id reclaims the slot");
    assert_eq!((rows[0].id.as_str(), rows[0].created_at), ("c2", 77));
}

/// Re-putting a live credential: identical content (revision aside) is Ok and writes nothing;
/// different content is refused, naming the slot it holds.
#[test]
fn reputting_a_live_credential_is_idempotent_or_refused() {
    let s = MemoryStore::new();
    s.put_key(&key("k")).unwrap();
    let c = credential("c1", "k", "P1");
    s.put_credential(&c).unwrap();
    let stored = s.list_credentials("k").unwrap()[0].revision;

    s.put_credential(&c).unwrap();
    s.put_key_with_credential(&key("k"), &c).unwrap();
    assert_eq!(
        s.list_credentials("k").unwrap()[0].revision,
        stored,
        "an identical re-put wrote"
    );

    let text = |op: &str| {
        format!("{op}: slot 0 for key 'k' kind 'generic' holds a live credential; revoke it first")
    };
    let mut changed = c.clone();
    changed.secret = "v1:plain:other".to_string();
    assert_eq!(
        s.put_credential(&changed).expect_err("changed secret").0,
        text("put_credential")
    );
    assert_eq!(
        s.put_key_with_credential(&key("k"), &changed)
            .expect_err("changed secret")
            .0,
        text("put_key_with_credential")
    );
    let mut moved = c.clone();
    moved.meta.public_id = "P9".to_string();
    assert!(s.put_credential(&moved).is_err());
    let row = s
        .lookup_credential_secret("generic", "P1")
        .unwrap()
        .unwrap();
    assert_eq!(
        row.secret, "v1:plain:sek",
        "a refused re-put changed the row"
    );
}

/// Revoking destroys the secret material: the revoked row keeps its metadata, loses its secret and
/// its form; a repeat revoke is a no-op.
#[test]
fn revoke_credential_destroys_the_secret() {
    let s = MemoryStore::new();
    s.put_key(&key("k")).unwrap();
    s.put_credential(&credential("c", "k", "P")).unwrap();
    let minted = s.list_credentials("k").unwrap()[0].revision;
    s.pin_clock(1_000);
    s.revoke_credential("c", "leaked").unwrap();

    let rows = s.list_credentials_since(minted).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].secret, "", "the secret outlived its revoke");
    assert_eq!(rows[0].meta.secret_form, SecretForm::None);
    assert_eq!(rows[0].meta.revoked_at, Some(1_000));
    assert_eq!(rows[0].meta.revoke_reason.as_deref(), Some("leaked"));
    assert!(rows[0].meta.revision > minted);

    s.revoke_credential("c", "again").unwrap();
    assert_eq!(s.list_credentials_since(minted).unwrap(), rows);
}

/// A stored entry is spent while its `expires_at >= now` (the caller's `now`); only `expires_at <
/// now` is lapsed. The amortised sweep drops the same rows and no others.
#[test]
fn a_redeemed_token_is_spent_through_its_expiry_second() {
    let s = MemoryStore::new();
    assert!(s.redeem_plane_token("ask", "n1", 100, 0).unwrap());
    assert!(!s.redeem_plane_token("ask", "n1", 500, 99).unwrap());
    assert!(
        !s.redeem_plane_token("ask", "n1", 500, 100).unwrap(),
        "spent at now == expires_at"
    );
    assert!(s.redeem_plane_token("ask", "n1", 500, 101).unwrap());

    assert!(s.redeem_plane_token("ask", "edge", 1_000, 0).unwrap());
    for i in 0..SWEEP_INTERVAL {
        assert!(s
            .redeem_plane_token("ask", &format!("m{i}"), 5_000, 1_000)
            .unwrap());
    }
    assert!(
        !s.redeem_plane_token("ask", "edge", 5_000, 1_000).unwrap(),
        "the sweep dropped a row that is still spent"
    );
}

fn events_of(s: &MemoryStore, task: &str) -> usize {
    s.list_plane_records("task_event", &PlaneSelector::Parent(task.into()))
        .unwrap()
        .len()
}

/// Deleting a task takes its `task_event` chain with it, and only that task's.
#[test]
fn deleting_a_task_deletes_its_event_chain() {
    let s = MemoryStore::new();
    for t in ["t1", "t2"] {
        s.upsert_plane_record(plane_task(t, PlaneDisposition::Active).view())
            .unwrap();
        for seq in 1..=2 {
            s.append_plane_record(plane_event(t, seq, b"e").view())
                .unwrap();
        }
    }
    s.delete_plane_record("task", "t1").unwrap();
    assert!(s.get_plane_record("task", "t1").unwrap().is_none());
    assert_eq!(events_of(&s, "t1"), 0, "the task's events outlived it");
    assert_eq!(events_of(&s, "t2"), 2);
    assert!(s.get_plane_record("task", "t2").unwrap().is_some());
}

/// A purge of `task` takes each purged task's event chain, under either retention; the count is
/// the task rows only, and a task that stays keeps its events.
#[test]
fn purging_tasks_takes_their_event_chains() {
    for retention in [Retention::Age, Retention::TerminalOnly] {
        let s = MemoryStore::new();
        for (t, ts, disposition) in [
            ("gone", 5, PlaneDisposition::Terminal),
            ("fresh", 50, PlaneDisposition::Terminal),
            ("active", 5, PlaneDisposition::Active),
        ] {
            let mut task = plane_task(t, disposition);
            task.ts = ts;
            s.upsert_plane_record(task.view()).unwrap();
            for seq in 1..=2 {
                s.append_plane_record(plane_event(t, seq, b"e").view())
                    .unwrap();
            }
        }
        let expect_active_gone = retention == Retention::Age;
        let purged = s.purge_plane_records("task", 10, retention).unwrap();
        assert_eq!(purged, if expect_active_gone { 2 } else { 1 });
        assert_eq!(
            events_of(&s, "gone"),
            0,
            "{retention:?}: events outlived their task"
        );
        assert_eq!(events_of(&s, "fresh"), 2);
        assert_eq!(
            events_of(&s, "active"),
            if expect_active_gone { 0 } else { 2 }
        );
    }

    let s = MemoryStore::new();
    let mut task = plane_task("t", PlaneDisposition::Terminal);
    task.ts = 5;
    s.upsert_plane_record(task.view()).unwrap();
    s.append_plane_record(plane_event("t", 1, b"e").view())
        .unwrap();
    assert_eq!(s.purge_plane_records_before("task", 10).unwrap(), 1);
    assert_eq!(events_of(&s, "t"), 0);
}
