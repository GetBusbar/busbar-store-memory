// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! Tests for `crates/store-memory/src/v3.rs`: the store v3 slots' dedupe (S1-S4) and the count
//! batches.

use super::*;
use busbar_contract::abi::sdk::store::OpRefused;
use busbar_contract::abi::store::{OpId, OP_ID_RETENTION_SECS};
use busbar_contract::records::{ModelTokensDelta, UsageDelta};

fn op(n: u64) -> OpId {
    OpId::from_parts(7, n)
}

fn delta(requests: i64, input: i64) -> UsageDelta {
    UsageDelta {
        requests,
        billable_requests: requests,
        models: vec![ModelTokensDelta {
            model: "m".to_string(),
            usage_units: [("input".to_string(), input)].into_iter().collect(),
        }],
    }
}

#[test]
fn the_memory_store_states_it_is_ephemeral_and_refuses_forks() {
    assert_eq!(
        <MemoryStore as busbar_contract::abi::sdk::store::StoreSlots>::TAIL,
        busbar_contract::abi::sdk::store::Tail {
            ephemeral: true,
            durable_plane: false,
            fork_refusal: true,
        }
    );
}

#[test]
fn a_replayed_usage_batch_applies_once() {
    let s = MemoryStore::new();
    let cells = [("k", 60, delta(1, 10))];
    s.v3_add_usage_batch(op(1), &cells).expect("first");
    s.v3_add_usage_batch(op(1), &cells)
        .expect("replay answers the original");
    assert_eq!(s.get_usage("k", 60).expect("read").requests, 1);
}

#[test]
fn equal_bodies_under_distinct_op_ids_both_apply() {
    let s = MemoryStore::new();
    let cells = [("k", 60, delta(2, 4))];
    s.v3_add_usage_batch(op(1), &cells).expect("a");
    s.v3_add_usage_batch(op(2), &cells).expect("b");
    assert_eq!(s.get_usage("k", 60).expect("read").requests, 4);
}

#[test]
fn a_reused_op_id_with_a_different_body_is_a_conflict_and_applies_nothing() {
    let s = MemoryStore::new();
    s.v3_add_usage_batch(op(1), &[("k", 60, delta(1, 1))])
        .expect("first");
    assert_eq!(
        s.v3_add_usage_batch(op(1), &[("k", 60, delta(5, 5))]),
        Err(OpRefused::Conflict)
    );
    assert_eq!(s.get_usage("k", 60).expect("read").requests, 1);
}

#[test]
fn an_op_id_reused_across_slots_is_a_conflict() {
    let s = MemoryStore::new();
    s.v3_add_usage_batch(op(1), &[("k", 60, delta(1, 1))])
        .expect("usage");
    assert_eq!(
        s.v3_add_metering_batch(op(1), &[]),
        Err(OpRefused::Conflict)
    );
    assert!(s.list_metering(86_400).expect("list").is_empty());
}

#[test]
fn a_usage_batch_applies_its_cells_in_order() {
    let s = MemoryStore::new();
    let neg = UsageDelta {
        requests: 0,
        billable_requests: -1,
        models: vec![],
    };
    let pos = UsageDelta {
        requests: 0,
        billable_requests: 1,
        models: vec![],
    };
    s.v3_add_usage_batch(op(1), &[("k", 60, neg), ("k", 60, pos)])
        .expect("batch");
    // The floor at zero makes order matter: -1 then +1 is 1, not 0.
    assert_eq!(s.get_usage("k", 60).expect("read").billable_requests, 1);
}

#[test]
fn a_metering_batch_replay_applies_once() {
    let s = MemoryStore::new();
    let d = MeteringDelta {
        key_id: "k".into(),
        bucket: 86_400,
        model: "m".into(),
        provider: "p".into(),
        tokens_input: 3,
        tokens_output: 0,
        tokens_cache_read: 0,
        tokens_cache_write: 0,
        requests: 2,
        billable_requests: 2,
        key_group_at_use: String::new(),
        pricing_version: String::new(),
        priced_from_ms: 0,
        usage_units: Default::default(),
    };
    s.v3_add_metering_batch(op(1), std::slice::from_ref(&d))
        .expect("a");
    s.v3_add_metering_batch(op(1), std::slice::from_ref(&d))
        .expect("replay");
    let rows = s.list_metering(86_400).expect("list");
    assert_eq!(rows.iter().map(|r| r.requests).sum::<u64>(), 2);
}

#[test]
fn an_op_id_is_forgotten_after_its_retention() {
    let s = MemoryStore::new();
    s.pin_clock(1_000);
    s.v3_add_usage_batch(op(1), &[("k", 60, delta(1, 1))])
        .expect("a");
    s.pin_clock(1_000 + OP_ID_RETENTION_SECS);
    // Recording another op sweeps the expired one; the old op_id then reads as new.
    s.v3_add_usage_batch(op(2), &[("j", 60, delta(1, 1))])
        .expect("b");
    s.v3_add_usage_batch(op(1), &[("k", 60, delta(1, 1))])
        .expect("new again");
    assert_eq!(s.get_usage("k", 60).expect("read").requests, 2);
}

#[test]
fn an_op_id_is_remembered_inside_its_retention() {
    let s = MemoryStore::new();
    s.pin_clock(1_000);
    s.v3_add_usage_batch(op(1), &[("k", 60, delta(1, 1))])
        .expect("a");
    s.pin_clock(1_000 + OP_ID_RETENTION_SECS - 1);
    // Recording another op sweeps, but the first op is still inside its retention: its replay
    // applies nothing.
    s.v3_add_usage_batch(op(2), &[("j", 60, delta(1, 1))])
        .expect("b");
    s.v3_add_usage_batch(op(1), &[("k", 60, delta(1, 1))])
        .expect("replay");
    assert_eq!(s.get_usage("k", 60).expect("read").requests, 1);
}

/// THE DEDUPE LOG HOLDS A FIXED-SIZE RECORD PER OP (the store dedupe rule: "same" means the op's
/// value fields, kept at least `OP_ID_RETENTION_SECS`). Production mints a fresh `op_id` for every
/// write, so whatever the log keeps per op is kept for every write the node makes for a day: it must
/// not grow with the size of what was written. Sixty-four 500-byte records rendered as text held
/// over 100 KiB in the log; the log keeps a digest of the value fields instead.
#[test]
fn the_dedupe_log_keeps_a_fixed_size_record_per_op() {
    let s = MemoryStore::new();
    let big: Vec<(&str, u64, UsageDelta)> = (0..64).map(|w| ("k", w, delta(1, 500))).collect();
    s.v3_add_usage_batch(op(1), &big).expect("apply");
    let held = s.v3.held().op_bytes;
    assert!(held <= 64, "one op keeps {held} bytes in the dedupe log");
    // The digest still tells the same op from a different one.
    s.v3_add_usage_batch(op(1), &big).expect("replay");
    assert!(matches!(
        s.v3_add_usage_batch(op(1), &big[..1]),
        Err(OpRefused::Conflict)
    ));
}

#[test]
fn a_plane_append_replays_once_and_a_reused_op_id_conflicts() {
    use busbar_contract::records::{PlaneDisposition, PlaneRecord, PlaneSelector};
    let s = MemoryStore::new();
    let event = |seq: u64, body: &[u8]| PlaneRecord {
        kind: "task_event".to_string(),
        id: "t1".to_string(),
        parent: Some("t1".to_string()),
        seq,
        ts: 0,
        disposition: PlaneDisposition::Active,
        body: body.to_vec(),
    };
    let (a, b) = (event(1, b"a"), event(2, b"b"));
    s.v3_append_plane_record_op(op(1), a.view()).expect("first");
    s.v3_append_plane_record_op(op(1), a.view())
        .expect("replay");
    assert_eq!(
        s.v3_append_plane_record_op(op(1), b.view()),
        Err(OpRefused::Conflict)
    );
    let rows = s
        .list_plane_records("task_event", &PlaneSelector::All)
        .expect("list");
    assert_eq!(rows, vec![b"a".to_vec()]);
}

/// A NEW op_id whose append forks an occupied `(parent, seq)` answers `Failed` and records
/// nothing: the same op_id retried with a non-forking record applies (not Replay/Conflict).
#[test]
fn a_failed_append_does_not_record_its_op_id() {
    use busbar_contract::records::{PlaneDisposition, PlaneRecord, PlaneSelector};
    let s = MemoryStore::new();
    let event = |seq: u64, body: &[u8]| PlaneRecord {
        kind: "task_event".to_string(),
        id: "t1".to_string(),
        parent: Some("t1".to_string()),
        seq,
        ts: 0,
        disposition: PlaneDisposition::Active,
        body: body.to_vec(),
    };
    s.v3_append_plane_record_op(op(1), event(1, b"a").view())
        .expect("first");
    assert!(matches!(
        s.v3_append_plane_record_op(op(2), event(1, b"fork").view()),
        Err(OpRefused::Failed(_))
    ));
    s.v3_append_plane_record_op(op(2), event(2, b"b").view())
        .expect("the retry applies: the failed op was never recorded");
    let rows = s
        .list_plane_records("task_event", &PlaneSelector::All)
        .expect("list");
    assert_eq!(rows, vec![b"a".to_vec(), b"b".to_vec()]);
}

fn metering(priced_from_ms: u64) -> MeteringDelta {
    MeteringDelta {
        key_id: "k".into(),
        bucket: 86_400,
        model: "m".into(),
        provider: "p".into(),
        tokens_input: 1,
        tokens_output: 0,
        tokens_cache_read: 0,
        tokens_cache_write: 0,
        requests: 1,
        billable_requests: 1,
        key_group_at_use: String::new(),
        pricing_version: String::new(),
        priced_from_ms,
        usage_units: Default::default(),
    }
}

fn plane(kind: &str, parent: Option<&str>, seq: u64, ts: u64) -> busbar_contract::records::PlaneRecord {
    busbar_contract::records::PlaneRecord {
        kind: kind.to_string(),
        id: "r".to_string(),
        parent: parent.map(str::to_string),
        seq,
        ts,
        disposition: busbar_contract::records::PlaneDisposition::Active,
        body: b"b".to_vec(),
    }
}

/// A u64 a signed column cannot hold is refused before any write, on the trait methods (an error
/// naming the method and field) and on the op slots (`OpRefused::Refused`, nothing recorded).
#[test]
fn a_value_a_signed_column_cannot_hold_is_refused_before_any_write() {
    let over = i64::MAX as u64 + 1;
    let s = MemoryStore::new();

    for (rec, field) in [
        (plane("task_event", Some("p"), over, 0), "seq"),
        (plane("task_event", Some("p"), 1, over), "ts"),
    ] {
        let e = s.append_plane_record(rec.view()).expect_err("append");
        assert!(
            e.0.starts_with(&format!("append_plane_record: {field} {over} exceeds")),
            "{}",
            e.0
        );
        assert!(matches!(
            s.v3_append_plane_record_op(op(1), rec.view()),
            Err(OpRefused::Refused(_))
        ));
    }
    for (rec, field) in [
        (plane("task", None, over, 0), "seq"),
        (plane("task", None, 0, over), "ts"),
    ] {
        let e = s.upsert_plane_record(rec.view()).expect_err("upsert");
        assert!(
            e.0.starts_with(&format!("upsert_plane_record: {field} {over} exceeds")),
            "{}",
            e.0
        );
    }
    assert!(s.get_plane_record("task", "r").unwrap().is_none());
    assert!(s
        .list_plane_records("task_event", &PlaneSelector::All)
        .unwrap()
        .is_empty());
    // The refused op_id was never recorded: it is still free.
    s.v3_append_plane_record_op(op(1), plane("task_event", Some("p"), 1, 1).view())
        .expect("op id reusable");

    let e = s.add_metering(&metering(over)).expect_err("metering");
    assert!(
        e.0.starts_with(&format!("add_metering: priced_from_ms {over} exceeds")),
        "{}",
        e.0
    );
    assert!(matches!(
        s.v3_add_metering_batch(op(2), &[metering(1), metering(over)]),
        Err(OpRefused::Refused(_))
    ));
    assert!(
        s.list_metering(86_400).unwrap().is_empty(),
        "a refused batch applied part of itself"
    );
    s.v3_add_metering_batch(op(2), &[metering(1)])
        .expect("op id reusable");

    for (exp, now, field) in [(over, 0, "expires_at"), (0, over, "now")] {
        let e = s
            .redeem_plane_token("ask", "n", exp, now)
            .expect_err("redeem");
        assert!(
            e.0.starts_with(&format!("redeem_plane_token: {field} {over} exceeds")),
            "{}",
            e.0
        );
    }
    assert!(s.redeem_plane_token("ask", "n", 10, 0).unwrap(), "nothing was spent");
}
