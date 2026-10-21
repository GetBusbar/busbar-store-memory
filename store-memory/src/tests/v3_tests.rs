// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

//! Tests for `crates/store-memory/src/v3.rs`: the store v3 slots' dedupe (S1-S4), the reserve
//! grant rule pinned to 1.5.5, slice release clamping, window caps, batches, the ledger
//! streams and the session directory.

use super::*;
use busbar_contract::abi::sdk::store::{
    Cap, CapsRefused, Cell, CellKey, Dimension, Grant, OpRefused, OpResult, ReserveRefused,
};
use busbar_contract::abi::store::{OpId, OP_ID_RETENTION_SECS};
use busbar_contract::kinds::RecordBytes;
use busbar_contract::records::{AuditRecord, ModelTokensDelta, UsageDelta};

fn op(n: u64) -> OpId {
    OpId::from_parts(7, n)
}

fn key(dimension: Dimension<'static>) -> CellKey<'static> {
    CellKey {
        bucket: "b",
        pool: None,
        dimension,
        window_start: 1_000,
    }
}

fn cap(dimension: Dimension<'static>, cap: u64, config_gen: u64) -> Cap<'static> {
    Cap {
        key: key(dimension),
        cap,
        config_gen,
    }
}

fn cell(dimension: Dimension<'static>, amount: u64) -> Cell<'static> {
    Cell {
        key: key(dimension),
        amount,
    }
}

/// `reserve`, its grants collected.
fn reserve(
    s: &MemoryStore,
    op: OpId,
    epoch: u64,
    cells: &[Cell<'_>],
) -> Result<Vec<Grant>, ReserveRefused> {
    let mut grants = Vec::new();
    s.v3_reserve(op, epoch, cells.iter().copied(), &mut grants)
        .map(|()| grants)
}

/// `slice_release`, its amounts collected.
fn release(s: &MemoryStore, op: OpId, epoch: u64, items: &[(u64, u64)]) -> OpResult<Vec<u64>> {
    let mut released = Vec::new();
    s.v3_slice_release(op, epoch, items.iter().copied(), &mut released)
        .map(|()| released)
}

fn capped(dimension: Dimension<'static>, c: u64) -> MemoryStore {
    let s = MemoryStore::new();
    s.v3_window_caps(op(1_000_000), &[cap(dimension, c, 1)])
        .expect("caps");
    s
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

fn audit(seq: u64, action: &str) -> AuditRecord {
    AuditRecord {
        seq,
        ts: 1,
        action: action.to_string(),
        resource: "r".to_string(),
        outcome: "ok".to_string(),
        principal: "p".to_string(),
        prev_hash: String::new(),
        hash: format!("h{seq}{action}"),
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
    s.v3_add_usage_op(op(1), "k", 60, &delta(1, 1))
        .expect("usage");
    assert_eq!(
        s.v3_append_audit_op(op(1), &audit(1, "a")),
        Err(OpRefused::Conflict)
    );
    assert!(s.list_audit().expect("list").is_empty());
}

#[test]
fn a_failed_write_is_not_recorded_so_a_retry_is_evaluated_afresh() {
    let s = MemoryStore::new();
    s.append_audit(&audit(1, "a")).expect("seed");
    // A fork FAILS and is not recorded under the op_id ...
    assert!(matches!(
        s.v3_append_audit_op(op(9), &audit(1, "forked")),
        Err(OpRefused::Failed(_))
    ));
    // ... so the same op_id with a different, applicable body is new, not a conflict.
    s.v3_append_audit_op(op(9), &audit(2, "b")).expect("fresh");
    assert_eq!(s.list_audit().expect("list").len(), 2);
}

#[test]
fn an_audit_batch_with_one_fork_applies_none_of_it() {
    let s = MemoryStore::new();
    s.append_audit(&audit(2, "a")).expect("seed");
    let batch = [audit(1, "x"), audit(2, "forked")];
    assert!(matches!(
        s.v3_append_audit_batch(op(1), &batch),
        Err(OpRefused::Failed(_))
    ));
    assert_eq!(s.list_audit().expect("list").len(), 1);
    // Two different records at one seq INSIDE the batch are a fork too.
    let inner = [audit(5, "x"), audit(5, "y")];
    assert!(s.v3_append_audit_batch(op(2), &inner).is_err());
    assert_eq!(s.list_audit().expect("list").len(), 1);
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
fn a_reserve_with_no_cap_pushed_is_refused_naming_the_cell() {
    let s = MemoryStore::new();
    assert_eq!(
        reserve(&s, op(1), 0, &[cell(Dimension::Requests, 1)]),
        Err(ReserveRefused::NoCap { cell: 0 })
    );
}

#[test]
fn a_grant_is_always_the_whole_amount() {
    let s = capped(Dimension::NanoUnits, 100);
    let g = reserve(&s, op(1), 0, &[cell(Dimension::NanoUnits, 60)]).expect("grant");
    assert_eq!(g.len(), 1);
    assert_eq!(g[0].granted, 60);
}

#[test]
fn requests_refuse_when_used_plus_amount_passes_the_cap() {
    let s = capped(Dimension::Requests, 2);
    reserve(&s, op(1), 0, &[cell(Dimension::Requests, 2)]).expect("at the cap");
    assert_eq!(
        reserve(&s, op(2), 0, &[cell(Dimension::Requests, 1)]),
        Err(ReserveRefused::Exhausted { cell: 0 })
    );
}

#[test]
fn a_class_meter_grants_the_draw_that_crosses_the_cap_and_refuses_at_it() {
    let s = capped(Dimension::Class("tokens"), 10);
    reserve(&s, op(1), 0, &[cell(Dimension::Class("tokens"), 9)]).expect("under");
    reserve(&s, op(2), 0, &[cell(Dimension::Class("tokens"), 50)])
        .expect("crossing is granted whole (1.5.5 `tokens >= cap`)");
    assert_eq!(
        reserve(&s, op(3), 0, &[cell(Dimension::Class("tokens"), 1)]),
        Err(ReserveRefused::Exhausted { cell: 0 })
    );
}

#[test]
fn money_refuses_a_draw_that_would_pass_the_cap() {
    let s = capped(Dimension::NanoUnits, 100);
    assert_eq!(
        reserve(&s, op(1), 0, &[cell(Dimension::NanoUnits, 101)]),
        Err(ReserveRefused::Exhausted { cell: 0 })
    );
    reserve(&s, op(2), 0, &[cell(Dimension::NanoUnits, 100)]).expect("exactly the cap");
}

#[test]
fn an_overflowing_draw_is_exhausted_not_wrapped() {
    let s = capped(Dimension::Requests, u64::MAX);
    reserve(&s, op(1), 0, &[cell(Dimension::Requests, u64::MAX - 1)]).expect("near max");
    assert_eq!(
        reserve(&s, op(2), 0, &[cell(Dimension::Requests, 5)]),
        Err(ReserveRefused::Exhausted { cell: 0 })
    );
}

#[test]
fn a_chain_draw_is_all_or_nothing() {
    let s = MemoryStore::new();
    s.v3_window_caps(
        op(100),
        &[
            cap(Dimension::Requests, 10, 1),
            cap(Dimension::NanoUnits, 5, 1),
        ],
    )
    .expect("caps");
    // The second cell fails, so the first draws nothing either.
    assert_eq!(
        reserve(
            &s,
            op(1),
            0,
            &[cell(Dimension::Requests, 10), cell(Dimension::NanoUnits, 6)]
        ),
        Err(ReserveRefused::Exhausted { cell: 1 })
    );
    reserve(&s, op(2), 0, &[cell(Dimension::Requests, 10)])
        .expect("the first cell's headroom is untouched");
}

#[test]
fn two_cells_on_one_slot_count_against_each_other() {
    let s = capped(Dimension::Requests, 3);
    assert_eq!(
        reserve(
            &s,
            op(1),
            0,
            &[cell(Dimension::Requests, 2), cell(Dimension::Requests, 2)]
        ),
        Err(ReserveRefused::Exhausted { cell: 1 })
    );
}

#[test]
fn a_replayed_reserve_answers_the_same_grants_and_draws_nothing_more() {
    let s = capped(Dimension::Requests, 10);
    let a = reserve(&s, op(1), 0, &[cell(Dimension::Requests, 6)]).expect("a");
    let b = reserve(&s, op(1), 0, &[cell(Dimension::Requests, 6)]).expect("replay");
    assert_eq!(a, b);
    // Only 6 are drawn: 4 more fit, 5 do not.
    assert_eq!(
        reserve(&s, op(2), 0, &[cell(Dimension::Requests, 5)]),
        Err(ReserveRefused::Exhausted { cell: 0 })
    );
    reserve(&s, op(3), 0, &[cell(Dimension::Requests, 4)]).expect("the rest");
}

#[test]
fn a_reserve_op_id_reused_with_another_body_is_a_conflict() {
    let s = capped(Dimension::Requests, 10);
    reserve(&s, op(1), 0, &[cell(Dimension::Requests, 1)]).expect("a");
    assert_eq!(
        reserve(&s, op(1), 0, &[cell(Dimension::Requests, 2)]),
        Err(ReserveRefused::Conflict)
    );
}

#[test]
fn a_refused_reserve_is_not_recorded() {
    let s = capped(Dimension::Requests, 1);
    reserve(&s, op(1), 0, &[cell(Dimension::Requests, 1)]).expect("fill");
    assert_eq!(
        reserve(&s, op(2), 0, &[cell(Dimension::Requests, 1)]),
        Err(ReserveRefused::Exhausted { cell: 0 })
    );
    s.v3_window_caps(op(3), &[cap(Dimension::Requests, 2, 2)])
        .expect("raise");
    // The same op_id is evaluated afresh and now fits.
    reserve(&s, op(2), 0, &[cell(Dimension::Requests, 1)]).expect("afresh");
}

#[test]
fn the_memory_store_never_answers_a_stale_epoch() {
    let s = capped(Dimension::Requests, 10);
    reserve(&s, op(1), 9, &[cell(Dimension::Requests, 1)]).expect("epoch 9");
    reserve(&s, op(2), 1, &[cell(Dimension::Requests, 1)])
        .expect("an older epoch is not stale on a node-local store");
}

#[test]
fn slice_release_is_clamped_deduped_and_frees_headroom() {
    let s = capped(Dimension::Requests, 10);
    let g = reserve(&s, op(1), 0, &[cell(Dimension::Requests, 10)]).expect("draw all");
    let id = g[0].slice_id;
    assert_eq!(release(&s, op(2), 0, &[(id, 4)]), Ok(vec![4]));
    assert_eq!(
        release(&s, op(2), 0, &[(id, 4)]),
        Ok(vec![4]),
        "a replay answers the original and takes nothing more back"
    );
    // Clamped to what the slice has left (6), never more.
    assert_eq!(release(&s, op(3), 0, &[(id, u64::MAX)]), Ok(vec![6]));
    // All 10 are free again.
    reserve(&s, op(4), 0, &[cell(Dimension::Requests, 10)]).expect("headroom back");
}

#[test]
fn releasing_an_unknown_slice_fails_and_applies_nothing() {
    let s = capped(Dimension::Requests, 10);
    let g = reserve(&s, op(1), 0, &[cell(Dimension::Requests, 10)]).expect("draw");
    assert!(matches!(
        release(&s, op(2), 0, &[(g[0].slice_id, 3), (999, 1)]),
        Err(OpRefused::Failed(_))
    ));
    // The known item was not applied either: still exhausted.
    assert_eq!(
        reserve(&s, op(3), 0, &[cell(Dimension::Requests, 1)]),
        Err(ReserveRefused::Exhausted { cell: 0 })
    );
}

#[test]
fn window_caps_newest_generation_wins_and_equal_generation_conflicts() {
    let s = MemoryStore::new();
    s.v3_window_caps(op(1), &[cap(Dimension::Requests, 1, 5)])
        .expect("gen 5");
    s.v3_window_caps(op(2), &[cap(Dimension::Requests, 99, 4)])
        .expect("an older generation is ignored");
    assert_eq!(
        reserve(&s, op(3), 0, &[cell(Dimension::Requests, 2)]),
        Err(ReserveRefused::Exhausted { cell: 0 })
    );
    assert_eq!(
        s.v3_window_caps(
            op(4),
            &[
                cap(Dimension::Requests, 3, 6),
                cap(Dimension::Requests, 7, 6)
            ]
        ),
        Err(CapsRefused::CapConflict { index: 1 }),
        "equal gen with a different cap refuses the WHOLE push"
    );
    // Nothing of the refused push applied: the cap is still 1.
    assert_eq!(
        reserve(&s, op(5), 0, &[cell(Dimension::Requests, 2)]),
        Err(ReserveRefused::Exhausted { cell: 0 })
    );
    s.v3_window_caps(op(6), &[cap(Dimension::Requests, 3, 6)])
        .expect("gen 6");
    reserve(&s, op(7), 0, &[cell(Dimension::Requests, 2)]).expect("cap 3");
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
fn append_batch_advances_the_stream_head_once_per_op() {
    let s = MemoryStore::new();
    let r = |b: u8| RecordBytes::new(vec![b]).expect("record");
    assert_eq!(
        s.v3_append_batch(op(1), "journal", &[r(1), r(2)])
            .expect("a")
            .seq,
        2
    );
    assert_eq!(
        s.v3_append_batch(op(1), "journal", &[r(1), r(2)])
            .expect("replay")
            .seq,
        2
    );
    assert_eq!(
        s.v3_append_batch(op(2), "journal", &[r(3)]).expect("b").seq,
        3
    );
    let heads = s.v3_heads().expect("heads");
    assert_eq!(heads.len(), 1);
    assert_eq!(heads[0].0, "journal");
    assert_eq!(heads[0].1.seq, 3);
}

#[test]
fn sessions_are_listed_per_principal_and_removed() {
    let s = MemoryStore::new();
    s.v3_session_put(1, "n1", "alice").expect("put");
    s.v3_session_put(2, "n2", "alice").expect("put");
    s.v3_session_put(3, "n1", "bob").expect("put");
    assert_eq!(
        s.v3_sessions_for("alice").expect("list"),
        vec![(1, "n1".to_string()), (2, "n2".to_string())]
    );
    s.v3_session_remove(1).expect("remove");
    s.v3_session_remove(1).expect("absent is Ok");
    assert_eq!(
        s.v3_sessions_for("alice").expect("list"),
        vec![(2, "n2".to_string())]
    );
}

/// The store kind's shared cases, as a NODE-LOCAL store runs them: one constant epoch, slices that
/// never expire, and a release that applies once per slice and closes it
/// (`busbar_contract::testkit::store_v3`).
struct SingleNode;

impl busbar_contract::testkit::store_v3::Harness for SingleNode {
    type Store = MemoryStore;
    fn open(&self) -> MemoryStore {
        MemoryStore::new()
    }
    fn now_ms(&self) -> u64 {
        0
    }
    fn advance_ms(&self, _: u64) {}
}

#[test]
fn the_single_node_rule_and_the_release_rule_hold() {
    busbar_contract::testkit::store_v3::single_node_store(&SingleNode);
}

/// THE DEDUPE LOG HOLDS A FIXED-SIZE RECORD PER OP (the store dedupe rule: "same" means the op's
/// value fields, kept at least `OP_ID_RETENTION_SECS`). Production mints a fresh `op_id` for every
/// write, so whatever the log keeps per op is kept for every write the node makes for a day: it must
/// not grow with the size of what was written. Sixty-four 500-byte records rendered as text held
/// over 100 KiB in the log; the log keeps a digest of the value fields instead.
#[test]
fn the_dedupe_log_keeps_a_fixed_size_record_per_op() {
    let s = MemoryStore::new();
    let big: Vec<RecordBytes> = (0..64)
        .map(|_| RecordBytes::new(vec![0xab; 500]).expect("record"))
        .collect();
    s.v3_append_batch(op(1), "journal", &big).expect("append");
    let held = s.v3.held().op_bytes;
    assert!(held <= 64, "one op keeps {held} bytes in the dedupe log");
    // A one-byte write keeps exactly as much as the 32 KB one: what an op keeps does not grow with
    // what it wrote.
    let small = RecordBytes::new(vec![0xab; 1]).expect("record");
    s.v3_append_batch(op(2), "journal", &[small])
        .expect("append");
    assert_eq!(
        s.v3.held().op_bytes,
        2 * held,
        "a 32 KB op keeps more in the dedupe log than a one-byte op"
    );
    // The digest still tells the same op from a different one.
    s.v3_append_batch(op(1), "journal", &big).expect("replay");
    assert!(matches!(
        s.v3_append_batch(op(1), "journal", &big[..1]),
        Err(OpRefused::Conflict)
    ));
}

/// A LEDGER STREAM KEEPS ITS HEAD, NOT ITS ROWS. `heads` is the one read of a stream, and it reads
/// only the count, so a stream's records are not kept: what a stream holds is its name and its
/// head, however many rows are appended to it.
#[test]
fn a_ledger_stream_keeps_its_head_and_not_its_rows() {
    let s = MemoryStore::new();
    let big = || RecordBytes::new(vec![0xcd; 500]).expect("record");
    let head = s
        .v3_append_batch(op(1), "journal", &[big(), big()])
        .expect("append");
    assert_eq!(head.seq, 2);
    let held = s.v3.held().journal_bytes;
    assert_eq!(
        held,
        "journal".len() + std::mem::size_of::<u64>(),
        "the stream keeps more than its name and head"
    );
    s.v3_append_batch(op(2), "journal", &[big(), big(), big()])
        .expect("append");
    assert_eq!(
        s.v3.held().journal_bytes,
        held,
        "the stream's rows are kept"
    );
    assert_eq!(s.v3_heads().expect("heads")[0].1.seq, 5);
}

/// A key of `dimension` in the window starting at `window_start` (milliseconds; `0`, the window
/// that never rolls: the gauges and the caps that never reset).
fn at(dimension: Dimension<'static>, window_start: u64) -> CellKey<'static> {
    CellKey {
        window_start,
        ..key(dimension)
    }
}

/// THE RETENTION SWEEP IS THE TOKEN LEDGER'S (`RecordStore::purge_windows_before`: "Retention
/// sweep for the (bucket_id, window_start) token ledger"; Q128 store-memory, readers sm1 MEDIUM and
/// sm2 LOW on 0db18e3): its seconds cutoff purges the ledger's windows and touches no money slot. A
/// window-0 gauge and its live lease, and a rolling money window keyed in milliseconds, keep their
/// caps, counters and slices. RED before the fix: the sweep took every money slot whose window
/// start fell under the seconds cutoff, window 0's gauges and live leases among them.
#[test]
fn the_retention_sweep_purges_the_token_ledger_and_no_money_slot() {
    let s = MemoryStore::new();
    s.v3_window_caps(
        op(1_000_000),
        &[
            Cap {
                key: at(Dimension::Concurrency, 0),
                cap: 4,
                config_gen: 1,
            },
            Cap {
                key: at(Dimension::Requests, 1_000),
                cap: 10,
                config_gen: 1,
            },
        ],
    )
    .expect("caps");
    let lease = s.v3_reserve(
        op(1),
        0,
        [Cell {
            key: at(Dimension::Concurrency, 0),
            amount: 1,
        }]
        .into_iter(),
        &mut Vec::new(),
    );
    assert!(lease.is_ok(), "{lease:?}");
    s.add_usage("b", 1_000, &delta(1, 1)).expect("usage");
    Store::purge_windows_before(&s, 2_000).expect("purge");
    assert_eq!(
        s.get_usage("b", 1_000).expect("read").requests,
        0,
        "the ledger window went"
    );
    let held = s.v3.held();
    assert_eq!((held.caps, held.used, held.slices), (2, 1, 1), "{held:?}");
}

/// ROLLING MONEY WINDOWS ARE BOUNDED (Q128 store-memory sm1 LOW): a cap pushed for a newer window
/// of a slot's family (bucket, pool, dimension, class) closes the family's windows older than the
/// one before it: their open slices end and their caps and counters retire. Window 0 never rolls
/// and is never retired. RED before the fix:
/// every window's cap and counter stayed for the life of the process.
#[test]
fn a_newer_windows_caps_retire_the_older_rolling_windows() {
    let s = MemoryStore::new();
    let push = |n: u64, window_start: u64| {
        s.v3_window_caps(
            op(1_000_000 + n),
            &[Cap {
                key: at(Dimension::Requests, window_start),
                cap: 10,
                config_gen: 1,
            }],
        )
        .expect("caps");
    };
    s.v3_window_caps(
        op(999),
        &[Cap {
            key: at(Dimension::Concurrency, 0),
            cap: 4,
            config_gen: 1,
        }],
    )
    .expect("gauge");
    push(1, 60_000);
    let draw = |n: u64, window_start: u64| {
        let mut grants = Vec::new();
        s.v3_reserve(
            op(n),
            0,
            [Cell {
                key: at(Dimension::Requests, window_start),
                amount: 1,
            }]
            .into_iter(),
            &mut grants,
        )
        .expect("draw");
        grants
    };
    let open = draw(10, 60_000);
    push(2, 120_000);
    // 120_000 is the newest and 60_000 the window before it: both kept, the slice open.
    let held = s.v3.held();
    assert_eq!((held.caps, held.used, held.slices), (3, 1, 1), "{held:?}");
    push(3, 180_000);
    // 60_000 closes, and its open slice ends with it; 120_000 (the window before the newest),
    // 180_000 and the window-0 gauge stay.
    let held = s.v3.held();
    assert_eq!((held.caps, held.used, held.slices), (3, 0, 0), "{held:?}");
    assert_eq!(
        release(&s, op(12), 0, &[(open[0].slice_id, 1)]).expect("release"),
        vec![0],
        "a slice whose window closed returns nothing more"
    );
}

/// A SLICE WHOSE UNSPENT REACHES ZERO CLOSES AT ONCE (abi::store money.rs: "it closes when nothing is
/// left"; ARCHITECT ruling on the memory store's spent slices). A unit that spent its whole grant
/// releases `0`: nothing is left to return, so the slice closes then, and a later release of it
/// returns `0`, rather than staying open for the life of its window.
#[test]
fn a_slice_released_with_nothing_unspent_closes_at_once() {
    let s = capped(Dimension::Requests, 10);
    let g = reserve(&s, op(1), 0, &[cell(Dimension::Requests, 2)]).expect("draw");
    assert_eq!(
        release(&s, op(2), 0, &[(g[0].slice_id, 0)]).expect("release"),
        vec![0]
    );
    assert_eq!(s.v3.held().slices, 0, "a fully spent slice stays open");
    // Closed: a later release of it returns nothing, and the spent units stay drawn.
    assert_eq!(
        release(&s, op(3), 0, &[(g[0].slice_id, 2)]).expect("release"),
        vec![0]
    );
    assert!(matches!(
        reserve(&s, op(4), 0, &[cell(Dimension::Requests, 9)]),
        Err(ReserveRefused::Exhausted { cell: 0 })
    ));
    // A partial release still leaves the slice open for the rest.
    let g = reserve(&s, op(5), 0, &[cell(Dimension::Requests, 4)]).expect("draw");
    assert_eq!(
        release(&s, op(6), 0, &[(g[0].slice_id, 1)]).expect("release"),
        vec![1]
    );
    assert_eq!(s.v3.held().slices, 1);
}

/// A PARTLY SPENT SLICE IS USABLE UNTIL ITS WINDOW CLOSES, THEN ENDS AND FREES IT (`abi::store`
/// money.rs slice life: "A partial release leaves the slice open for the rest"; rule (c), "past
/// `valid_until_ms` the slice is EXPIRED, and whatever of it is still unreturned goes back to its
/// window's headroom"; rule (d), a node-local slice may never expire, so its validity ends with its
/// window). RED before the fix: the slice stayed open for the life of the process and pinned its
/// window's cap and counter, which never retired.
#[test]
fn a_partly_spent_slice_is_usable_until_its_window_closes_then_frees_it() {
    let s = MemoryStore::new();
    let push = |n: u64, window_start: u64| {
        s.v3_window_caps(
            op(1_000_000 + n),
            &[Cap {
                key: at(Dimension::Requests, window_start),
                cap: 10,
                config_gen: 1,
            }],
        )
        .expect("caps");
    };
    push(1, 60_000);
    let g = reserve(
        &s,
        op(10),
        0,
        &[Cell {
            key: at(Dimension::Requests, 60_000),
            amount: 6,
        }],
    )
    .expect("draw");
    let id = g[0].slice_id;
    // A partial release: 2 back, the slice stays open for the rest.
    assert_eq!(
        release(&s, op(11), 0, &[(id, 2)]).expect("release"),
        vec![2]
    );
    assert_eq!(s.v3.held().slices, 1, "a partial release closed the slice");
    push(2, 120_000);
    // 60_000 is now the window before the newest: still open, and the slice still usable.
    assert_eq!(
        release(&s, op(12), 0, &[(id, 1)]).expect("release"),
        vec![1]
    );
    let held = s.v3.held();
    assert_eq!((held.caps, held.used, held.slices), (2, 1, 1), "{held:?}");
    // 60_000 closes: the slice ends with it, and its cap and counter retire.
    push(3, 180_000);
    let held = s.v3.held();
    assert_eq!(
        (held.caps, held.used, held.slices),
        (2, 0, 0),
        "the partly spent slice still pins its closed window: {held:?}"
    );
    assert_eq!(
        release(&s, op(13), 0, &[(id, 3)]).expect("release"),
        vec![0],
        "an ended slice returns nothing more"
    );
}

/// A RELEASE NEVER WALKS THE CLOSED SLICES (coordinator ruling on the closed-slice sweep). Closed
/// slices are kept for `OP_ID_RETENTION_SECS`, indexed by close time, and a release forgets only
/// the expired ones from the front of that index. With more than 1024 slices closed inside the
/// retention, a release examines one index entry, not all of them; past the retention they are
/// forgotten at every size. RED before the fix: once more than 1024 slices were closed, every
/// release walked the whole closed map under the v3 lock, and below 1024 none was ever forgotten.
#[test]
fn a_release_touches_a_bounded_number_of_closed_slices() {
    const N: u64 = 1_500;
    let s = capped(Dimension::Requests, N + 1);
    s.pin_clock(1_000);
    let cells: Vec<Cell<'static>> = (0..N).map(|_| cell(Dimension::Requests, 1)).collect();
    let g = reserve(&s, op(1), 0, &cells).expect("draw");
    for (i, grant) in g.iter().enumerate() {
        release(&s, op(10 + i as u64), 0, &[(grant.slice_id, 1)]).expect("release");
    }
    assert_eq!(s.v3.held().closed, N as usize);
    let last = reserve(&s, op(5_000), 0, &[cell(Dimension::Requests, 1)]).expect("draw");
    s.v3.take_closed_touched();
    release(&s, op(5_001), 0, &[(last[0].slice_id, 1)]).expect("release");
    let touched = s.v3.take_closed_touched();
    assert!(
        touched <= 1,
        "a release examined {touched} closed slices of {N}"
    );
    // Past the retention, the next release forgets them all.
    s.pin_clock(1_000 + OP_ID_RETENTION_SECS);
    let next = reserve(&s, op(5_002), 0, &[cell(Dimension::Requests, 1)]).expect("draw");
    release(&s, op(5_003), 0, &[(next[0].slice_id, 1)]).expect("release");
    assert_eq!(
        s.v3.held().closed,
        1,
        "closed slices past their retention are kept"
    );
}

/// A RELEASE ON WINDOW 0 IS FINAL (`abi::store` money.rs slice life (e)): window 0 never rolls, so
/// a partly spent window-0 slice has no window close to end it and, on this node-local store, no
/// expiry either. Its release takes back what it names, the spent rest stays counted, and the
/// slice closes, so holders that are done leave the slices map at its baseline size. RED before the
/// fix: every partly spent window-0 slice stayed open for the life of the process.
#[test]
fn partly_spent_window_zero_slices_close_on_their_release() {
    const N: u64 = 500;
    let s = MemoryStore::new();
    let w0 = at(Dimension::NanoUnits, 0);
    s.v3_window_caps(
        op(1_000_000),
        &[Cap {
            key: w0,
            cap: 2 * N + 6,
            config_gen: 1,
        }],
    )
    .expect("caps");
    let baseline = s.v3.held().slices;
    for n in 0..N {
        let g = reserve(&s, op(10 + 2 * n), 0, &[Cell { key: w0, amount: 3 }]).expect("draw");
        // The holder spent 2 of its 3 and hands back 1: its release is final.
        assert_eq!(
            release(&s, op(11 + 2 * n), 0, &[(g[0].slice_id, 1)]).expect("release"),
            vec![1]
        );
    }
    assert_eq!(
        s.v3.held().slices,
        baseline,
        "partly spent window-0 slices outlive their holders"
    );
    // The spent 2 per holder stay counted: 2 * N drawn, so 6 more fill the cap and 1 is refused.
    reserve(&s, op(5_000), 0, &[Cell { key: w0, amount: 6 }]).expect("the rest of the cap");
    assert_eq!(
        reserve(&s, op(5_001), 0, &[Cell { key: w0, amount: 1 }]),
        Err(ReserveRefused::Exhausted { cell: 0 })
    );
}
