// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors

use super::*;
use busbar_contract::abi::store::OpId;
use busbar_contract::records::{
    MeteringDelta, ModelTokens, ModelTokensDelta, RecordStore, UsageDelta, UsageLedger,
};
use std::collections::BTreeMap;

fn delta(scale: u32, amount: i128, requests: i64) -> UsageDelta {
    UsageDelta {
        requests,
        billable_requests: requests,
        models: vec![ModelTokensDelta {
            scale,
            model: "exact-model".into(),
            usage_units: BTreeMap::from([
                ("input".into(), amount),
                ("audio_seconds".into(), amount),
            ]),
        }],
    }
}

fn meter(key: &str, scale: u32, amount: i128, requests: u64) -> MeteringDelta {
    MeteringDelta {
        scale,
        key_id: key.into(),
        bucket: 0,
        model: "exact-model".into(),
        provider: "provider".into(),
        tokens_input: amount,
        tokens_output: 0,
        tokens_cache_read: 0,
        tokens_cache_write: 0,
        requests,
        billable_requests: requests,
        key_group_at_use: "group".into(),
        pricing_version: "opening".into(),
        priced_from_ms: 0,
        usage_units: BTreeMap::from([("audio_seconds".into(), amount)]),
    }
}

fn exercise(store: &dyn RecordStore, key: &str) {
    store.add_usage(key, 0, &delta(0, 2, 1)).unwrap();
    let op = OpId::from_parts(0xe81, 1);
    let cells = [(key, 0, delta(6, 123_456, 2))];
    store.add_usage_batch_deduped(op, &cells).unwrap();
    store.add_usage_batch_deduped(op, &cells).unwrap();
    let got = store.get_usage(key, 0).unwrap();
    assert_eq!((got.requests, got.billable_requests), (3, 3));
    assert_eq!(got.models[0].scale, 6);
    assert_eq!(got.models[0].usage_units["input"], 2_123_456);
    assert_eq!(got.models[0].usage_units["audio_seconds"], 2_123_456);
    store.add_usage(key, 0, &delta(6, -123_455, 0)).unwrap();
    assert_eq!(
        store.get_usage(key, 0).unwrap().models[0].usage_units["input"],
        2_000_001
    );
    assert!(store
        .add_usage_batch_deduped(op, &[(key, 0, delta(6, 1, 2))])
        .is_err());

    store.add_metering(&meter(key, 0, 2, 1)).unwrap();
    let op = OpId::from_parts(0xe81, 2);
    let meters = [meter(key, 6, 123_456, 2)];
    store.add_metering_batch_deduped(op, &meters).unwrap();
    store.add_metering_batch_deduped(op, &meters).unwrap();
    let before = store.list_metering(0).unwrap();
    let got = before.iter().find(|row| row.key_id == key).unwrap();
    assert_eq!(
        (
            got.scale,
            got.tokens_input,
            got.requests,
            got.billable_requests
        ),
        (6, 2_123_456, 3, 3)
    );
    assert_eq!(got.usage_units["audio_seconds"], 2_123_456);
    let op = OpId::from_parts(0xe81, 3);
    assert!(store
        .add_metering_batch_deduped(op, &[meter(key, 6, 1, 1), meter(key, 6, i128::MAX, 1)])
        .is_err());
    assert_eq!(
        store.list_metering(0).unwrap(),
        before,
        "overflow rolls the entire batch back"
    );
    store
        .add_metering_batch_deduped(op, &[meter(key, 6, 1, 1)])
        .unwrap();

    let max = UsageLedger {
        requests: 7,
        billable_requests: 5,
        models: vec![ModelTokens {
            scale: 6,
            model: "exact-model".into(),
            usage_units: BTreeMap::from([("input".into(), i128::MAX)]),
        }],
    };
    store.put_usage(key, 0, &max).unwrap();
    let op = OpId::from_parts(0xe81, 4);
    assert!(store
        .add_usage_batch_deduped(op, &[(key, 1, delta(6, 1, 1)), (key, 0, delta(6, 1, 1))])
        .is_err());
    assert_eq!(store.get_usage(key, 0).unwrap(), max);
    assert_eq!(store.get_usage(key, 1).unwrap(), UsageLedger::default());
    // A refused operation leaves no receipt; the same id can now commit another body.
    store
        .add_usage_batch_deduped(op, &[(key, 1, delta(6, 1, 1))])
        .unwrap();
    assert_eq!(
        store.get_usage(key, 1).unwrap().models[0].usage_units["input"],
        1
    );
    assert!(store.add_usage(key, 2, &delta(3, 1, 1)).is_err());
    assert_eq!(store.get_usage(key, 2).unwrap(), UsageLedger::default());
    exercise_wide(store, key);
}

fn exercise_wide(store: &dyn RecordStore, key: &str) {
    use busbar_contract::records::correction::{
        canonical_metering, Decision, Request, UsageAddress, UsageChange, VERSION,
    };

    // Exceed both old machine-word quantity domains without changing request counters.
    let wide = i128::from(u64::MAX) + 17;
    let bucket = format!("{key}-wide");
    store.add_usage(&bucket, 0, &delta(6, wide, 1)).unwrap();
    let op = OpId::from_parts(0xe82, 1);
    let cells = [(bucket.as_str(), 0, delta(6, wide + 1, 2))];
    store.add_usage_batch_deduped(op, &cells).unwrap();
    store.add_usage_batch_deduped(op, &cells).unwrap();
    let before = store.get_usage(&bucket, 0).unwrap();
    assert_eq!((before.requests, before.billable_requests), (3, 3));
    assert_eq!(before.models[0].scale, 6);
    assert_eq!(before.models[0].usage_units["input"], 2 * wide + 1);
    assert_eq!(before.models[0].usage_units["audio_seconds"], 2 * wide + 1);
    assert!(store
        .add_usage_batch_deduped(op, &[(bucket.as_str(), 0, delta(6, wide + 2, 2))])
        .is_err());
    assert_eq!(store.get_usage(&bucket, 0).unwrap(), before);

    // Ordinary refunds still floor at zero, including MIN when checked addition is valid.
    let refund_bucket = format!("{key}-refund");
    store
        .add_usage(&refund_bucket, 0, &delta(6, wide, 1))
        .unwrap();
    store
        .add_usage(&refund_bucket, 0, &delta(6, i128::MIN, 0))
        .unwrap();
    let refunded = store.get_usage(&refund_bucket, 0).unwrap();
    assert_eq!((refunded.requests, refunded.billable_requests), (1, 1));
    assert!(refunded.models[0]
        .usage_units
        .values()
        .all(|amount| *amount == 0));
    let refund_op = OpId::from_parts(0xe82, 6);
    let refund_cells = [
        (refund_bucket.as_str(), 1, delta(6, wide, 1)),
        (refund_bucket.as_str(), 1, delta(6, i128::MIN, 0)),
    ];
    store
        .add_usage_batch_deduped(refund_op, &refund_cells)
        .unwrap();
    store
        .add_usage_batch_deduped(refund_op, &refund_cells)
        .unwrap();
    assert_eq!(store.get_usage(&refund_bucket, 1).unwrap(), refunded);

    // A late scale-conversion overflow leaves every row and the operation receipt untouched.
    let invalid_op = OpId::from_parts(0xe82, 2);
    assert!(store
        .add_usage_batch_deduped(
            invalid_op,
            &[
                (bucket.as_str(), 1, delta(6, 1, 1)),
                (bucket.as_str(), 0, delta(0, i128::MAX, 1)),
            ],
        )
        .is_err());
    assert_eq!(store.get_usage(&bucket, 0).unwrap(), before);
    assert_eq!(store.get_usage(&bucket, 1).unwrap(), UsageLedger::default());
    store
        .add_usage_batch_deduped(invalid_op, &[(bucket.as_str(), 1, delta(6, 1, 1))])
        .unwrap();

    // Every metering quantity column and the open map survives the actual Store codec.
    let mut quantities = meter(key, 6, wide, 1);
    quantities.model = "wide-model".into();
    quantities.tokens_output = wide + 1;
    quantities.tokens_cache_read = wide + 2;
    quantities.tokens_cache_write = wide + 3;
    let meter_op = OpId::from_parts(0xe82, 3);
    store
        .add_metering_batch_deduped(meter_op, std::slice::from_ref(&quantities))
        .unwrap();
    store
        .add_metering_batch_deduped(meter_op, std::slice::from_ref(&quantities))
        .unwrap();
    let mut metering = store.list_metering(0).unwrap();
    let row = metering
        .iter()
        .find(|row| row.model == "wide-model")
        .unwrap();
    assert_eq!((row.requests, row.billable_requests, row.scale), (1, 1, 6));
    assert_eq!(row.tokens_input, wide);
    assert_eq!(row.tokens_output, wide + 1);
    assert_eq!(row.tokens_cache_read, wide + 2);
    assert_eq!(row.tokens_cache_write, wide + 3);
    assert_eq!(row.usage_units["audio_seconds"], wide);
    quantities.tokens_input += 1;
    assert!(store
        .add_metering_batch_deduped(meter_op, &[quantities])
        .is_err());
    assert_eq!(store.list_metering(0).unwrap(), metering);

    // Whole-row correction compares wide metering and usage snapshots and reaches i128::MAX.
    canonical_metering(&mut metering);
    let mut replacement = before.clone();
    for amount in replacement.models[0].usage_units.values_mut() {
        *amount = i128::MAX;
    }
    let candidate = Request {
        version: VERSION,
        op_id: OpId::from_parts(0xe82, 4),
        key_id: key.into(),
        day_start: 0,
        metering,
        usage: vec![UsageChange {
            address: UsageAddress {
                bucket_id: bucket.clone(),
                window_start: 0,
            },
            expected: before,
            replacement: replacement.clone(),
        }],
    };
    let encoded = candidate.encode().unwrap();
    assert_eq!(Request::decode(&encoded).unwrap(), candidate);
    let json = std::str::from_utf8(&encoded).unwrap();
    assert!(json.contains(&format!("\"input\":{}", i128::MAX)));
    assert_eq!(
        store.apply_usage_correction(&candidate).unwrap(),
        Decision::Applied
    );
    assert_eq!(
        store.apply_usage_correction(&candidate).unwrap(),
        Decision::Applied
    );
    assert_eq!(store.get_usage(&bucket, 0).unwrap(), replacement);
    let mut changed = candidate.clone();
    changed.usage[0].replacement.models[0]
        .usage_units
        .insert("input".into(), i128::MAX - 1);
    assert!(store.apply_usage_correction(&changed).is_err());
    assert_eq!(store.get_usage(&bucket, 0).unwrap(), replacement);

    // A late negative replacement cannot publish an earlier valid replacement or consume the ID.
    let correction_op = OpId::from_parts(0xe82, 5);
    let empty = store.get_usage(&bucket, 1).unwrap();
    let mut invalid = replacement.clone();
    invalid.models[0]
        .usage_units
        .insert("input".into(), i128::MIN);
    let mut rejected = Request {
        op_id: correction_op,
        usage: vec![
            UsageChange {
                address: UsageAddress {
                    bucket_id: bucket.clone(),
                    window_start: 0,
                },
                expected: replacement.clone(),
                replacement: empty.clone(),
            },
            UsageChange {
                address: UsageAddress {
                    bucket_id: bucket.clone(),
                    window_start: 1,
                },
                expected: empty.clone(),
                replacement: invalid,
            },
        ],
        ..candidate.clone()
    };
    assert!(store.apply_usage_correction(&rejected).is_err());
    assert_eq!(store.get_usage(&bucket, 0).unwrap(), replacement);
    assert_eq!(store.get_usage(&bucket, 1).unwrap(), empty);
    rejected.usage[1].replacement = replacement;
    assert_eq!(
        store.apply_usage_correction(&rejected).unwrap(),
        Decision::Applied
    );
    assert_eq!(store.get_usage(&bucket, 0).unwrap(), empty);
    assert_eq!(
        store.get_usage(&bucket, 1).unwrap(),
        rejected.usage[1].replacement
    );
}

#[test]
fn exact_counts_decimal_legacy_replay_and_atomic_overflow() {
    let store = MemoryStore::new();
    store.put_key(&key("exact-counts")).unwrap();
    exercise(&store, "exact-counts");
}
