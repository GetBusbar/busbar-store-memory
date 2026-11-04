// SPDX-License-Identifier: Apache-2.0
mod support;
use busbar_contract::abi::store::OpId;
use busbar_contract::records::{ModelTokensDelta, RecordStore, UsageDelta};
use busbar_contract::store_calls::StoreCalls;
fn block<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(f)
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
fn the_compiled_in_shortcut_answers_a_replayed_write_differently() {
    for dropped in [false, true] {
        let table = support::open(dropped);
        let cells = [("k", 60u64, delta(1, 5))];
        block(async {
            table
                .add_usage_batch(OpId::from_parts(2, 1), &cells)
                .await
                .expect("write");
            table
                .add_usage_batch(OpId::from_parts(2, 1), &cells)
                .await
                .expect("retry");
        });
        let through_table = RecordStore::get_usage(&table, "k", 60).expect("read");
        let shortcut = busbar_store_memory::MemoryStore::new();
        shortcut.add_usage("k", 60, &cells[0].2).expect("write");
        shortcut.add_usage("k", 60, &cells[0].2).expect("retry");
        let through_shortcut = shortcut.get_usage("k", 60).expect("read");
        assert_eq!(through_table.requests, 1);
        assert_eq!(through_shortcut.requests, 2);
    }
}

#[test]
fn a_true_replay_is_deduped() {
    for dropped in [false, true] {
        let s = support::open(dropped);
        let op = OpId::from_parts(0xb007_0004, 1);
        let cells = [("k", 60u64, delta(1, 0))];
        block(async {
            s.add_usage_batch(op, &cells).await.expect("the write");
            s.add_usage_batch(op, &cells).await.expect("its replay");
        });
        assert_eq!(
            s.get_usage("k", 60).expect("read").requests,
            1,
            "the same op_id applies once"
        );
    }
}
