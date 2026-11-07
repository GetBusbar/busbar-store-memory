// SPDX-License-Identifier: Apache-2.0
//! The normal composition-root axis selects the actual neutral memory door.
mod support;
use busbar_contract::persistence::{Durability, Read, ReadResult};
use std::sync::Arc;
#[test]
fn normal_axis_opens_a_fresh_ephemeral_dataset_and_returns_authoritative_time() {
    support::block(async {
        let first = support::open_axis().await;
        let second = support::open_axis().await;
        assert_eq!(first.info().durability, Durability::Ephemeral);
        assert_ne!(first.info().dataset, second.info().dataset);
        let ReadResult::Values {
            observed_at,
            values,
        } = first
            .read(Arc::new(Read {
                dataset: first.info().dataset,
                addresses: vec![],
                time: None,
            }))
            .await
            .unwrap()
        else {
            panic!("coherent empty read")
        };
        assert!(values.is_empty());
        assert_eq!(observed_at.authority, first.info().clock_authority);
        assert_eq!(
            first.clock(first.info().dataset).await.unwrap().authority,
            observed_at.authority
        );
    });
}
