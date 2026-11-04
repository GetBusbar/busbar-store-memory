// SPDX-License-Identifier: Apache-2.0
mod support;
use busbar_contract::abi::store::OpId;
use busbar_contract::kinds::RecordBytes;
use busbar_contract::records::{
    CredentialMeta, CredentialSecret, ModelTokensDelta, PlaneDisposition, PlaneRecord,
    PlaneSelector, RecordStore, SecretForm, UsageDelta, VirtualKey,
};
use busbar_contract::store_calls::{StoreCalls, StoreFailure};
use busbar_plugin_loader::dispatch::kinds::store::StoreFacts;
use busbar_plugin_loader::dispatch::LinkedRow;
fn run<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(f)
}

fn op(n: u64) -> OpId {
    OpId::from_parts(9, n)
}

fn key(id: &str) -> VirtualKey {
    VirtualKey {
        id: id.to_string(),
        generation_hash: format!("h_{id}"),
        name: "t".to_string(),
        enabled: true,
        ..Default::default()
    }
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

fn record(kind: &str, id: &str, parent: Option<&str>, seq: u64, body: Vec<u8>) -> PlaneRecord {
    PlaneRecord {
        kind: kind.to_string(),
        id: id.to_string(),
        parent: parent.map(str::to_string),
        seq,
        ts: 1,
        disposition: PlaneDisposition::Active,
        body,
    }
}

#[test]
fn the_memory_store_states_its_facts_in_its_tail() {
    for dropped in [false, true] {
        let s = support::open(dropped);
        assert_eq!(
            s.facts(),
            StoreFacts {
                ephemeral: true,
                durable_plane: false,
                fork_refusal: true,
                record_transaction: true,
                usage_correction: true,
                secret_refs: vec!["token".to_string()],
                credential_rewrap: true,
            }
        );
        assert!(!s.name().is_empty(), "the store states its name");
    }
}

#[test]
fn keys_round_trip_through_the_table() {
    for dropped in [false, true] {
        let store = support::open(dropped);
        let s: &dyn RecordStore = &store;
        s.put_key(&key("a")).expect("put");
        s.put_key(&key("b")).expect("put");
        assert_eq!(s.get_key("a").expect("get").map(|k| k.id), Some("a".into()));
        assert_eq!(s.get_key("zz").expect("absent"), None);
        let mut ids: Vec<String> = s
            .list_keys()
            .expect("list")
            .into_iter()
            .map(|k| k.id)
            .collect();
        ids.sort();
        assert_eq!(ids, ["a", "b"]);
        s.delete_key("a").expect("delete");
        assert!(s
            .get_key("a")
            .expect("get")
            .expect("tombstone")
            .deleted_at
            .is_some());
        assert!(s.delete_key("nope").is_err(), "an unknown id is FAILED");
    }
}

#[test]
fn the_tombstone_precondition_crosses_as_a_failure() {
    for dropped in [false, true] {
        let store = support::open(dropped);
        let s: &dyn RecordStore = &store;
        s.put_key(&key("a")).expect("put");
        s.delete_key("a").expect("delete");
        let err = s
            .put_key(&key("a"))
            .expect_err("a live write over a tombstone");
        assert!(!err.0.is_empty());
    }
}

#[test]
fn credentials_cross_as_secret_blobs_and_read_back() {
    for dropped in [false, true] {
        let store = support::open(dropped);
        let s: &dyn RecordStore = &store;
        s.put_key(&key("k1")).expect("key");
        let secret = CredentialSecret {
            meta: CredentialMeta {
                id: "c1".into(),
                key_id: "k1".into(),
                kind: "generic".into(),
                slot: 0,
                public_id: "pub".into(),
                secret_form: SecretForm::Recoverable,
                created_at: 0,
                updated_at: 0,
                expires_at: None,
                revoked_at: None,
                revoke_reason: None,
                revision: 0,
            },
            secret: "s3cret".into(),
        };
        s.put_credential(&secret).expect("put");
        let since = s.list_credentials_since(0).expect("since");
        assert_eq!(since.len(), 1);
        assert_eq!(since[0].meta.id, "c1");
        assert_eq!(since[0].secret, "s3cret");
        assert_eq!(s.list_credentials("k1").expect("list").len(), 1);
    }
}

#[test]
fn plane_records_and_tokens_cross_the_request_path_slots() {
    for dropped in [false, true] {
        const LIVE_REPLAY_DEADLINE: u64 = u64::MAX;
        let store = support::open(dropped);
        let s: &dyn RecordStore = &store;
        s.upsert_plane_record(record("task", "t1", None, 0, b"body".to_vec()).view())
            .expect("upsert");
        assert_eq!(
            s.get_plane_record("task", "t1").expect("get"),
            Some(b"body".to_vec())
        );
        assert_eq!(s.get_plane_record("task", "t-absent").expect("get"), None);
        s.append_plane_record(record("event", "e", Some("t1"), 1, b"one".to_vec()).view())
            .expect("append");
        s.append_plane_record(record("event", "e", Some("t1"), 2, b"two".to_vec()).view())
            .expect("append");
        assert_eq!(
            s.list_plane_records("event", &PlaneSelector::Parent("t1".into()))
                .expect("list"),
            vec![b"one".to_vec(), b"two".to_vec()]
        );
        assert_eq!(
            s.list_plane_record_parents("event").expect("parents"),
            vec!["t1".to_string()]
        );
        assert!(s
            .redeem_plane_token("tok", "x", LIVE_REPLAY_DEADLINE, 1)
            .expect("first"));
        assert!(!s
            .redeem_plane_token("tok", "x", LIVE_REPLAY_DEADLINE, 1)
            .expect("second"));
        s.delete_plane_record("task", "t1").expect("delete");
        assert_eq!(s.get_plane_record("task", "t1").expect("get"), None);
    }
}

#[test]
fn records_put_get_and_scan_through_the_typed_surface() {
    for dropped in [false, true] {
        let s = support::open(dropped);
        let v = |b: &[u8]| RecordBytes::new(b.to_vec()).expect("record");
        run(async {
            s.record_put("sch", b"a/1", &v(b"one")).await.expect("put");
            s.record_put("sch", b"a/2", &v(b"two")).await.expect("put");
            s.record_put("sch", b"b/1", &v(b"other"))
                .await
                .expect("put");
            assert_eq!(
                StoreCalls::record_get(&s, "sch", b"a/2")
                    .await
                    .expect("get"),
                Some(v(b"two"))
            );
            assert_eq!(
                StoreCalls::record_get(&s, "sch", b"zz").await.expect("get"),
                None
            );
            let rows = s.record_scan("sch", b"a/", 10).await.expect("scan");
            assert_eq!(
                rows,
                vec![(b"a/1".to_vec(), v(b"one")), (b"a/2".to_vec(), v(b"two"))]
            );
            assert_eq!(s.record_scan("sch", b"a/", 1).await.expect("scan").len(), 1);
            assert!(s
                .record_scan("sch", b"a/", 0)
                .await
                .expect("scan")
                .is_empty());
        });
    }
}

#[test]
fn a_usage_batch_replays_once_and_a_reused_op_id_is_a_conflict() {
    for dropped in [false, true] {
        let s = support::open(dropped);
        run(async {
            let cells = [("k", 60, delta(1, 10))];
            s.add_usage_batch(op(1), &cells).await.expect("a");
            s.add_usage_batch(op(1), &cells).await.expect("replay");
            let other = [("k", 60, delta(5, 5))];
            assert_eq!(
                s.add_usage_batch(op(1), &other).await,
                Err(StoreFailure::Conflict)
            );
        });
        assert_eq!(s.get_usage("k", 60).expect("read").requests, 1);
    }
}

#[test]
fn a_metering_batch_applies_through_the_typed_surface() {
    for dropped in [false, true] {
        let s = support::open(dropped);
        let m = busbar_contract::records::MeteringDelta {
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
            priced_from_ms: 0,
            usage_units: Default::default(),
        };
        run(async {
            s.add_metering_batch(op(1), std::slice::from_ref(&m))
                .await
                .expect("metering");
        });
        assert_eq!(
            RecordStore::list_metering(&s, 86_400).expect("list").len(),
            1
        );
    }
}

#[test]
fn the_ephemeral_store_states_mark_ephemeral_on_its_statement_not_its_tail() {
    for dropped in [false, true] {
        use busbar_contract::abi::mechanism::door::MARK_EPHEMERAL;
        use busbar_contract::abi::mechanism::rendering::read;
        use busbar_contract::abi::store::StoreTail;

        let rendering = if dropped {
            busbar_plugin_loader::dispatch::rendering_of_library(&support::artifact())
                .expect("the actual dropped library renders")
                .expect("the actual dropped library exports its door")
        } else {
            LinkedRow::of(busbar_store_memory::door)
                .expect("the memory store states its Statement")
                .statement
        };
        let st = read(&rendering).expect("the actual door rendering reads back");
        assert_eq!(st.marks & MARK_EPHEMERAL, MARK_EPHEMERAL);
        assert_eq!(st.kind_tail_size as usize, std::mem::size_of::<StoreTail>());
    }
}

#[test]
fn a_store_refresh_refuses_naming_the_restart_that_applies_settings() {
    for dropped in [false, true] {
        use busbar_contract::abi::mechanism::call::{OutHead, Outcome};
        use busbar_contract::abi::mechanism::lifecycle::{slot as life, RefreshIn};
        use busbar_contract::abi::sdk::door::{blank_in, blank_out};
        let s = support::open(dropped);
        let mut i: RefreshIn = blank_in();
        i.generation = 2;
        let mut f = busbar_plugin_loader::dispatch::Frame::new(i, blank_out::<OutHead>());
        let c = s.plugin().call(life::REFRESH, &mut f);
        assert_ne!(
            c.outcome,
            Outcome::Ready,
            "a store refresh claimed it applied new settings"
        );
        let text = String::from_utf8_lossy(&c.error.unwrap_or_default()).into_owned();
        assert!(
            text.contains("restart"),
            "the refusal names the restart: {text:?}"
        );
    }
}

#[test]
fn a_parented_upserted_record_reads_back_by_its_own_id() {
    let child = |id: &str, body: &[u8]| PlaneRecord {
        kind: "task".to_string(),
        id: id.to_string(),
        parent: Some("ctx-1".to_string()),
        seq: 0,
        ts: 0,
        disposition: PlaneDisposition::Active,
        body: body.to_vec(),
    };
    for dropped in [false, true] {
        let store = support::open(dropped);
        let s: &dyn RecordStore = &store;
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
}

#[test]
fn audit_rows_are_ordered_idempotent_and_fork_protected() {
    use busbar_contract::records::AuditRecord;
    for dropped in [false, true] {
        let store = support::open(dropped);
        let row = |seq| AuditRecord {
            seq,
            ts: 1,
            action: "key.create".into(),
            resource: "key:k".into(),
            outcome: "applied".into(),
            principal: "operator".into(),
            prev_hash: String::new(),
            hash: format!("hash-{seq}"),
        };
        let first = row(1);
        let second = row(2);
        store.append_audit(&second).unwrap();
        store.append_audit(&first).unwrap();
        store.append_audit(&first).unwrap();
        let mut fork = first.clone();
        fork.hash = "different".into();
        assert!(store.append_audit(&fork).is_err());
        assert_eq!(
            RecordStore::list_audit(&store).unwrap(),
            vec![first, second.clone()]
        );
        assert_eq!(
            RecordStore::list_audit_tail(&store, 1).unwrap(),
            vec![second]
        );
        assert!(RecordStore::list_audit_tail(&store, 0).unwrap().is_empty());
    }
}
