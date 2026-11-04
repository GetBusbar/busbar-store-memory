// SPDX-License-Identifier: Apache-2.0
//! Actual memory store product behavior through the linked and dropped doors.
use busbar_contract::abi::store::OpId;
use busbar_contract::kinds::RecordBytes;
use busbar_contract::records::{
    CredentialMeta, CredentialSecret, MeteringDelta, ModelTokensDelta, PlaneDisposition,
    PlaneRecord, PlaneSelector, RecordStore, ScopeRef, SecretForm, UsageDelta, UsageLedger,
    VirtualKey,
};
use busbar_contract::store_calls::StoreCalls;
use busbar_plugin_loader::dispatch::kinds::store::Store;
use busbar_plugin_loader::dispatch::{
    self, Bind, ConnTable, DispatchConfig, Dispatcher, LinkedRow, NoSink,
};
use busbar_plugin_loader::store_v3::LoadedStore;
mod support;
use std::collections::BTreeMap;
use std::fmt::{Debug, Display};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

static NEXT: AtomicU64 = AtomicU64::new(1);
fn mint() -> OpId {
    OpId::from_parts(1, NEXT.fetch_add(1, Ordering::Relaxed))
}
const LIVE_REPLAY_DEADLINE: u64 = u64::MAX;
fn ans<T: Debug, E: Display>(r: Result<T, E>) -> String {
    match r {
        Ok(v) => format!("= {v:?}"),
        Err(e) => format!("! {e}"),
    }
}
fn text_of(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}
fn block<T>(rt: &tokio::runtime::Runtime, f: impl std::future::Future<Output = T>) -> T {
    rt.block_on(f)
}
fn plane_kind(key: &str) -> &'static str {
    match key {
        "server" => "engine_server",
        "tool" => "engine_tool",
        _ => panic!("unknown scope kind"),
    }
}
fn bind(d: &Arc<Dispatcher>) -> Bind {
    Bind {
        instance: Arc::from("memory-product"),
        max_inflight_cap: 64,
        sink: Arc::new(NoSink),
        dispatcher: d.adopter(),
        conns: ConnTable::NoNeeds,
    }
}

fn exercise(dropped: bool) -> BTreeMap<&'static str, String> {
    let d = Arc::new(Dispatcher::new(DispatchConfig::default()));
    let row = LinkedRow::of(busbar_store_memory::door).expect("actual linked Statement");
    let path = support::artifact();
    let p = if dropped {
        dispatch::load_dropped::<Store>(&path, &row.statement, bind(&d))
    } else {
        dispatch::load_linked::<Store>(&row, bind(&d))
    }
    .expect("actual store loads");
    let held = Arc::clone(&p.inner);
    let first_calls =
        || held.crossings.load(Ordering::SeqCst) - held.resumes.load(Ordering::SeqCst);
    let before_facts = first_calls();
    assert!(!p.name().is_empty());
    assert_eq!(
        first_calls() - before_facts,
        0,
        "facts do not cross the door"
    );
    let before_wrong_kind = first_calls();
    let wrong = if dropped {
        dispatch::load_dropped::<dispatch::kinds::hook::Hook>(&path, &row.statement, bind(&d))
    } else {
        dispatch::load_linked::<dispatch::kinds::hook::Hook>(&row, bind(&d))
    };
    assert!(matches!(
        wrong,
        Err(dispatch::LoadError::ManifestKind { .. })
    ));
    assert_eq!(
        first_calls() - before_wrong_kind,
        0,
        "wrong kind is refused before crossing"
    );
    let open_pin = 1 + u64::from(p.has_ready());
    let before_open = first_calls();
    let st = LoadedStore::open(p, Arc::clone(&d), b"{}", mint).expect("actual memory opens");
    assert_eq!(
        first_calls() - before_open,
        open_pin,
        "open plus its declared READY"
    );
    let ready_pin = u64::from(st.plugin().has_ready());
    let before_ready = first_calls();
    let ready = st.plugin().ready(&d, std::time::Duration::from_secs(30));
    assert!(ready.is_ok(), "actual door must become ready: {ready:?}");
    assert_eq!(first_calls() - before_ready, ready_pin, "explicit ready");
    assert!(
        st.facts().ephemeral,
        "memory cannot claim durable restart semantics"
    );
    let b: &dyn RecordStore = &st;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let mut answers = BTreeMap::new();
    answers.insert("open", format!("Ready {:?}", st.facts()));
    answers.insert(
        "refused as another kind",
        format!(
            "refused={}",
            matches!(wrong, Err(dispatch::LoadError::ManifestKind { .. }))
        ),
    );
    fn op(n: u64) -> OpId {
        OpId::from_parts(2, n)
    }

    fn key(id: &str, group: Option<&str>) -> VirtualKey {
        VirtualKey {
            id: id.to_string(),
            generation_hash: format!("h_{id}"),
            name: format!("key {id}"),
            enabled: true,
            group: group.map(str::to_string),
            created_at: 100,
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

    fn meter(key_id: &str, provider: &str, input: u64) -> MeteringDelta {
        MeteringDelta {
            key_id: key_id.into(),
            bucket: 86_400,
            model: "m".into(),
            provider: provider.into(),
            tokens_input: input,
            tokens_output: 1,
            tokens_cache_read: 0,
            tokens_cache_write: 0,
            requests: 1,
            billable_requests: 1,
            key_group_at_use: "g".into(),
            pricing_version: "v1".into(),
            priced_from_ms: 0,
            usage_units: Default::default(),
        }
    }

    fn secret(kind: &str, id: &str, key_id: &str, public_id: &str) -> CredentialSecret {
        CredentialSecret {
            meta: CredentialMeta {
                id: id.into(),
                key_id: key_id.into(),
                kind: kind.into(),
                slot: 0,
                public_id: public_id.into(),
                secret_form: SecretForm::Recoverable,
                created_at: 5,
                updated_at: 5,
                expires_at: None,
                revoked_at: None,
                revoke_reason: None,
                revision: 0,
            },
            secret: format!("v1:plain:s3cret-{id}"),
        }
    }

    fn plane(kind: &str, id: &str, parent: Option<&str>, seq: u64, body: &[u8]) -> PlaneRecord {
        PlaneRecord {
            kind: kind.into(),
            id: id.into(),
            parent: parent.map(str::to_string),
            seq,
            ts: 7,
            disposition: if seq == 2 {
                PlaneDisposition::Terminal
            } else {
                PlaneDisposition::Active
            },
            body: body.to_vec(),
        }
    }

    fn scoped_key() -> VirtualKey {
        VirtualKey {
            id: "vk_scopekinds".into(),
            generation_hash: "binding:vk_scopekinds:1".into(),
            name: "scope-kinds".into(),
            allowed_scopes: Some(vec![
                ScopeRef::pool("fast"),
                ScopeRef {
                    kind: plane_kind("server").into(),
                    value: "filesystem".into(),
                },
                ScopeRef {
                    kind: plane_kind("tool").into(),
                    value: "filesystem_read_file".into(),
                },
            ]),
            enabled: true,
            created_at: 1_700_000_000,
            ..Default::default()
        }
    }

    let keys = |r: Result<Vec<VirtualKey>, busbar_contract::records::RecordStoreError>| {
        ans(r.map(|mut ks| {
            ks.sort_by(|a, b| a.id.cmp(&b.id));
            ks.into_iter()
                .map(|k| (k.id, k.revision, k.deleted_at.is_some(), k.group))
                .collect::<Vec<_>>()
        }))
    };
    let before = first_calls();
    answers.insert("put grouped", ans(b.put_key(&key("a", Some("g")))));
    assert_eq!(first_calls() - before, 1, "put grouped");
    let before = first_calls();
    answers.insert("put plain", ans(b.put_key(&key("b", None))));
    assert_eq!(first_calls() - before, 1, "put plain");
    let before = first_calls();
    answers.insert(
        "get grouped",
        ans(b.get_key("a").map(|k| k.map(|k| (k.id, k.group)))),
    );
    assert_eq!(first_calls() - before, 2, "get grouped");
    let before = first_calls();
    answers.insert("get absent", ans(b.get_key("zz").map(|k| k.map(|k| k.id))));
    assert_eq!(first_calls() - before, 1, "get absent");
    let before = first_calls();
    answers.insert("key list", keys(b.list_keys()));
    assert_eq!(first_calls() - before, 2, "key list");
    let before = first_calls();
    answers.insert("delete plain", ans(b.delete_key("b")));
    assert_eq!(first_calls() - before, 1, "delete plain");
    let before = first_calls();
    answers.insert("delete absent", ans(b.delete_key("zz")));
    assert_eq!(first_calls() - before, 1, "delete absent");
    let before = first_calls();
    answers.insert("relive plain", ans(b.put_key(&key("b", None))));
    assert_eq!(first_calls() - before, 1, "relive plain");
    let before = first_calls();
    answers.insert("keys since 0", keys(b.list_keys_since(0)));
    assert_eq!(first_calls() - before, 2, "keys since 0");

    let (ub, w) = ("k", 60);
    let before = first_calls();
    answers.insert("usage untouched", ans(b.get_usage(ub, w)));
    assert_eq!(first_calls() - before, 2, "usage untouched");
    let before = first_calls();
    answers.insert("add usage", ans(b.add_usage(ub, w, &delta(2, 30))));
    assert_eq!(first_calls() - before, 1, "add usage");
    let before = first_calls();
    answers.insert("usage", ans(b.get_usage(ub, w)));
    assert_eq!(first_calls() - before, 2, "usage");
    let mut set = UsageLedger::default();
    set.apply_delta(&delta(9, 1));
    let before = first_calls();
    answers.insert("put usage", ans(b.put_usage("j", w * 2, &set)));
    assert_eq!(first_calls() - before, 1, "put usage");
    let before = first_calls();
    answers.insert("usage j", ans(b.get_usage("j", w * 2)));
    assert_eq!(first_calls() - before, 2, "usage j");
    let before = first_calls();
    answers.insert("usage again", ans(b.get_usage(ub, w)));
    assert_eq!(first_calls() - before, 2, "usage again");

    let before = first_calls();
    answers.insert("meter p 3", ans(b.add_metering(&meter("a", "p", 3))));
    assert_eq!(first_calls() - before, 1, "meter p 3");
    let before = first_calls();
    answers.insert("meter p 4", ans(b.add_metering(&meter("a", "p", 4))));
    assert_eq!(first_calls() - before, 1, "meter p 4");
    let before = first_calls();
    answers.insert("meter q 1", ans(b.add_metering(&meter("a", "q", 1))));
    assert_eq!(first_calls() - before, 1, "meter q 1");
    let before = first_calls();
    answers.insert(
        "metering",
        ans(b.list_metering(86_400).map(|mut v| {
            v.sort_by(|x, y| x.provider.cmp(&y.provider));
            v
        })),
    );
    assert_eq!(first_calls() - before, 2, "metering");

    let before = first_calls();
    answers.insert("put k1", ans(b.put_key(&key("k1", None))));
    assert_eq!(first_calls() - before, 1, "put k1");
    let before = first_calls();
    answers.insert(
        "cred c1",
        ans(b.put_credential(&secret("conformance", "c1", "k1", "pub1"))),
    );
    assert_eq!(first_calls() - before, 1, "cred c1");
    let before = first_calls();
    answers.insert(
        "cred c9 (live slot)",
        ans(b.put_credential(&secret("conformance", "c9", "k1", "pub9"))),
    );
    assert_eq!(first_calls() - before, 1, "cred c9 (live slot)");
    let before = first_calls();
    answers.insert(
        "key+cred k2",
        ans(b.put_key_with_credential(
                &key("k2", None),
                &secret("conformance", "c2", "k2", "pub2"),
            )),
    );
    assert_eq!(first_calls() - before, 1, "key+cred k2");
    let before = first_calls();
    answers.insert(
        "creds k1",
        ans(b
            .list_credentials("k1")
            .map(|v| v.into_iter().map(|c| c.id).collect::<Vec<_>>())),
    );
    assert_eq!(first_calls() - before, 2, "creds k1");
    let before = first_calls();
    answers.insert("revoke c1", ans(b.revoke_credential("c1", "rotated")));
    assert_eq!(first_calls() - before, 1, "revoke c1");
    let before = first_calls();
    answers.insert("revoke absent", ans(b.revoke_credential("zz", "x")));
    assert_eq!(first_calls() - before, 1, "revoke absent");
    let before = first_calls();
    answers.insert(
        "creds since",
        ans(b.list_credentials_since(0).map(|mut v| {
            v.sort_by(|x, y| x.meta.id.cmp(&y.meta.id));
            v.into_iter()
                .map(|c| (c.meta.id, c.meta.revoked_at.is_some()))
                .collect::<Vec<_>>()
        })),
    );
    assert_eq!(first_calls() - before, 2, "creds since");

    let before = first_calls();
    answers.insert("audit", ans(b.list_audit()));
    assert_eq!(first_calls() - before, 1, "audit");
    let before = first_calls();
    answers.insert("audit tail 1", ans(b.list_audit_tail(1)));
    assert_eq!(first_calls() - before, 1, "audit tail 1");
    let before = first_calls();
    answers.insert("deny x", ans(b.add_denylist("x", "why")));
    assert_eq!(first_calls() - before, 1, "deny x");
    let before = first_calls();
    answers.insert(
        "deny",
        ans(b.list_denylist().map(|mut v| {
            v.sort();
            v
        })),
    );
    assert_eq!(first_calls() - before, 2, "deny");

    let bodies = |r: busbar_contract::records::RecordStoreResult<Vec<Vec<u8>>>| {
        ans(r.map(|v| {
            let mut v: Vec<String> = v.iter().map(|b| text_of(b)).collect();
            v.sort();
            v
        }))
    };
    let before = first_calls();
    answers.insert(
        "upsert t1",
        ans(b.upsert_plane_record(plane("task", "t1", None, 0, b"{}").view())),
    );
    assert_eq!(first_calls() - before, 1, "upsert t1");
    let before = first_calls();
    answers.insert(
        "get t1",
        ans(b
            .get_plane_record("task", "t1")
            .map(|v| v.map(|v| text_of(&v)))),
    );
    assert_eq!(first_calls() - before, 1, "get t1");
    let before = first_calls();
    answers.insert(
        "append e1",
        ans(b.append_plane_record(plane("ev", "e", Some("t1"), 1, b"1").view())),
    );
    assert_eq!(first_calls() - before, 1, "append e1");
    let before = first_calls();
    answers.insert(
        "append e2",
        ans(b.append_plane_record(plane("ev", "e", Some("t1"), 2, b"2").view())),
    );
    assert_eq!(first_calls() - before, 1, "append e2");
    let before = first_calls();
    answers.insert(
        "append e2 fork",
        ans(b.append_plane_record(plane("ev", "e", Some("t1"), 2, b"x").view())),
    );
    assert_eq!(first_calls() - before, 1, "append e2 fork");
    let before = first_calls();
    answers.insert(
        "list ev t1",
        bodies(b.list_plane_records("ev", &PlaneSelector::Parent("t1".into()))),
    );
    assert_eq!(first_calls() - before, 1, "list ev t1");
    let before = first_calls();
    answers.insert(
        "parents ev",
        ans(b.list_plane_record_parents("ev").map(|mut v| {
            v.sort();
            v
        })),
    );
    assert_eq!(first_calls() - before, 2, "parents ev");
    let before = first_calls();
    answers.insert(
        "purge ev",
        ans(b.purge_plane_records("ev", 100, busbar_contract::records::Retention::Age)),
    );
    assert_eq!(first_calls() - before, 1, "purge ev");

    let before = first_calls();
    answers.insert(
        "append rt1",
        ans(b.append_plane_record(plane("rt", "r", Some("p"), 1, b"active").view())),
    );
    assert_eq!(first_calls() - before, 1, "append rt1");
    let before = first_calls();
    answers.insert(
        "append rt2",
        ans(b.append_plane_record(plane("rt", "r", Some("p"), 2, b"terminal").view())),
    );
    assert_eq!(first_calls() - before, 1, "append rt2");
    let before = first_calls();
    answers.insert(
        "purge rt terminal-only",
        ans(b.purge_plane_records("rt", 100, busbar_contract::records::Retention::TerminalOnly)),
    );
    assert_eq!(first_calls() - before, 1, "purge rt terminal-only");
    let before = first_calls();
    answers.insert(
        "list rt kept",
        bodies(b.list_plane_records("rt", &PlaneSelector::Parent("p".into()))),
    );
    assert_eq!(first_calls() - before, 1, "list rt kept");
    let before = first_calls();
    answers.insert(
        "purge rt age",
        ans(b.purge_plane_records("rt", 100, busbar_contract::records::Retention::Age)),
    );
    assert_eq!(first_calls() - before, 1, "purge rt age");
    let before = first_calls();
    answers.insert(
        "list rt aged",
        bodies(b.list_plane_records("rt", &PlaneSelector::Parent("p".into()))),
    );
    assert_eq!(first_calls() - before, 1, "list rt aged");
    let before = first_calls();
    answers.insert("delete t1", ans(b.delete_plane_record("task", "t1")));
    assert_eq!(first_calls() - before, 1, "delete t1");
    let before = first_calls();
    answers.insert(
        "get t1 after",
        ans(b
            .get_plane_record("task", "t1")
            .map(|v| v.map(|v| text_of(&v)))),
    );
    assert_eq!(first_calls() - before, 1, "get t1 after");

    let before = first_calls();
    answers.insert(
        "upsert c1 under p",
        ans(b.upsert_plane_record(plane("ctask", "c1", Some("p"), 0, b"c1").view())),
    );
    assert_eq!(first_calls() - before, 1, "upsert c1 under p");
    let before = first_calls();
    answers.insert(
        "upsert c2 under p",
        ans(b.upsert_plane_record(plane("ctask", "c2", Some("p"), 0, b"c2").view())),
    );
    assert_eq!(first_calls() - before, 1, "upsert c2 under p");
    let before = first_calls();
    answers.insert(
        "get c1",
        ans(b
            .get_plane_record("ctask", "c1")
            .map(|v| v.map(|v| text_of(&v)))),
    );
    assert_eq!(first_calls() - before, 1, "get c1");
    let before = first_calls();
    answers.insert(
        "list ctask p",
        bodies(b.list_plane_records("ctask", &PlaneSelector::Parent("p".into()))),
    );
    assert_eq!(first_calls() - before, 1, "list ctask p");
    let before = first_calls();
    answers.insert("delete c1", ans(b.delete_plane_record("ctask", "c1")));
    assert_eq!(first_calls() - before, 1, "delete c1");
    let before = first_calls();
    answers.insert(
        "get c1 after",
        ans(b
            .get_plane_record("ctask", "c1")
            .map(|v| v.map(|v| text_of(&v)))),
    );
    assert_eq!(first_calls() - before, 1, "get c1 after");
    let before = first_calls();
    answers.insert(
        "get c2",
        ans(b
            .get_plane_record("ctask", "c2")
            .map(|v| v.map(|v| text_of(&v)))),
    );
    assert_eq!(first_calls() - before, 1, "get c2");
    let before = first_calls();
    answers.insert("token live", ans(b.plane_token_live("tok", "x", 100, 1)));
    assert_eq!(first_calls() - before, 1, "token live");
    let before = first_calls();
    answers.insert(
        "redeem",
        ans(b.redeem_plane_token("tok", "x", LIVE_REPLAY_DEADLINE, 1)),
    );
    assert_eq!(first_calls() - before, 1, "redeem");
    let before = first_calls();
    answers.insert(
        "redeem again",
        ans(b.redeem_plane_token("tok", "x", LIVE_REPLAY_DEADLINE, 1)),
    );
    assert_eq!(first_calls() - before, 1, "redeem again");

    let batch = [(ub, w, delta(1, 5)), (ub, w, delta(1, 5))];
    let before = first_calls();
    answers.insert(
        "usage batch",
        ans(block(&rt, st.add_usage_batch(op(8), &batch))),
    );
    assert_eq!(first_calls() - before, 1, "usage batch");
    let before = first_calls();
    answers.insert(
        "usage batch replay",
        ans(block(&rt, st.add_usage_batch(op(8), &batch))),
    );
    assert_eq!(first_calls() - before, 1, "usage batch replay");
    let before = first_calls();
    answers.insert(
        "usage batch conflict",
        ans(block(&rt, st.add_usage_batch(op(8), &batch[..1]))),
    );
    assert_eq!(first_calls() - before, 1, "usage batch conflict");
    let before = first_calls();
    answers.insert(
        "metering batch",
        ans(block(
            &rt,
            st.add_metering_batch(op(9), &[meter("a", "p", 1)]),
        )),
    );
    assert_eq!(first_calls() - before, 1, "metering batch");
    let rec = |v: &[u8]| RecordBytes::new(v.to_vec()).expect("a record");
    let shown = |v: Option<RecordBytes>| v.map(|v| text_of(v.as_slice()));
    let before = first_calls();
    answers.insert(
        "record put a/1",
        ans(block(&rt, st.record_put("sch", b"a/1", &rec(b"one")))),
    );
    assert_eq!(first_calls() - before, 1, "record put a/1");
    let before = first_calls();
    answers.insert(
        "record put a/2",
        ans(block(&rt, st.record_put("sch", b"a/2", &rec(b"two")))),
    );
    assert_eq!(first_calls() - before, 1, "record put a/2");
    let before = first_calls();
    answers.insert(
        "record get",
        ans(block(&rt, StoreCalls::record_get(&st, "sch", b"a/1")).map(shown)),
    );
    assert_eq!(first_calls() - before, 1, "record get");
    let before = first_calls();
    answers.insert(
        "record get absent",
        ans(block(&rt, StoreCalls::record_get(&st, "sch", b"zz")).map(shown)),
    );
    assert_eq!(first_calls() - before, 1, "record get absent");
    let scan = || {
        ans(block(&rt, st.record_scan("sch", b"a/", 5)).map(|mut v| {
            v.sort_by(|x, y| x.0.cmp(&y.0));
            v.into_iter()
                .map(|(k, v)| (text_of(&k), text_of(v.as_slice())))
                .collect::<Vec<_>>()
        }))
    };
    let before = first_calls();
    answers.insert("record scan", (scan)());
    assert_eq!(first_calls() - before, 1, "record scan");

    let before = first_calls();
    answers.insert(
        "record put a/1 empty",
        ans(block(&rt, st.record_put("sch", b"a/1", &rec(b"")))),
    );
    assert_eq!(first_calls() - before, 1, "record put a/1 empty");
    let before = first_calls();
    answers.insert(
        "record get emptied",
        ans(block(&rt, StoreCalls::record_get(&st, "sch", b"a/1")).map(shown)),
    );
    assert_eq!(first_calls() - before, 1, "record get emptied");
    let before = first_calls();
    answers.insert("record scan emptied", (scan)());
    assert_eq!(first_calls() - before, 1, "record scan emptied");
    let before = first_calls();
    answers.insert(
        "typed upsert",
        ans(block(
            &rt,
            StoreCalls::upsert_plane_record(&st, plane("task", "t2", None, 0, b"b").view()),
        )),
    );
    assert_eq!(first_calls() - before, 1, "typed upsert");
    let before = first_calls();
    answers.insert(
        "typed get",
        ans(block(&rt, StoreCalls::get_plane_record(&st, "task", "t2"))
            .map(|v| v.map(|v| text_of(&v)))),
    );
    assert_eq!(first_calls() - before, 1, "typed get");
    let before = first_calls();
    answers.insert(
        "typed append",
        ans(block(
            &rt,
            StoreCalls::append_plane_record(
                &st,
                op(13),
                plane("ev", "f", Some("t2"), 1, b"1").view(),
            ),
        )),
    );
    assert_eq!(first_calls() - before, 1, "typed append");
    let before = first_calls();
    answers.insert(
        "typed list",
        bodies(
            block(
                &rt,
                StoreCalls::list_plane_records(&st, "ev", &PlaneSelector::Parent("t2".into())),
            )
            .map_err(|e| busbar_contract::records::RecordStoreError(e.to_string())),
        ),
    );
    assert_eq!(first_calls() - before, 1, "typed list");
    let before = first_calls();
    answers.insert(
        "typed delete",
        ans(block(
            &rt,
            StoreCalls::delete_plane_record(&st, "task", "t2"),
        )),
    );
    assert_eq!(first_calls() - before, 1, "typed delete");
    let before = first_calls();
    answers.insert(
        "typed redeem",
        ans(block(
            &rt,
            StoreCalls::redeem_plane_token(&st, "tok", "y", LIVE_REPLAY_DEADLINE, 1),
        )),
    );
    assert_eq!(first_calls() - before, 1, "typed redeem");
    let before = first_calls();
    answers.insert(
        "typed live",
        ans(block(
            &rt,
            StoreCalls::plane_token_live(&st, "tok", "y", 100, 1),
        )),
    );
    assert_eq!(first_calls() - before, 1, "typed live");
    let before = first_calls();
    answers.insert("usage at the end", ans(b.get_usage(ub, w)));
    assert_eq!(first_calls() - before, 2, "usage at the end");

    let before = first_calls();
    answers.insert("usage purge", ans(block(&rt, st.purge_usage_before(w * 2))));
    assert_eq!(first_calls() - before, 1, "usage purge");
    let before = first_calls();
    answers.insert("usage purged", ans(b.get_usage(ub, w)));
    assert_eq!(first_calls() - before, 2, "usage purged");
    let before = first_calls();
    answers.insert("usage kept", ans(b.get_usage("j", w * 2)));
    assert_eq!(first_calls() - before, 2, "usage kept");

    let sk = scoped_key();
    let before = first_calls();
    answers.insert("scope-kind put", ans(b.put_key(&sk)));
    assert_eq!(first_calls() - before, 1, "scope-kind put");
    let before = first_calls();
    let (answer, back) = match b.get_key(&sk.id) {
        Ok(Some(k)) => {
            let want = VirtualKey {
                revision: k.revision,
                ..scoped_key()
            };
            let same = serde_json::to_vec(&k).ok() == serde_json::to_vec(&want).ok();
            let line = format!(
                "identical={same} plane_grants={}",
                k.scope_allowed(plane_kind("tool"), "filesystem_read_file")
                    && k.scope_allowed(plane_kind("server"), "filesystem")
                    && !k.scope_allowed("pool", "filesystem_read_file")
            );
            (line, Some(k))
        }
        other => (ans(other.map(|k| k.map(|k| k.id))), None),
    };
    answers.insert("scope-kind get", answer);
    assert_eq!(first_calls() - before, 2, "scope-kind get");
    let before = first_calls();
    answers.insert(
        "scope-kind list",
        match b.list_keys() {
            Ok(ks) => {
                let listed = ks.iter().find(|k| k.id == sk.id);
                format!(
                    "identical={}",
                    listed.is_some()
                        && listed.and_then(|k| serde_json::to_vec(k).ok())
                            == back.as_ref().and_then(|k| serde_json::to_vec(k).ok())
                )
            }
            Err(e) => format!("! {e}"),
        },
    );
    assert_eq!(first_calls() - before, 2, "scope-kind list");

    let at = |label: &str| answers.get(label).expect(label).as_str();
    let tail = |label: &str| at(label).split_once(' ').map_or("", |x| x.1);
    for (label, answer) in &answers {
        assert!(!answer.contains("faulted"), "{label}: {answer}");
    }
    assert!(at("open").starts_with("Ready "), "open: {}", at("open"));
    assert_eq!(at("refused as another kind"), "refused=true");
    for label in [
        "put grouped",
        "put plain",
        "delete plain",
        "add usage",
        "put usage",
        "meter p 3",
        "cred c1",
        "key+cred k2",
        "revoke c1",
        "deny x",
        "upsert t1",
        "append e1",
        "append e2",
        "delete t1",
        "usage batch",
        "metering batch",
        "record put a/1",
        "record put a/1 empty",
        "typed upsert",
        "typed delete",
        "scope-kind put",
    ] {
        assert!(at(label).starts_with("= "), "{label}: {}", at(label));
    }
    assert_eq!(
        at("get grouped"),
        format!("= Some(({:?}, Some({:?})))", "a", "g"),
        "a key put is the key got"
    );
    for label in [
        "get absent",
        "get t1 after",
        "record get absent",
        "record get emptied",
    ] {
        assert_eq!(at(label), "= None", "{label}");
    }
    for label in ["delete absent", "relive plain", "append e2 fork"] {
        assert!(
            at(label).starts_with("! "),
            "refused: {label}: {}",
            at(label)
        );
    }
    assert_eq!(
        at("list ev t1"),
        "= [\"1\", \"2\"]",
        "a refused sequence fork leaves both original events intact"
    );
    assert!(
        at("cred c9 (live slot)").starts_with("! "),
        "a live slot is not taken twice"
    );
    assert_eq!(at("usage batch replay"), at("usage batch"));
    assert_eq!(at("usage batch conflict"), "! STORE_OPID_CONFLICT");
    for label in ["upsert c1 under p", "upsert c2 under p", "delete c1"] {
        assert!(at(label).starts_with("= "), "{label}: {}", at(label));
    }
    assert_eq!(
        at("get c1"),
        "= Some(\"c1\")",
        "a parented upsert reads back by its own id"
    );
    assert_eq!(
        at("list ctask p"),
        "= [\"c1\", \"c2\"]",
        "two ids under one parent are two records, both listed under it"
    );
    assert_eq!(at("get c1 after"), "= None", "deleted by its own id");
    assert_eq!(at("get c2"), "= Some(\"c2\")", "deleting c1 leaves c2");

    for label in ["append rt1", "append rt2"] {
        assert!(at(label).starts_with("= "), "{label}: {}", at(label));
    }
    assert_eq!(
        at("purge rt terminal-only"),
        "= 1",
        "a terminal-only purge drops the old TERMINAL row and only it"
    );
    assert_eq!(
        at("list rt kept"),
        r#"= ["active"]"#,
        "a terminal-only purge keeps an ACTIVE row however old"
    );
    assert_eq!(
        at("purge rt age"),
        "= 1",
        "an age purge drops the old ACTIVE row"
    );
    assert_eq!(
        at("list rt aged"),
        "= []",
        "nothing old survives an age purge"
    );
    assert_eq!(at("redeem"), "= true");
    assert_eq!(at("redeem again"), "= false", "a token is redeemed once");
    assert_eq!(at("record get"), "= Some(\"one\")");
    assert_eq!(
        at("record scan"),
        "= [(\"a/1\", \"one\"), (\"a/2\", \"two\")]"
    );
    assert_eq!(
        at("record scan emptied"),
        "= [(\"a/2\", \"two\")]",
        "an empty put removes the row: the scan never returns it"
    );

    assert!(
        tail("usage again").contains("UsageLedger { requests: 2,"),
        "{}",
        at("usage again")
    );
    assert!(
        tail("usage at the end").contains("UsageLedger { requests: 4,"),
        "{}",
        at("usage at the end")
    );
    assert_eq!(at("usage purge"), "= 1", "the one window before the cutoff");
    assert!(
        tail("usage purged").contains("UsageLedger { requests: 0,"),
        "{}",
        at("usage purged")
    );
    assert!(
        tail("usage kept").contains("UsageLedger { requests: 9,"),
        "{}",
        at("usage kept")
    );
    assert_eq!(at("scope-kind get"), "identical=true plane_grants=true");
    assert_eq!(at("scope-kind list"), "identical=true");
    answers
}
#[test]
fn linked_and_actual_dropped_memory_preserve_the_store_contract() {
    assert_eq!(exercise(false), exercise(true));
}
#[test]
fn the_statement_declares_exactly_its_secret_fields() {
    let stated = busbar_contract::abi::mechanism::rendering::read(
        &dispatch::rendering_of(busbar_store_memory::door).expect("the door renders"),
    )
    .expect("the rendering reads back");
    assert_eq!(stated.secret_refs, ["token".to_string()]);
}

#[test]
fn the_declared_needs_are_the_statements_and_the_requested_profile_is_used() {
    let row = LinkedRow::of_macro(busbar_store_memory::MACRO_DOOR).expect("actual Statement");
    let statement = busbar_contract::abi::mechanism::rendering::read(&row.statement).unwrap();
    let declares: serde_json::Value =
        serde_json::from_str(include_str!("../../store-memory/declares.json")).unwrap();
    let mut stated: Vec<_> = statement
        .needs
        .iter()
        .map(|n| n.transport.as_str())
        .collect();
    stated.sort();
    stated.dedup();
    let mut declared: Vec<_> = match declares.get("needs") {
        None => Vec::new(),
        Some(needs) => needs
            .as_array()
            .expect("declares.json needs must be a list when present")
            .iter()
            .map(|v| {
                v.as_str()
                    .expect("every declared need must be a transport scheme")
            })
            .collect(),
    };
    declared.sort();
    declared.dedup();
    assert_eq!(stated, declared);
    if std::env::var_os("BUSBAR_EXPECT_RELEASE").is_some() {
        #[cfg(debug_assertions)]
        panic!("release profile requested");
    }
}
