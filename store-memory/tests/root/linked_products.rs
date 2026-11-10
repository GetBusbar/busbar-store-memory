// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2026 Busbar Inc and contributors
use crate::root::test_plugins as plugins;
use std::sync::Arc;
use std::time::{Duration, Instant};
fn gov_with_store(store: &str) -> busbar_kernel::config::StoreCfg {
    busbar_kernel::config::StoreCfg {
        module: store.into(),
        settings: Default::default(),
    }
}
fn plugins_cfg(dir: &std::path::Path, enabled: bool) -> busbar_kernel::config::PluginsCfg {
    busbar_kernel::config::PluginsCfg {
        enabled,
        dir: dir.to_string_lossy().into_owned(),
        ..Default::default()
    }
}
#[test]
fn a_store_row_reads_alike_by_door_function_or_by_macro_door() {
    use busbar_kernel::preflight::{LinkedStore, LinkedStoreDoor, StoreRow};
    const BY_FN: LinkedStore = StoreRow::<LinkedStore>((
        busbar_store_memory::linked::STORE.0,
        busbar_store_memory::linked::STORE.1,
        busbar_store_memory::door,
        busbar_store_memory::linked::STORE.3,
    ))
    .linked();
    const BY_MACRO: LinkedStore =
        StoreRow::<LinkedStoreDoor>(busbar_store_memory::linked::STORE).linked();
    assert_eq!(
        (BY_FN.0, BY_FN.1, BY_FN.3),
        (BY_MACRO.0, BY_MACRO.1, BY_MACRO.3)
    );
    assert_eq!(BY_FN.2 as usize, BY_MACRO.2 as usize, "one door function");
    assert_eq!(
        BY_FN.2 as usize,
        busbar_store_memory::door as *const () as usize
    );
}
#[test]
fn the_built_in_store_is_a_linked_row_of_the_store_axis() {
    const CHILD: &str = "BUSBAR_CENTRAL2_LINKED_CHILD";
    const WITNESS: &str = "actual linked product assertions completed";
    if std::env::var_os(CHILD).is_none() {
        struct Child(std::process::Child);
        impl Drop for Child {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let name = std::thread::current()
            .name()
            .expect("the test runner names its test thread")
            .to_string();
        let mut child = Child(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", &name, "--nocapture"])
                .env(CHILD, "1")
                .stdout(std::process::Stdio::piped())
                .spawn()
                .expect("start the isolated actual-root test"),
        );
        let until = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                use std::io::Read;
                let mut output = String::new();
                child
                    .0
                    .stdout
                    .take()
                    .unwrap()
                    .read_to_string(&mut output)
                    .unwrap();
                assert!(
                    status.success(),
                    "the actual-root usage witness failed: {status}: {output}"
                );
                assert!(
                    output.contains(WITNESS),
                    "the exact child did not complete its assertions: {output}"
                );
                return;
            }
            assert!(
                Instant::now() < until,
                "the actual-root usage witness timed out"
            );
            std::thread::park_timeout(Duration::from_millis(5));
        }
    }
    crate::dist_root_install::install();
    let store = gov_with_store(busbar_store_memory::linked::STORE.0);
    let name = store.module.as_str();
    let reg = busbar_kernel::preflight::plugins_preflight(
        Some(&store),
        None,
        &Default::default(),
        &Default::default(),
        &busbar_kernel::config::PluginsCfg::default(),
        &Default::default(),
    )
    .expect("the configured store resolves on the axis");
    let row = reg.resolve(name).expect("the configured store is a row");
    assert_eq!(
        (row.kind.as_str(), row.ephemeral),
        (busbar_contract::abi::mechanism::kind::STORE, true)
    );
    let stores = reg
        .linked()
        .into_iter()
        .filter(|p| p.kind == busbar_contract::abi::mechanism::kind::STORE);
    assert_eq!((stores.count(), reg.loadable().len()), (1, 0));
    reg.store_door(name)
        .expect("the row states its door to boot");
    let canonical = row.name.clone();
    assert_ne!(canonical, name, "the row's canonical name is not its key");
    assert_eq!(row.key, name, "the row prints its key");
    reg.store_door(&canonical)
        .expect("the canonical name opens the same store");
    let by_canonical = busbar_kernel::preflight::plugins_preflight(
        Some(&gov_with_store(&canonical)),
        None,
        &Default::default(),
        &Default::default(),
        &busbar_kernel::config::PluginsCfg::default(),
        &Default::default(),
    )
    .expect("`store.module: <canonical>` resolves as the key does");
    assert!(by_canonical.resolve(&canonical).expect("resolves").linked);
    println!("{WITNESS}");
}
#[test]
fn a_linked_rows_refusals_print_its_key() {
    const CHILD: &str = "BUSBAR_CENTRAL2_LINKED_CHILD";
    const WITNESS: &str = "actual linked product assertions completed";
    if std::env::var_os(CHILD).is_none() {
        struct Child(std::process::Child);
        impl Drop for Child {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let name = std::thread::current()
            .name()
            .expect("the test runner names its test thread")
            .to_string();
        let mut child = Child(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", &name, "--nocapture"])
                .env(CHILD, "1")
                .stdout(std::process::Stdio::piped())
                .spawn()
                .expect("start the isolated actual-root test"),
        );
        let until = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                use std::io::Read;
                let mut output = String::new();
                child
                    .0
                    .stdout
                    .take()
                    .unwrap()
                    .read_to_string(&mut output)
                    .unwrap();
                assert!(
                    status.success(),
                    "the actual-root usage witness failed: {status}: {output}"
                );
                assert!(
                    output.contains(WITNESS),
                    "the exact child did not complete its assertions: {output}"
                );
                return;
            }
            assert!(
                Instant::now() < until,
                "the actual-root usage witness timed out"
            );
            std::thread::park_timeout(Duration::from_millis(5));
        }
    }
    crate::dist_root_install::install();
    let store = gov_with_store(busbar_store_memory::linked::STORE.0);
    let key = store.module.clone();
    let dir = plugins::scratch("printed-key");
    let plugins = plugins_cfg(&dir, true);
    let reg = busbar_kernel::preflight::plugins_preflight(
        Some(&store),
        None,
        &Default::default(),
        &Default::default(),
        &plugins,
        &Default::default(),
    )
    .expect("the store resolves");
    let canonical = reg.resolve(&key).expect("the row").name;
    for word in [key.as_str(), canonical.as_str()] {
        let hooks: std::collections::HashMap<String, busbar_kernel::config::HookCfg> =
            serde_json::from_value(serde_json::json!({ "h": { "module": word, "kind": "tap" } }))
                .expect("hooks");
        let refused = busbar_kernel::preflight::plugins_preflight(
            Some(&store),
            None,
            &Default::default(),
            &hooks,
            &plugins,
            &Default::default(),
        )
        .expect_err("a store is not a hook");
        assert!(
            refused.contains(&format!(
                "resolves to plugin '{key}' of kind 'store', not a `hook` plugin"
            )),
            "{word}: {refused}"
        );
        let providers: busbar_kernel::config::IdentityProviders =
            serde_json::from_value(serde_json::json!({ "x": { "module": word } })).expect("idps");
        let refused = busbar_kernel::preflight::plugins_preflight(
            Some(&store),
            None,
            &providers,
            &Default::default(),
            &plugins,
            &Default::default(),
        )
        .expect_err("a store is not an auth plugin");
        assert!(
            refused.contains(&format!(
                "identity-providers.x.module: '{word}' resolves to plugin '{key}' of kind 'store'"
            )),
            "{word}: {refused}"
        );
        let dialect = busbar_kernel::plane_driver::dispatch::provider_dialects()
            .into_iter()
            .next()
            .expect("actual provider dialect");
        let provider_defs = serde_json::from_value(serde_json::json!({
            "acme": {"protocol": dialect, "base_url": "https://api.example.com"}
        }))
        .expect("the actual provider catalog entry parses");
        let deploy = busbar_kernel::config::deploy_from_deserializer(serde_json::json!({
            "providers": {"acme": {"protocol": dialect,
                "base_url": "https://api.example.com", "api_key": "none"}},
            "models": {}, "auth": {"chain": [], "admin_auth": []},
            "store": {"module": busbar_store_memory::linked::STORE.0},
        }))
        .expect("actual root input parses");
        let mut cfg = busbar_kernel::config::resolve(&deploy, &provider_defs)
            .expect("actual root input resolves");
        let refusal =
            |cfg: &busbar_kernel::config::RootCfg| match busbar_kernel::config_validate::check(
                cfg,
                busbar_kernel::config_validate::Check {
                    plugins: &plugins,
                    unset_env_vars: &[],
                    root: &busbar_kernel::config_validate::no_root_rules,
                    fetch: None,
                    resolve_builtins: false,
                },
            ) {
                Err(busbar_kernel::config_validate::Refusal::Plugins(message)) => message,
                other => panic!("the genuine registry policy must refuse the kind: {other:?}"),
            };
        cfg.secrets.insert(word.to_string(), Default::default());
        assert_eq!(
            refusal(&cfg),
            format!(
                "secrets.{word}: plugin '{key}' has kind 'store', not 'secret'; only a kind: secret plugin can back a `secrets:` block entry"
            )
        );
        cfg.secrets.clear();
        cfg.providers.get_mut("acme").unwrap().api_key = busbar_kernel::config::SecretRef::Source(
            busbar_contract::secret_ref::SourceReference {
                module: word.to_string(),
                settings: Default::default(),
            },
        );
        let refused = refusal(&cfg);
        assert!(
            refused.contains(&format!(
                "references secret module '{word}', but plugin '{key}' has kind 'store'"
            )),
            "{word}: {refused}"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
    println!("{WITNESS}");
}
