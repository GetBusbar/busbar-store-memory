<!-- fleet:header:begin (rendered by `busbar-release plugin heal` from GetBusbar/busbar-release template/ and busbar's plugins.yaml; edit it there) -->
# busbar-store-memory

First-party signed kind:store plugin cdylib: the in-memory store, packaged as a droppable busbar plugin. Drop the signed tarball into plugins/.

| kind | alias | crate | busbar | license |
|---|---|---|---|---|
| `store` | `memory` | `busbar-store-memory-plugin` | 1.6.0 (pinned in `.busbar-ref`) | Apache-2.0 |

[![ci](https://github.com/GetBusbar/busbar-store-memory/actions/workflows/ci.yml/badge.svg?branch=dev)](https://github.com/GetBusbar/busbar-store-memory/actions/workflows/ci.yml)
<!-- fleet:header:end -->

## What it is for

`busbar-store-memory` is a `kind: store` busbar plugin.

Everything it holds is in RAM and lost on restart, and its RAM grows with the record count (the kernel-held records map is never aged out); long-lived deployments use a durable store.

## Config

Configured under the `memory` module name.

| setting | type | default | meaning |
|---|---|---|---|
| `token` | any | absent | accepted and ignored (a 1.5.5 config key; it may hold a secret reference) |

The store takes no other settings: an empty value, `null` and `{}` all mean none, and any other
key is refused (``unknown setting `k` (accepted: token)``).

## Versioning

This plugin is versioned independently of busbar: **v1.6.0** here, and it requires **busbar
1.6.0+**, the release whose store interface it implements. Nothing is persisted, so there is no
database to upgrade.

## Build

```bash
cargo build --release -p busbar-store-memory-plugin
```

## Tests

```bash
cargo test --workspace --locked
```

The store needs no server and no environment variable, so nothing is skipped locally or in CI: a
failing test fails the build in both.

## License

Apache-2.0. See [LICENSE](LICENSE).

Usage windows at least 31 days old are purged; window `0` is the all-time ledger and is retained. The host can request a strict cutoff through the usage-purge slot. Billing metering rows are retained for the life of this store; operators own billing-history retention. This backend is ephemeral: restarting or discarding it loses all rows. Use a durable store when history must survive a restart.
