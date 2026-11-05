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

Everything it holds is in RAM and lost on restart. RAM grows with keys and tombstones,
credentials and revoked metadata, denylist entries, audit, billing metering, plane records,
and kernel-held records. Those rows have no automatic age sweep; the host explicitly removes
plane and kernel-held records. Long-lived deployments use a durable store.

Usage retention runs after every 256 `add_usage` cells (including batch cells), removing nonzero
windows at least 31 days old. `put_usage` and reads do not trigger that sweep; an idle store can
retain older windows until another sweep or the host's explicit usage-purge request. Window `0`
is the all-time ledger and remains retained. Billing rows remain for this store's life;
operators own billing-history retention. Expired token claims are swept every 256 redemptions;
applied-op receipts are swept when another new operation is applied, after their 24-hour
retention. Idle stores can retain expired entries until that activity resumes.

## Config

Configured under the `memory` module name.

The store takes no settings or secret references: an empty value, `null` and `{}` all mean none,
and every setting key is refused (``unknown setting `k` (accepted: none)``).
When upgrading older configuration, `busbar --migrate-config` calls this plugin's one-shot
migration to remove the formerly ignored `store.settings.token` before secret resolution.
This applies to both `memory` and `busbar-store-memory`. The migration reports the removal
without resolving or displaying the discarded value; repeating it makes no further change.

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
cargo build -p busbar-store-memory-plugin --lib --locked
cargo test --workspace --locked
```

The store needs no server and no environment variable, so nothing is skipped locally or in CI: a
failing test fails the build in both.

## Differences from 1.5.5

Credential removal now wipes secret buffers before freeing them, including key deletion and
tombstone cascades. Credential retry comparisons avoid allocating an extra secret copy. These
changes harden process-memory hygiene without changing valid credential operations.

## License

Apache-2.0. See [LICENSE](LICENSE).
