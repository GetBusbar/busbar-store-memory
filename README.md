<!-- fleet:header:begin (rendered by `busbar-release plugin heal` from GetBusbar/busbar-release template/ and busbar's plugins.yaml; edit it there) -->
# busbar-store-memory

First-party signed kind:store plugin cdylib: the in-memory store, packaged as a droppable busbar plugin. Drop the signed tarball into plugins/.

| kind | alias | crate | busbar | license |
|---|---|---|---|---|
| `store` | `memory` | `busbar-store-memory-plugin` | 1.6.0 (pinned in `.busbar-ref`) | Apache-2.0 |

<!-- fleet:header:end -->

## What it is for

`busbar-store-memory` is a `kind: store` busbar plugin.

Everything it holds is in RAM and lost on restart. The plugin implements coherent reads,
ordered generation-checked pages, atomic conditional commits and an authoritative clock.
Values remain opaque and keep wiping ownership. Core owns credentials, accounting, claims,
collection relationships and retention; this backend does not interpret their schemas.

Each open creates fresh nonzero dataset, atomic-domain and clock identities. Receipt replay,
execution fencing and namespace generations are part of the native primitive contract.
Pre-open inspection uses the same exclusive state lock as native operations. A fresh process
starts empty; this backend has no durable historical generation to recover after process exit.
An existing nonempty state without valid current dataset control is refused, never reported empty.
No retained legacy generation is exposed by this implementation; native generation-upgrade
operations return `MissingCapability`. Prepared opening rechecks the original state and exact
opaque admission marker under the same lock.

## Config

Configured under `memory` or its canonical name `busbar-store-memory`.

Blank settings, `null` and `{}` use host resource admission. The optional positive
`input_budget_bytes` setting supplies an explicit native request allocation budget;
unknown settings are refused. No secret references are required.

Ordinary startup uses the same door's configuration migration to remove the formerly ignored
`store.settings.token` before secret resolution. The conversion never resolves or displays its
value; repeating it changes nothing.

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

The normal door now exposes the neutral primitive Store contract. Domain operations run in
core over the shared persistence owners. There is no typed Store backend or alternate runtime.

## License

Apache-2.0. See [LICENSE](LICENSE).
