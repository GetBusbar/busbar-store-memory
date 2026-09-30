<!-- fleet:header:begin (rendered by `cargo xtask fleet render` from GetBusbar/busbar's plugins.yaml; edit it there) -->
# busbar-store-memory

First-party signed kind:store plugin cdylib: the in-memory store, packaged as a droppable busbar plugin. Drop the signed tarball into plugins/.

| kind | alias | crate | busbar | license |
|---|---|---|---|---|
| `store` | `memory` | `busbar-store-memory-plugin` | 1.6.0 (pinned in `.busbar-ref`) | Apache-2.0 |

[![ci](https://github.com/GetBusbar/busbar-store-memory/actions/workflows/ci.yml/badge.svg?branch=dev)](https://github.com/GetBusbar/busbar-store-memory/actions/workflows/ci.yml)
<!-- fleet:header:end -->

## What it is for

`busbar-store-memory` is a `kind: store` busbar plugin.

## Config

Configured under the `memory` module name.

## Build

```bash
cargo build --release -p busbar-store-memory-plugin
```

## Tests

```bash
cargo test --workspace --locked
```

## License

Apache-2.0. See [LICENSE](LICENSE).
