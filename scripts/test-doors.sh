#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Build the actual package immediately before loading its image. Missing images fail in Rust.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo build -p busbar-store-memory-plugin --lib "$@"
cargo test --workspace "$@"
