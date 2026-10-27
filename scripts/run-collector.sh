#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
bash ./scripts/cargo-wsl.sh build --release --locked --bin collector
if [[ -x work/toolchain/cargo/bin/cargo ]]; then
    binary=/var/tmp/voluntas-challenge/target/release/collector
else
    binary="${CARGO_TARGET_DIR:-target}/release/collector"
fi
data_dir="${COLLECTOR_DATA_DIR:-/var/tmp/voluntas-challenge/data}"
mkdir -p -- "$data_dir"
data_dir="$(cd -- "$data_dir" && pwd -P)"
if [[ "$data_dir" == /mnt/[a-z]/* ]]; then
    echo 'Warning: recordings are on a Windows mount; use native Linux storage for latency tests.' >&2
fi
name="capture-$(date -u +%Y%m%dT%H%M%S)-$$"
echo "Live archive: $data_dir/$name.sbe" >&2
echo "Live coverage report: $data_dir/$name.coverage.json" >&2
exec "$binary" --output "$data_dir/$name.sbe" --coverage-report "$data_dir/$name.coverage.json" --no-stdout "$@"
