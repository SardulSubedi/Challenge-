#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
mkdir -p data
exec ./target/release/collector --output "data/capture-$(date -u +%Y%m%dT%H%M%S)-$$.sbe" --no-stdout "$@"
