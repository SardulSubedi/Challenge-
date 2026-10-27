#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
if [[ -x work/toolchain/cargo/bin/cargo ]]; then
    export RUSTUP_HOME="$PWD/work/toolchain/rustup"
    export PATH="$PWD/work/toolchain/cargo/bin:$PATH"
    # Native Linux storage avoids slow dependency unpacking on /mnt/c.
    export CARGO_HOME="/var/tmp/voluntas-challenge/cargo"
    export CARGO_TARGET_DIR="/var/tmp/voluntas-challenge/target"
fi
exec cargo "$@"
