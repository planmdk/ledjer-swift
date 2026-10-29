#!/usr/bin/env bash
# Runs `swift <args>` in a Swift container (swift:6.1 + libsqlite3-dev), for
# machines without a Swift toolchain. Build products are cached in ./.build-linux.
# Usage: ./swift.sh test [--filter NAME]
set -euo pipefail
[ $# -ge 1 ] || { echo "usage: $0 <swift subcommand> [args], e.g. $0 test" >&2; exit 64; }
cd "$(dirname "$0")"
podman image exists ledjer-swift-dev || podman build -q -t ledjer-swift-dev -f Containerfile .
exec podman run --rm --init -v "$PWD":/pkg:Z -w /pkg ledjer-swift-dev \
  swift "$1" --scratch-path /pkg/.build-linux "${@:2}"
