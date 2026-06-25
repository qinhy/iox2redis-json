#!/usr/bin/env bash
set -euo pipefail

# Build a command frame as hex, then send it to the stdio demo server.
frame=$(cargo run --quiet --bin iox2redis-client -- ping)
printf '%s\n' "$frame" | cargo run --quiet --bin iox2redis-server -- /demo/service/
