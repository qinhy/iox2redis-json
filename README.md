# iox2redis-json

A small Redis-like JSON key-value store rewritten in **pure Rust**.

The crate keeps the original project's focused command set and binary frame protocol, but removes the Python package, `redis-py` integration, and Python runtime dependencies. The core library is transport-agnostic and exposes a request handler that can be embedded behind an IPC transport.

## What is implemented

* `PING`
* `SET` / `GET`
* `DEL` / `EXISTS` / `MGET` / `KEYS`
* `JSON.SET` / `JSON.GET` with root paths `$` and `.`
* `DUMP` / `LOAD` using a compact internal dump payload
* Binary command and response frames with protocol magic `IX2R`

This is intentionally not a full Redis server.

## Project layout

```text
iox2redis-json/
  Cargo.toml
  src/
    lib.rs
    codec.rs
    store.rs
    transport.rs
    bin/
      iox2redis-server.rs
      iox2redis-client.rs
  tests/
    codec.rs
    store.rs
  docs/
    architecture.md
```

## Build and test

```bash
cargo test
cargo build --release
```

The default build has no third-party Rust dependencies, so it builds without downloading crates. The original Python project used iceoryx2 for IPC; this Rust rewrite keeps the core transport-agnostic and reserves an explicit `iox2` feature for the native iceoryx2 adapter so the crate can still be built and tested in offline environments.

## Demo transport

The pure Rust rewrite includes a minimal line-oriented stdio transport for demos and integration tests. Each input line is a hex-encoded binary request frame, and each output line is the hex-encoded response frame.

Start the demo server:

```bash
cargo run --bin iox2redis-server -- /your/topic/to/iox2_server/
```

Encode a command frame:

```bash
cargo run --bin iox2redis-client -- set plain hello
cargo run --bin iox2redis-client -- get plain
cargo run --bin iox2redis-client -- json-set user:1 '{"name":"Ada","age":37}'
```

## Embedding

```rust
use iox2redis::codec::{decode_response, encode_command, WireValue};
use iox2redis::JsonStore;

let mut store = JsonStore::new();
let request = encode_command([
    WireValue::from("SET"),
    WireValue::from("plain"),
    WireValue::from("hello"),
])?;
let response = store.handle_payload(&request)?;
let frame = decode_response(&response)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Transport note

The previous Python implementation used the Python `iceoryx2` bindings directly. The Rust rewrite should still use iceoryx2 for production IPC, but the default crate keeps the protocol and store independent of any specific transport. The `iox2` feature now marks the intended native adapter boundary. A native Rust iceoryx2 adapter can call `JsonStore::handle_payload()` with request bytes and send the returned response bytes.
