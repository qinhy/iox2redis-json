# iox2redis-json Rust full version

This is a full Rust rewrite of the uploaded Python `iox2redis-json` package, packaged as a Cargo project.

It keeps the same binary frame format (`IX2R`, protocol version `1`) and implements the same Redis-like command subset:

- `PING`
- `SET` with `EX`, `PX`, `NX`, `XX`, `GET`
- `GET`
- `DEL`
- `EXISTS`
- `MGET`
- `KEYS` with Redis-style glob support: `*`, `?`, escapes, and character classes such as `[abc]` / `[!abc]`
- `DUMP` / `LOAD` with TTL preservation
- `JSON.SET` / `JSON.GET` for root paths `$` and `.`
- `const:*` write-once namespace, including `const:server_info`

## What is improved compared with the previous loose `.rs` files

The previous Rust version was a compact skeleton. This version adds:

- real JSON validation through `serde_json`
- TTL-aware `DUMP` / `LOAD`
- `const:*` routing and server metadata
- legacy JSON frame decode fallback for compatibility
- stronger CLI validation through `clap`
- reusable direct-client abstractions
- tests for protocol, TTL, const routing, and JSON behavior
- graceful error messages instead of silent defaults
- optional native iceoryx2 request/response transport behind `--features iox2`

## Build

```bash
cargo build
```

With native iceoryx2 transport:

```bash
cargo build --features iox2
```

The default build does not require iceoryx2 and works offline once Rust dependencies are available.

## Run the hex stdio server

The default transport is a deterministic hex-line transport. It reads one hex-encoded request frame per line from stdin and writes one hex-encoded response frame per line to stdout. Ctrl-C is handled gracefully; the server exits cleanly even while waiting for stdin, and stray malformed terminal input is ignored with a warning instead of becoming a process error.

```bash
cargo run --bin iox2redis-server -- /redis/json
```

Generate a request:

```bash
cargo run --bin iox2redis-client -- ping
cargo run --bin iox2redis-client -- set hello world
cargo run --bin iox2redis-client -- json-set user '{"name":"Ada"}'
```

Send the generated hex line to the server process to receive a hex response.

Decode a response:

```bash
cargo run --bin iox2redis-client -- decode-response <HEX_RESPONSE>
```

## Run with iceoryx2

```bash
cargo run --features iox2 --bin iox2redis-server -- --transport iox2 /redis/json
cargo run --features iox2 --bin iox2redis-client -- --transport iox2 --service /redis/json ping
```

The Rust adapter uses iceoryx2 dynamic byte slices with request/response payload type `[u8] -> [u8]`. Ctrl-C / SIGTERM during `Node::wait()` is treated as normal shutdown, including iceoryx2 `TerminationRequest` wait results.

## Notes

- This is a Redis-like demo store, not a complete Redis server.
- JSON paths other than `$` and `.` intentionally return an error, matching the Python demo behavior.
- Pipelines and RESP command packing are intentionally not implemented.
- I could not run `cargo check` in the current environment because Rust/Cargo is not installed here. The project is structured as a normal Cargo crate and includes tests for local validation.

## Graceful iceoryx2 shutdown and log level notes

The native iceoryx2 transport initializes the iceoryx2 log level before creating a node:

```rust
set_log_level_from_env_or(LogLevel::Error);
```

That means `IOX2_LOG_LEVEL` is honored when present, and the default is quiet (`Error`) instead of `Info`.

`Ctrl-C` during `Node::wait()` may be reported by iceoryx2 as either `NodeWaitFailure::TerminationRequest` or `NodeWaitFailure::Interrupt`. Both are treated as normal shutdown and should not make the server exit with an error.
