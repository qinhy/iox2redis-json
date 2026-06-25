# Architecture

`iox2redis-json` is now a pure Rust crate with three layers:

1. `codec` encodes and decodes request/response frames.
2. `store` implements the in-memory Redis-like command subset.
3. `transport` contains a tiny stdio demo transport and shared transport helpers.

## Wire protocol

Binary frames begin with:

```text
IX2R | version:u8 | frame_type:u8
```

Command frames then contain:

```text
command_len:u16 | argc:u16 | command:utf8 | typed_arg...
```

Response frames contain:

```text
kind:u8 | payload
```

Supported value tags:

* `0`: none
* `1`: bytes with `u32` length prefix
* `2`: UTF-8 string with `u32` length prefix
* `3`: signed 64-bit integer

Supported response kinds:

* `simple`
* `bulk`
* `array`
* `nil`
* `error`
* `integer`
* `pong`

## Store model

`JsonStore` keeps a `HashMap<String, StoredValue>`. Values can be bytes, strings, integers, or JSON text. JSON commands intentionally support only the root paths `$` and `.`.

The store is transport-agnostic: callers pass request bytes to `handle_payload()` and receive response bytes.


## iceoryx2 dependency strategy

The production transport for this project is still expected to be iceoryx2 request/response IPC. The pure Rust core intentionally keeps that dependency behind an explicit `iox2` feature boundary so codec and store tests can run without network access or platform-specific IPC setup. The adapter boundary is `JsonStore::handle_payload()`: an iceoryx2 server receives request bytes, passes them to the store, and sends the returned response bytes.
