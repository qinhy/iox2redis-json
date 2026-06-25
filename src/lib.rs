//! Full Rust implementation of the iox2redis-json protocol and in-memory store.
//!
//! The crate implements the Python package's command semantics in Rust:
//! `PING`, `SET`, `GET`, `DEL`, `EXISTS`, `MGET`, `KEYS`, `DUMP`, `LOAD`,
//! `JSON.SET`, and `JSON.GET`, including TTL-aware dump/load and the
//! write-once `const:*` namespace used by the server wrapper.
//!
//! The default build has no native IPC dependency and can run a deterministic
//! hex-over-stdio transport. Enable the `iox2` Cargo feature to build the
//! iceoryx2 request/response adapter.

pub mod client;
pub mod codec;
pub mod store;
pub mod transport;

pub use client::{DirectClient, RedisValue};
pub use codec::{
    decode_command, decode_response, encode_command, encode_response, CodecError, CommandFrame,
    ResponseFrame, ResponseKind, WireValue,
};
pub use store::{ConstJsonStore, Iox2JsonServer, JsonStore, ServerConfig, ServerInfo, StoreError};
