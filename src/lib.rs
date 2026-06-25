//! Pure Rust implementation of the small iox2redis-json protocol and store.
//!
//! The crate intentionally implements a compact Redis-like subset rather than
//! a full Redis server: `PING`, `SET`, `GET`, `DEL`, `EXISTS`, `MGET`, `KEYS`,
//! `DUMP`, `LOAD`, `JSON.SET`, and `JSON.GET`.

pub mod codec;
pub mod store;
pub mod transport;

pub use codec::{decode_command, decode_response, encode_command, encode_response, CodecError};
pub use store::{JsonStore, StoreError};
