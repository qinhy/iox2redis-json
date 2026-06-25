use crate::store::{JsonStore, StoreError};
use std::io::{self, BufRead, Write};

pub fn service_name_from_host(host: &str) -> String {
    host.strip_prefix("iox2://")
        .unwrap_or(host)
        .trim_matches('/')
        .to_owned()
}

pub fn serve_hex_stdio(mut store: JsonStore) -> Result<(), StoreError> {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    for line in stdin.lock().lines() {
        let line = line.map_err(|e| StoreError::Io(e.to_string()))?;
        if line.trim().is_empty() {
            continue;
        }
        let request = decode_hex(line.trim()).map_err(StoreError::Io)?;
        let response = store.handle_payload(&request)?;
        writeln!(stdout, "{}", encode_hex(&response)).map_err(|e| StoreError::Io(e.to_string()))?;
        stdout.flush().ok();
    }
    Ok(())
}

pub fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub fn decode_hex(text: &str) -> Result<Vec<u8>, String> {
    if text.len() % 2 != 0 {
        return Err("hex input must have an even length".into());
    }
    let mut out = Vec::with_capacity(text.len() / 2);
    for pair in text.as_bytes().chunks_exact(2) {
        out.push((hex_digit(pair[0])? << 4) | hex_digit(pair[1])?);
    }
    Ok(out)
}
fn hex_digit(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(format!("invalid hex digit: {}", byte as char)),
    }
}

#[cfg(feature = "iox2")]
pub mod iox2 {
    use crate::store::StoreError;

    /// Placeholder for the native iceoryx2 request/response transport.
    ///
    /// The pure Rust core is ready to be embedded behind iceoryx2 by passing
    /// request payload bytes into `JsonStore::handle_payload`. This repository
    /// keeps the `iox2` feature explicit so the default build remains usable in
    /// offline environments where Cargo cannot download the upstream crate.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Iox2TransportConfig {
        pub service_name: String,
        pub max_payload_size: usize,
        pub poll_ns: u64,
    }

    impl Iox2TransportConfig {
        pub fn new(service_name: impl Into<String>) -> Self {
            Self {
                service_name: super::service_name_from_host(&service_name.into()),
                max_payload_size: 64 * 1024,
                poll_ns: 100_000,
            }
        }
    }

    pub fn unavailable_error() -> StoreError {
        StoreError::Io("native iceoryx2 adapter is not linked in this offline build; add the iceoryx2 crate and implement the request_response adapter behind the iox2 feature".to_owned())
    }
}
