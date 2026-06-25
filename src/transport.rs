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
