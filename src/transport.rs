use crate::store::StoreError;
use std::io::{self, BufRead, Write};

pub fn service_name_from_host(host: &str) -> String {
    host.strip_prefix("iox2://")
        .unwrap_or(host)
        .trim_matches('/')
        .to_owned()
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
        return Err("hex input must have an even length".to_owned());
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

pub fn serve_hex_stdio<F>(mut handler: F) -> Result<(), StoreError>
where
    F: FnMut(&[u8]) -> Result<Vec<u8>, StoreError>,
{
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    for line in stdin.lock().lines() {
        let line = line.map_err(|error| StoreError::Io(error.to_string()))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let request = decode_hex(trimmed).map_err(StoreError::Io)?;
        let response = handler(&request)?;
        writeln!(stdout, "{}", encode_hex(&response))
            .map_err(|error| StoreError::Io(error.to_string()))?;
        stdout
            .flush()
            .map_err(|error| StoreError::Io(error.to_string()))?;
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct Iox2TransportConfig {
    pub service_name: String,
    pub max_payload_size: usize,
    pub poll_ns: u64,
    pub timeout: std::time::Duration,
}

impl Iox2TransportConfig {
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name_from_host(&service_name.into()),
            max_payload_size: 64 * 1024,
            poll_ns: 100_000,
            timeout: std::time::Duration::from_secs(1),
        }
    }
}

#[cfg(feature = "iox2")]
pub mod iox2 {
    use super::{service_name_from_host, Iox2TransportConfig};
    use crate::store::StoreError;
    use iceoryx2::prelude::*;
    use std::time::{Duration, Instant};

    /// One-shot iceoryx2 request/response client for dynamic byte slices.
    ///
    /// The object stores only configuration and creates an iceoryx2 node/port per
    /// request. This keeps the public API stable across iceoryx2 minor releases
    /// and avoids exposing iceoryx2's large generic port types from this crate.
    #[derive(Clone, Debug)]
    pub struct Iox2RpcClient {
        config: Iox2TransportConfig,
    }

    impl Iox2RpcClient {
        pub fn new(config: Iox2TransportConfig) -> Self {
            Self { config }
        }

        pub fn request(&self, payload: &[u8]) -> Result<Vec<u8>, StoreError> {
            if payload.len() > self.config.max_payload_size {
                return Err(StoreError::Io(format!(
                    "request payload is {} bytes, max_payload_size={}",
                    payload.len(), self.config.max_payload_size
                )));
            }

            let service_name_string = service_name_from_host(&self.config.service_name);
            let service_name: ServiceName = service_name_string.as_str().try_into().map_err(|error| {
                StoreError::Io(format!("invalid service name {service_name_string:?}: {error}"))
            })?;
            let node = NodeBuilder::new()
                .create::<ipc::Service>()
                .map_err(|error| StoreError::Io(error.to_string()))?;
            let service = node
                .service_builder(&service_name)
                .request_response::<[u8], [u8]>()
                .open_or_create()
                .map_err(|error| StoreError::Io(error.to_string()))?;
            let client = service
                .client_builder()
                .initial_max_slice_len(self.config.max_payload_size)
                .create()
                .map_err(|error| StoreError::Io(error.to_string()))?;

            let request = client
                .loan_slice_uninit(payload.len())
                .map_err(|error| StoreError::Io(error.to_string()))?
                .write_from_slice(payload);
            let pending = request
                .send()
                .map_err(|error| StoreError::Io(error.to_string()))?;

            let deadline = Instant::now() + self.config.timeout;
            while Instant::now() < deadline {
                if let Some(response) = pending
                    .receive()
                    .map_err(|error| StoreError::Io(error.to_string()))?
                {
                    return Ok(response.payload().to_vec());
                }
                node.wait(Duration::from_nanos(self.config.poll_ns))
                    .map_err(|error| StoreError::Io(error.to_string()))?;
            }
            Err(StoreError::Io(format!(
                "timeout waiting for iceoryx2 response from /{service_name_string}/"
            )))
        }
    }

    /// iceoryx2 request/response server for dynamic byte slices.
    pub struct Iox2RpcServer {
        config: Iox2TransportConfig,
    }

    impl Iox2RpcServer {
        pub fn new(config: Iox2TransportConfig) -> Self {
            Self { config }
        }

        pub fn serve_forever<F>(&self, mut handler: F) -> Result<(), StoreError>
        where
            F: FnMut(&[u8]) -> Result<Vec<u8>, StoreError>,
        {
            let service_name_string = service_name_from_host(&self.config.service_name);
            let service_name: ServiceName = service_name_string.as_str().try_into().map_err(|error| {
                StoreError::Io(format!("invalid service name {service_name_string:?}: {error}"))
            })?;
            let node = NodeBuilder::new()
                .create::<ipc::Service>()
                .map_err(|error| StoreError::Io(error.to_string()))?;
            let service = node
                .service_builder(&service_name)
                .request_response::<[u8], [u8]>()
                .open_or_create()
                .map_err(|error| StoreError::Io(error.to_string()))?;
            let server = service
                .server_builder()
                .initial_max_slice_len(self.config.max_payload_size)
                .create()
                .map_err(|error| StoreError::Io(error.to_string()))?;

            while node
                .wait(Duration::from_nanos(self.config.poll_ns))
                .is_ok()
            {
                while let Some(active_request) = server
                    .receive()
                    .map_err(|error| StoreError::Io(error.to_string()))?
                {
                    let response_bytes = handler(active_request.payload())?;
                    if response_bytes.len() > self.config.max_payload_size {
                        return Err(StoreError::Io(format!(
                            "response payload is {} bytes, max_payload_size={}",
                            response_bytes.len(), self.config.max_payload_size
                        )));
                    }
                    active_request
                        .loan_slice_uninit(response_bytes.len())
                        .map_err(|error| StoreError::Io(error.to_string()))?
                        .write_from_slice(&response_bytes)
                        .send()
                        .map_err(|error| StoreError::Io(error.to_string()))?;
                }
            }
            Ok(())
        }
    }
}
