use crate::codec::{decode_response, encode_command, CodecError, ResponseFrame, ResponseKind, WireValue};
use serde_json::Value;
use std::fmt;

#[derive(Clone, Debug, PartialEq)]
pub enum RedisValue {
    Nil,
    Bytes(Vec<u8>),
    Integer(i64),
    Array(Vec<RedisValue>),
}

impl RedisValue {
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytes(bytes) => Some(bytes),
            _ => None,
        }
    }

    pub fn into_bytes(self) -> Option<Vec<u8>> {
        match self {
            Self::Bytes(bytes) => Some(bytes),
            _ => None,
        }
    }
}

impl fmt::Display for RedisValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RedisValue::Nil => write!(f, "(nil)"),
            RedisValue::Bytes(bytes) => write!(f, "{}", String::from_utf8_lossy(bytes)),
            RedisValue::Integer(value) => write!(f, "{value}"),
            RedisValue::Array(values) => {
                for (idx, value) in values.iter().enumerate() {
                    if idx > 0 {
                        writeln!(f)?;
                    }
                    write!(f, "{}) {value}", idx + 1)?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("codec error: {0}")]
    Codec(#[from] CodecError),
    #[error("server error: {0}")]
    Server(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("unexpected response: {0}")]
    Unexpected(String),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

pub trait RpcTransport {
    fn request(&mut self, payload: &[u8]) -> Result<Vec<u8>, ClientError>;
}

pub struct DirectClient<T> {
    transport: T,
}

impl<T: RpcTransport> DirectClient<T> {
    pub fn new(transport: T) -> Self {
        Self { transport }
    }

    pub fn execute<I, V>(&mut self, parts: I) -> Result<RedisValue, ClientError>
    where
        I: IntoIterator<Item = V>,
        V: Into<WireValue>,
    {
        let request = encode_command(parts)?;
        let response = decode_response(&self.transport.request(&request)?)?;
        response_to_redis_value(response)
    }

    pub fn ping(&mut self) -> Result<bool, ClientError> {
        Ok(matches!(self.execute(["PING"])?, RedisValue::Bytes(bytes) if bytes == b"PONG"))
    }

    pub fn set_bytes(&mut self, key: impl Into<WireValue>, value: Vec<u8>) -> Result<bool, ClientError> {
        Ok(matches!(
            self.execute(vec![WireValue::from("SET"), key.into(), WireValue::Bytes(value)])?,
            RedisValue::Bytes(bytes) if bytes == b"OK"
        ))
    }

    pub fn get_bytes(&mut self, key: impl Into<WireValue>) -> Result<Option<Vec<u8>>, ClientError> {
        match self.execute(vec![WireValue::from("GET"), key.into()])? {
            RedisValue::Nil => Ok(None),
            RedisValue::Bytes(bytes) => Ok(Some(bytes)),
            other => Err(ClientError::Unexpected(format!("GET returned {other:?}"))),
        }
    }

    pub fn keys(&mut self, pattern: impl Into<WireValue>) -> Result<Vec<Vec<u8>>, ClientError> {
        match self.execute(vec![WireValue::from("KEYS"), pattern.into()])? {
            RedisValue::Array(values) => values
                .into_iter()
                .map(|value| match value {
                    RedisValue::Bytes(bytes) => Ok(bytes),
                    other => Err(ClientError::Unexpected(format!("KEYS item {other:?}"))),
                })
                .collect(),
            other => Err(ClientError::Unexpected(format!("KEYS returned {other:?}"))),
        }
    }

    pub fn set_json(&mut self, key: impl Into<WireValue>, value: Value) -> Result<bool, ClientError> {
        let payload = serde_json::to_string(&value)?;
        Ok(matches!(
            self.execute(vec![
                WireValue::from("JSON.SET"),
                key.into(),
                WireValue::from("$"),
                WireValue::from(payload),
            ])?,
            RedisValue::Bytes(bytes) if bytes == b"OK"
        ))
    }

    pub fn get_json(&mut self, key: impl Into<WireValue>) -> Result<Option<Value>, ClientError> {
        match self.execute(vec![WireValue::from("JSON.GET"), key.into(), WireValue::from("$")])? {
            RedisValue::Nil => Ok(None),
            RedisValue::Bytes(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            other => Err(ClientError::Unexpected(format!("JSON.GET returned {other:?}"))),
        }
    }

    pub fn dump(&mut self, key: impl Into<WireValue>) -> Result<Option<Vec<u8>>, ClientError> {
        match self.execute(vec![WireValue::from("DUMP"), key.into()])? {
            RedisValue::Nil => Ok(None),
            RedisValue::Bytes(bytes) => Ok(Some(bytes)),
            other => Err(ClientError::Unexpected(format!("DUMP returned {other:?}"))),
        }
    }

    pub fn load(
        &mut self,
        key: impl Into<WireValue>,
        payload: Vec<u8>,
        nx: bool,
        xx: bool,
    ) -> Result<bool, ClientError> {
        let mut parts = vec![WireValue::from("LOAD"), key.into(), WireValue::Bytes(payload)];
        if nx {
            parts.push(WireValue::from("NX"));
        }
        if xx {
            parts.push(WireValue::from("XX"));
        }
        Ok(matches!(self.execute(parts)?, RedisValue::Bytes(bytes) if bytes == b"OK"))
    }
}

pub fn response_to_redis_value(frame: ResponseFrame) -> Result<RedisValue, ClientError> {
    match frame.kind {
        ResponseKind::Simple => Ok(RedisValue::Bytes(value_to_bytes(frame.value))),
        ResponseKind::Bulk => match frame.value {
            Some(value) => Ok(RedisValue::Bytes(wire_to_bytes(&value))),
            None => Ok(RedisValue::Nil),
        },
        ResponseKind::Integer => Ok(RedisValue::Integer(
            frame
                .value
                .map(|value| value.as_lossy_key().parse::<i64>().unwrap_or_default())
                .unwrap_or_default(),
        )),
        ResponseKind::Array => Ok(RedisValue::Array(
            frame
                .array
                .into_iter()
                .map(|item| match item {
                    Some(value) => RedisValue::Bytes(wire_to_bytes(&value)),
                    None => RedisValue::Nil,
                })
                .collect(),
        )),
        ResponseKind::Nil => Ok(RedisValue::Nil),
        ResponseKind::Pong => Ok(RedisValue::Bytes(
            frame
                .value
                .map(|value| wire_to_bytes(&value))
                .unwrap_or_else(|| b"PONG".to_vec()),
        )),
        ResponseKind::Error => Err(ClientError::Server(
            frame.message.unwrap_or_else(|| "ERR unknown error".to_owned()),
        )),
    }
}

fn value_to_bytes(value: Option<WireValue>) -> Vec<u8> {
    value.map(|value| wire_to_bytes(&value)).unwrap_or_default()
}

fn wire_to_bytes(value: &WireValue) -> Vec<u8> {
    match value {
        WireValue::None => Vec::new(),
        WireValue::Bytes(bytes) => bytes.clone(),
        WireValue::Str(text) => text.as_bytes().to_vec(),
        WireValue::Json(value) => serde_json::to_vec(value).unwrap_or_default(),
    }
}

#[cfg(feature = "iox2")]
impl RpcTransport for crate::transport::iox2::Iox2RpcClient {
    fn request(&mut self, payload: &[u8]) -> Result<Vec<u8>, ClientError> {
        crate::transport::iox2::Iox2RpcClient::request(self, payload)
            .map_err(|error| ClientError::Transport(error.to_string()))
    }
}
