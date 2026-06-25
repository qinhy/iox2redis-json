use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

pub const PROTOCOL_VERSION: u8 = 1;
const MAGIC: &[u8; 4] = b"IX2R";
const FRAME_COMMAND: u8 = 1;
const FRAME_RESPONSE: u8 = 2;
const TAG_NONE: u8 = 0;
const TAG_BYTES: u8 = 1;
const TAG_STR: u8 = 2;
const TAG_JSON: u8 = 3;

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("truncated frame")]
    Truncated,
    #[error("invalid binary frame magic")]
    InvalidMagic,
    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u8),
    #[error("unexpected frame type: {0}")]
    UnexpectedFrameType(u8),
    #[error("missing command")]
    MissingCommand,
    #[error("command name is too long")]
    CommandTooLong,
    #[error("too many command arguments")]
    TooManyArgs,
    #[error("unknown value tag: {0}")]
    UnknownValueTag(u8),
    #[error("unknown response kind: {0}")]
    UnknownResponseKind(u8),
    #[error("trailing bytes in frame")]
    TrailingBytes,
    #[error("utf-8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("base64 error: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("tagged value must be an object")]
    TaggedValueNotObject,
    #[error("unknown value tag: {0}")]
    UnknownJsonValueTag(String),
    #[error("array response value must be a list")]
    ArrayResponseNotList,
}

#[derive(Clone, Debug, PartialEq)]
pub enum WireValue {
    None,
    Bytes(Vec<u8>),
    Str(String),
    Json(Value),
}

impl WireValue {
    pub fn as_lossy_key(&self) -> String {
        match self {
            Self::None => String::new(),
            Self::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            Self::Str(text) => text.clone(),
            Self::Json(value) => value_to_compact_json(value),
        }
    }

    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        match self {
            Self::None => None,
            Self::Bytes(bytes) => Some(bytes.clone()),
            Self::Str(text) => Some(text.as_bytes().to_vec()),
            Self::Json(value) => Some(value_to_compact_json(value).into_bytes()),
        }
    }

    pub fn as_bulk_bytes(&self) -> Option<Vec<u8>> {
        self.to_bytes()
    }
}

impl From<&str> for WireValue {
    fn from(value: &str) -> Self {
        Self::Str(value.to_owned())
    }
}
impl From<String> for WireValue {
    fn from(value: String) -> Self {
        Self::Str(value)
    }
}
impl From<&String> for WireValue {
    fn from(value: &String) -> Self {
        Self::Str(value.clone())
    }
}
impl From<Vec<u8>> for WireValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}
impl From<&[u8]> for WireValue {
    fn from(value: &[u8]) -> Self {
        Self::Bytes(value.to_vec())
    }
}
impl From<&Vec<u8>> for WireValue {
    fn from(value: &Vec<u8>) -> Self {
        Self::Bytes(value.clone())
    }
}
impl From<Value> for WireValue {
    fn from(value: Value) -> Self {
        Self::Json(value)
    }
}
impl From<i64> for WireValue {
    fn from(value: i64) -> Self {
        Self::Json(Value::from(value))
    }
}
impl From<i32> for WireValue {
    fn from(value: i32) -> Self {
        Self::Json(Value::from(value))
    }
}
impl From<u64> for WireValue {
    fn from(value: u64) -> Self {
        Self::Json(Value::from(value))
    }
}
impl From<bool> for WireValue {
    fn from(value: bool) -> Self {
        Self::Json(Value::from(value))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CommandFrame {
    pub command: String,
    pub args: Vec<WireValue>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResponseKind {
    Simple,
    Bulk,
    Array,
    Nil,
    Error,
    Integer,
    Pong,
}

impl ResponseKind {
    pub fn code(&self) -> u8 {
        match self {
            Self::Simple => 1,
            Self::Bulk => 2,
            Self::Array => 3,
            Self::Nil => 4,
            Self::Error => 5,
            Self::Integer => 6,
            Self::Pong => 7,
        }
    }

    pub fn from_code(code: u8) -> Result<Self, CodecError> {
        match code {
            1 => Ok(Self::Simple),
            2 => Ok(Self::Bulk),
            3 => Ok(Self::Array),
            4 => Ok(Self::Nil),
            5 => Ok(Self::Error),
            6 => Ok(Self::Integer),
            7 => Ok(Self::Pong),
            other => Err(CodecError::UnknownResponseKind(other)),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Simple => "simple",
            Self::Bulk => "bulk",
            Self::Array => "array",
            Self::Nil => "nil",
            Self::Error => "error",
            Self::Integer => "integer",
            Self::Pong => "pong",
        }
    }

    pub fn from_str(kind: &str) -> Result<Self, CodecError> {
        match kind {
            "simple" => Ok(Self::Simple),
            "bulk" => Ok(Self::Bulk),
            "array" => Ok(Self::Array),
            "nil" => Ok(Self::Nil),
            "error" => Ok(Self::Error),
            "integer" => Ok(Self::Integer),
            "pong" => Ok(Self::Pong),
            _ => Err(CodecError::UnknownJsonValueTag(kind.to_owned())),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResponseFrame {
    pub kind: ResponseKind,
    pub value: Option<WireValue>,
    pub array: Vec<Option<WireValue>>,
    pub message: Option<String>,
}

impl ResponseFrame {
    pub fn simple(value: impl Into<String>) -> Self {
        Self {
            kind: ResponseKind::Simple,
            value: Some(WireValue::Str(value.into())),
            array: Vec::new(),
            message: None,
        }
    }

    pub fn bulk(value: impl Into<WireValue>) -> Self {
        Self {
            kind: ResponseKind::Bulk,
            value: Some(value.into()),
            array: Vec::new(),
            message: None,
        }
    }

    pub fn array(array: Vec<Option<WireValue>>) -> Self {
        Self {
            kind: ResponseKind::Array,
            value: None,
            array,
            message: None,
        }
    }

    pub fn nil() -> Self {
        Self {
            kind: ResponseKind::Nil,
            value: None,
            array: Vec::new(),
            message: None,
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            kind: ResponseKind::Error,
            value: None,
            array: Vec::new(),
            message: Some(message.into()),
        }
    }

    pub fn integer(value: i64) -> Self {
        Self {
            kind: ResponseKind::Integer,
            value: Some(WireValue::Json(Value::from(value))),
            array: Vec::new(),
            message: None,
        }
    }

    pub fn pong() -> Self {
        Self {
            kind: ResponseKind::Pong,
            value: None,
            array: Vec::new(),
            message: None,
        }
    }
}

pub fn encode_command<I, V>(parts: I) -> Result<Vec<u8>, CodecError>
where
    I: IntoIterator<Item = V>,
    V: Into<WireValue>,
{
    let mut values: Vec<WireValue> = parts.into_iter().map(Into::into).collect();
    if values.is_empty() {
        return Err(CodecError::MissingCommand);
    }
    let command = values.remove(0).as_lossy_key().to_uppercase();
    let command_bytes = command.as_bytes();
    if command_bytes.len() > u16::MAX as usize {
        return Err(CodecError::CommandTooLong);
    }
    if values.len() > u16::MAX as usize {
        return Err(CodecError::TooManyArgs);
    }

    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.push(PROTOCOL_VERSION);
    out.push(FRAME_COMMAND);
    out.extend_from_slice(&(command_bytes.len() as u16).to_be_bytes());
    out.extend_from_slice(&(values.len() as u16).to_be_bytes());
    out.extend_from_slice(command_bytes);
    for value in values {
        encode_value(&mut out, &value)?;
    }
    Ok(out)
}

pub fn decode_command(payload: &[u8]) -> Result<CommandFrame, CodecError> {
    if !payload.starts_with(MAGIC) {
        return decode_json_command(payload);
    }

    let mut cursor = Cursor::new(payload);
    decode_header(&mut cursor, FRAME_COMMAND)?;
    let command_len = cursor.u16()? as usize;
    let argc = cursor.u16()? as usize;
    let command = String::from_utf8(cursor.bytes(command_len)?.to_vec())?;
    if command.is_empty() {
        return Err(CodecError::MissingCommand);
    }

    let mut args = Vec::with_capacity(argc);
    for _ in 0..argc {
        args.push(decode_value(&mut cursor)?);
    }
    if !cursor.done() {
        return Err(CodecError::TrailingBytes);
    }

    Ok(CommandFrame {
        command: command.to_uppercase(),
        args,
    })
}

pub fn encode_response(frame: &ResponseFrame) -> Result<Vec<u8>, CodecError> {
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.push(PROTOCOL_VERSION);
    out.push(FRAME_RESPONSE);
    out.push(frame.kind.code());

    match frame.kind {
        ResponseKind::Nil => {}
        ResponseKind::Error => pack(
            &mut out,
            frame
                .message
                .as_deref()
                .unwrap_or("ERR unknown error")
                .as_bytes(),
        ),
        ResponseKind::Array => {
            out.extend_from_slice(&(frame.array.len() as u32).to_be_bytes());
            for item in &frame.array {
                match item {
                    Some(value) => encode_value(&mut out, value)?,
                    None => encode_value(&mut out, &WireValue::None)?,
                }
            }
        }
        ResponseKind::Pong if frame.value.is_none() => encode_value(&mut out, &WireValue::None)?,
        _ => encode_value(&mut out, frame.value.as_ref().unwrap_or(&WireValue::None))?,
    }

    Ok(out)
}

pub fn decode_response(payload: &[u8]) -> Result<ResponseFrame, CodecError> {
    if !payload.starts_with(MAGIC) {
        return decode_json_response(payload);
    }

    let mut cursor = Cursor::new(payload);
    decode_header(&mut cursor, FRAME_RESPONSE)?;
    let kind = ResponseKind::from_code(cursor.u8()?)?;

    match kind {
        ResponseKind::Nil => {
            if !cursor.done() {
                return Err(CodecError::TrailingBytes);
            }
            Ok(ResponseFrame::nil())
        }
        ResponseKind::Error => {
            let message = String::from_utf8(cursor.len_prefixed()?.to_vec())?;
            if !cursor.done() {
                return Err(CodecError::TrailingBytes);
            }
            Ok(ResponseFrame::error(if message.is_empty() {
                "ERR unknown error".to_owned()
            } else {
                message
            }))
        }
        ResponseKind::Array => {
            let count = cursor.u32()? as usize;
            let mut array = Vec::with_capacity(count);
            for _ in 0..count {
                let value = decode_value(&mut cursor)?;
                if value == WireValue::None {
                    array.push(None);
                } else {
                    array.push(Some(value));
                }
            }
            if !cursor.done() {
                return Err(CodecError::TrailingBytes);
            }
            Ok(ResponseFrame::array(array))
        }
        other => {
            let value = decode_value(&mut cursor)?;
            if !cursor.done() {
                return Err(CodecError::TrailingBytes);
            }
            Ok(ResponseFrame {
                kind: other,
                value: if value == WireValue::None { None } else { Some(value) },
                array: Vec::new(),
                message: None,
            })
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonFrame {
    v: u8,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    args: Option<Vec<Value>>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    value: Option<Value>,
    #[serde(default)]
    message: Option<String>,
}

pub fn encode_json_value(value: &WireValue) -> Value {
    match value {
        WireValue::None => serde_json::json!({ "type": "json", "data": null }),
        WireValue::Bytes(bytes) => serde_json::json!({
            "type": "bytes",
            "data": BASE64.encode(bytes),
        }),
        WireValue::Str(text) => serde_json::json!({ "type": "str", "data": text }),
        WireValue::Json(value) => serde_json::json!({ "type": "json", "data": value }),
    }
}

pub fn decode_json_value(value: &Value) -> Result<WireValue, CodecError> {
    let obj = value.as_object().ok_or(CodecError::TaggedValueNotObject)?;
    let tag = obj
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| CodecError::UnknownJsonValueTag("<missing>".to_owned()))?;
    let data = obj.get("data").unwrap_or(&Value::Null);
    match tag {
        "bytes" => {
            let text = data.as_str().unwrap_or_default();
            Ok(WireValue::Bytes(BASE64.decode(text.as_bytes())?))
        }
        "str" => Ok(WireValue::Str(data.as_str().unwrap_or_default().to_owned())),
        "json" => Ok(WireValue::Json(data.clone())),
        other => Err(CodecError::UnknownJsonValueTag(other.to_owned())),
    }
}

fn decode_json_command(payload: &[u8]) -> Result<CommandFrame, CodecError> {
    let frame: JsonFrame = serde_json::from_slice(payload)?;
    if frame.v != PROTOCOL_VERSION {
        return Err(CodecError::UnsupportedVersion(frame.v));
    }
    let command = frame.command.ok_or(CodecError::MissingCommand)?.to_uppercase();
    if command.is_empty() {
        return Err(CodecError::MissingCommand);
    }
    let args = frame
        .args
        .unwrap_or_default()
        .iter()
        .map(decode_json_value)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(CommandFrame { command, args })
}

fn decode_json_response(payload: &[u8]) -> Result<ResponseFrame, CodecError> {
    let frame: JsonFrame = serde_json::from_slice(payload)?;
    if frame.v != PROTOCOL_VERSION {
        return Err(CodecError::UnsupportedVersion(frame.v));
    }
    let kind_text = frame.kind.as_deref().unwrap_or("");
    let kind = ResponseKind::from_str(kind_text)?;
    match kind {
        ResponseKind::Bulk => Ok(ResponseFrame::bulk(decode_json_value(
            &frame.value.unwrap_or(Value::Null),
        )?)),
        ResponseKind::Array => {
            let items = frame
                .value
                .unwrap_or(Value::Array(Vec::new()))
                .as_array()
                .ok_or(CodecError::ArrayResponseNotList)?
                .iter()
                .map(|item| {
                    if item.is_null() {
                        Ok(None)
                    } else {
                        Ok(Some(decode_json_value(item)?))
                    }
                })
                .collect::<Result<Vec<_>, CodecError>>()?;
            Ok(ResponseFrame::array(items))
        }
        ResponseKind::Error => Ok(ResponseFrame::error(
            frame.message.unwrap_or_else(|| "ERR unknown error".to_owned()),
        )),
        ResponseKind::Nil => Ok(ResponseFrame::nil()),
        ResponseKind::Pong => Ok(ResponseFrame::pong()),
        ResponseKind::Integer => Ok(ResponseFrame::integer(
            frame.value.as_ref().and_then(Value::as_i64).unwrap_or_default(),
        )),
        ResponseKind::Simple => Ok(ResponseFrame::simple(
            frame
                .value
                .map(value_to_string)
                .unwrap_or_else(|| "OK".to_owned()),
        )),
    }
}

fn encode_value(out: &mut Vec<u8>, value: &WireValue) -> Result<(), CodecError> {
    match value {
        WireValue::None => out.push(TAG_NONE),
        WireValue::Bytes(bytes) => {
            out.push(TAG_BYTES);
            pack(out, bytes);
        }
        WireValue::Str(text) => {
            out.push(TAG_STR);
            pack(out, text.as_bytes());
        }
        WireValue::Json(value) => {
            out.push(TAG_JSON);
            let data = serde_json::to_vec(value)?;
            pack(out, &data);
        }
    }
    Ok(())
}

fn decode_value(cursor: &mut Cursor<'_>) -> Result<WireValue, CodecError> {
    match cursor.u8()? {
        TAG_NONE => Ok(WireValue::None),
        TAG_BYTES => Ok(WireValue::Bytes(cursor.len_prefixed()?.to_vec())),
        TAG_STR => Ok(WireValue::Str(String::from_utf8(
            cursor.len_prefixed()?.to_vec(),
        )?)),
        TAG_JSON => Ok(WireValue::Json(serde_json::from_slice(
            cursor.len_prefixed()?,
        )?)),
        other => Err(CodecError::UnknownValueTag(other)),
    }
}

fn decode_header(cursor: &mut Cursor<'_>, expected_frame_type: u8) -> Result<(), CodecError> {
    if cursor.bytes(4)? != MAGIC {
        return Err(CodecError::InvalidMagic);
    }
    let version = cursor.u8()?;
    if version != PROTOCOL_VERSION {
        return Err(CodecError::UnsupportedVersion(version));
    }
    let frame_type = cursor.u8()?;
    if frame_type != expected_frame_type {
        return Err(CodecError::UnexpectedFrameType(frame_type));
    }
    Ok(())
}

fn pack(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

pub fn value_to_compact_json(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned())
}

pub fn value_to_string(value: Value) -> String {
    match value {
        Value::String(text) => text,
        other => value_to_compact_json(&other),
    }
}

struct Cursor<'a> {
    payload: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(payload: &'a [u8]) -> Self {
        Self { payload, offset: 0 }
    }

    fn done(&self) -> bool {
        self.offset == self.payload.len()
    }

    fn bytes(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        let end = self.offset.checked_add(n).ok_or(CodecError::Truncated)?;
        if end > self.payload.len() {
            return Err(CodecError::Truncated);
        }
        let out = &self.payload[self.offset..end];
        self.offset = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.bytes(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, CodecError> {
        let bytes = self.bytes(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn u32(&mut self) -> Result<u32, CodecError> {
        let bytes = self.bytes(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn len_prefixed(&mut self) -> Result<&'a [u8], CodecError> {
        let len = self.u32()? as usize;
        self.bytes(len)
    }
}

impl fmt::Display for WireValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WireValue::None => write!(f, "(nil)"),
            WireValue::Bytes(bytes) => write!(f, "{}", String::from_utf8_lossy(bytes)),
            WireValue::Str(text) => write!(f, "{text}"),
            WireValue::Json(value) => write!(f, "{}", value_to_compact_json(value)),
        }
    }
}
