use std::fmt;

pub const PROTOCOL_VERSION: u8 = 1;
const MAGIC: &[u8; 4] = b"IX2R";
const FRAME_COMMAND: u8 = 1;
const FRAME_RESPONSE: u8 = 2;
const TAG_NONE: u8 = 0;
const TAG_BYTES: u8 = 1;
const TAG_STR: u8 = 2;
const TAG_JSON: u8 = 3;

#[derive(Debug, PartialEq, Eq)]
pub enum CodecError {
    Truncated,
    InvalidMagic,
    UnsupportedVersion(u8),
    UnexpectedFrameType(u8),
    MissingCommand,
    CommandTooLong,
    TooManyArgs,
    UnknownValueTag(u8),
    UnknownResponseKind(u8),
    TrailingBytes,
    Utf8(String),
}
impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for CodecError {}
impl From<std::string::FromUtf8Error> for CodecError {
    fn from(value: std::string::FromUtf8Error) -> Self {
        Self::Utf8(value.to_string())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WireValue {
    None,
    Bytes(Vec<u8>),
    Str(String),
    /// Raw compact JSON text compatible with the original Python TAG_JSON value.
    Json(String),
}
impl WireValue {
    pub fn as_lossy_key(&self) -> String {
        match self {
            Self::None => String::new(),
            Self::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
            Self::Str(s) => s.clone(),
            Self::Json(json) => json.clone(),
        }
    }
    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        match self {
            Self::None => None,
            Self::Bytes(b) => Some(b.clone()),
            Self::Str(s) => Some(s.as_bytes().to_vec()),
            Self::Json(json) => Some(json.as_bytes().to_vec()),
        }
    }
}
impl From<&str> for WireValue {
    fn from(v: &str) -> Self {
        Self::Str(v.to_owned())
    }
}
impl From<String> for WireValue {
    fn from(v: String) -> Self {
        Self::Str(v)
    }
}
impl From<&[u8]> for WireValue {
    fn from(v: &[u8]) -> Self {
        Self::Bytes(v.to_vec())
    }
}
impl From<Vec<u8>> for WireValue {
    fn from(v: Vec<u8>) -> Self {
        Self::Bytes(v)
    }
}
impl From<i64> for WireValue {
    fn from(v: i64) -> Self {
        Self::Json(v.to_string())
    }
}
impl From<i32> for WireValue {
    fn from(v: i32) -> Self {
        Self::Json((v as i64).to_string())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
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
#[derive(Clone, Debug, PartialEq, Eq)]
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
            array: vec![],
            message: None,
        }
    }
    pub fn bulk(value: WireValue) -> Self {
        Self {
            kind: ResponseKind::Bulk,
            value: Some(value),
            array: vec![],
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
            array: vec![],
            message: None,
        }
    }
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            kind: ResponseKind::Error,
            value: None,
            array: vec![],
            message: Some(message.into()),
        }
    }
    pub fn integer(value: i64) -> Self {
        Self {
            kind: ResponseKind::Integer,
            value: Some(WireValue::Json(value.to_string())),
            array: vec![],
            message: None,
        }
    }
    pub fn pong() -> Self {
        Self {
            kind: ResponseKind::Pong,
            value: None,
            array: vec![],
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
    if command.len() > u16::MAX as usize {
        return Err(CodecError::CommandTooLong);
    }
    if values.len() > u16::MAX as usize {
        return Err(CodecError::TooManyArgs);
    }
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.push(PROTOCOL_VERSION);
    out.push(FRAME_COMMAND);
    out.extend_from_slice(&(command.len() as u16).to_be_bytes());
    out.extend_from_slice(&(values.len() as u16).to_be_bytes());
    out.extend_from_slice(command.as_bytes());
    for value in values {
        encode_value(&mut out, &value);
    }
    Ok(out)
}

pub fn decode_command(payload: &[u8]) -> Result<CommandFrame, CodecError> {
    let mut c = Cursor::new(payload);
    decode_header(&mut c, FRAME_COMMAND)?;
    let command_len = c.u16()? as usize;
    let argc = c.u16()? as usize;
    let command = String::from_utf8(c.bytes(command_len)?.to_vec())?;
    if command.is_empty() {
        return Err(CodecError::MissingCommand);
    }
    let mut args = Vec::with_capacity(argc);
    for _ in 0..argc {
        args.push(decode_value(&mut c)?);
    }
    if !c.done() {
        return Err(CodecError::TrailingBytes);
    }
    Ok(CommandFrame {
        command: command.to_uppercase(),
        args,
    })
}

pub fn encode_response(frame: &ResponseFrame) -> Result<Vec<u8>, CodecError> {
    let kind = match frame.kind {
        ResponseKind::Simple => 1,
        ResponseKind::Bulk => 2,
        ResponseKind::Array => 3,
        ResponseKind::Nil => 4,
        ResponseKind::Error => 5,
        ResponseKind::Integer => 6,
        ResponseKind::Pong => 7,
    };
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.push(PROTOCOL_VERSION);
    out.push(FRAME_RESPONSE);
    out.push(kind);
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
                encode_value(&mut out, item.as_ref().unwrap_or(&WireValue::None));
            }
        }
        ResponseKind::Pong if frame.value.is_none() => encode_value(&mut out, &WireValue::None),
        _ => encode_value(&mut out, frame.value.as_ref().unwrap_or(&WireValue::None)),
    }
    Ok(out)
}

pub fn decode_response(payload: &[u8]) -> Result<ResponseFrame, CodecError> {
    let mut c = Cursor::new(payload);
    decode_header(&mut c, FRAME_RESPONSE)?;
    let kind = c.u8()?;
    match kind {
        1 => one(&mut c, ResponseKind::Simple),
        2 => one(&mut c, ResponseKind::Bulk),
        3 => {
            let n = c.u32()? as usize;
            let mut array = Vec::with_capacity(n);
            for _ in 0..n {
                let v = decode_value(&mut c)?;
                array.push(if v == WireValue::None { None } else { Some(v) });
            }
            if !c.done() {
                return Err(CodecError::TrailingBytes);
            }
            Ok(ResponseFrame::array(array))
        }
        4 => {
            if !c.done() {
                return Err(CodecError::TrailingBytes);
            }
            Ok(ResponseFrame::nil())
        }
        5 => {
            let msg = String::from_utf8(c.len_prefixed()?.to_vec())?;
            if !c.done() {
                return Err(CodecError::TrailingBytes);
            }
            Ok(ResponseFrame::error(msg))
        }
        6 => one(&mut c, ResponseKind::Integer),
        7 => one(&mut c, ResponseKind::Pong),
        other => Err(CodecError::UnknownResponseKind(other)),
    }
}
fn one(c: &mut Cursor<'_>, kind: ResponseKind) -> Result<ResponseFrame, CodecError> {
    let v = decode_value(c)?;
    if !c.done() {
        return Err(CodecError::TrailingBytes);
    }
    Ok(ResponseFrame {
        kind,
        value: if v == WireValue::None { None } else { Some(v) },
        array: vec![],
        message: None,
    })
}
fn decode_header(c: &mut Cursor<'_>, expected: u8) -> Result<(), CodecError> {
    if c.bytes(4)? != MAGIC {
        return Err(CodecError::InvalidMagic);
    }
    let version = c.u8()?;
    if version != PROTOCOL_VERSION {
        return Err(CodecError::UnsupportedVersion(version));
    }
    let frame = c.u8()?;
    if frame != expected {
        return Err(CodecError::UnexpectedFrameType(frame));
    }
    Ok(())
}
fn encode_value(out: &mut Vec<u8>, value: &WireValue) {
    match value {
        WireValue::None => out.push(TAG_NONE),
        WireValue::Bytes(b) => {
            out.push(TAG_BYTES);
            pack(out, b);
        }
        WireValue::Str(s) => {
            out.push(TAG_STR);
            pack(out, s.as_bytes());
        }
        WireValue::Json(json) => {
            out.push(TAG_JSON);
            pack(out, json.as_bytes());
        }
    }
}
fn decode_value(c: &mut Cursor<'_>) -> Result<WireValue, CodecError> {
    match c.u8()? {
        TAG_NONE => Ok(WireValue::None),
        TAG_BYTES => Ok(WireValue::Bytes(c.len_prefixed()?.to_vec())),
        TAG_STR => Ok(WireValue::Str(String::from_utf8(
            c.len_prefixed()?.to_vec(),
        )?)),
        TAG_JSON => Ok(WireValue::Json(String::from_utf8(
            c.len_prefixed()?.to_vec(),
        )?)),
        other => Err(CodecError::UnknownValueTag(other)),
    }
}
fn pack(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}
struct Cursor<'a> {
    p: &'a [u8],
    o: usize,
}
impl<'a> Cursor<'a> {
    fn new(p: &'a [u8]) -> Self {
        Self { p, o: 0 }
    }
    fn done(&self) -> bool {
        self.o == self.p.len()
    }
    fn bytes(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        let end = self.o.checked_add(n).ok_or(CodecError::Truncated)?;
        if end > self.p.len() {
            return Err(CodecError::Truncated);
        }
        let out = &self.p[self.o..end];
        self.o = end;
        Ok(out)
    }
    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.bytes(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, CodecError> {
        let b = self.bytes(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }
    fn u32(&mut self) -> Result<u32, CodecError> {
        let b = self.bytes(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn len_prefixed(&mut self) -> Result<&'a [u8], CodecError> {
        let n = self.u32()? as usize;
        self.bytes(n)
    }
}
