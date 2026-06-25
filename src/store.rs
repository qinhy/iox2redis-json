use crate::codec::{
    decode_command, encode_response, CodecError, CommandFrame, ResponseFrame, WireValue,
};
use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

const ROOT_PATHS: &[&str] = &["$", "."];
const DUMP_MAGIC: &[u8; 4] = b"IX2D";

#[derive(Debug)]
pub enum StoreError {
    Codec(CodecError),
    Io(String),
}
impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for StoreError {}
impl From<CodecError> for StoreError {
    fn from(value: CodecError) -> Self {
        Self::Codec(value)
    }
}

#[derive(Clone, Debug)]
struct StoredValue {
    value: WireValue,
    expires_at: Option<Instant>,
    is_json: bool,
}

#[derive(Default, Debug)]
pub struct JsonStore {
    items: HashMap<String, StoredValue>,
}
impl JsonStore {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn handle_payload(&mut self, payload: &[u8]) -> Result<Vec<u8>, StoreError> {
        let response = match decode_command(payload) {
            Ok(command) => self.handle(command),
            Err(error) => ResponseFrame::error(format!("ERR codec {error}")),
        };
        Ok(encode_response(&response)?)
    }
    pub fn handle(&mut self, frame: CommandFrame) -> ResponseFrame {
        match frame.command.as_str() {
            "PING" => {
                if frame.args.is_empty() {
                    ResponseFrame::pong()
                } else {
                    ResponseFrame::bulk(frame.args[0].clone())
                }
            }
            "SET" => self.set(&frame.args),
            "GET" => self.get(&frame.args),
            "DEL" => self.del(&frame.args),
            "EXISTS" => self.exists(&frame.args),
            "MGET" => self.mget(&frame.args),
            "KEYS" => self.keys(&frame.args),
            "DUMP" => self.dump(&frame.args),
            "LOAD" => self.load(&frame.args),
            "JSON.SET" => self.json_set(&frame.args),
            "JSON.GET" => self.json_get(&frame.args),
            cmd => ResponseFrame::error(format!("ERR unsupported command {cmd}")),
        }
    }
    fn purge_if_expired(&mut self, key: &str) {
        if self
            .items
            .get(key)
            .and_then(|i| i.expires_at)
            .is_some_and(|d| d <= Instant::now())
        {
            self.items.remove(key);
        }
    }
    fn get_item(&mut self, key: &str) -> Option<StoredValue> {
        self.purge_if_expired(key);
        self.items.get(key).cloned()
    }
    fn set(&mut self, args: &[WireValue]) -> ResponseFrame {
        if args.len() < 2 {
            return wrong_args("SET");
        }
        let key = args[0].as_lossy_key();
        let old = self.get_item(&key);
        let mut expires_at = None;
        let (mut nx, mut xx, mut get_old) = (false, false, false);
        let mut idx = 2;
        while idx < args.len() {
            let opt = args[idx].as_lossy_key().to_uppercase();
            match opt.as_str() {
                "EX" if idx + 1 < args.len() => {
                    let s = args[idx + 1]
                        .as_lossy_key()
                        .parse::<f64>()
                        .unwrap_or(0.0)
                        .max(0.0);
                    expires_at = Some(Instant::now() + Duration::from_secs_f64(s));
                    idx += 2;
                }
                "PX" if idx + 1 < args.len() => {
                    let ms = args[idx + 1]
                        .as_lossy_key()
                        .parse::<f64>()
                        .unwrap_or(0.0)
                        .max(0.0);
                    expires_at = Some(Instant::now() + Duration::from_secs_f64(ms / 1000.0));
                    idx += 2;
                }
                "NX" => {
                    nx = true;
                    idx += 1;
                }
                "XX" => {
                    xx = true;
                    idx += 1;
                }
                "GET" => {
                    get_old = true;
                    idx += 1;
                }
                _ => return ResponseFrame::error(format!("ERR unsupported SET option {opt}")),
            }
        }
        if nx && xx {
            return ResponseFrame::error("ERR NX and XX options are mutually exclusive");
        }
        if (nx && old.is_some()) || (xx && old.is_none()) {
            return ResponseFrame::nil();
        }
        self.items.insert(
            key,
            StoredValue {
                value: args[1].clone(),
                expires_at,
                is_json: false,
            },
        );
        if get_old {
            old.map_or_else(ResponseFrame::nil, |i| ResponseFrame::bulk(i.value))
        } else {
            ResponseFrame::simple("OK")
        }
    }
    fn get(&mut self, args: &[WireValue]) -> ResponseFrame {
        if args.len() != 1 {
            return wrong_args("GET");
        }
        match self.get_item(&args[0].as_lossy_key()) {
            Some(i) if i.is_json => ResponseFrame::bulk(json_text_value(&i.value)),
            Some(i) => ResponseFrame::bulk(i.value),
            None => ResponseFrame::nil(),
        }
    }
    fn del(&mut self, args: &[WireValue]) -> ResponseFrame {
        let mut n = 0;
        for raw in args {
            let key = raw.as_lossy_key();
            self.purge_if_expired(&key);
            if self.items.remove(&key).is_some() {
                n += 1;
            }
        }
        ResponseFrame::integer(n)
    }
    fn exists(&mut self, args: &[WireValue]) -> ResponseFrame {
        let mut n = 0;
        for raw in args {
            if self.get_item(&raw.as_lossy_key()).is_some() {
                n += 1;
            }
        }
        ResponseFrame::integer(n)
    }
    fn mget(&mut self, args: &[WireValue]) -> ResponseFrame {
        ResponseFrame::array(
            args.iter()
                .map(|raw| {
                    self.get_item(&raw.as_lossy_key()).map(|i| {
                        if i.is_json {
                            json_text_value(&i.value)
                        } else {
                            i.value
                        }
                    })
                })
                .collect(),
        )
    }
    fn keys(&mut self, args: &[WireValue]) -> ResponseFrame {
        if args.len() != 1 {
            return wrong_args("KEYS");
        }
        let pattern = args[0].as_lossy_key();
        let keys: Vec<String> = self.items.keys().cloned().collect();
        for key in &keys {
            self.purge_if_expired(key);
        }
        ResponseFrame::array(
            self.items
                .keys()
                .filter(|k| glob(&pattern, k))
                .cloned()
                .map(WireValue::Str)
                .map(Some)
                .collect(),
        )
    }
    fn dump(&mut self, args: &[WireValue]) -> ResponseFrame {
        if args.len() != 1 {
            return wrong_args("DUMP");
        }
        match self.get_item(&args[0].as_lossy_key()) {
            Some(i) => ResponseFrame::bulk(WireValue::Bytes(encode_dump(&i))),
            None => ResponseFrame::nil(),
        }
    }
    fn load(&mut self, args: &[WireValue]) -> ResponseFrame {
        if args.len() < 2 {
            return wrong_args("LOAD");
        }
        let key = args[0].as_lossy_key();
        let old = self.get_item(&key);
        let (mut nx, mut xx) = (false, false);
        for opt in &args[2..] {
            match opt.as_lossy_key().to_uppercase().as_str() {
                "NX" => nx = true,
                "XX" => xx = true,
                other => {
                    return ResponseFrame::error(format!("ERR unsupported LOAD option {other}"))
                }
            }
        }
        if nx && xx {
            return ResponseFrame::error("ERR NX and XX options are mutually exclusive");
        }
        if (nx && old.is_some()) || (xx && old.is_none()) {
            return ResponseFrame::nil();
        }
        match args[1].to_bytes().and_then(|b| decode_dump(&b)) {
            Some(item) => {
                self.items.insert(key, item);
                ResponseFrame::simple("OK")
            }
            None => ResponseFrame::error("ERR invalid dump payload"),
        }
    }
    fn json_set(&mut self, args: &[WireValue]) -> ResponseFrame {
        if args.len() < 3 {
            return wrong_args("JSON.SET");
        }
        let key = args[0].as_lossy_key();
        let path = args[1].as_lossy_key();
        if !ROOT_PATHS.contains(&path.as_str()) {
            return ResponseFrame::error("ERR only root path '$' is supported");
        }
        let old = self.get_item(&key);
        let (mut nx, mut xx) = (false, false);
        for opt in &args[3..] {
            match opt.as_lossy_key().to_uppercase().as_str() {
                "NX" => nx = true,
                "XX" => xx = true,
                other => {
                    return ResponseFrame::error(format!("ERR unsupported JSON.SET option {other}"))
                }
            }
        }
        if nx && xx {
            return ResponseFrame::error("ERR NX and XX options are mutually exclusive");
        }
        if (nx && old.is_some()) || (xx && old.is_none()) {
            return ResponseFrame::nil();
        }
        let text = String::from_utf8_lossy(&args[2].to_bytes().unwrap_or_default()).into_owned();
        if !looks_like_json(&text) {
            return ResponseFrame::error("ERR invalid JSON");
        }
        self.items.insert(
            key,
            StoredValue {
                value: WireValue::Str(compact_json_text(&text)),
                expires_at: None,
                is_json: true,
            },
        );
        ResponseFrame::simple("OK")
    }
    fn json_get(&mut self, args: &[WireValue]) -> ResponseFrame {
        if !(1..=2).contains(&args.len()) {
            return wrong_args("JSON.GET");
        }
        if args.len() == 2 && !ROOT_PATHS.contains(&args[1].as_lossy_key().as_str()) {
            return ResponseFrame::error("ERR only root path '$' is supported");
        }
        match self.get_item(&args[0].as_lossy_key()) {
            Some(i) => ResponseFrame::bulk(json_text_value(&i.value)),
            None => ResponseFrame::nil(),
        }
    }
}
fn wrong_args(command: &str) -> ResponseFrame {
    ResponseFrame::error(format!("ERR wrong number of arguments for {command}"))
}
fn json_text_value(value: &WireValue) -> WireValue {
    match value {
        WireValue::Str(s) => WireValue::Str(s.clone()),
        WireValue::Bytes(b) => WireValue::Str(String::from_utf8_lossy(b).into_owned()),
        WireValue::Json(json) => WireValue::Str(json.clone()),
        WireValue::None => WireValue::Str("null".into()),
    }
}
fn looks_like_json(text: &str) -> bool {
    let t = text.trim();
    matches!(t.as_bytes().first(), Some(b'{') | Some(b'[') | Some(b'"'))
        || matches!(t, "true" | "false" | "null")
        || t.parse::<f64>().is_ok()
}
fn compact_json_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut in_str = false;
    let mut esc = false;
    for ch in text.chars() {
        if in_str {
            out.push(ch);
            if esc {
                esc = false;
            } else if ch == '\\' {
                esc = true;
            } else if ch == '"' {
                in_str = false;
            }
        } else if ch == '"' {
            in_str = true;
            out.push(ch);
        } else if !ch.is_whitespace() {
            out.push(ch);
        }
    }
    out
}
fn encode_dump(item: &StoredValue) -> Vec<u8> {
    let mut out = DUMP_MAGIC.to_vec();
    let (kind, data) = match &item.value {
        WireValue::Bytes(bytes) => ("bytes", base64_encode(bytes)),
        WireValue::Str(text) => ("str", json_escape(text)),
        WireValue::Json(json) => ("json", json.clone()),
        WireValue::None => ("json", "null".to_owned()),
    };
    let payload = if kind == "json" {
        format!(
            r#"{{"v":1,"is_json":{},"ttl_ms":null,"value":{{"type":"json","data":{}}}}}"#,
            item.is_json, data
        )
    } else {
        format!(
            r#"{{"v":1,"is_json":{},"ttl_ms":null,"value":{{"type":"{}","data":"{}"}}}}"#,
            item.is_json, kind, data
        )
    };
    out.extend_from_slice(payload.as_bytes());
    out
}

fn decode_dump(payload: &[u8]) -> Option<StoredValue> {
    if !payload.starts_with(DUMP_MAGIC) {
        return None;
    }
    let text = std::str::from_utf8(&payload[DUMP_MAGIC.len()..]).ok()?;
    let is_json = text.contains(r#""is_json":true"#);
    let value = if text.contains(r#""type":"bytes""#) {
        WireValue::Bytes(base64_decode(&extract_json_string_field(text, "data")?).ok()?)
    } else if text.contains(r#""type":"str""#) {
        WireValue::Str(extract_json_string_field(text, "data")?)
    } else if text.contains(r#""type":"json""#) {
        WireValue::Json(extract_json_data_value(text)?)
    } else {
        return None;
    };
    Some(StoredValue {
        value,
        expires_at: None,
        is_json,
    })
}

fn json_escape(text: &str) -> String {
    let mut out = String::new();
    for ch in text.chars() {
        match ch {
            '"' => out.push_str(r#"\""#),
            '\\' => out.push_str(r#"\\"#),
            '\n' => out.push_str(r#"\n"#),
            '\r' => out.push_str(r#"\r"#),
            '\t' => out.push_str(r#"\t"#),
            c if c.is_control() => out.push_str(&format!(r#"\u{:04x}"#, c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn extract_json_string_field(text: &str, field: &str) -> Option<String> {
    let marker = format!(r#""{field}":"#);
    let start = text.find(&marker)? + marker.len();
    let bytes = text.as_bytes();
    if bytes.get(start) != Some(&b'"') {
        return None;
    }
    let mut out = String::new();
    let mut idx = start + 1;
    while idx < bytes.len() {
        match bytes[idx] {
            b'"' => return Some(out),
            b'\\' => {
                idx += 1;
                match *bytes.get(idx)? {
                    b'"' => out.push('"'),
                    b'\\' => out.push('\\'),
                    b'/' => out.push('/'),
                    b'n' => out.push('\n'),
                    b'r' => out.push('\r'),
                    b't' => out.push('\t'),
                    other => out.push(other as char),
                }
            }
            b => out.push(b as char),
        }
        idx += 1;
    }
    None
}

fn extract_json_data_value(text: &str) -> Option<String> {
    let marker = r#""data":"#;
    let start = text.find(marker)? + marker.len();
    let end = text[start..].rfind("}}").map(|n| start + n)?;
    Some(text[start..end].trim().to_owned())
}

fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        out.push(TABLE[(b0 >> 2) as usize] as char);
        out.push(TABLE[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        out.push(if chunk.len() > 1 {
            TABLE[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[(b2 & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

fn base64_decode(text: &str) -> Result<Vec<u8>, ()> {
    let mut out = Vec::new();
    let mut buf = [0u8; 4];
    for chunk in text.as_bytes().chunks(4) {
        if chunk.len() != 4 {
            return Err(());
        }
        let mut pad = 0;
        for (idx, &b) in chunk.iter().enumerate() {
            if b == b'=' {
                buf[idx] = 0;
                pad += 1;
            } else {
                buf[idx] = base64_value(b).ok_or(())?;
            }
        }
        out.push((buf[0] << 2) | (buf[1] >> 4));
        if pad < 2 {
            out.push((buf[1] << 4) | (buf[2] >> 2));
        }
        if pad < 1 {
            out.push((buf[2] << 6) | buf[3]);
        }
    }
    Ok(out)
}
fn base64_value(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}
fn glob(pattern: &str, text: &str) -> bool {
    fn go(p: &[u8], t: &[u8]) -> bool {
        if p.is_empty() {
            return t.is_empty();
        }
        match p[0] {
            b'*' => go(&p[1..], t) || (!t.is_empty() && go(p, &t[1..])),
            b'?' => !t.is_empty() && go(&p[1..], &t[1..]),
            b'\\' => p.len() > 1 && !t.is_empty() && p[1] == t[0] && go(&p[2..], &t[1..]),
            c => !t.is_empty() && c == t[0] && go(&p[1..], &t[1..]),
        }
    }
    go(pattern.as_bytes(), text.as_bytes())
}
