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
        WireValue::Int(i) => WireValue::Str(i.to_string()),
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
    out.push(if item.is_json { 1 } else { 0 });
    let data = item.value.to_bytes().unwrap_or_default();
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    out.extend_from_slice(&data);
    out
}
fn decode_dump(payload: &[u8]) -> Option<StoredValue> {
    if !payload.starts_with(DUMP_MAGIC) || payload.len() < 9 {
        return None;
    }
    let is_json = payload[4] == 1;
    let len = u32::from_be_bytes([payload[5], payload[6], payload[7], payload[8]]) as usize;
    if payload.len() != 9 + len {
        return None;
    }
    Some(StoredValue {
        value: if is_json {
            WireValue::Str(String::from_utf8_lossy(&payload[9..]).into_owned())
        } else {
            WireValue::Bytes(payload[9..].to_vec())
        },
        expires_at: None,
        is_json,
    })
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
