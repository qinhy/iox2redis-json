use crate::codec::{
    decode_command, decode_json_value, encode_json_value, encode_response, value_to_compact_json,
    CodecError, CommandFrame, ResponseFrame, WireValue,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

pub const SERVER_NAME: &str = "iox2redis-json";
pub const DEFAULT_MAX_PAYLOAD_SIZE: usize = 64 * 1024;
pub const DEFAULT_POLL_NS: u64 = 100_000;
pub const CONST_KEY_PREFIX: &str = "const:";
pub const SERVER_INFO_KEY: &str = "const:server_info";
const DUMP_MAGIC: &[u8; 4] = b"IX2D";
const DUMP_FORMAT_VERSION: u8 = 1;

const ROOT_PATHS: &[&str] = &["$", "."];

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("codec error: {0}")]
    Codec(#[from] CodecError),
    #[error("io error: {0}")]
    Io(String),
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Clone, Debug)]
struct StoredValue {
    value: WireValue,
    expires_at: Option<Instant>,
    is_json: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ServerInfo {
    pub name: String,
    pub service_path: String,
    pub max_payload_size: usize,
    pub max_payload_size_text: String,
    pub poll_value: u64,
    pub poll_unit: String,
    pub poll_is_default: bool,
    pub const_key_prefix: String,
    pub server_info_key: String,
    pub poll_text: String,
}

impl ServerInfo {
    pub fn ready_message(&self) -> String {
        format!("IOX2REDIS_READY {}", self.service_path)
    }

    pub fn to_json_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or_else(|_| json!({}))
    }
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub service_name: String,
    pub max_payload_size: usize,
    pub poll_ns: Option<u64>,
    pub poll_ms: Option<u64>,
}

impl ServerConfig {
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: crate::transport::service_name_from_host(&service_name.into()),
            max_payload_size: DEFAULT_MAX_PAYLOAD_SIZE,
            poll_ns: None,
            poll_ms: None,
        }
    }

    pub fn poll_duration(&self) -> Duration {
        Duration::from_nanos(self.effective_poll_ns())
    }

    pub fn effective_poll_ns(&self) -> u64 {
        if let Some(ns) = self.poll_ns {
            ns
        } else if let Some(ms) = self.poll_ms {
            ms.saturating_mul(1_000_000)
        } else {
            DEFAULT_POLL_NS
        }
    }

    pub fn validate(&self) -> Result<(), StoreError> {
        if self.service_name.trim_matches('/').is_empty() {
            return Err(StoreError::InvalidConfig(
                "service name must not be empty".to_owned(),
            ));
        }
        if self.max_payload_size == 0 {
            return Err(StoreError::InvalidConfig(
                "max_payload_size must be greater than zero".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn info(&self) -> ServerInfo {
        let service_name = crate::transport::service_name_from_host(&self.service_name);
        let (poll_value, poll_unit, poll_is_default) = if let Some(ns) = self.poll_ns {
            (ns, "ns".to_owned(), false)
        } else if let Some(ms) = self.poll_ms {
            (ms, "ms".to_owned(), false)
        } else {
            (DEFAULT_POLL_NS, "ns".to_owned(), true)
        };
        let poll_text = format!(
            "{} {}{}",
            poll_value,
            poll_unit,
            if poll_is_default { " (default)" } else { "" }
        );
        ServerInfo {
            name: SERVER_NAME.to_owned(),
            service_path: format!("/{service_name}/"),
            max_payload_size: self.max_payload_size,
            max_payload_size_text: format_bytes(self.max_payload_size),
            poll_value,
            poll_unit,
            poll_is_default,
            const_key_prefix: CONST_KEY_PREFIX.to_owned(),
            server_info_key: SERVER_INFO_KEY.to_owned(),
            poll_text,
        }
    }
}

#[derive(Debug, Default)]
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
            "PING" => self.ping(&frame.args),
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

    fn ping(&mut self, args: &[WireValue]) -> ResponseFrame {
        if args.is_empty() {
            ResponseFrame::pong()
        } else {
            ResponseFrame::bulk(args[0].clone())
        }
    }

    fn set(&mut self, args: &[WireValue]) -> ResponseFrame {
        self.set_inner(args, false)
    }

    fn set_inner(&mut self, args: &[WireValue], const_mode: bool) -> ResponseFrame {
        if args.len() < 2 {
            return wrong_args("SET");
        }
        let key = key_to_str(&args[0]);
        if const_mode {
            if !is_const_key(&key) {
                return const_prefix_error();
            }
            if self.key_exists(&key) {
                return const_key_error(&key);
            }
        }

        let old = self.get_item(&key);
        let mut expires_at = None;
        let mut nx = false;
        let mut xx = false;
        let mut get_old = false;
        let mut idx = 2;

        while idx < args.len() {
            let opt = key_to_str(&args[idx]).to_uppercase();
            match opt.as_str() {
                "EX" if idx + 1 < args.len() => {
                    if const_mode {
                        return ResponseFrame::error("ERR expiration is not allowed for constant keys");
                    }
                    let seconds = match parse_nonnegative_float(&args[idx + 1]) {
                        Ok(seconds) => seconds,
                        Err(message) => return ResponseFrame::error(message),
                    };
                    expires_at = Some(Instant::now() + Duration::from_secs_f64(seconds));
                    idx += 2;
                }
                "PX" if idx + 1 < args.len() => {
                    if const_mode {
                        return ResponseFrame::error("ERR expiration is not allowed for constant keys");
                    }
                    let millis = match parse_nonnegative_float(&args[idx + 1]) {
                        Ok(millis) => millis,
                        Err(message) => return ResponseFrame::error(message),
                    };
                    expires_at = Some(Instant::now() + Duration::from_secs_f64(millis / 1000.0));
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
            old.map_or_else(ResponseFrame::nil, |item| ResponseFrame::bulk(item.value))
        } else {
            ResponseFrame::simple("OK")
        }
    }

    fn get(&mut self, args: &[WireValue]) -> ResponseFrame {
        if args.len() != 1 {
            return wrong_args("GET");
        }
        match self.get_item(&key_to_str(&args[0])) {
            Some(item) => ResponseFrame::bulk(value_for_get(&item)),
            None => ResponseFrame::nil(),
        }
    }

    fn del(&mut self, args: &[WireValue]) -> ResponseFrame {
        let mut count = 0;
        for raw_key in args {
            let key = key_to_str(raw_key);
            self.purge_if_expired(&key);
            if self.items.remove(&key).is_some() {
                count += 1;
            }
        }
        ResponseFrame::integer(count)
    }

    fn exists(&mut self, args: &[WireValue]) -> ResponseFrame {
        let mut count = 0;
        for raw_key in args {
            if self.key_exists(&key_to_str(raw_key)) {
                count += 1;
            }
        }
        ResponseFrame::integer(count)
    }

    fn mget(&mut self, args: &[WireValue]) -> ResponseFrame {
        ResponseFrame::array(
            args.iter()
                .map(|raw_key| self.value_for_mget(&key_to_str(raw_key)))
                .collect(),
        )
    }

    fn keys(&mut self, args: &[WireValue]) -> ResponseFrame {
        if args.len() != 1 {
            return wrong_args("KEYS");
        }
        let pattern = key_to_str(&args[0]);
        ResponseFrame::array(
            self.matching_keys(&pattern)
                .into_iter()
                .map(WireValue::Str)
                .map(Some)
                .collect(),
        )
    }

    fn dump(&mut self, args: &[WireValue]) -> ResponseFrame {
        if args.len() != 1 {
            return wrong_args("DUMP");
        }
        match self.get_item(&key_to_str(&args[0])) {
            Some(item) => ResponseFrame::bulk(WireValue::Bytes(encode_dump_payload(&item))),
            None => ResponseFrame::nil(),
        }
    }

    fn load(&mut self, args: &[WireValue]) -> ResponseFrame {
        self.load_inner(args, false)
    }

    fn load_inner(&mut self, args: &[WireValue], const_mode: bool) -> ResponseFrame {
        if args.len() < 2 {
            return wrong_args("LOAD");
        }
        let key = key_to_str(&args[0]);
        if const_mode && !is_const_key(&key) {
            return const_prefix_error();
        }

        let old = self.get_item(&key);
        let mut nx = false;
        let mut xx = false;
        for raw_opt in &args[2..] {
            let opt = key_to_str(raw_opt).to_uppercase();
            match opt.as_str() {
                "NX" => nx = true,
                "XX" => xx = true,
                _ => return ResponseFrame::error(format!("ERR unsupported LOAD option {opt}")),
            }
        }

        if nx && xx {
            return ResponseFrame::error("ERR NX and XX options are mutually exclusive");
        }

        if const_mode {
            if old.is_some() {
                return if nx { ResponseFrame::nil() } else { const_key_error(&key) };
            }
            if xx {
                return ResponseFrame::nil();
            }
        } else if (nx && old.is_some()) || (xx && old.is_none()) {
            return ResponseFrame::nil();
        }

        let item = match decode_dump_payload(&args[1]) {
            Ok(item) => item,
            Err(error) => {
                return ResponseFrame::error(format!("ERR invalid dump payload: {error}"));
            }
        };

        if const_mode && item.expires_at.is_some() {
            return ResponseFrame::error("ERR expiration is not allowed for constant keys");
        }

        self.items.insert(key, item);
        ResponseFrame::simple("OK")
    }

    fn json_set(&mut self, args: &[WireValue]) -> ResponseFrame {
        self.json_set_inner(args, false)
    }

    fn json_set_inner(&mut self, args: &[WireValue], const_mode: bool) -> ResponseFrame {
        if args.len() < 3 {
            return wrong_args("JSON.SET");
        }
        let key = key_to_str(&args[0]);
        if const_mode {
            if !is_const_key(&key) {
                return const_prefix_error();
            }
            if self.key_exists(&key) {
                return const_key_error(&key);
            }
        }

        let path = key_to_str(&args[1]);
        if !ROOT_PATHS.contains(&path.as_str()) {
            return ResponseFrame::error("ERR only root path '$' is supported");
        }

        let old = self.get_item(&key);
        let mut nx = false;
        let mut xx = false;
        for raw_opt in &args[3..] {
            let opt = key_to_str(raw_opt).to_uppercase();
            match opt.as_str() {
                "NX" => nx = true,
                "XX" => xx = true,
                _ => return ResponseFrame::error(format!("ERR unsupported JSON.SET option {opt}")),
            }
        }

        if nx && xx {
            return ResponseFrame::error("ERR NX and XX options are mutually exclusive");
        }
        if (nx && old.is_some()) || (xx && old.is_none()) {
            return ResponseFrame::nil();
        }

        let text = value_to_json_text(&args[2]);
        let value = match serde_json::from_str::<Value>(&text) {
            Ok(value) => value,
            Err(error) => return ResponseFrame::error(format!("ERR invalid JSON: {}", error)),
        };

        self.items.insert(
            key,
            StoredValue {
                value: WireValue::Json(value),
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
        if args.len() == 2 && !ROOT_PATHS.contains(&key_to_str(&args[1]).as_str()) {
            return ResponseFrame::error("ERR only root path '$' is supported");
        }

        match self.get_item(&key_to_str(&args[0])) {
            Some(item) => ResponseFrame::bulk(value_for_json_get(&item.value)),
            None => ResponseFrame::nil(),
        }
    }

    fn purge_if_expired(&mut self, key: &str) {
        let expired = self
            .items
            .get(key)
            .and_then(|item| item.expires_at)
            .is_some_and(|expires_at| expires_at <= Instant::now());
        if expired {
            self.items.remove(key);
        }
    }

    fn get_item(&mut self, key: &str) -> Option<StoredValue> {
        self.purge_if_expired(key);
        self.items.get(key).cloned()
    }

    pub fn key_exists(&mut self, key: &str) -> bool {
        self.get_item(key).is_some()
    }

    pub fn value_for_mget(&mut self, key: &str) -> Option<WireValue> {
        self.get_item(key).map(|item| value_for_get(&item))
    }

    pub fn matching_keys(&mut self, pattern: &str) -> Vec<String> {
        let matcher = match compile_redis_glob(pattern) {
            Ok(matcher) => matcher,
            Err(_) => return Vec::new(),
        };
        let keys: Vec<String> = self.items.keys().cloned().collect();
        for key in &keys {
            self.purge_if_expired(key);
        }
        self.items
            .keys()
            .filter(|key| matcher.is_match(key))
            .cloned()
            .collect()
    }
}

#[derive(Debug, Default)]
pub struct ConstJsonStore {
    inner: JsonStore,
}

impl ConstJsonStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn initialize_json(&mut self, key: &str, value: Value) -> Result<(), StoreError> {
        if !is_const_key(key) {
            return Err(StoreError::InvalidConfig(format!(
                "constant keys must start with {CONST_KEY_PREFIX:?}"
            )));
        }
        if self.inner.key_exists(key) {
            return Err(StoreError::InvalidConfig(format!(
                "constant key is already set: {key}"
            )));
        }
        self.inner.items.insert(
            key.to_owned(),
            StoredValue {
                value: WireValue::Json(value),
                expires_at: None,
                is_json: true,
            },
        );
        Ok(())
    }

    pub fn handle(&mut self, frame: CommandFrame) -> ResponseFrame {
        match frame.command.as_str() {
            "SET" => self.inner.set_inner(&frame.args, true),
            "JSON.SET" => self.inner.json_set_inner(&frame.args, true),
            "DEL" => ResponseFrame::error("ERR constant keys cannot be deleted"),
            "LOAD" => self.inner.load_inner(&frame.args, true),
            _ => self.inner.handle(frame),
        }
    }

    pub fn key_exists(&mut self, key: &str) -> bool {
        self.inner.key_exists(key)
    }

    pub fn value_for_mget(&mut self, key: &str) -> Option<WireValue> {
        self.inner.value_for_mget(key)
    }

    pub fn matching_keys(&mut self, pattern: &str) -> Vec<String> {
        self.inner.matching_keys(pattern)
    }
}

pub struct Iox2JsonServer {
    pub config: ServerConfig,
    pub info: ServerInfo,
    store: JsonStore,
    const_store: ConstJsonStore,
}

impl Iox2JsonServer {
    pub fn new(config: ServerConfig) -> Result<Self, StoreError> {
        config.validate()?;
        let info = config.info();
        let mut const_store = ConstJsonStore::new();
        const_store.initialize_json(SERVER_INFO_KEY, info.to_json_value())?;
        Ok(Self {
            config,
            info,
            store: JsonStore::new(),
            const_store,
        })
    }

    pub fn with_stores(
        config: ServerConfig,
        store: JsonStore,
        mut const_store: ConstJsonStore,
    ) -> Result<Self, StoreError> {
        config.validate()?;
        let info = config.info();
        if const_store.key_exists(SERVER_INFO_KEY) {
            return Err(StoreError::InvalidConfig(format!(
                "{SERVER_INFO_KEY:?} is reserved by the server"
            )));
        }
        const_store.initialize_json(SERVER_INFO_KEY, info.to_json_value())?;
        Ok(Self {
            config,
            info,
            store,
            const_store,
        })
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
            "GET" | "SET" | "JSON.GET" | "JSON.SET" | "DUMP" | "LOAD" => {
                if frame.args.is_empty() {
                    return self.store.handle(frame);
                }
                let key = key_to_str(&frame.args[0]);
                if is_const_key(&key) {
                    self.const_store.handle(frame)
                } else {
                    self.store.handle(frame)
                }
            }
            "DEL" => self.routed_del(&frame.args),
            "EXISTS" => self.routed_exists(&frame.args),
            "MGET" => self.routed_mget(&frame.args),
            "KEYS" => self.routed_keys(&frame.args),
            _ => self.store.handle(frame),
        }
    }

    fn routed_del(&mut self, args: &[WireValue]) -> ResponseFrame {
        if args.iter().any(|raw| is_const_key(&key_to_str(raw))) {
            return ResponseFrame::error("ERR constant keys cannot be deleted");
        }
        self.store.handle(CommandFrame {
            command: "DEL".to_owned(),
            args: args.to_vec(),
        })
    }

    fn routed_exists(&mut self, args: &[WireValue]) -> ResponseFrame {
        let mut count = 0;
        for raw_key in args {
            let key = key_to_str(raw_key);
            let exists = if is_const_key(&key) {
                self.const_store.key_exists(&key)
            } else {
                self.store.key_exists(&key)
            };
            if exists {
                count += 1;
            }
        }
        ResponseFrame::integer(count)
    }

    fn routed_mget(&mut self, args: &[WireValue]) -> ResponseFrame {
        let mut values = Vec::with_capacity(args.len());
        for raw_key in args {
            let key = key_to_str(raw_key);
            values.push(if is_const_key(&key) {
                self.const_store.value_for_mget(&key)
            } else {
                self.store.value_for_mget(&key)
            });
        }
        ResponseFrame::array(values)
    }

    fn routed_keys(&mut self, args: &[WireValue]) -> ResponseFrame {
        if args.len() != 1 {
            return wrong_args("KEYS");
        }
        let pattern = key_to_str(&args[0]);
        let mut seen = HashSet::new();
        let mut keys = Vec::new();
        for key in self
            .store
            .matching_keys(&pattern)
            .into_iter()
            .chain(self.const_store.matching_keys(&pattern))
        {
            if seen.insert(key.clone()) {
                keys.push(Some(WireValue::Str(key)));
            }
        }
        ResponseFrame::array(keys)
    }
}

#[derive(Serialize, Deserialize)]
struct DumpFrame {
    v: u8,
    is_json: bool,
    ttl_ms: Option<u64>,
    value: Value,
}

fn encode_dump_payload(item: &StoredValue) -> Vec<u8> {
    let ttl_ms = item.expires_at.map(|expires_at| {
        expires_at
            .saturating_duration_since(Instant::now())
            .as_millis()
            .min(u128::from(u64::MAX)) as u64
    });
    let frame = DumpFrame {
        v: DUMP_FORMAT_VERSION,
        is_json: item.is_json,
        ttl_ms,
        value: encode_json_value(&item.value),
    };
    let mut out = DUMP_MAGIC.to_vec();
    out.extend_from_slice(
        serde_json::to_string(&frame)
            .unwrap_or_else(|_| "{}".to_owned())
            .as_bytes(),
    );
    out
}

fn decode_dump_payload(value: &WireValue) -> Result<StoredValue, StoreError> {
    let raw = value
        .to_bytes()
        .ok_or_else(|| StoreError::Io("dump payload must be bytes or str".to_owned()))?;
    if !raw.starts_with(DUMP_MAGIC) {
        return Err(StoreError::Io("invalid dump payload magic".to_owned()));
    }
    let frame: DumpFrame = serde_json::from_slice(&raw[DUMP_MAGIC.len()..])?;
    if frame.v != DUMP_FORMAT_VERSION {
        return Err(StoreError::Io(format!(
            "unsupported dump format version: {}",
            frame.v
        )));
    }
    let decoded = decode_json_value(&frame.value)?;
    let expires_at = frame
        .ttl_ms
        .map(|ttl| Instant::now() + Duration::from_millis(ttl));
    Ok(StoredValue {
        value: decoded,
        expires_at,
        is_json: frame.is_json,
    })
}

fn key_to_str(value: &WireValue) -> String {
    value.as_lossy_key()
}

fn value_for_get(item: &StoredValue) -> WireValue {
    if item.is_json {
        value_for_json_get(&item.value)
    } else {
        item.value.clone()
    }
}

fn value_for_json_get(value: &WireValue) -> WireValue {
    match value {
        WireValue::None => WireValue::Str("null".to_owned()),
        WireValue::Bytes(bytes) => WireValue::Str(String::from_utf8_lossy(bytes).into_owned()),
        WireValue::Str(text) => {
            if serde_json::from_str::<Value>(text).is_ok() {
                WireValue::Str(compact_json_text(text))
            } else {
                WireValue::Str(value_to_compact_json(&Value::String(text.clone())))
            }
        }
        WireValue::Json(value) => WireValue::Str(value_to_compact_json(value)),
    }
}

fn value_to_json_text(value: &WireValue) -> String {
    match value {
        WireValue::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        WireValue::Str(text) => text.clone(),
        WireValue::Json(value) => value_to_compact_json(value),
        WireValue::None => "null".to_owned(),
    }
}

fn compact_json_text(text: &str) -> String {
    serde_json::from_str::<Value>(text)
        .map(|value| value_to_compact_json(&value))
        .unwrap_or_else(|_| text.to_owned())
}

fn parse_nonnegative_float(value: &WireValue) -> Result<f64, String> {
    let text = key_to_str(value);
    let parsed = text
        .parse::<f64>()
        .map_err(|_| "ERR invalid expire time in SET".to_owned())?;
    if !parsed.is_finite() || parsed < 0.0 {
        return Err("ERR invalid expire time in SET".to_owned());
    }
    Ok(parsed)
}

fn wrong_args(command: &str) -> ResponseFrame {
    ResponseFrame::error(format!("ERR wrong number of arguments for {command}"))
}

fn is_const_key(key: &str) -> bool {
    key.starts_with(CONST_KEY_PREFIX)
}

fn const_key_error(key: &str) -> ResponseFrame {
    ResponseFrame::error(format!("ERR constant key {key:?} is already set"))
}

fn const_prefix_error() -> ResponseFrame {
    ResponseFrame::error(format!("ERR constant key must start with {CONST_KEY_PREFIX:?}"))
}

fn format_bytes(size: usize) -> String {
    let units = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = size as f64;
    for unit in units {
        if value.abs() < 1024.0 || unit == "TiB" {
            if value.fract() == 0.0 {
                return format!("{} {unit}", value as u64);
            }
            return format!("{value:.1} {unit}");
        }
        value /= 1024.0;
    }
    format!("{size} B")
}

fn compile_redis_glob(pattern: &str) -> Result<Regex, regex::Error> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut idx = 0;
    let mut regex = String::from(r"\A");

    while idx < chars.len() {
        match chars[idx] {
            '*' => {
                regex.push_str(".*");
                idx += 1;
            }
            '?' => {
                regex.push('.');
                idx += 1;
            }
            '\\' => {
                idx += 1;
                if idx < chars.len() {
                    regex.push_str(&regex::escape(&chars[idx].to_string()));
                    idx += 1;
                } else {
                    regex.push_str(&regex::escape("\\"));
                }
            }
            '[' => {
                let start = idx;
                idx += 1;
                let mut negated = false;
                if idx < chars.len() && matches!(chars[idx], '^' | '!') {
                    negated = true;
                    idx += 1;
                }
                let mut class = String::new();
                if idx < chars.len() && chars[idx] == ']' {
                    class.push_str(r"\]");
                    idx += 1;
                }
                let mut closed = false;
                while idx < chars.len() {
                    let ch = chars[idx];
                    if ch == ']' {
                        closed = true;
                        idx += 1;
                        break;
                    }
                    if ch == '\\' && idx + 1 < chars.len() {
                        idx += 1;
                        class.push_str(&regex::escape(&chars[idx].to_string()));
                    } else if ch == '-' {
                        class.push('-');
                    } else {
                        class.push_str(&regex::escape(&ch.to_string()));
                    }
                    idx += 1;
                }
                if closed && !class.is_empty() {
                    regex.push('[');
                    if negated {
                        regex.push('^');
                    }
                    regex.push_str(&class);
                    regex.push(']');
                } else {
                    regex.push_str(&regex::escape("["));
                    idx = start + 1;
                }
            }
            ch => {
                regex.push_str(&regex::escape(&ch.to_string()));
                idx += 1;
            }
        }
    }

    regex.push_str(r"\z");
    Regex::new(&regex)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::encode_command;
    use std::thread;

    fn exec(store: &mut JsonStore, parts: Vec<WireValue>) -> ResponseFrame {
        let payload = encode_command(parts).unwrap();
        let command = crate::codec::decode_command(&payload).unwrap();
        store.handle(command)
    }

    #[test]
    fn json_set_rejects_invalid_json() {
        let mut store = JsonStore::new();
        let response = exec(
            &mut store,
            vec!["JSON.SET".into(), "a".into(), "$".into(), "{".into()],
        );
        assert_eq!(response.kind, crate::codec::ResponseKind::Error);
    }

    #[test]
    fn dump_load_preserves_ttl() {
        let mut store = JsonStore::new();
        let _ = exec(
            &mut store,
            vec![
                "SET".into(),
                "a".into(),
                "b".into(),
                "PX".into(),
                "80".into(),
            ],
        );
        let dump = exec(&mut store, vec!["DUMP".into(), "a".into()]);
        let payload = dump.value.unwrap();
        let _ = exec(&mut store, vec!["LOAD".into(), "copy".into(), payload]);
        thread::sleep(Duration::from_millis(120));
        let got = exec(&mut store, vec!["GET".into(), "copy".into()]);
        assert_eq!(got.kind, crate::codec::ResponseKind::Nil);
    }

    #[test]
    fn keys_support_character_classes() {
        let mut store = JsonStore::new();
        let _ = exec(&mut store, vec!["SET".into(), "foo1".into(), "x".into()]);
        let _ = exec(&mut store, vec!["SET".into(), "foo2".into(), "x".into()]);
        let response = exec(&mut store, vec!["KEYS".into(), "foo[12]".into()]);
        assert_eq!(response.array.len(), 2);
    }
}
