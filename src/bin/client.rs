use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use clap::{Parser, Subcommand, ValueEnum};
use iox2redis_json::client::response_to_redis_value;
use iox2redis_json::codec::{decode_response, encode_command, WireValue};
use iox2redis_json::transport;

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum TransportKind {
    Hex,
    Iox2,
}

#[derive(Parser, Debug)]
#[command(about = "Encode or send iox2redis-json commands")]
struct Args {
    #[arg(long, value_enum, default_value_t = TransportKind::Hex)]
    transport: TransportKind,

    #[arg(long, default_value = "/your/topic/to/iox2_server/")]
    service: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Ping,
    Set {
        key: String,
        value: String,
        #[arg(long)]
        ex: Option<f64>,
        #[arg(long)]
        px: Option<f64>,
        #[arg(long)]
        nx: bool,
        #[arg(long)]
        xx: bool,
        #[arg(long)]
        get: bool,
    },
    Get { key: String },
    Del { keys: Vec<String> },
    Exists { keys: Vec<String> },
    Mget { keys: Vec<String> },
    Keys { pattern: String },
    JsonSet {
        key: String,
        json: String,
        #[arg(long, default_value = "$")]
        path: String,
        #[arg(long)]
        nx: bool,
        #[arg(long)]
        xx: bool,
    },
    JsonGet {
        key: String,
        #[arg(default_value = "$")]
        path: String,
    },
    Dump { key: String },
    Load {
        key: String,
        /// Base64-encoded DUMP payload.
        payload_b64: String,
        #[arg(long)]
        nx: bool,
        #[arg(long)]
        xx: bool,
    },
    Raw { parts: Vec<String> },
    DecodeResponse { hex: String },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if let Command::DecodeResponse { hex } = &args.command {
        let bytes = transport::decode_hex(hex)?;
        let response = decode_response(&bytes)?;
        println!("{}", response_to_redis_value(response)?);
        return Ok(());
    }

    let parts = parts_for_command(args.command)?;
    let request = encode_command(parts)?;

    match args.transport {
        TransportKind::Hex => {
            println!("{}", transport::encode_hex(&request));
            Ok(())
        }
        TransportKind::Iox2 => send_iox2(&args.service, &request),
    }
}

fn parts_for_command(command: Command) -> Result<Vec<WireValue>, Box<dyn std::error::Error>> {
    let mut parts: Vec<WireValue> = Vec::new();
    match command {
        Command::Ping => parts.push("PING".into()),
        Command::Set {
            key,
            value,
            ex,
            px,
            nx,
            xx,
            get,
        } => {
            parts.extend(["SET".into(), key.into(), value.into()]);
            if let Some(seconds) = ex {
                parts.extend(["EX".into(), seconds.to_string().into()]);
            }
            if let Some(milliseconds) = px {
                parts.extend(["PX".into(), milliseconds.to_string().into()]);
            }
            if nx {
                parts.push("NX".into());
            }
            if xx {
                parts.push("XX".into());
            }
            if get {
                parts.push("GET".into());
            }
        }
        Command::Get { key } => parts.extend(["GET".into(), key.into()]),
        Command::Del { keys } => {
            parts.push("DEL".into());
            parts.extend(keys.into_iter().map(WireValue::from));
        }
        Command::Exists { keys } => {
            parts.push("EXISTS".into());
            parts.extend(keys.into_iter().map(WireValue::from));
        }
        Command::Mget { keys } => {
            parts.push("MGET".into());
            parts.extend(keys.into_iter().map(WireValue::from));
        }
        Command::Keys { pattern } => parts.extend(["KEYS".into(), pattern.into()]),
        Command::JsonSet { key, json, path, nx, xx } => {
            parts.extend(["JSON.SET".into(), key.into(), path.into(), json.into()]);
            if nx {
                parts.push("NX".into());
            }
            if xx {
                parts.push("XX".into());
            }
        }
        Command::JsonGet { key, path } => parts.extend(["JSON.GET".into(), key.into(), path.into()]),
        Command::Dump { key } => parts.extend(["DUMP".into(), key.into()]),
        Command::Load { key, payload_b64, nx, xx } => {
            parts.extend(["LOAD".into(), key.into(), WireValue::Bytes(BASE64.decode(payload_b64)?)].into_iter());
            if nx {
                parts.push("NX".into());
            }
            if xx {
                parts.push("XX".into());
            }
        }
        Command::Raw { parts: raw } => {
            if raw.is_empty() {
                return Err("raw command requires at least one part".into());
            }
            parts.extend(raw.into_iter().map(WireValue::from));
        }
        Command::DecodeResponse { .. } => unreachable!(),
    }
    Ok(parts)
}

#[cfg(feature = "iox2")]
fn send_iox2(service: &str, request: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    use iox2redis_json::transport::{iox2::Iox2RpcClient, Iox2TransportConfig};
    let mut transport = Iox2RpcClient::new(Iox2TransportConfig::new(service));
    let response_bytes = iox2redis_json::client::RpcTransport::request(&mut transport, request)?;
    let response = decode_response(&response_bytes)?;
    println!("{}", response_to_redis_value(response)?);
    Ok(())
}

#[cfg(not(feature = "iox2"))]
fn send_iox2(_service: &str, _request: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    Err("iox2 transport requested, but this binary was built without `--features iox2`".into())
}
