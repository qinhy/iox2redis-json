use clap::{Parser, ValueEnum};
use iox2redis_json::store::{ServerConfig, DEFAULT_MAX_PAYLOAD_SIZE, DEFAULT_POLL_NS};
use iox2redis_json::{transport, Iox2JsonServer, StoreError};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum TransportKind {
    Hex,
    Iox2,
}

#[derive(Parser, Debug)]
#[command(about = "Run a Redis-like JSON server over iceoryx2-compatible request/response frames")]
struct Args {
    /// iceoryx2 service path, e.g. /your/topic/to/iox2_server/
    #[arg(default_value = "/your/topic/to/iox2_server/")]
    service: String,

    /// Transport backend. `hex` reads hex frames from stdin and writes hex responses to stdout.
    #[arg(long, value_enum, default_value_t = TransportKind::Hex)]
    transport: TransportKind,

    /// Maximum request and response payload size in bytes.
    #[arg(long, default_value_t = DEFAULT_MAX_PAYLOAD_SIZE)]
    max_payload_size: usize,

    /// iceoryx2 wait duration in nanoseconds.
    #[arg(long)]
    poll_ns: Option<u64>,

    /// Legacy millisecond wait duration; ignored when --poll-ns is set.
    #[arg(long)]
    poll_ms: Option<u64>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let mut config = ServerConfig::new(args.service);
    config.max_payload_size = args.max_payload_size;
    config.poll_ns = args.poll_ns;
    config.poll_ms = args.poll_ms;

    let mut server = Iox2JsonServer::new(config.clone())?;
    print_started(&server);

    let stopping = Arc::new(AtomicBool::new(false));
    let stop_flag = stopping.clone();
    ctrlc::set_handler(move || {
        stop_flag.store(true, Ordering::SeqCst);
    })?;

    let result = match args.transport {
        TransportKind::Hex => {
            transport::serve_hex_stdio_until(stopping.clone(), |payload| server.handle_payload(payload))
        }
        TransportKind::Iox2 => serve_iox2(server, stopping.clone()),
    };

    eprintln!("[{}] server stopped", iox2redis_json::store::SERVER_NAME);
    if stopping.load(Ordering::SeqCst) {
        Ok(())
    } else {
        result.map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })
    }
}

fn print_started(server: &Iox2JsonServer) {
    let info = &server.info;
    eprintln!("[{}] server started", info.name);
    eprintln!("  Service:            {}", info.service_path);
    eprintln!(
        "  Max payload size:   {} ({} bytes)",
        info.max_payload_size_text, info.max_payload_size
    );
    eprintln!("  Poll interval:      {}", info.poll_text);
    eprintln!("  Constant namespace: {}* (write-once)", info.const_key_prefix);
    eprintln!("  Server information: {}", info.server_info_key);
    eprintln!("{}", info.ready_message());
}

#[cfg(feature = "iox2")]
fn serve_iox2(mut server: Iox2JsonServer, stopping: Arc<AtomicBool>) -> Result<(), StoreError> {
    use iox2redis_json::transport::{iox2::Iox2RpcServer, Iox2TransportConfig};
    let mut transport_config = Iox2TransportConfig::new(server.config.service_name.clone());
    transport_config.max_payload_size = server.config.max_payload_size;
    transport_config.poll_ns = server.config.effective_poll_ns().max(DEFAULT_POLL_NS);
    Iox2RpcServer::new(transport_config).serve_until(stopping, |payload| server.handle_payload(payload))
}

#[cfg(not(feature = "iox2"))]
fn serve_iox2(_server: Iox2JsonServer, _stopping: Arc<AtomicBool>) -> Result<(), StoreError> {
    Err(StoreError::Io(
        "iox2 transport requested, but this binary was built without `--features iox2`".to_owned(),
    ))
}
