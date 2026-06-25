use iox2redis::{transport, JsonStore};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/your/topic/to/iox2_server/".to_owned());
    let service_name = transport::service_name_from_host(&service);
    eprintln!("IOX2REDIS_READY /{service_name}/");
    transport::serve_hex_stdio(JsonStore::new())?;
    Ok(())
}
