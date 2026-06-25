use iox2redis::codec::{encode_command, WireValue};
use iox2redis::transport::encode_hex;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let Some(cmd) = args.next() else {
        eprintln!("usage: iox2redis-client ping | set KEY VALUE | get KEY | json-set KEY JSON | json-get KEY");
        std::process::exit(2);
    };
    let frame = match cmd.as_str() {
        "ping" => encode_command(vec![WireValue::from("PING")])?,
        "set" => encode_command(vec![
            WireValue::from("SET"),
            WireValue::from(args.next().unwrap_or_default()),
            WireValue::from(args.next().unwrap_or_default()),
        ])?,
        "get" => encode_command(vec![
            WireValue::from("GET"),
            WireValue::from(args.next().unwrap_or_default()),
        ])?,
        "json-set" => encode_command(vec![
            WireValue::from("JSON.SET"),
            WireValue::from(args.next().unwrap_or_default()),
            WireValue::from("$"),
            WireValue::from(args.next().unwrap_or_default()),
        ])?,
        "json-get" => encode_command(vec![
            WireValue::from("JSON.GET"),
            WireValue::from(args.next().unwrap_or_default()),
            WireValue::from("$"),
        ])?,
        other => {
            eprintln!("unknown command: {other}");
            std::process::exit(2);
        }
    };
    println!("{}", encode_hex(&frame));
    Ok(())
}
