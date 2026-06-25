use iox2redis_json::codec::{decode_command, decode_response, encode_command, ResponseKind, WireValue};
use iox2redis_json::store::{Iox2JsonServer, ServerConfig};
use serde_json::json;
use std::thread;
use std::time::Duration;

fn roundtrip(server: &mut Iox2JsonServer, parts: Vec<WireValue>) -> iox2redis_json::ResponseFrame {
    let request = encode_command(parts).unwrap();
    let response = server.handle_payload(&request).unwrap();
    decode_response(&response).unwrap()
}

#[test]
fn binary_codec_roundtrips_commands() {
    let payload = encode_command(vec![
        WireValue::from("JSON.SET"),
        WireValue::from("key"),
        WireValue::from("$"),
        WireValue::Json(json!({"a": 1})),
    ])
    .unwrap();
    let frame = decode_command(&payload).unwrap();
    assert_eq!(frame.command, "JSON.SET");
    assert_eq!(frame.args.len(), 3);
}

#[test]
fn server_exposes_const_server_info() {
    let mut server = Iox2JsonServer::new(ServerConfig::new("/redis/json/")).unwrap();
    let response = roundtrip(
        &mut server,
        vec!["JSON.GET".into(), "const:server_info".into(), "$".into()],
    );
    assert_eq!(response.kind, ResponseKind::Bulk);
    let text = response.value.unwrap().as_lossy_key();
    assert!(text.contains("iox2redis-json"));
}

#[test]
fn constant_keys_are_write_once_and_not_deletable() {
    let mut server = Iox2JsonServer::new(ServerConfig::new("/redis/json/")).unwrap();
    let ok = roundtrip(
        &mut server,
        vec!["JSON.SET".into(), "const:my".into(), "$".into(), "{\"x\":1}".into()],
    );
    assert_eq!(ok.kind, ResponseKind::Simple);

    let again = roundtrip(
        &mut server,
        vec!["JSON.SET".into(), "const:my".into(), "$".into(), "{\"x\":2}".into()],
    );
    assert_eq!(again.kind, ResponseKind::Error);

    let deleted = roundtrip(&mut server, vec!["DEL".into(), "const:my".into()]);
    assert_eq!(deleted.kind, ResponseKind::Error);
}

#[test]
fn dump_load_keeps_ttl() {
    let mut server = Iox2JsonServer::new(ServerConfig::new("/redis/json/")).unwrap();
    let _ = roundtrip(
        &mut server,
        vec!["SET".into(), "tmp".into(), "v".into(), "PX".into(), "60".into()],
    );
    let dump = roundtrip(&mut server, vec!["DUMP".into(), "tmp".into()]);
    let payload = dump.value.unwrap();
    let loaded = roundtrip(&mut server, vec!["LOAD".into(), "copy".into(), payload]);
    assert_eq!(loaded.kind, ResponseKind::Simple);
    thread::sleep(Duration::from_millis(100));
    let got = roundtrip(&mut server, vec!["GET".into(), "copy".into()]);
    assert_eq!(got.kind, ResponseKind::Nil);
}
