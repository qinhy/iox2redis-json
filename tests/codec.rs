use iox2redis::codec::{
    decode_command, decode_response, encode_command, encode_response, ResponseFrame, WireValue,
};

#[test]
fn command_round_trip_with_bytes() {
    let payload = encode_command([
        WireValue::from("SET"),
        WireValue::from("k"),
        WireValue::from(b"\0bin".as_slice()),
    ])
    .unwrap();
    let frame = decode_command(&payload).unwrap();
    assert_eq!(frame.command, "SET");
    assert_eq!(
        frame.args,
        vec![WireValue::from("k"), WireValue::from(b"\0bin".as_slice())]
    );
}

#[test]
fn bulk_response_round_trip() {
    let payload =
        encode_response(&ResponseFrame::bulk(WireValue::from(b"value".as_slice()))).unwrap();
    let frame = decode_response(&payload).unwrap();
    assert_eq!(frame.value, Some(WireValue::from(b"value".as_slice())));
}

#[test]
fn array_response_round_trip() {
    let payload = encode_response(&ResponseFrame::array(vec![
        Some(WireValue::from("a")),
        None,
        Some(WireValue::from("c")),
    ]))
    .unwrap();
    let frame = decode_response(&payload).unwrap();
    assert_eq!(
        frame.array,
        vec![Some(WireValue::from("a")), None, Some(WireValue::from("c"))]
    );
}

#[test]
fn json_tag_matches_original_python_wire_format() {
    let payload = encode_command([
        WireValue::from("JSON.SET"),
        WireValue::from("doc"),
        WireValue::from("$"),
        WireValue::Json(r#"{"a":1}"#.to_owned()),
    ])
    .unwrap();
    // magic + version + command frame + command_len + argc + "JSON.SET" + two string args
    // The JSON payload argument must use tag 3 followed by a u32 length, matching Python codec.py.
    assert!(payload
        .windows(6)
        .any(|window| window == [3, 0, 0, 0, 7, b'{']));
    let frame = decode_command(&payload).unwrap();
    assert_eq!(frame.args[2], WireValue::Json(r#"{"a":1}"#.to_owned()));
}
