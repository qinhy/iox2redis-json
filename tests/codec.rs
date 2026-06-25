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
