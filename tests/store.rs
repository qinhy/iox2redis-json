use iox2redis::codec::{decode_response, encode_command, ResponseKind, WireValue};
use iox2redis::JsonStore;

fn run(store: &mut JsonStore, values: Vec<WireValue>) -> iox2redis::codec::ResponseFrame {
    let request = encode_command(values).unwrap();
    let response = store.handle_payload(&request).unwrap();
    decode_response(&response).unwrap()
}

#[test]
fn set_get_plain_value() {
    let mut store = JsonStore::new();
    assert_eq!(
        run(
            &mut store,
            vec!["SET".into(), "plain".into(), "hello".into()]
        )
        .value,
        Some("OK".into())
    );
    assert_eq!(
        run(&mut store, vec!["GET".into(), "plain".into()]).value,
        Some("hello".into())
    );
}

#[test]
fn missing_get_is_nil() {
    let mut store = JsonStore::new();
    assert_eq!(
        run(&mut store, vec!["GET".into(), "missing".into()]).kind,
        ResponseKind::Nil
    );
}

#[test]
fn json_set_get() {
    let mut store = JsonStore::new();
    assert_eq!(
        run(
            &mut store,
            vec![
                "JSON.SET".into(),
                "doc".into(),
                "$".into(),
                r#"{"a":1}"#.into()
            ]
        )
        .value,
        Some("OK".into())
    );
    assert_eq!(
        run(
            &mut store,
            vec!["JSON.GET".into(), "doc".into(), "$".into()]
        )
        .value,
        Some(r#"{"a":1}"#.into())
    );
}

#[test]
fn del_exists_mget() {
    let mut store = JsonStore::new();
    run(&mut store, vec!["SET".into(), "a".into(), "1".into()]);
    run(&mut store, vec!["SET".into(), "b".into(), "2".into()]);
    assert_eq!(
        run(
            &mut store,
            vec!["EXISTS".into(), "a".into(), "b".into(), "c".into()]
        )
        .value,
        Some(WireValue::Json("2".to_owned()))
    );
    assert_eq!(
        run(
            &mut store,
            vec!["MGET".into(), "a".into(), "c".into(), "b".into()]
        )
        .array,
        vec![Some("1".into()), None, Some("2".into())]
    );
    assert_eq!(
        run(
            &mut store,
            vec!["DEL".into(), "a".into(), "b".into(), "c".into()]
        )
        .value,
        Some(WireValue::Json("2".to_owned()))
    );
}

#[test]
fn dump_format_is_python_compatible_json_envelope() {
    let mut store = JsonStore::new();
    run(
        &mut store,
        vec!["SET".into(), "plain".into(), "hello".into()],
    );
    let dump = run(&mut store, vec!["DUMP".into(), "plain".into()]);
    let Some(WireValue::Bytes(payload)) = dump.value else {
        panic!("expected dump bytes")
    };
    assert!(payload.starts_with(b"IX2D"));
    let text = std::str::from_utf8(&payload[4..]).unwrap();
    assert!(text.contains(r#""v":1"#));
    assert!(text.contains(r#""is_json":false"#));
    assert!(text.contains(r#""type":"str"#));
    assert!(text.contains(r#""data":"hello"#));

    let mut restored = JsonStore::new();
    assert_eq!(
        run(
            &mut restored,
            vec!["LOAD".into(), "copy".into(), WireValue::Bytes(payload)]
        )
        .value,
        Some("OK".into())
    );
    assert_eq!(
        run(&mut restored, vec!["GET".into(), "copy".into()]).value,
        Some("hello".into())
    );
}
