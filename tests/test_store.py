from __future__ import annotations

import json

from iox2redis.codec import decode_response, encode_command, response_to_redis_value
from iox2redis.server import JsonStore


def run(store: JsonStore, *args):
    response = decode_response(store.handle_payload(encode_command(args)))
    return response_to_redis_value(response)


def test_set_get_plain_value() -> None:
    store = JsonStore()
    assert run(store, "SET", "plain", "hello") == b"OK"
    assert run(store, "GET", "plain") == b"hello"


def test_missing_get_is_none() -> None:
    store = JsonStore()
    assert run(store, "GET", "missing") is None


def test_json_set_get() -> None:
    store = JsonStore()
    assert run(store, "JSON.SET", "doc", "$", json.dumps({"a": 1})) == b"OK"
    raw = run(store, "JSON.GET", "doc", "$")
    assert json.loads(raw.decode("utf-8")) == {"a": 1}


def test_del_exists_mget() -> None:
    store = JsonStore()
    run(store, "SET", "a", "1")
    run(store, "SET", "b", "2")
    assert run(store, "EXISTS", "a", "b", "c") == 2
    assert run(store, "MGET", "a", "c", "b") == [b"1", None, b"2"]
    assert run(store, "DEL", "a", "b", "c") == 2
    assert run(store, "EXISTS", "a", "b") == 0
