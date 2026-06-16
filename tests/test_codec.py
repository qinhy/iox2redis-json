from __future__ import annotations

from iox2redis.codec import (
    ResponseFrame,
    decode_command,
    decode_response,
    encode_command,
    encode_response,
    response_to_redis_value,
)


def test_command_round_trip_with_bytes() -> None:
    payload = encode_command(["SET", "k", b"\x00bin"])
    frame = decode_command(payload)
    assert frame.command == "SET"
    assert frame.args == ["k", b"\x00bin"]


def test_bulk_response_round_trip() -> None:
    payload = encode_response(ResponseFrame("bulk", b"value"))
    frame = decode_response(payload)
    assert response_to_redis_value(frame) == b"value"


def test_array_response_round_trip() -> None:
    payload = encode_response(ResponseFrame("array", [b"a", None, "c"]))
    frame = decode_response(payload)
    assert response_to_redis_value(frame) == [b"a", None, b"c"]
