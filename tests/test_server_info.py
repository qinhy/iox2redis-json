from __future__ import annotations

import json

from iox2redis.codec import decode_response, encode_command, response_to_redis_value
from iox2redis.server import Iox2JsonServer, _print_server_started


def run(server: Iox2JsonServer, *args):
    response = decode_response(server.handle_payload(encode_command(args)))
    return response_to_redis_value(response)


def test_print_server_started_includes_iceoryx2_version(monkeypatch, capsys) -> None:
    monkeypatch.setattr("iox2redis.server._iceoryx2_version", lambda: "9.8.7")
    server = Iox2JsonServer("/test_service/")

    _print_server_started(server)

    assert "  Iceoryx2 version:   9.8.7\n" in capsys.readouterr().out


def test_server_info_contains_iceoryx2_version(monkeypatch) -> None:
    monkeypatch.setattr("iox2redis.server._iceoryx2_version", lambda: "9.8.7")
    server = Iox2JsonServer("/test_service/")

    raw = run(server, "JSON.GET", "const:server_info", "$")

    assert json.loads(raw.decode("utf-8"))["iceoryx2_version"] == "9.8.7"
