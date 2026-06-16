from __future__ import annotations

import importlib.util
import json
import os
import queue
import threading
import uuid
from collections.abc import Iterator

import pytest

from iox2redis import direct_redis_for
from iox2redis.server import Iox2JsonServer


BENCH_ROUNDS = int(os.getenv("IOX2REDIS_BENCH_ROUNDS", "1000"))
BENCH_PAYLOAD_SIZE = int(os.getenv("IOX2REDIS_BENCH_PAYLOAD_SIZE", "256"))
BENCH_POLL_NS = int(os.getenv("IOX2REDIS_BENCH_POLL_NS", "100_000"))
REQUIRE_IOX2 = os.getenv("IOX2REDIS_REQUIRE_IOX2", "0") == "1"


def _skip_or_fail(reason: str, request: pytest.FixtureRequest) -> None:
    if REQUIRE_IOX2 or request.config.getoption("benchmark_only", default=False):
        pytest.fail(reason)
    pytest.skip(reason)


@pytest.fixture(scope="session")
def iox2redis_host(request: pytest.FixtureRequest) -> Iterator[str]:
    if importlib.util.find_spec("iceoryx2") is None:
        _skip_or_fail(
            "iceoryx2 is not importable. Run `uv sync --dev`, then verify "
            "with `uv run python -c \"import iceoryx2\"`.",
            request,
        )

    raw_host = f"/iox2redis_direct_bench_{os.getpid()}_{uuid.uuid4().hex}"
    server = Iox2JsonServer(raw_host, poll_ns=BENCH_POLL_NS)
    errors: queue.Queue[BaseException] = queue.Queue()
    old_service_type = os.environ.get("IOX2REDIS_SERVICE_TYPE")
    # if os.name == "nt":
    #     os.environ["IOX2REDIS_SERVICE_TYPE"] = "local"

    def serve() -> None:
        try:
            server.serve_forever()
        except BaseException as exc:  # noqa: BLE001
            errors.put(exc)

    try:
        server.open()
        thread = threading.Thread(target=serve, daemon=True)
        thread.start()

        client = direct_redis_for(
            raw_host,
            response_timeout=2.0,
            poll_ns=BENCH_POLL_NS,
        )

        try:
            assert client.ping() is True
        except Exception as exc:  # noqa: BLE001
            if not errors.empty():
                exc = errors.get()
            _skip_or_fail(f"iox2redis server started, but PING failed: {exc!r}", request)
        finally:
            client.close()

        yield raw_host

    finally:
        server.close()
        thread.join(timeout=3.0)
        if old_service_type is None:
            os.environ.pop("IOX2REDIS_SERVICE_TYPE", None)
        else:
            os.environ["IOX2REDIS_SERVICE_TYPE"] = old_service_type


@pytest.fixture()
def client(iox2redis_host: str):
    client = direct_redis_for(
        iox2redis_host,
        response_timeout=2.0,
        poll_ns=BENCH_POLL_NS,
    )
    try:
        yield client
    finally:
        client.close()


@pytest.mark.iox2
def test_benchmark_direct_ping(client, benchmark):
    result = benchmark.pedantic(
        client.ping,
        rounds=BENCH_ROUNDS,
        iterations=1,
    )

    assert result is True


@pytest.mark.iox2
def test_benchmark_direct_set_bytes(client, benchmark):
    payload = b"x" * BENCH_PAYLOAD_SIZE
    counter = 0

    def run_once():
        nonlocal counter
        counter += 1
        return client.set_bytes(f"bench:direct:set:{counter}".encode(), payload)

    result = benchmark.pedantic(
        run_once,
        rounds=BENCH_ROUNDS,
        iterations=1,
    )

    assert result is True


@pytest.mark.iox2
def test_benchmark_direct_get_bytes(client, benchmark):
    payload = b"x" * BENCH_PAYLOAD_SIZE
    key = b"bench:direct:get"

    assert client.set_bytes(key, payload) is True

    result = benchmark.pedantic(
        lambda: client.get_bytes(key),
        rounds=BENCH_ROUNDS,
        iterations=1,
    )

    assert result == payload


@pytest.mark.iox2
def test_benchmark_direct_set_get_bytes(client, benchmark):
    payload = b"x" * BENCH_PAYLOAD_SIZE
    counter = 0

    def run_once():
        nonlocal counter
        counter += 1
        key = f"bench:direct:set-get:{counter}".encode()
        assert client.set_bytes(key, payload) is True
        return client.get_bytes(key)

    result = benchmark.pedantic(
        run_once,
        rounds=BENCH_ROUNDS,
        iterations=1,
    )

    assert result == payload


@pytest.mark.iox2
def test_benchmark_direct_set_json_bytes(client, benchmark):
    payload = json.dumps(
        {
            "name": "Ada",
            "age": 37,
            "enabled": True,
            "tags": ["iox2", "redis", "json"],
            "padding": "x" * BENCH_PAYLOAD_SIZE,
        },
        separators=(",", ":"),
    ).encode("utf-8")
    counter = 0

    def run_once():
        nonlocal counter
        counter += 1
        return client.set_json_bytes(f"bench:direct:set-json:{counter}", payload)

    result = benchmark.pedantic(
        run_once,
        rounds=BENCH_ROUNDS,
        iterations=1,
    )

    assert result is True


@pytest.mark.iox2
def test_benchmark_direct_get_json_bytes(client, benchmark):
    payload = json.dumps(
        {
            "name": "Ada",
            "age": 37,
            "enabled": True,
            "tags": ["iox2", "redis", "json"],
            "padding": "x" * BENCH_PAYLOAD_SIZE,
        },
        separators=(",", ":"),
    ).encode("utf-8")
    key = "bench:direct:get-json"

    assert client.set_json_bytes(key, payload) is True

    result = benchmark.pedantic(
        lambda: client.get_json_bytes(key),
        rounds=BENCH_ROUNDS,
        iterations=1,
    )

    assert result == payload
