from __future__ import annotations

import importlib.util
import os
import queue
import threading
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor

import pytest

from iox2redis import redis_for
from iox2redis.server import Iox2JsonServer

POLL_NS = int(os.getenv("IOX2REDIS_TEST_POLL_NS", "100_000"))
CLIENT_COUNT = int(os.getenv("IOX2REDIS_MULTI_CLIENTS", "4"))
ROUNDS_PER_CLIENT = int(os.getenv("IOX2REDIS_MULTI_CLIENT_ROUNDS", "100"))
REQUIRE_IOX2 = os.getenv("IOX2REDIS_REQUIRE_IOX2", "0") == "1"


def _skip_or_fail(reason: str) -> None:
    if REQUIRE_IOX2:
        pytest.fail(reason)
    pytest.skip(reason)


@pytest.fixture(scope="module")
def iox2redis_host() -> Iterator[str]:
    if importlib.util.find_spec("iceoryx2") is None:
        _skip_or_fail(
            "iceoryx2 is not importable. Run `uv sync --dev`, then verify "
            'with `uv run python -c "import iceoryx2"`.'
        )

    raw_host = f"/iox2redis_multi_client_{os.getpid()}_{uuid.uuid4().hex}"
    server = Iox2JsonServer(raw_host, poll_ns=POLL_NS)
    server_errors: queue.Queue[BaseException] = queue.Queue()
    stopping = threading.Event()

    def serve() -> None:
        try:
            server.serve_forever()
        except BaseException as exc:  # noqa: BLE001
            expected_close_race = (
                stopping.is_set()
                and isinstance(exc, AttributeError)
                and "NoneType" in str(exc)
                and "receive" in str(exc)
            )
            if not expected_close_race:
                server_errors.put(exc)

    server.open()
    server_thread = threading.Thread(
        target=serve,
        name="iox2redis-test-server",
        daemon=True,
    )
    server_thread.start()

    probe = redis_for(
        host=raw_host,
        decode_responses=False,
        response_timeout=2.0,
        poll_ns=POLL_NS,
    )
    try:
        assert probe.ping() is True
    except Exception as client_exc:
        try:
            server_exc = server_errors.get_nowait()
        except queue.Empty:
            reason = f"iox2redis server started, but the readiness PING failed: {client_exc!r}"
        else:
            reason = f"readiness client error: {client_exc!r}; server error: {server_exc!r}"
        stopping.set()
        server.close()
        server_thread.join(timeout=3.0)
        _skip_or_fail(reason)
    finally:
        probe.close()

    try:
        yield raw_host
    finally:
        stopping.set()
        server.close()
        server_thread.join(timeout=3.0)

        if server_thread.is_alive():
            pytest.fail("iox2redis server thread did not stop")

        try:
            server_exc = server_errors.get_nowait()
        except queue.Empty:
            pass
        else:
            pytest.fail(f"iox2redis server thread failed: {server_exc!r}")


@pytest.mark.iox2
def test_concurrent_clients_do_not_cross_responses(iox2redis_host: str) -> None:
    """Concurrent clients must receive only their own request responses."""

    # Include the test thread so every worker begins its request loop together.
    start = threading.Barrier(CLIENT_COUNT + 1, timeout=10.0)

    def worker(client_id: int) -> None:
        # A client belongs to one thread. This tests multi-client behavior
        # without assuming that a single client object is thread-safe.
        client = redis_for(
            host=iox2redis_host,
            decode_responses=False,
            response_timeout=2.0,
            poll_ns=POLL_NS,
        )
        try:
            start.wait()

            for round_id in range(ROUNDS_PER_CLIENT):
                key = f"multi:{client_id}:{round_id}".encode()
                expected = (f"value-from-client-{client_id}-round-{round_id}").encode()

                assert client.set(key, expected) is True
                actual = client.get(key)

                # Unique values make response cross-talk immediately visible.
                assert actual == expected, (
                    f"client {client_id}, round {round_id}: expected {expected!r}, got {actual!r}"
                )
        finally:
            client.close()

    with ThreadPoolExecutor(
        max_workers=CLIENT_COUNT,
        thread_name_prefix="iox2redis-client",
    ) as executor:
        futures = [executor.submit(worker, client_id) for client_id in range(CLIENT_COUNT)]

        start.wait()

        # Calling result() propagates assertion and transport failures from
        # the worker threads into the pytest test.
        for future in futures:
            future.result()

    # Verify every concurrent write from a fresh client after all worker
    # clients have disconnected.
    verifier = redis_for(
        host=iox2redis_host,
        decode_responses=False,
        response_timeout=2.0,
        poll_ns=POLL_NS,
    )
    try:
        for client_id in range(CLIENT_COUNT):
            for round_id in range(ROUNDS_PER_CLIENT):
                key = f"multi:{client_id}:{round_id}".encode()
                expected = (f"value-from-client-{client_id}-round-{round_id}").encode()
                assert verifier.get(key) == expected
    finally:
        verifier.close()
