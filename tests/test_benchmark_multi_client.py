from __future__ import annotations

import importlib.util
import os
import queue
import threading
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

import pytest

from iox2redis import redis_for
from iox2redis.server import Iox2JsonServer

BENCH_ROUNDS = int(os.getenv("IOX2REDIS_MULTI_BENCH_ROUNDS", "100"))
CLIENT_COUNT = int(os.getenv("IOX2REDIS_MULTI_BENCH_CLIENTS", "4"))
OPS_PER_CLIENT = int(os.getenv("IOX2REDIS_MULTI_BENCH_OPS", "50"))
BENCH_PAYLOAD_SIZE = int(os.getenv("IOX2REDIS_BENCH_PAYLOAD_SIZE", "256"))
BENCH_POLL_NS = int(os.getenv("IOX2REDIS_BENCH_POLL_NS", "100_000"))
RESPONSE_TIMEOUT = float(os.getenv("IOX2REDIS_MULTI_BENCH_RESPONSE_TIMEOUT", "5.0"))
BARRIER_TIMEOUT = float(os.getenv("IOX2REDIS_MULTI_BENCH_BARRIER_TIMEOUT", "15.0"))
REQUIRE_IOX2 = os.getenv("IOX2REDIS_REQUIRE_IOX2", "0") == "1"

Worker = Callable[[Any, int], Any]


def _skip_or_fail(reason: str, request: pytest.FixtureRequest) -> None:
    if REQUIRE_IOX2 or request.config.getoption(
        "benchmark_only",
        default=False,
    ):
        pytest.fail(reason)
    pytest.skip(reason)


@pytest.fixture(scope="session")
def iox2redis_host(request: pytest.FixtureRequest) -> Iterator[str]:
    if importlib.util.find_spec("iceoryx2") is None:
        _skip_or_fail(
            "iceoryx2 is not importable. Run `uv sync --dev`, then verify "
            'with `uv run python -c "import iceoryx2"`.',
            request,
        )

    raw_host = f"/iox2redis_multi_bench_{os.getpid()}_{uuid.uuid4().hex}"
    server = Iox2JsonServer(raw_host, poll_ns=BENCH_POLL_NS)
    server_errors: queue.Queue[BaseException] = queue.Queue()
    stopping = threading.Event()

    def serve() -> None:
        try:
            server.serve_forever()
        except BaseException as exc:  # noqa: BLE001
            # Iox2JsonServer currently has a close/receive race: close()
            # can clear the receiver while serve_forever() is polling it.
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
        name="iox2redis-multi-benchmark-server",
        daemon=True,
    )
    server_thread.start()

    probe = redis_for(
        host=raw_host,
        decode_responses=False,
        response_timeout=RESPONSE_TIMEOUT,
        poll_ns=BENCH_POLL_NS,
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
        _skip_or_fail(reason, request)
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


class MultiClientPool:
    """A fixed worker pool with one persistent Redis client per thread."""

    def __init__(self, host: str, client_count: int) -> None:
        if client_count < 2:
            raise ValueError("A multi-client benchmark requires at least 2 clients")

        self._host = host
        self._client_count = client_count
        self._thread_local = threading.local()
        self._client_id_lock = threading.Lock()
        self._next_client_id = 0
        self._executor = ThreadPoolExecutor(
            max_workers=client_count,
            thread_name_prefix="iox2redis-benchmark-client",
            initializer=self._initialize_worker,
        )

    def _initialize_worker(self) -> None:
        with self._client_id_lock:
            client_id = self._next_client_id
            self._next_client_id += 1

        self._thread_local.client_id = client_id
        self._thread_local.client = redis_for(
            host=self._host,
            decode_responses=False,
            response_timeout=RESPONSE_TIMEOUT,
            poll_ns=BENCH_POLL_NS,
        )

    def _run_worker(
        self,
        start: threading.Barrier,
        worker: Worker,
    ) -> Any:
        start.wait()
        return worker(
            self._thread_local.client,
            self._thread_local.client_id,
        )

    def run(self, worker: Worker) -> list[Any]:
        # Blocking every submitted task on the barrier forces the executor
        # to use all worker threads, and therefore all persistent clients.
        start = threading.Barrier(
            self._client_count + 1,
            timeout=BARRIER_TIMEOUT,
        )
        futures: list[Future[Any]] = [
            self._executor.submit(self._run_worker, start, worker)
            for _ in range(self._client_count)
        ]

        start.wait()
        return [future.result() for future in futures]

    def warmup(self) -> None:
        results = self.run(lambda client, _client_id: client.ping())
        assert results == [True] * self._client_count

    def _close_worker(self, start: threading.Barrier) -> None:
        start.wait()
        self._thread_local.client.close()

    def close(self) -> None:
        # As with run(), the barrier guarantees one close task per worker.
        start = threading.Barrier(
            self._client_count + 1,
            timeout=BARRIER_TIMEOUT,
        )
        futures = [
            self._executor.submit(self._close_worker, start) for _ in range(self._client_count)
        ]

        start.wait()
        try:
            for future in futures:
                future.result()
        finally:
            self._executor.shutdown(wait=True)

    def __enter__(self) -> MultiClientPool:
        self.warmup()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


@pytest.fixture()
def client_pool(iox2redis_host: str) -> Iterator[MultiClientPool]:
    with MultiClientPool(iox2redis_host, CLIENT_COUNT) as pool:
        yield pool


@pytest.mark.iox2
def test_benchmark_multi_client_ping(
    client_pool: MultiClientPool,
    benchmark,
) -> None:
    operations_per_round = CLIENT_COUNT * OPS_PER_CLIENT

    benchmark.extra_info["clients"] = CLIENT_COUNT
    benchmark.extra_info["operations_per_client"] = OPS_PER_CLIENT
    benchmark.extra_info["commands_per_round"] = operations_per_round

    def worker(client, _client_id: int) -> int:
        for _ in range(OPS_PER_CLIENT):
            assert client.ping() is True
        return OPS_PER_CLIENT

    def run_once() -> int:
        return sum(client_pool.run(worker))

    completed = benchmark.pedantic(
        run_once,
        rounds=BENCH_ROUNDS,
        iterations=1,
    )

    assert completed == operations_per_round


@pytest.mark.iox2
def test_benchmark_multi_client_set_get_bytes(
    client_pool: MultiClientPool,
    benchmark,
) -> None:
    payload = b"x" * BENCH_PAYLOAD_SIZE
    set_get_pairs_per_round = CLIENT_COUNT * OPS_PER_CLIENT
    commands_per_round = set_get_pairs_per_round * 2
    batch_id = 0

    benchmark.extra_info["clients"] = CLIENT_COUNT
    benchmark.extra_info["operations_per_client"] = OPS_PER_CLIENT
    benchmark.extra_info["set_get_pairs_per_round"] = set_get_pairs_per_round
    benchmark.extra_info["commands_per_round"] = commands_per_round
    benchmark.extra_info["payload_bytes"] = BENCH_PAYLOAD_SIZE

    def run_once() -> int:
        nonlocal batch_id
        batch_id += 1
        current_batch = batch_id

        def worker(client, client_id: int) -> int:
            for operation_id in range(OPS_PER_CLIENT):
                key = (f"bench:multi:{current_batch}:{client_id}:{operation_id}").encode()
                expected = (f"client-{client_id}:operation-{operation_id}:").encode() + payload

                assert client.set(key, expected) is True
                actual = client.get(key)
                assert actual == expected

            return OPS_PER_CLIENT

        return sum(client_pool.run(worker))

    completed = benchmark.pedantic(
        run_once,
        rounds=BENCH_ROUNDS,
        iterations=1,
    )

    assert completed == set_get_pairs_per_round
