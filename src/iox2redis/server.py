"""In-memory Redis-like JSON store and iceoryx2 server CLI."""

from __future__ import annotations

import argparse
import json
import re
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .codec import (
    CodecError,
    CommandFrame,
    ResponseFrame,
    decode_command,
    encode_response,
    key_to_str,
    value_to_json_text,
)

if TYPE_CHECKING:
    from .transport import Iox2RpcServer


@dataclass
class StoredValue:
    value: Any
    expires_at: float | None = None
    is_json: bool = False


def _compile_redis_glob(pattern: str) -> re.Pattern[str]:
    """Compile Redis-style glob syntax into a case-sensitive regex."""

    parts = [r"\A"]
    idx = 0
    while idx < len(pattern):
        char = pattern[idx]
        if char == "*":
            parts.append(".*")
            idx += 1
            continue
        if char == "?":
            parts.append(".")
            idx += 1
            continue
        if char == "\\":
            idx += 1
            if idx < len(pattern):
                parts.append(re.escape(pattern[idx]))
                idx += 1
            else:
                parts.append(re.escape("\\"))
            continue
        if char != "[":
            parts.append(re.escape(char))
            idx += 1
            continue

        end = idx + 1
        negated = False
        if end < len(pattern) and pattern[end] in {"^", "!"}:
            negated = True
            end += 1

        class_parts: list[str] = []
        if end < len(pattern) and pattern[end] == "]":
            class_parts.append(r"\]")
            end += 1

        closed = False
        while end < len(pattern):
            class_char = pattern[end]
            if class_char == "]":
                closed = True
                break
            if class_char == "\\" and end + 1 < len(pattern):
                end += 1
                class_parts.append(re.escape(pattern[end]))
            elif class_char == "-":
                class_parts.append("-")
            else:
                class_parts.append(re.escape(class_char))
            end += 1

        if not closed or not class_parts:
            parts.append(r"\[")
            idx += 1
            continue

        parts.append("[" + ("^" if negated else "") + "".join(class_parts) + "]")
        idx = end + 1

    parts.append(r"\Z")
    return re.compile("".join(parts), re.DOTALL)


class JsonStore:
    """Small Redis-like in-memory store used by the demo server."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._items: dict[str, StoredValue] = {}
        self._clock = clock

    def handle_payload(self, payload: bytes) -> bytes:
        try:
            command = decode_command(payload)
            response = self.handle(command)
        except CodecError as exc:
            response = ResponseFrame("error", message=f"ERR codec {exc}")
        except Exception as exc:  # noqa: BLE001 - returned to Redis client as ERR
            response = ResponseFrame("error", message=f"ERR {type(exc).__name__}: {exc}")
        return encode_response(response)

    def handle(self, frame: CommandFrame) -> ResponseFrame:
        cmd = frame.command.upper()
        args = frame.args
        if cmd == "PING":
            return self._ping(args)
        if cmd == "SET":
            return self._set(args)
        if cmd == "GET":
            return self._get(args)
        if cmd == "DEL":
            return self._del(args)
        if cmd == "EXISTS":
            return self._exists(args)
        if cmd == "MGET":
            return self._mget(args)
        if cmd == "KEYS":
            return self._keys(args)
        if cmd == "JSON.SET":
            return self._json_set(args)
        if cmd == "JSON.GET":
            return self._json_get(args)
        return ResponseFrame("error", message=f"ERR unsupported command {cmd}")

    def _purge_if_expired(self, key: str) -> None:
        item = self._items.get(key)
        if item is not None and item.expires_at is not None and item.expires_at <= self._clock():
            self._items.pop(key, None)

    def _get_item(self, key: str) -> StoredValue | None:
        self._purge_if_expired(key)
        return self._items.get(key)

    def _ping(self, args: list[Any]) -> ResponseFrame:
        if not args:
            return ResponseFrame("pong")
        return ResponseFrame("bulk", args[0])

    def _set(self, args: list[Any]) -> ResponseFrame:
        if len(args) < 2:
            return ResponseFrame("error", message="ERR wrong number of arguments for SET")

        key = key_to_str(args[0])
        value = args[1]
        old = self._get_item(key)

        expires_at: float | None = None
        nx = False
        xx = False
        get_old = False

        idx = 2
        while idx < len(args):
            opt = key_to_str(args[idx]).upper()
            if opt == "EX" and idx + 1 < len(args):
                expires_at = self._clock() + float(key_to_str(args[idx + 1]))
                idx += 2
            elif opt == "PX" and idx + 1 < len(args):
                expires_at = self._clock() + float(key_to_str(args[idx + 1])) / 1000.0
                idx += 2
            elif opt == "NX":
                nx = True
                idx += 1
            elif opt == "XX":
                xx = True
                idx += 1
            elif opt == "GET":
                get_old = True
                idx += 1
            else:
                return ResponseFrame("error", message=f"ERR unsupported SET option {opt}")

        exists = old is not None
        if nx and exists:
            return ResponseFrame("nil")
        if xx and not exists:
            return ResponseFrame("nil")

        self._items[key] = StoredValue(value=value, expires_at=expires_at, is_json=False)
        if get_old:
            return ResponseFrame("nil") if old is None else ResponseFrame("bulk", old.value)
        return ResponseFrame("simple", "OK")

    def _get(self, args: list[Any]) -> ResponseFrame:
        if len(args) != 1:
            return ResponseFrame("error", message="ERR wrong number of arguments for GET")
        key = key_to_str(args[0])
        item = self._get_item(key)
        if item is None:
            return ResponseFrame("nil")
        if item.is_json:
            return ResponseFrame(
                "bulk",
                json.dumps(item.value, separators=(",", ":"), ensure_ascii=False),
            )
        return ResponseFrame("bulk", item.value)

    def _del(self, args: list[Any]) -> ResponseFrame:
        count = 0
        for raw_key in args:
            key = key_to_str(raw_key)
            self._purge_if_expired(key)
            if key in self._items:
                count += 1
                del self._items[key]
        return ResponseFrame("integer", count)

    def _exists(self, args: list[Any]) -> ResponseFrame:
        count = 0
        for raw_key in args:
            key = key_to_str(raw_key)
            if self._get_item(key) is not None:
                count += 1
        return ResponseFrame("integer", count)

    def _mget(self, args: list[Any]) -> ResponseFrame:
        values: list[Any | None] = []
        for raw_key in args:
            key = key_to_str(raw_key)
            item = self._get_item(key)
            if item is None:
                values.append(None)
            elif item.is_json:
                values.append(json.dumps(item.value, separators=(",", ":"), ensure_ascii=False))
            else:
                values.append(item.value)
        return ResponseFrame("array", values)

    def _keys(self, args: list[Any]) -> ResponseFrame:
        if len(args) != 1:
            return ResponseFrame("error", message="ERR wrong number of arguments for KEYS")

        pattern = key_to_str(args[0])
        matcher = _compile_redis_glob(pattern)

        # KEYS must not expose entries that have expired but have not yet been
        # touched by another command. Iterate over a snapshot because purging
        # mutates the backing dictionary.
        for key in list(self._items):
            self._purge_if_expired(key)

        return ResponseFrame("array", [key for key in self._items if matcher.fullmatch(key)])

    def _json_set(self, args: list[Any]) -> ResponseFrame:
        if len(args) < 3:
            return ResponseFrame("error", message="ERR wrong number of arguments for JSON.SET")

        key = key_to_str(args[0])
        path = key_to_str(args[1])
        if path not in {"$", "."}:
            return ResponseFrame("error", message="ERR only root path '$' is supported")

        old = self._get_item(key)
        nx = False
        xx = False
        idx = 3
        while idx < len(args):
            opt = key_to_str(args[idx]).upper()
            if opt == "NX":
                nx = True
            elif opt == "XX":
                xx = True
            else:
                return ResponseFrame("error", message=f"ERR unsupported JSON.SET option {opt}")
            idx += 1

        if nx and old is not None:
            return ResponseFrame("nil")
        if xx and old is None:
            return ResponseFrame("nil")

        try:
            value = json.loads(value_to_json_text(args[2]))
        except json.JSONDecodeError as exc:
            return ResponseFrame("error", message=f"ERR invalid JSON: {exc.msg}")

        self._items[key] = StoredValue(value=value, is_json=True)
        return ResponseFrame("simple", "OK")

    def _json_get(self, args: list[Any]) -> ResponseFrame:
        if not (1 <= len(args) <= 2):
            return ResponseFrame("error", message="ERR wrong number of arguments for JSON.GET")

        key = key_to_str(args[0])
        if len(args) == 2 and key_to_str(args[1]) not in {"$", "."}:
            return ResponseFrame("error", message="ERR only root path '$' is supported")

        item = self._get_item(key)
        if item is None:
            return ResponseFrame("nil")

        value = item.value if item.is_json else value_to_json_text(item.value)
        return ResponseFrame(
            "bulk",
            json.dumps(value, separators=(",", ":"), ensure_ascii=False) if item.is_json else value,
        )


class Iox2JsonServer:
    """iceoryx2 request-response server for JsonStore."""

    def __init__(
        self,
        service_name: str,
        *,
        max_payload_size: int = 64 * 1024,
        poll_ns: int | None = None,
        poll_ms: int | None = None,
        store: JsonStore | None = None,
    ) -> None:
        self.service_name = service_name.strip("/")
        self.max_payload_size = max_payload_size
        self.poll_ns = poll_ns
        self.poll_ms = poll_ms
        self.store = store or JsonStore()
        self._transport: Iox2RpcServer | None = None
        self._closed = False

    def open(self) -> None:
        from .transport import Iox2RpcServer

        if self._transport is None:
            self._transport = Iox2RpcServer(
                self.service_name,
                max_payload_size=self.max_payload_size,
                poll_ns=self.poll_ns,
                poll_ms=self.poll_ms,
            )
            self._transport.open()

    def serve_once(self) -> int:
        self.open()
        assert self._transport is not None
        return self._transport.drain_requests(self.store.handle_payload)

    def serve_forever(self) -> None:
        self.open()
        while not self._closed:
            self.serve_once()

    def close(self) -> None:
        self._closed = True
        if self._transport is not None:
            self._transport.close()
            self._transport = None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a Redis-like JSON server over iceoryx2.")
    parser.add_argument("service", help="iceoryx2 service path, e.g. /your/topic/to/iox2_server/")
    parser.add_argument("--max-payload-size", type=int, default=64 * 1024)
    parser.add_argument(
        "--poll-ns",
        type=int,
        default=None,
        help="iceoryx2 wait duration in nanoseconds; defaults to 100000",
    )
    parser.add_argument(
        "--poll-ms",
        type=int,
        default=None,
        help="legacy millisecond wait duration, ignored when --poll-ns is set",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    server = Iox2JsonServer(
        args.service,
        max_payload_size=args.max_payload_size,
        poll_ns=args.poll_ns,
        poll_ms=args.poll_ms,
    )

    def _stop(_signum: int, _frame: Any) -> None:
        server.close()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    server.open()
    print(f"iox2redis-json server listening on /{server.service_name}/", flush=True)
    print(f"IOX2REDIS_READY {args.service}", flush=True)
    server.serve_forever()
    print("iox2redis-json server stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
