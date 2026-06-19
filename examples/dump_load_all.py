from __future__ import annotations

import base64
import json
import sys
from pathlib import Path
from typing import Any

from redis.exceptions import ResponseError

from iox2redis import redis_for

CONST_PREFIX = "const:"
DUMP_FILE_FORMAT = "iox2redis-json-dump-v1"


def _key_to_str(key: str | bytes) -> str:
    # This demo store treats keys as UTF-8 strings.
    return key.decode("utf-8") if isinstance(key, bytes) else str(key)


def dump_all_keys(r: Any, *, pattern: str = "*", include_const: bool = True) -> dict[str, str]:
    """Return {key: base64_dump_payload} for all matching live keys."""
    dumped: dict[str, str] = {}

    for raw_key in r.keys(pattern):
        key = _key_to_str(raw_key)
        if not include_const and key.startswith(CONST_PREFIX):
            continue

        payload = r.dump(key)
        if payload is None:
            # Key may have expired between KEYS and DUMP.
            continue

        dumped[key] = base64.b64encode(payload).decode("ascii")

    return dumped


def save_server_dump(
    r: Any,
    path: str | Path,
    *,
    pattern: str = "*",
    include_const: bool = True,
) -> int:
    keys = dump_all_keys(r, pattern=pattern, include_const=include_const)
    data = {
        "format": DUMP_FILE_FORMAT,
        "keys": keys,
    }
    Path(path).write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    return len(keys)


def load_server_dump(
    r: Any,
    path: str | Path,
    *,
    nx: bool = False,
    xx: bool = False,
    load_const: bool = True,
) -> dict[str, Any]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if data.get("format") != DUMP_FILE_FORMAT:
        raise ValueError(f"unsupported dump file format: {data.get('format')!r}")

    results: dict[str, Any] = {}
    for key, encoded_payload in data["keys"].items():
        is_const = key.startswith(CONST_PREFIX)
        if is_const and not load_const:
            results[key] = "SKIPPED const key"
            continue

        payload = base64.b64decode(encoded_payload.encode("ascii"))

        try:
            if is_const:
                # const:* keys are write-once. Restore them as create-only so
                # const:server_info and already-present user constants are not overwritten.
                results[key] = r.load(key, payload, nx=True)
            else:
                results[key] = r.load(key, payload, nx=nx, xx=xx)
        except ResponseError as exc:
            message = str(exc)
            if is_const and "constant keys cannot be loaded" in message:
                # Friendly behavior when talking to an older server binary.
                results[key] = "SKIPPED const key: server does not support LOAD const:* yet"
            elif is_const and "already set" in message:
                results[key] = None
            else:
                raise

    return results


def main() -> int:
    service = sys.argv[1] if len(sys.argv) > 1 else "/your/topic/to/iox2_server/"
    dump_path = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("iox2redis-dump.json")

    r = redis_for(host=service, decode_responses=True)

    # Demo data.
    r.set("plain", "hello")
    r.set_json("user:1", {"name": "Ada", "age": 37, "tags": ["math", "computing"]})
    r.set_json("const:app_config", {"feature": "dump-load-const", "enabled": True}, nx=True)
    # r.set("const:app_config", "dump-load-const")

    count = save_server_dump(r, dump_path)
    print(f"DUMP ALL -> wrote {count} keys to {dump_path}")

    # To restore into the same or another server, connect r to that server and load:
    results = load_server_dump(r, dump_path)
    print("LOAD ALL ->")
    for key, result in results.items():
        print(f"  {key}: {result}")

    print("GET plain ->", r.get("plain"))
    print("GET JSON ->", r.get_json("user:1"))
    print("GET const ->", r.get("const:app_config"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
