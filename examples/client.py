from __future__ import annotations

import sys

from iox2redis import redis_for


def main() -> int:
    service = sys.argv[1] if len(sys.argv) > 1 else "/your/topic/to/iox2_server/"
    r = redis_for(host=service, decode_responses=True)

    print("PING ->", r.ping())
    print("SET plain ->", r.set("plain", "hello"))
    print("GET plain ->", r.get("plain"))

    doc = {"name": "Ada", "age": 37, "tags": ["math", "computing"]}
    print("SET JSON ->", r.set_json("user:1", doc))
    print("GET JSON ->", r.get_json("user:1"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
