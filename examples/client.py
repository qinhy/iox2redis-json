from __future__ import annotations

import sys

from iox2redis import redis_for


def main() -> int:
    service = sys.argv[1] if len(sys.argv) > 1 else "/your/topic/to/iox2_server/"
    r = redis_for(host=service, decode_responses=True)
    res = "PING -> "+str(r.ping())
    res += "\nSET plain -> "+str(r.set("plain", "hello"))
    res += "\nGET plain -> "+str(r.get("plain"))
    doc = {"name": "Ada", "age": 37, "tags": ["math", "computing"]}
    res += "\nSET JSON -> "+str(r.set_json("user:1", doc))
    res += "\nGET JSON -> "+str(r.get_json("user:1"))
    print("#####################################")
    print(res)
    print("#####################################")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
