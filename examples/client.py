from __future__ import annotations

import sys

from iox2redis import redis_for


def _dump_len(payload: bytes | None) -> str:
    return "None" if payload is None else f"{len(payload)} bytes"


def main() -> int:
    service = sys.argv[1] if len(sys.argv) > 1 else "/your/topic/to/iox2_server/"
    r = redis_for(host=service, decode_responses=True)

    res = "PING -> " + str(r.ping())

    res += "\nSET plain -> " + str(r.set("plain", "hello"))
    res += "\nGET plain -> " + str(r.get("plain"))

    plain_dump:bytes = r.dump("plain")
    res += "\nDUMP plain -> " + _dump_len(plain_dump)
    if plain_dump is not None:
        res += "\nLOAD plain:copy -> " + str(r.load("plain:copy", plain_dump))
        res += "\nGET plain:copy -> " + str(r.get("plain:copy"))
        res += "\nLOAD plain NX existing -> " + str(r.load("plain", plain_dump, nx=True))
        res += "\nLOAD plain:missing XX -> " + str(r.load("plain:missing", plain_dump, xx=True))

    doc = {"name": "Ada", "age": 37, "tags": ["math", "computing"]}
    res += "\nSET JSON -> " + str(r.set_json("user:1", doc))
    res += "\nGET JSON -> " + str(r.get_json("user:1"))

    json_dump = r.dump("user:1")
    res += "\nDUMP JSON -> " + _dump_len(json_dump)
    if json_dump is not None:
        res += "\nLOAD JSON copy -> " + str(r.load("user:copy", json_dump))
        res += "\nGET JSON copy -> " + str(r.get_json("user:copy"))

    res += "\nDUMP missing -> " + _dump_len(r.dump("missing"))

    print("#####################################")
    print(res)
    print("#####################################")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())