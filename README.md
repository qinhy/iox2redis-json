# iox2redis-json

A small Redis-like JSON `SET` / `GET` client and server over [iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2) IPC, with a `redis-py`-style client API.

This lets you write code like:

```python
from iox2redis import redis_for

r = redis_for(host="/your/topic/to/iox2_server/", decode_responses=True)

r.set_json("user:1", {"name": "Ada", "age": 37})
print(r.get_json("user:1"))
```

The `host="/your/topic/to/iox2_server/"` value is treated as an iceoryx2 service name, not as a TCP hostname.

---

## Why?

`redis-py` normally talks Redis Serialization Protocol over TCP or Unix sockets.

This project keeps the convenient Redis client shape, but replaces the transport with an iceoryx2 request-response IPC transport for a simple JSON key-value server.

It is intentionally small and focused:

* `PING`
* `SET`
* `GET`
* `set_json`
* `get_json`

It is **not** a full Redis server implementation.

---

## Project layout

```text
iox2redis-json/
  pyproject.toml
  README.md
  LICENSE
  src/iox2redis/
    __init__.py
    client.py
    connection.py
    transport.py
    codec.py
    server.py
    store.py
  examples/
    client.py
    server.py
  tests/
    test_codec.py
    test_store.py
  docs/
    architecture.md
```

---

## Requirements

* Python 3.11+
* `uv`
* `redis-py`
* optional: `iceoryx2` Python package for live IPC usage

Install `uv` if needed:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

---

## Install

Clone or unzip the repo, then run:

```bash
uv sync --extra iox2 --dev
```

For development without iceoryx2 installed:

```bash
uv sync --dev
```

---

## Run tests

```bash
uv run pytest -q
```

Run linting:

```bash
uv run ruff check .
```

Run type checking:

```bash
uv run mypy src
```

---

## Start the iceoryx2 JSON server

```bash
uv run iox2redis-server /your/topic/to/iox2_server/
```

Equivalent module form:

```bash
uv run python -m iox2redis.server /your/topic/to/iox2_server/
```

The server creates or opens an iceoryx2 request-response service using the normalized service name:

```text
your/topic/to/iox2_server
```

Leading and trailing slashes are stripped.

---

## Run the example client

In another terminal:

```bash
uv run python examples/client.py /your/topic/to/iox2_server/
```

Example client code:

```python
from iox2redis import redis_for

r = redis_for("/your/topic/to/iox2_server/", decode_responses=True)

print(r.ping())

r.set("plain", "hello")
print(r.get("plain"))

r.set_json("user:1", {"name": "Ada", "age": 37})
print(r.get_json("user:1"))
```

---

## Python usage

```python
from iox2redis import redis_for

r = redis_for(host="/your/topic/to/iox2_server/", decode_responses=True)

r.set("hello", "world")
assert r.get("hello") == "world"

r.set_json("config", {"enabled": True, "limit": 10})
assert r.get_json("config") == {"enabled": True, "limit": 10}
```

For a normal Redis TCP server, use a regular hostname:

```python
from iox2redis import redis_for

r = redis_for(host="localhost", port=6379, decode_responses=True)
r.set("hello", "redis")
print(r.get("hello"))
```

The helper chooses the transport based on `host`:

```text
host starts with "/"  -> iceoryx2 transport
otherwise             -> normal redis-py TCP transport
```

---

## API

### `redis_for(...)`

```python
redis_for(host="localhost", **kwargs)
```

Creates a Redis-style client.

When `host` starts with `/`, an iceoryx2-backed Redis client is created.

When `host` does not start with `/`, a normal `redis.Redis` client is created.

Example:

```python
r = redis_for("/your/topic/to/iox2_server/", decode_responses=True)
```

### `set_json(key, value)`

Stores a JSON-serializable Python object.

```python
r.set_json("user:1", {"name": "Ada"})
```

### `get_json(key)`

Loads a JSON value from the server.

```python
user = r.get_json("user:1")
```

Returns `None` if the key does not exist.

---

## Wire protocol

Client requests are JSON objects encoded as UTF-8 bytes.

Example `SET` request:

```json
{
  "cmd": "SET",
  "args": [
    {
      "type": "bytes",
      "value": "dXNlcjox"
    },
    {
      "type": "bytes",
      "value": "eyJuYW1lIjoiQWRhIn0="
    }
  ]
}
```

Example successful response:

```json
{
  "type": "ok"
}
```

Example `GET` response:

```json
{
  "type": "bulk",
  "value": {
    "type": "bytes",
    "value": "eyJuYW1lIjoiQWRhIn0="
  }
}
```

Missing key response:

```json
{
  "type": "nil"
}
```

Error response:

```json
{
  "error": "ERR unsupported command DEL"
}
```

---

## Supported commands

### `PING`

```python
r.ping()
```

Returns:

```python
True
```

### `SET`

```python
r.set("key", "value")
```

Returns:

```python
True
```

### `GET`

```python
r.get("key")
```

Returns the value or `None`.

---

## Design notes

This project does not modify `redis-py`.

Instead, it uses a custom Redis connection class and connection pool:

```python
redis.ConnectionPool(
    connection_class=Iox2Connection,
    host="/your/topic/to/iox2_server/",
)
```

That keeps normal Redis usage unchanged while adding iceoryx2 support for path-like hosts.

---

## Limitations

This is a simple JSON key-value server, not Redis.

Unsupported features include:

* Redis protocol compatibility
* pipelines
* transactions
* pub/sub
* Lua scripting
* Redis modules
* persistence
* clustering
* authentication
* ACLs
* eviction policies
* expiration / TTL

Use real Redis when you need full Redis behavior.

Use this project when you want a tiny Redis-shaped client API over iceoryx2 IPC.

---

## Development

Install dependencies:

```bash
uv sync --extra iox2 --dev
```

Run tests:

```bash
uv run pytest -q
```

Format and lint:

```bash
uv run ruff format .
uv run ruff check .
```

Type check:

```bash
uv run mypy src
```

Build package:

```bash
uv build
```

---

## Example development loop

Terminal 1:

```bash
uv run iox2redis-server /your/topic/to/iox2_server/
```

Terminal 2:

```bash
uv run python examples/client.py /your/topic/to/iox2_server/
```

---

## License

MIT
