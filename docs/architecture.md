# Architecture

`iox2redis-json` uses redis-py's connection-class extension point.

- `redis_for(host="localhost")` returns a normal redis-py TCP client.
- `redis_for(host="/some/iox2/service/")` returns a redis-py client with an `Iox2Connection` connection pool.

`Iox2Connection` does not speak RESP over a socket. Instead, it maps redis-py command arguments into a compact JSON command frame and sends that frame as a dynamic byte slice through an iceoryx2 request-response service.

## Why not patch redis-py host resolution?

A host string beginning with `/` is not a network host. It is an iceoryx2 service name. Keeping this behavior inside a custom `Connection` avoids surprising changes to normal redis-py TCP and Unix socket behavior.

## Wire envelope

Command frame:

```json
{"v":1,"command":"SET","args":[{"type":"str","data":"key"},{"type":"bytes","data":"dmFsdWU="}]}
```

Response frame:

```json
{"v":1,"kind":"simple","value":"OK"}
```

Kinds:

- `simple`
- `bulk`
- `integer`
- `array`
- `nil`
- `pong`
- `error`
