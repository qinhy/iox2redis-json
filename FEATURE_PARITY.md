# Python-to-Rust feature parity checklist

| Python feature | Rust status | Notes |
|---|---:|---|
| Binary `IX2R` command/response codec | ✅ | Same magic/version and value tags. |
| Legacy JSON frame decode fallback | ✅ | Decodes legacy command/response frames. |
| `PING` | ✅ | Optional payload returns bulk payload. |
| `SET` | ✅ | Supports `EX`, `PX`, `NX`, `XX`, `GET`. Invalid TTL returns an error. |
| `GET` | ✅ | JSON values are returned as compact JSON text. |
| `DEL` | ✅ | Routed server rejects const keys. |
| `EXISTS` | ✅ | Routed across normal and const stores. |
| `MGET` | ✅ | Routed across normal and const stores. |
| `KEYS` glob | ✅ | Supports `*`, `?`, escapes, `[abc]`, `[!abc]`. |
| `DUMP` / `LOAD` | ✅ | Uses `IX2D` payload and preserves remaining TTL in `ttl_ms`. |
| `JSON.SET` / `JSON.GET` | ✅ | Validates via `serde_json`; root paths `$` and `.` only. |
| `JSON.SET NX/XX` | ✅ | Same root-only behavior as Python. |
| `const:*` namespace | ✅ | Write-once, no delete, no expiry, server info key. |
| `const:server_info` | ✅ | Created by `Iox2JsonServer`. |
| redis-py integration | N/A | Rust exposes a direct client abstraction instead. |
| iceoryx2 request/response transport | ✅ optional | Build with `--features iox2`. Default build uses hex stdio. |
| Pipelines / packed RESP | intentionally unsupported | Same limitation as Python connection class. |
