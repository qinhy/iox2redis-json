from __future__ import annotations

import base64
import os
from typing import Any

from flask import Flask, jsonify, request

try:
    from redis.exceptions import ResponseError
except Exception:  # pragma: no cover - keeps the API importable if redis is absent.
    ResponseError = Exception  # type: ignore[misc,assignment]

from iox2redis import redis_for

DEFAULT_SERVICE = "/your/topic/to/iox2_server/"


def create_app() -> Flask:
    app = Flask(__name__)

    service = os.environ.get("IOX2REDIS_SERVICE", DEFAULT_SERVICE)

    # Text client for normal REST operations.
    text_client = redis_for(host=service, decode_responses=True)

    # Binary client for DUMP/LOAD so serialized payloads are not corrupted by
    # text decoding. REST transports these bytes as base64 strings.
    binary_client = redis_for(host=service, decode_responses=False)

    def error(message: str, status_code: int = 400):
        return jsonify({"error": message}), status_code

    def parse_json_body() -> dict[str, Any] | None:
        if not request.is_json:
            return None
        payload = request.get_json(silent=True)
        return payload if isinstance(payload, dict) else None

    @app.errorhandler(ResponseError)
    def handle_response_error(exc):
        return error(str(exc), 400)

    @app.get("/health")
    def health():
        return jsonify({"ok": bool(text_client.ping()), "service": service})

    @app.get("/keys")
    def list_keys():
        pattern = request.args.get("pattern", "*")
        keys = text_client.keys(pattern)
        return jsonify({"pattern": pattern, "keys": list(keys)})

    @app.put("/keys/<path:key>")
    def set_key(key: str):
        payload = parse_json_body()
        if payload is None or "value" not in payload:
            return error("Expected JSON body like {'value': 'hello'}")

        ok = text_client.set(key, payload["value"])
        return jsonify({"key": key, "set": bool(ok)})

    @app.get("/keys/<path:key>")
    def get_key(key: str):
        value = text_client.get(key)
        if value is None:
            return error(f"Key not found: {key}", 404)
        return jsonify({"key": key, "value": value})

    @app.delete("/keys/<path:key>")
    def delete_key(key: str):
        deleted = text_client.delete(key)
        return jsonify({"key": key, "deleted": int(deleted)})

    @app.put("/json/<path:key>")
    def set_json_key(key: str):
        if not request.is_json:
            return error("Expected any JSON body to store")

        document = request.get_json(silent=True)
        ok = text_client.set_json(key, document)
        return jsonify({"key": key, "set": bool(ok)})

    @app.get("/json/<path:key>")
    def get_json_key(key: str):
        value = text_client.get_json(key)
        if value is None:
            return error(f"Key not found: {key}", 404)
        return jsonify({"key": key, "value": value})

    @app.get("/dump/<path:key>")
    def dump_key(key: str):
        payload = binary_client.dump(key)
        if payload is None:
            return error(f"Key not found: {key}", 404)
        return jsonify(
            {
                "key": key,
                "payload_base64": base64.b64encode(payload).decode("ascii"),
                "payload_bytes": len(payload),
            }
        )

    @app.post("/load/<path:key>")
    def load_key(key: str):
        payload = parse_json_body()
        if payload is None or "payload_base64" not in payload:
            return error(
                "Expected JSON body like {'payload_base64': '...', 'nx': false, 'xx': false}"
            )

        try:
            raw_payload = base64.b64decode(payload["payload_base64"], validate=True)
        except Exception:
            return error("payload_base64 is not valid base64")

        nx = bool(payload.get("nx", False))
        xx = bool(payload.get("xx", False))
        if nx and xx:
            return error("nx and xx cannot both be true")

        result = binary_client.load(key, raw_payload, nx=nx, xx=xx)
        return jsonify({"key": key, "loaded": result})

    return app


app = create_app()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
