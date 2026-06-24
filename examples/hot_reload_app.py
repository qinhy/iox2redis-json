from __future__ import annotations

import argparse
import json
import queue
import signal
import sys
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass
from multiprocessing import Event, Process, Queue
from pathlib import Path
from typing import Any

DEFAULT_SERVICE = "/your/topic/to/iox2_server/"
DEFAULT_CONF_KEY = "DemoAlgorithm:conf"
DEFAULT_LOG_KEY = "DemoAlgorithm:logs"
DEFAULT_CONF_POLL_SECONDS = 1.0
DEFAULT_DEMO_STEP_SECONDS = 0.2
DEFAULT_MAX_LOG_ENTRIES = 100
DEFAULT_WEB_HOST = "127.0.0.1"
DEFAULT_WEB_PORT = 5000
DEFAULT_WEB_REFRESH_SECONDS = 0.5
JSON_DUMPS_KWARGS: dict[str, Any] = {"sort_keys": True, "separators": (",", ":")}

WEB_INDEX_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>DemoAlgorithm Realtime Logs</title>
  <style>
    :root { font-family: system-ui, -apple-system, BlinkMacSystemFont, sans-serif; }
    body { margin: 2rem; background: #101114; color: #f4f4f5; }
    header { display: flex; justify-content: space-between; gap: 1rem; }
    h1 { margin: 0 0 .25rem; font-size: 1.5rem; }
    code { background: #27272a; padding: .15rem .35rem; border-radius: .35rem; }
    #status { color: #a1a1aa; font-size: .9rem; }
    #logs { display: grid; gap: .5rem; margin-top: 1rem; }
    .log { border: 1px solid #3f3f46; border-radius: .75rem; padding: .75rem; }
    .log { background: #18181b; }
    .log-top { display: flex; gap: .75rem; color: #a1a1aa; font-size: .85rem; }
    .level { text-transform: uppercase; font-weight: 700; color: #f4f4f5; }
    .message { margin-top: .35rem; font-size: 1rem; }
    pre { margin: .5rem 0 0; white-space: pre-wrap; color: #d4d4d8; }
    button { border: 1px solid #52525b; background: #27272a; color: #f4f4f5; }
    button { border-radius: .5rem; padding: .4rem .7rem; cursor: pointer; }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>DemoAlgorithm Realtime Logs</h1>
      <div>Redis log key: <code id="log-key"></code></div>
      <div id="status">connecting…</div>
    </div>
    <button onclick="clearVisibleLogs()">Clear visible logs</button>
  </header>
  <main id="logs"></main>
  <script>
    const $ = id => document.getElementById(id);
    const logsEl = $("logs"), statusEl = $("status"), logKeyEl = $("log-key");
    const seen = new Set();
    const esc = value => {
      const div = document.createElement("div");
      div.textContent = value == null ? "" : String(value);
      return div.innerHTML;
    };
    const fmt = ts => ts ? new Date(ts * 1000).toLocaleString() : "";
    function renderRecord(record) {
      const id = JSON.stringify([
        record.ts, record.level, record.message, record.step, record.result,
      ]);
      if (seen.has(id)) return;
      seen.add(id);
      const fields = { ...record };
      delete fields.ts;
      delete fields.level;
      delete fields.message;
      const node = document.createElement("article");
      node.className = "log";
      node.innerHTML = `
        <div class="log-top">
          <span class="level">${esc(record.level || "info")}</span>
          <span>${esc(fmt(record.ts))}</span>
        </div>
        <div class="message">${esc(record.message || "")}</div>
        <pre>${esc(JSON.stringify(fields, null, 2))}</pre>`;
      logsEl.prepend(node);
    }
    function clearVisibleLogs() { logsEl.innerHTML = ""; seen.clear(); }
    function show(payload) {
      logKeyEl.textContent = payload.log_key;
      payload.logs.forEach(renderRecord);
      statusEl.textContent = `connected; showing ${seen.size} records`;
    }
    fetch("/api/logs")
      .then(response => response.json())
      .then(payload => {
        show(payload);
        statusEl.textContent = `loaded ${payload.logs.length} records; waiting…`;
      })
      .catch(error => { statusEl.textContent = `failed to load logs: ${error}`; });
    const stream = new EventSource("/api/logs/stream");
    stream.addEventListener("open", () => {
      statusEl.textContent = "connected; waiting for updates…";
    });
    stream.addEventListener("snapshot", event => show(JSON.parse(event.data)));
    stream.addEventListener("log", event => {
      renderRecord(JSON.parse(event.data));
      statusEl.textContent = `connected; showing ${seen.size} records`;
    });
    stream.addEventListener("error", () => {
      statusEl.textContent = "connection lost; browser will retry automatically…";
    });
  </script>
</body>
</html>
"""


@dataclass(frozen=True)
class RedisSettings:
    service: str = DEFAULT_SERVICE
    conf_key: str = DEFAULT_CONF_KEY
    log_key: str = DEFAULT_LOG_KEY


@dataclass(frozen=True)
class RunSettings:
    poll_seconds: float = DEFAULT_CONF_POLL_SECONDS
    step_seconds: float = DEFAULT_DEMO_STEP_SECONDS
    max_logs: int = DEFAULT_MAX_LOG_ENTRIES


@dataclass(frozen=True)
class WebSettings:
    host: str = DEFAULT_WEB_HOST
    port: int = DEFAULT_WEB_PORT
    refresh_seconds: float = DEFAULT_WEB_REFRESH_SECONDS
    debug: bool = False


def require_json(value: Any, expected_type: type, name: str) -> Any:
    if value is None:
        return {} if expected_type is dict else []
    if isinstance(value, expected_type):
        return value
    label = "object" if expected_type is dict else "array"
    raise TypeError(f"{name} must be a JSON {label}, got {type(value).__name__}")


def compact_json(value: Any) -> str:
    return json.dumps(value, **JSON_DUMPS_KWARGS)


def sse(event: str, data: Any) -> str:
    return f"event: {event}\ndata: {compact_json(data)}\n\n"


def timestamp(record: dict[str, Any]) -> float:
    try:
        return float(record.get("ts") or 0.0)
    except (TypeError, ValueError):
        return 0.0


class RedisJsonStore:
    def __init__(self, service: str) -> None:
        self.service = service
        self._redis: Any | None = None

    @property
    def redis(self) -> Any:
        if self._redis is None:
            from iox2redis import redis_for

            self._redis = redis_for(host=self.service, decode_responses=True)
        return self._redis

    def get_object(self, key: str, name: str | None = None) -> dict[str, Any]:
        return require_json(self.redis.get_json(key), dict, name or f"JSON at {key!r}")

    def set_object(self, key: str, value: dict[str, Any]) -> Any:
        return self.redis.set_json(key, value)

    def read_logs(self, key: str, limit: int | None = None) -> list[dict[str, Any]]:
        raw_logs = require_json(self.redis.get_json(key), list, f"logs at {key!r}")
        logs = [record for record in raw_logs if isinstance(record, dict)]
        return logs if limit is None else logs[-max(1, limit) :]

    def append_log(self, key: str, record: dict[str, Any], max_entries: int) -> None:
        self.redis.set_json(key, [*self.read_logs(key), record][-max_entries:])


class RedisLogger:
    def __init__(
        self,
        settings: RedisSettings,
        max_entries: int = DEFAULT_MAX_LOG_ENTRIES,
        queue_size: int = 1000,
    ) -> None:
        self.settings = settings
        self.max_entries = max_entries
        self.store = RedisJsonStore(settings.service)
        self.records: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=queue_size)
        self.stop_event = threading.Event()
        self.worker = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker.start()

    def info(self, message: str, **fields: Any) -> None:
        self.log("INFO", message, **fields)

    def error(self, message: str, **fields: Any) -> None:
        self.log("ERROR", message, **fields)

    def log(self, level: str, message: str, **fields: Any) -> None:
        record = {"ts": time.time(), "level": level, "message": message, **fields}
        printable = {k: v for k, v in fields.items()}
        print(f"[{level}] {message} {printable}")
        try:
            self.records.put_nowait(record)
        except queue.Full:
            print("[logger-error] log queue full; dropping log record")

    def close(self) -> None:
        self.stop_event.set()
        self.worker.join(timeout=2.0)

    def _worker_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                record = self.records.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                self.store.append_log(self.settings.log_key, record, self.max_entries)
            except Exception as exc:
                print(f"[logger-error] failed to write log to Redis: {exc!r}")


class DemoAlgorithm:
    def __init__(self, logger: RedisLogger, step_seconds: float) -> None:
        self.logger = logger
        self.step_seconds = step_seconds
        self.conf: dict[str, Any] = {}
        self.step_index = 0

    def apply_conf(self, update: dict[str, Any]) -> None:
        self.conf.update(update)
        self.logger.info("applied conf for next step", conf=self.conf)

    def step(self) -> None:
        self.step_index += 1
        if self.conf.get("enabled", True):
            self._run_enabled_step()
        else:
            self.logger.info("algorithm step", step=self.step_index, enabled=False)
        if self.step_seconds > 0:
            time.sleep(self.step_seconds)

    def _run_enabled_step(self) -> None:
        multiplier = self.conf.get("multiplier", 1)
        self.logger.info(
            "algorithm step",
            step=self.step_index,
            enabled=True,
            msg=self.conf.get("message", "hello"),
            multiplier=multiplier,
            result=self.step_index * multiplier,
        )


def run_config_poller_child(
    service: str,
    conf_key: str,
    out_queue: Queue,
    stop_event: Event,
    poll_seconds: float,
) -> None:
    store = RedisJsonStore(service)
    last: str | None = None
    while not stop_event.is_set():
        try:
            conf = store.get_object(conf_key, f"config at {conf_key!r}")
            encoded = compact_json(conf)
            if encoded != last:
                last = encoded
                out_queue.put({"type": "conf", "value": conf})
        except Exception as exc:
            out_queue.put({"type": "error", "value": repr(exc)})
        stop_event.wait(poll_seconds)


class ConfigPoller:
    def __init__(self, settings: RedisSettings, poll_seconds: float) -> None:
        self.updates: Queue = Queue()
        self.stop_event = Event()
        self.process = Process(
            target=run_config_poller_child,
            args=(
                settings.service,
                settings.conf_key,
                self.updates,
                self.stop_event,
                poll_seconds,
            ),
            daemon=True,
        )

    def start(self) -> None:
        self.process.start()

    def install_shutdown_handlers(self) -> None:
        def shutdown(_signum: int, _frame: Any) -> None:
            self.stop_event.set()

        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)

    def stop(self) -> None:
        self.stop_event.set()
        self.process.join(timeout=2.0)
        if self.process.is_alive():
            self.process.terminate()
            self.process.join(timeout=2.0)

    def drain_updates(self, algorithm: DemoAlgorithm) -> None:
        while True:
            try:
                msg = self.updates.get_nowait()
            except queue.Empty:
                return
            if msg.get("type") == "conf":
                algorithm.apply_conf(msg["value"])
            else:
                print(f"[child-error] {msg['value']}", file=sys.stderr)


class HotReloadApp:
    def __init__(self, redis: RedisSettings, run_settings: RunSettings) -> None:
        self.redis = redis
        self.run_settings = run_settings
        self.poller = ConfigPoller(redis, run_settings.poll_seconds)
        self.logger = RedisLogger(redis, max_entries=run_settings.max_logs)
        self.algorithm = DemoAlgorithm(self.logger, run_settings.step_seconds)

    def run(self) -> int:
        self.poller.start()
        self.poller.install_shutdown_handlers()
        self.print_help()
        try:
            while not self.poller.stop_event.is_set():
                self.poller.drain_updates(self.algorithm)
                self.algorithm.step()
        finally:
            self.logger.close()
            self.poller.stop()
            print("[main] stopped")
        return 0

    def print_help(self) -> None:
        script = Path(sys.argv[0]).name or "hot_reload_app.py"
        print(
            f"[main] running. service={self.redis.service!r}, "
            f"conf_key={self.redis.conf_key!r}, log_key={self.redis.log_key!r}"
        )
        print(
            f"[main] hot config poll interval: {self.run_settings.poll_seconds}s, in child process"
        )
        print(
            f"[main] demo algorithm step duration: {self.run_settings.step_seconds}s, "
            "in main process"
        )
        print("[main] update confs from another process with:")
        print(
            f"       python {script} set --service {self.redis.service!r} "
            f"--key {self.redis.conf_key!r} --message 'updated' "
            "--multiplier 10 --enabled"
        )
        print("[main] view logs in a browser with:")
        print(
            f"       python {script} web --service {self.redis.service!r} "
            f"--log-key {self.redis.log_key!r}"
        )


class LogsWebServer:
    def __init__(self, redis: RedisSettings, web: WebSettings) -> None:
        self.redis = redis
        self.web = web
        self.store = RedisJsonStore(redis.service)

    def run(self) -> int:
        app = self.create_app()
        print(
            f"[web] reading logs from service={self.redis.service!r}, "
            f"log_key={self.redis.log_key!r}"
        )
        print(f"[web] open http://{self.web.host}:{self.web.port}/")
        app.run(
            host=self.web.host,
            port=self.web.port,
            debug=self.web.debug,
            threaded=True,
            use_reloader=False,
        )
        return 0

    def create_app(self) -> Any:
        try:
            from flask import Flask, Response, jsonify, request, stream_with_context
        except ImportError as exc:
            msg = "Flask is required. Install it with: pip install flask"
            raise RuntimeError(msg) from exc

        app = Flask(__name__)

        @app.get("/")
        def index() -> str:
            return WEB_INDEX_HTML

        @app.get("/api/logs")
        def api_logs() -> Any:
            limit = requested_limit(request.args.get("limit"))
            logs = self.store.read_logs(self.redis.log_key, limit)
            return jsonify({"log_key": self.redis.log_key, "count": len(logs), "logs": logs})

        @app.get("/api/logs/stream")
        def stream() -> Any:
            return Response(
                stream_with_context(self.events()),
                mimetype="text/event-stream",
            )

        return app

    def events(self) -> Iterable[str]:
        last_ts = 0.0
        try:
            logs = self.store.read_logs(self.redis.log_key)
            last_ts = max(map(timestamp, logs), default=0.0)
            yield sse("snapshot", {"log_key": self.redis.log_key, "logs": logs})
        except Exception as exc:
            yield sse("error", {"message": repr(exc)})

        while True:
            try:
                for record in self.store.read_logs(self.redis.log_key):
                    ts = timestamp(record)
                    if ts > last_ts:
                        last_ts = max(last_ts, ts)
                        yield sse("log", record)
            except GeneratorExit:
                return
            except Exception as exc:
                yield sse("error", {"message": repr(exc)})
            time.sleep(self.web.refresh_seconds)


def requested_limit(raw_limit: str | None) -> int:
    try:
        return max(1, int(raw_limit or DEFAULT_MAX_LOG_ENTRIES))
    except ValueError:
        return DEFAULT_MAX_LOG_ENTRIES


def set_config(redis: RedisSettings, args: argparse.Namespace) -> int:
    store = RedisJsonStore(redis.service)
    update = parse_update(args)
    current = (
        {}
        if args.replace
        else store.get_object(
            redis.conf_key,
            f"existing config at {redis.conf_key!r}",
        )
    )
    next_conf = {**current, **update}
    ok = store.set_object(redis.conf_key, next_conf)
    print(f"SET JSON {redis.conf_key!r} -> {ok}")
    print(f"updated fields -> {update}")
    print(f"new conf -> {store.redis.get_json(redis.conf_key)}")
    return 0


def parse_update(args: argparse.Namespace) -> dict[str, Any]:
    update = require_json(json.loads(args.conf), dict, "--conf") if args.conf else {}
    update.update(
        {
            key: value
            for key, value in {
                "message": args.message,
                "multiplier": args.multiplier,
                "enabled": enabled_value(args),
            }.items()
            if value is not None
        }
    )
    if update:
        return update
    msg = "nothing to set; use --message, --multiplier, --enabled/--disabled, or --conf"
    raise ValueError(msg)


def enabled_value(args: argparse.Namespace) -> bool | None:
    if args.enabled:
        return True
    if args.disabled:
        return False
    return None


def add_args(parser: argparse.ArgumentParser, *specs: tuple[Any, ...]) -> None:
    for flags, kwargs in specs:
        parser.add_argument(*flags, **kwargs)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="child process polls config; main process hot-applies it."
    )
    shared = argparse.ArgumentParser(add_help=False)
    add_args(
        shared,
        (("--service",), {"default": DEFAULT_SERVICE}),
        (("--key",), {"default": DEFAULT_CONF_KEY}),
    )
    subcommands = parser.add_subparsers(dest="command", required=True)
    add_run_parser(subcommands, shared)
    add_web_parser(subcommands, shared)
    add_set_parser(subcommands, shared)
    return parser


def add_run_parser(subcommands: Any, shared: argparse.ArgumentParser) -> None:
    run_parser = subcommands.add_parser(
        "run",
        parents=[shared],
        help="run the program with child-process config polling",
    )
    add_args(
        run_parser,
        (("--poll-seconds",), {"type": float, "default": DEFAULT_CONF_POLL_SECONDS}),
        (("--step-seconds",), {"type": float, "default": DEFAULT_DEMO_STEP_SECONDS}),
        (("--log-key",), {"default": DEFAULT_LOG_KEY}),
        (("--max-logs",), {"type": int, "default": DEFAULT_MAX_LOG_ENTRIES}),
    )


def add_web_parser(subcommands: Any, shared: argparse.ArgumentParser) -> None:
    web_parser = subcommands.add_parser(
        "web",
        parents=[shared],
        help="serve a realtime Flask web page for Redis logs",
    )
    add_args(
        web_parser,
        (("--log-key",), {"default": DEFAULT_LOG_KEY}),
        (("--host",), {"default": DEFAULT_WEB_HOST}),
        (("--port",), {"type": int, "default": DEFAULT_WEB_PORT}),
        (
            ("--refresh-seconds",),
            {"type": float, "default": DEFAULT_WEB_REFRESH_SECONDS},
        ),
        (("--debug",), {"action": "store_true"}),
    )


def add_set_parser(subcommands: Any, shared: argparse.ArgumentParser) -> None:
    set_parser = subcommands.add_parser(
        "set",
        parents=[shared],
        help="set config fields",
    )
    add_args(
        set_parser,
        (("--message",), {"help": "set conf['message']"}),
        (("--multiplier",), {"type": int, "help": "set conf['multiplier']"}),
        (("--conf",), {"help": "advanced: JSON object, e.g. '{\"enabled\": true}'"}),
        (("--replace",), {"action": "store_true", "help": "replace instead of merge"}),
    )
    enabled = set_parser.add_mutually_exclusive_group()
    enabled.add_argument("--enabled", action="store_true", help="set enabled to true")
    enabled.add_argument("--disabled", action="store_true", help="set enabled to false")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(sys.argv[1:] if argv is None else argv)
    redis = RedisSettings(
        args.service,
        args.key,
        getattr(args, "log_key", DEFAULT_LOG_KEY),
    )
    if args.command == "run":
        settings = RunSettings(args.poll_seconds, args.step_seconds, args.max_logs)
        return HotReloadApp(redis, settings).run()
    if args.command == "web":
        settings = WebSettings(args.host, args.port, args.refresh_seconds, args.debug)
        return LogsWebServer(redis, settings).run()
    if args.command == "set":
        return set_config(redis, args)
    raise ValueError(f"unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
