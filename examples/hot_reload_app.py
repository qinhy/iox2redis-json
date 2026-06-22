from __future__ import annotations

import argparse
import json
import queue
import signal
import sys
import time
from multiprocessing import Event, Process, Queue
from pathlib import Path
from typing import Any

DEFAULT_SERVICE = "/your/topic/to/iox2_server/"
DEFAULT_CONF_KEY = "demo:conf"
DEFAULT_CONF_POLL_SECONDS = 1.0
DEFAULT_DEMO_STEP_SECONDS = 0.2


def redis_client(service: str):
    from iox2redis import redis_for

    return redis_for(host=service, decode_responses=True)


def normalized_json(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def require_json_object(value: Any, name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be a JSON object, got {type(value).__name__}")
    return value


class DemoAlgorithm:
    """Small demo algorithm. Replace this with your real program."""

    def __init__(self, step_seconds: float = DEFAULT_DEMO_STEP_SECONDS) -> None:
        self.conf: dict[str, Any] = {}
        self.step_index = 0
        self.step_seconds = step_seconds

    def apply_conf(self, update: dict[str, Any]) -> None:
        self.conf.update(update)
        print(f"[main] applied conf for next step: {self.conf}")

    def step(self) -> None:
        self.step_index += 1

        if self.conf.get("enabled", True):
            message = self.conf.get("message", "hello")
            multiplier = self.conf.get("multiplier", 1)
            result = self.step_index * multiplier
            print(f"[algorithm] step={self.step_index}: {message}; result={result}")
        else:
            print(f"[algorithm] step={self.step_index}: disabled")

        if self.step_seconds > 0:
            time.sleep(self.step_seconds)


def config_reader_child(
    service: str,
    conf_key: str,
    out_queue: Queue,
    stop_event: Event,
    poll_seconds: float,
) -> None:
    """Poll config in a child process and send only changed configs to main."""
    redis = redis_client(service)
    last_conf_json: str | None = None

    while not stop_event.is_set():
        try:
            conf = require_json_object(redis.get_json(conf_key), f"config at {conf_key!r}")
            conf_json = normalized_json(conf)

            if conf_json != last_conf_json:
                last_conf_json = conf_json
                out_queue.put({"type": "conf", "value": conf})
        except Exception as exc:  # Keep polling through temporary server/config errors.
            out_queue.put({"type": "error", "value": repr(exc)})

        stop_event.wait(poll_seconds)


def drain_config_updates(updates: Queue, algorithm: DemoAlgorithm) -> None:
    """Apply every queued config update without blocking the algorithm loop."""
    while True:
        try:
            msg = updates.get_nowait()
        except queue.Empty:
            return

        if msg.get("type") == "conf":
            algorithm.apply_conf(msg["value"])
        elif msg.get("type") == "error":
            print(f"[child-error] {msg['value']}", file=sys.stderr)


def install_shutdown_handlers(stop_event: Event) -> None:
    def shutdown(_signum: int, _frame: Any) -> None:
        stop_event.set()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)


def stop_child(child: Process, stop_event: Event) -> None:
    stop_event.set()
    child.join(timeout=2.0)
    if child.is_alive():
        child.terminate()
        child.join(timeout=2.0)


def print_startup_help(
    service: str, conf_key: str, poll_seconds: float, step_seconds: float
) -> None:
    script = Path(sys.argv[0]).name or "config_hot_reload_demo_refactored.py"
    print(f"[main] running. service={service!r}, conf_key={conf_key!r}")
    print(f"[main] hot config poll interval: {poll_seconds}s, in child process")
    print(f"[main] demo algorithm step duration: {step_seconds}s, in main process")
    print("[main] update confs from another process with:")
    print(
        f"       python {script} set --service {service!r} --key {conf_key!r} "
        "--message 'updated' --multiplier 10 --enabled"
    )


def run_app(service: str, conf_key: str, poll_seconds: float, step_seconds: float) -> int:
    updates: Queue = Queue()
    stop_event = Event()
    child = Process(
        target=config_reader_child,
        args=(service, conf_key, updates, stop_event, poll_seconds),
        daemon=True,
    )

    child.start()
    install_shutdown_handlers(stop_event)
    print_startup_help(service, conf_key, poll_seconds, step_seconds)

    algorithm = DemoAlgorithm(step_seconds=step_seconds)
    try:
        while not stop_event.is_set():
            drain_config_updates(updates, algorithm)
            algorithm.step()
    finally:
        stop_child(child, stop_event)
        print("[main] stopped")

    return 0


def parse_conf_json(raw_conf: str | None) -> dict[str, Any]:
    if raw_conf is None:
        return {}
    return require_json_object(json.loads(raw_conf), "--conf")


def build_conf_update(args: argparse.Namespace) -> dict[str, Any]:
    update = parse_conf_json(args.conf)

    cli_fields = {
        "message": args.message,
        "multiplier": args.multiplier,
        "enabled": True if args.enabled else False if args.disabled else None,
    }
    update.update({key: value for key, value in cli_fields.items() if value is not None})

    if not update:
        raise ValueError(
            "nothing to set; use --message, --multiplier, --enabled/--disabled, or --conf"
        )
    return update


def set_conf(service: str, conf_key: str, args: argparse.Namespace) -> int:
    update = build_conf_update(args)
    redis = redis_client(service)

    if args.replace:
        next_conf = update
    else:
        current_conf = require_json_object(
            redis.get_json(conf_key), f"existing config at {conf_key!r}"
        )
        next_conf = {**current_conf, **update}

    ok = redis.set_json(conf_key, next_conf)
    print(f"SET JSON {conf_key!r} -> {ok}")
    print(f"updated fields -> {update}")
    print(f"new conf -> {redis.get_json(conf_key)}")
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Demo: child process polls config; main process hot-applies it."
    )
    shared = argparse.ArgumentParser(add_help=False)
    shared.add_argument("--service", default=DEFAULT_SERVICE)
    shared.add_argument("--key", default=DEFAULT_CONF_KEY)

    subcommands = parser.add_subparsers(dest="command", required=True)

    run_parser = subcommands.add_parser(
        "run", parents=[shared], help="run the program with child-process config polling"
    )
    run_parser.add_argument(
        "--poll-seconds",
        type=float,
        default=DEFAULT_CONF_POLL_SECONDS,
        help="child-process hot-config polling interval; default: 1.0",
    )
    run_parser.add_argument(
        "--step-seconds",
        type=float,
        default=DEFAULT_DEMO_STEP_SECONDS,
        help="demo-only algorithm step duration; set 0 for no artificial delay",
    )

    set_parser = subcommands.add_parser("set", parents=[shared], help="set config fields")
    set_parser.add_argument("--message", help="set conf['message']")
    set_parser.add_argument("--multiplier", type=int, help="set conf['multiplier']")
    set_parser.add_argument(
        "--conf", help="advanced: JSON object, for example: '{\"enabled\": true}'"
    )
    set_parser.add_argument("--replace", action="store_true", help="replace instead of merge")

    enabled = set_parser.add_mutually_exclusive_group()
    enabled.add_argument("--enabled", action="store_true", help="set conf['enabled'] to true")
    enabled.add_argument("--disabled", action="store_true", help="set conf['enabled'] to false")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.command == "run":
        return run_app(args.service, args.key, args.poll_seconds, args.step_seconds)
    if args.command == "set":
        return set_conf(args.service, args.key, args)
    raise ValueError(f"unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
