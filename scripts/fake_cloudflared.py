#!/usr/bin/env python3
"""A stand-in for cloudflared that never contacts Cloudflare.

Servers under test run this through `T3CODE_CLOUDFLARED_PATH`. `version` prints a
pinned version line. Any other invocation records how it was started, as one JSON
line appended to `fake-cloudflared.jsonl` beside the executable, then logs the lines a
real connector prints once its tunnel registers, and waits for a signal. Nothing is read
from the environment, so the recorded environment is exactly what the server passed:

- `argv`: the arguments after the executable;
- `envAdded` / `envRemoved` / `envChanged`: names that differ from the environment
  the parent process was started with (`/proc/<ppid>/environ`), and `tunnelToken`,
  the `TUNNEL_TOKEN` value;
- `stdin`, `stdinOpen`, `stdout`, `stderr`: what each descriptor is (pipes and
  socket pairs both count as `pipe`) and whether stdin is still open;
- `sameProcessGroup`: whether it shares its parent's process group.

`fake-cloudflared-options.json` beside the executable, `{"exitAfter": seconds,
"exitTimes": n}`, makes the first n starts exit with status 3 after that delay, for
restart tests.
"""
import datetime
import json
import os
import select
import signal
import socket
import stat
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

VERSION_LINE = "cloudflared version 2026.10.4 (built 2026-10-30-1200 UTC)"


def descriptor_kind(fd):
    try:
        mode = os.fstat(fd).st_mode
    except OSError:
        return "closed"
    if stat.S_ISFIFO(mode) or stat.S_ISSOCK(mode):
        return "pipe"
    if stat.S_ISCHR(mode):
        return "tty" if os.isatty(fd) else "char"
    if stat.S_ISREG(mode):
        return "file"
    return "other"


def stdin_open():
    try:
        readable, _, _ = select.select([0], [], [], 0)
    except (OSError, ValueError):
        return False
    if not readable:
        return True
    try:
        return os.read(0, 1) != b""
    except OSError:
        return False


def parent_environment():
    try:
        with open(f"/proc/{os.getppid()}/environ", "rb") as handle:
            raw = handle.read()
    except OSError:
        return None
    entries = {}
    for item in raw.split(b"\0"):
        if b"=" in item:
            key, value = item.split(b"=", 1)
            entries[key.decode(errors="replace")] = value.decode(errors="replace")
    return entries


HERE = os.path.dirname(os.path.abspath(sys.argv[0]))
RECORD = os.path.join(HERE, "fake-cloudflared.jsonl")
OPTIONS = os.path.join(HERE, "fake-cloudflared-options.json")


def options():
    try:
        with open(OPTIONS, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return {}


def record(entry):
    path = RECORD
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, sort_keys=True) + "\n")
        handle.flush()


def log(message):
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    sys.stderr.write(f"{stamp} {message}\n")
    sys.stderr.flush()


class Ready(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({"status": 200, "readyConnections": 1, "connectorId": "fake"}).encode()
        if self.path != "/ready":
            body = b"404 page not found\n"
            self.send_response(404)
        else:
            self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


def starts_so_far():
    if not os.path.exists(RECORD):
        return 0
    with open(RECORD, encoding="utf-8") as handle:
        return sum(1 for line in handle if '"event": "start"' in line)


def main():
    args = sys.argv[1:]
    if args[:1] == ["version"] or args[:1] == ["--version"]:
        print(VERSION_LINE)
        return 0

    parent = parent_environment()
    env = dict(os.environ)
    entry = {
        "event": "start",
        "pid": os.getpid(),
        "argv": args,
        "tunnelToken": env.get("TUNNEL_TOKEN"),
        "stdin": descriptor_kind(0),
        "stdinOpen": stdin_open(),
        "stdout": descriptor_kind(1),
        "stderr": descriptor_kind(2),
        "sameProcessGroup": os.getpgid(0) == os.getpgid(os.getppid()),
        "cwd": os.getcwd(),
    }
    if parent is not None:
        entry["envAdded"] = sorted(set(env) - set(parent))
        entry["envRemoved"] = sorted(set(parent) - set(env))
        entry["envChanged"] = sorted(k for k in set(env) & set(parent) if env[k] != parent[k])
    previous_starts = starts_so_far()
    record(entry)

    stopping = threading.Event()

    def on_signal(signum, _frame):
        record({"event": "signal", "pid": os.getpid(), "signal": signal.Signals(signum).name})
        stopping.set()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    metrics = ThreadingHTTPServer(("127.0.0.1", 0), Ready)
    threading.Thread(target=metrics.serve_forever, daemon=True).start()
    port = metrics.server_address[1]
    log("INF Starting tunnel tunnelID=00000000-0000-4000-8000-000000000000")
    log(f"INF Starting metrics server on 127.0.0.1:{port}/metrics")
    log("INF Registered tunnel connection connIndex=0 "
        f"connection={uuid.uuid4()} event=0 ip=198.41.200.73 location=sea08 protocol=quic")

    settings = options()
    exit_after = settings.get("exitAfter")
    exit_times = int(settings.get("exitTimes", 1))
    deadline = None
    if exit_after is not None and previous_starts < exit_times:
        deadline = time.monotonic() + float(exit_after)
    while not stopping.is_set():
        if deadline is not None and time.monotonic() >= deadline:
            record({"event": "exit", "pid": os.getpid(), "code": 3})
            log("ERR fake connector exiting for a restart test")
            return 3
        stopping.wait(0.05)
    log("INF Initiating graceful shutdown due to signal")
    return 0


if __name__ == "__main__":
    sys.exit(main())
