#!/usr/bin/env python3
"""A stand-in for the `opencode` CLI in provider status tests; never the real binary.

Usage: fake_opencode.py <spec.json> <arguments...>

  {"version": {"stdout": "1.15.0", "stderr": "", "exit": 0, "sleep": 0},
   "serve": {"routes": {"GET /global/health": {"status": 200, "body": {...},
                                                "contentType": "application/json", "sleep": 0}},
             "exit": {"code": 1, "stderr": "..."},   # exit before listening
             "silent": true}}                          # never report the URL

`opencode --version` answers from "version". `opencode serve --hostname=H
--port=P` listens on H:P (or another port when P is taken), prints OpenCode's "opencode server listening on
http://H:P" line and answers each `METHOD /path` from "serve.routes" (404
otherwise) until it is stopped. A route with "sequence": [route, ...] answers
its successive requests with each entry in turn, the last one repeating. A
route with "sse": true holds the response open as an event stream (OpenCode
2's `GET /api/event`), and "serve.events": {"METHOD /path": [event, ...]}
sends those events on every open stream once that request was answered. In
place of a list, {"sequence": [[event, ...], ...]} sends each list for one
request in turn (an OpenCode 1.x turn's events, prompt by prompt), the last
one repeating, and an event {"sleep": seconds} pauses the stream. Text
"{{body.<field>}}" in an event becomes that field of the request's JSON body
(the message id T3 gave a prompt). A route with "status": 204 answers with no
body. Every launch appends its arguments to
FAKE_PROVIDER_LOG.args (with the port replaced by PORT), and every request
`METHOD /path` to one of OpenCode's routes (plus " basic" when it carries
Basic credentials) to FAKE_PROVIDER_LOG.http, and every `/session` (OpenCode
2: `/api/session`, and its MCP and instructions routes under `/api/experimental/`), `/mcp`, `/permission/...` and
`/question/...` request as JSON with its query and body.
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import queue
import re
import sys
import threading
import time

spec_path, args = sys.argv[1], sys.argv[2:]
log = os.environ.get("FAKE_PROVIDER_LOG")
if log:
    with open(log + ".args", "a") as out:
        out.write(re.sub(r"--port=\d+", "--port=PORT", " ".join(args)) + "\n")
with open(spec_path) as source:
    spec = json.load(source)


def emit(stream, text):
    if text:
        stream.write(text if text.endswith("\n") else text + "\n")
        stream.flush()


if args[:1] == ["--version"]:
    version = spec.get("version", {})
    emit(sys.stdout, version.get("stdout", ""))
    emit(sys.stderr, version.get("stderr", ""))
    time.sleep(version.get("sleep", 0))
    sys.exit(version.get("exit", 0))

if args[:1] != ["serve"]:
    sys.exit(0)

serve = spec.get("serve", {})
if "exit" in serve:
    emit(sys.stderr, serve["exit"].get("stderr", ""))
    sys.exit(serve["exit"].get("code", 1))
options = dict(arg[2:].split("=", 1) for arg in args[1:] if arg.startswith("--") and "=" in arg)
host, port = options.get("hostname", "127.0.0.1"), int(options.get("port", "0"))
routes = serve.get("routes", {})
served = {}
# Open event streams, each fed its own queue.
streams, streams_lock = [], threading.Lock()
OPENCODE_PATH = re.compile(r"^/(?:api/|global/|provider$|agent$|skill$|command$)")


# How many requests to each route sent their events ("sequence" event lists).
event_lists = {}
PLACEHOLDER = re.compile(r"\{\{body\.([A-Za-z0-9_]+)\}\}")


def substitute(value, fields):
    """`value` with each "{{body.<field>}}" replaced by that field of the request's body."""
    if isinstance(value, str):
        return PLACEHOLDER.sub(lambda match: str(fields.get(match.group(1), "")), value)
    if isinstance(value, list):
        return [substitute(item, fields) for item in value]
    if isinstance(value, dict):
        return {key: substitute(item, fields) for key, item in value.items()}
    return value


def wait_for_title_gate():
    """With FAKE_TITLE_GATE set (a scenario's "titleAfterTurn"), answer a title prompt only once
    that file exists: the parity driver creates it when the turn settled, so the title lands
    after the same events on both servers instead of racing the turn."""
    gate = os.environ.get("FAKE_TITLE_GATE")
    deadline = time.monotonic() + 60
    while gate and not os.path.exists(gate) and time.monotonic() < deadline:
        time.sleep(0.05)


class Handler(BaseHTTPRequestHandler):
    def answer(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        path, _, query = self.path.partition("?")
        basic = (self.headers.get("Authorization") or "").startswith("Basic ")
        # Only OpenCode's own routes are recorded: both servers pick the port by binding port 0,
        # so a T3 client of another scenario can briefly reach this one (`GET /ws`).
        if log and OPENCODE_PATH.match(path):
            with open(log + ".http", "a") as out:
                out.write(f"{self.command} {path}{' basic' if basic else ''}\n")
        # A session's requests (text generation, turns), T3's MCP server and the answers to
        # OpenCode's requests, with their query and body, as JSON.
        if log and re.match(r"^/(?:session|api/session|api/experimental/(?:mcp|session)/|mcp$|permission/|question/)", path):
            try:
                body = json.loads(raw) if raw else None
            except ValueError:
                body = raw.decode(errors="replace")
            with open(log + ".http", "a") as out:
                headers = {name.lower(): value for name, value in self.headers.items()
                           if name.lower().startswith("x-opencode") or name.lower() == "content-type"}
                out.write(json.dumps({"method": self.command, "path": path, "query": query,
                                      "basic": basic, "headers": headers, "body": body}, sort_keys=True) + "\n")
        key = f"{self.command} {path}"
        # A session's prompt is a title being generated.
        if self.command == "POST" and re.match(r"^(?:/session/[^/]+/message|/api/session/[^/]+/prompt)$", path):
            wait_for_title_gate()
        route = routes.get(key, {"status": 404, "body": {"error": "not found"}})
        if route.get("sse"):
            return self.stream()
        if "sequence" in route:
            count = served.get(key, 0)
            served[key] = count + 1
            route = route["sequence"][min(count, len(route["sequence"]) - 1)]
        time.sleep(route.get("sleep", 0))
        status = route.get("status", 200)
        body = b"" if status == 204 else json.dumps(route.get("body", {})).encode()
        try:
            self.send_response(status)
            if status != 204:
                self.send_header("Content-Type", route.get("contentType", "application/json"))
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass
        events = serve.get("events", {}).get(key, [])
        if isinstance(events, dict):
            count = event_lists.get(key, 0)
            event_lists[key] = count + 1
            lists = events.get("sequence") or [[]]
            events = lists[min(count, len(lists) - 1)]
        try:
            fields = json.loads(raw) if raw else {}
        except ValueError:
            fields = {}
        for event in events:
            if set(event) == {"sleep"}:
                time.sleep(event["sleep"])
                continue
            event = substitute(event, fields if isinstance(fields, dict) else {})
            with streams_lock:
                for stream in streams:
                    stream.put(event)

    def stream(self):
        """An event stream: each queued event as a `data:` frame, until the client goes."""
        events = queue.Queue()
        with streams_lock:
            streams.append(events)
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(b": connected\n\n")
            self.wfile.flush()
            while True:
                try:
                    event = events.get(timeout=5)
                    self.wfile.write(b"data: " + json.dumps(event).encode() + b"\n\n")
                except queue.Empty:
                    self.wfile.write(b": keepalive\n\n")
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            with streams_lock:
                streams.remove(events)

    do_GET = do_POST = do_DELETE = do_PATCH = do_PUT = answer

    def log_message(self, *_):
        pass


try:
    server = ThreadingHTTPServer((host, port), Handler)
except OSError:
    # Both servers pick the port by binding port 0 and releasing it, so a process of another
    # scenario can take it before this one binds. Listen elsewhere instead: both servers read the
    # address from the line below, and a lost port is not what any scenario tests.
    server = ThreadingHTTPServer((host, 0), Handler)
server.daemon_threads = True
if not serve.get("silent"):
    emit(sys.stdout, f"opencode server listening on http://{host}:{server.server_address[1]}")
try:
    server.serve_forever()
except KeyboardInterrupt:
    pass
