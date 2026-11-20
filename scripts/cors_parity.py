#!/usr/bin/env python3
"""Compare browser CORS answers (preflights and simple requests) from two T3 servers.

Browser clients such as the desktop renderer (origin ``t3code://app``) only see
a response when its CORS headers allow it, so the status and every
``access-control-*`` header, in wire order, must match the reference for each
request. Requests are unauthenticated and never mutate state.
"""
import argparse
import contextlib
from pathlib import Path
import secrets
import shlex
import tempfile

from parity_harness import http, managed_server

ORIGINS = ["t3code://app", "http://localhost:5173", None]
PREFLIGHT_PATHS = [
    "/",
    "/.well-known/t3/environment",
    "/api/auth/session",
    "/api/auth/browser-session",
    "/api/auth/websocket-ticket",
    "/oauth/token",
    "/api/projects",
    "/api/orchestration/shell",
    "/api/attachments/upload",
    "/api/observability/v1/traces",
    "/ws",
    "/no-such-route",
    "/mcp",
    "/mcp/",
    "/mcp?probe=1",
]
# Routes whose status the two servers agree on without credentials: public
# reads, auth failures on every kind of protected route, WebSocket
# negotiation failures and the MCP origin refusal.
SIMPLE = [
    ("GET", "/.well-known/t3/environment"),
    ("GET", "/api/auth/session"),
    ("GET", "/api/projects"),
    ("GET", "/api/orchestration/shell"),
    ("POST", "/api/auth/websocket-ticket"),
    ("GET", "/ws"),
    ("GET", "/ws?orchestrationProtocol=2"),
    ("GET", "/mcp"),
]
# The allowed headers are a fixed list, so a request for others (or for
# private-network access) must not change the answer.
ODD_PREFLIGHT = {
    "Access-Control-Request-Method": "DELETE",
    "Access-Control-Request-Headers": "x-foo",
    "Access-Control-Request-Private-Network": "true",
}


def answer(base, method, path, headers):
    status, actual, _ = http(base, method, path, b"" if method == "POST" else None, headers)
    cors = [(k.lower(), v) for k, v in actual.items() if k.lower().startswith("access-control-")]
    # Compression adds `vary: Accept-Encoding` on some routes only in the
    # reference; what matters to browsers is whether the answer varies by Origin.
    varies_by_origin = any("origin" in v.lower().split(", ") for k, v in actual.items() if k.lower() == "vary")
    return status, cors, varies_by_origin


def cases():
    for path in PREFLIGHT_PATHS:
        for origin in ORIGINS:
            headers = {"Access-Control-Request-Method": "GET",
                       "Access-Control-Request-Headers": "authorization,traceparent"}
            if origin:
                headers["Origin"] = origin
            yield f"OPTIONS {path} origin={origin}", "OPTIONS", path, headers
    yield "OPTIONS odd preflight", "OPTIONS", "/.well-known/t3/environment", {"Origin": "t3code://app", **ODD_PREFLIGHT}
    for method, path in SIMPLE:
        for origin in ORIGINS:
            yield f"{method} {path} origin={origin}", method, path, {"Origin": origin} if origin else {}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    credentials = secrets.token_urlsafe(32), secrets.token_bytes(32)
    failures = total = 0
    with tempfile.TemporaryDirectory() as temporary, contextlib.ExitStack() as stack:
        servers = [
            managed_server(stack, shlex.split(command), Path(temporary) / name, *credentials)[0]
            for name, command in (("original", args.original_command), ("rust", args.rust_command))
        ]
        for name, method, path, headers in cases():
            total += 1
            values = [answer(base, method, path, headers) for base in servers]
            if values[0] != values[1]:
                failures += 1
                print(f"FAIL {name}\n  original {values[0]}\n  rust     {values[1]}")
            else:
                print(f"PASS {name}")
    print(f"{total - failures}/{total} CORS fixtures matched")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
