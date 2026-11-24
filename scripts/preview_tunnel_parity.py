#!/usr/bin/env python3
"""Compare the preview tunnel (`GET /api/preview/tunnel?port=N`, upstream #39415) of both servers.

Each server gets its own home and a loopback echo server to tunnel to; nothing leaves loopback.
The suite compares each server's answers: a request that is not a WebSocket upgrade, bad ports,
a missing session, a closed port, the server's own port, bytes carried both ways (a short message
and a 300 KB one), the tunnel closing when `enablePreviewTunnel` is switched off, the refusal while
it is off, and a managed policy whose `limits.previewTunnel` is false (with its summary in the
config).
"""
import argparse
from contextlib import ExitStack, closing
import json
import os
import re
from pathlib import Path
import secrets
import shlex
import socket
import tempfile
import threading
import time

from websockets.exceptions import ConnectionClosed, InvalidStatus
from websockets.sync.client import connect

from parity_harness import encode, http, managed_server, recv_frame

REFUSALS = ("x-t3-tunnel-disabled", "x-t3-tunnel-port-refused")


def echo_server(stack):
    """A loopback TCP server that sends back whatever it receives."""
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(16)
    stack.callback(listener.close)

    def serve(connection):
        with connection:
            while True:
                try:
                    data = connection.recv(65536)
                except OSError:
                    return
                if not data:
                    return
                connection.sendall(data)

    def accept():
        while True:
            try:
                connection, _ = listener.accept()
            except OSError:
                return
            threading.Thread(target=serve, args=(connection,), daemon=True).start()

    threading.Thread(target=accept, daemon=True).start()
    return listener.getsockname()[1]


def closed_port():
    """A loopback port nothing listens on."""
    with closing(socket.socket()) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def refusal(error, port):
    """A refused handshake as status, body (its port and trace id masked) and the tunnel's refusal
    headers."""
    response = error.response
    body = (response.body or b"").decode(errors="replace").replace(str(port), "<port>")
    body = re.sub(r'"traceId":"[0-9a-f]+"', '"traceId":"<trace>"', body)
    return {"status": response.status_code,
            "body": body,
            "headers": {name: response.headers.get(name) for name in REFUSALS if response.headers.get(name)}}


def tunnel(url, cookie, port, **params):
    query = "&".join([f"port={port}"] + [f"{key}={value}" for key, value in params.items()])
    headers = {"Cookie": cookie} if cookie else {}
    return connect(url.replace("http", "ws", 1) + f"/api/preview/tunnel?{query}", additional_headers=headers,
                   proxy=None, compression=None, max_size=None, open_timeout=15)


def attempt(url, cookie, port):
    """The handshake's outcome: `open`, or the refusal."""
    try:
        with tunnel(url, cookie, port):
            return "open"
    except InvalidStatus as error:
        return refusal(error, port)


def round_trip(ws, payload):
    """Send `payload` and read until as many bytes came back."""
    ws.send(payload)
    received = b""
    deadline = time.monotonic() + 20
    while len(received) < len(payload) and time.monotonic() < deadline:
        message = ws.recv(timeout=max(0.1, deadline - time.monotonic()))
        received += message if isinstance(message, bytes) else message.encode()
    return received == payload


def rpc(url, cookie, tag, payload):
    with connect(url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2", additional_headers={"Cookie": cookie},
                 proxy=None, compression=None) as ws:
        ws.send(encode({"_tag": "Request", "id": "1", "tag": tag, "payload": payload, "headers": []}).decode())
        return json.loads(recv_frame(ws, timeout=15))["exit"]


def observe(command, root, echo):
    """One server's answers."""
    out = {}
    with ExitStack() as stack:
        url, cookie = managed_server(stack, command, root, secrets.token_hex(16), secrets.token_bytes(32))
        own = int(url.rsplit(":", 1)[1].split("/")[0])
        status, headers, body = http(url, "GET", f"/api/preview/tunnel?port={echo}", headers={"Cookie": cookie})
        out["not an upgrade"] = {"status": status, "body": body.decode(errors="replace")}
        out["port 0"] = attempt(url, cookie, 0)
        out["port not a number"] = attempt(url, cookie, "eighty")
        out["port above 65535"] = attempt(url, cookie, 70000)
        out["no session"] = attempt(url, None, echo)
        out["closed port"] = attempt(url, cookie, closed_port())
        out["own port"] = attempt(url, cookie, own)
        with tunnel(url, cookie, echo) as ws:
            out["short message"] = round_trip(ws, b"hello through the tunnel")
            out["300 KB message"] = round_trip(ws, os.urandom(300 * 1024))
        # Switching the tunnel off closes an open one.
        with tunnel(url, cookie, echo) as ws:
            ws.send(b"ping")
            ws.recv(timeout=10)
            out["switch off"] = rpc(url, cookie, "server.updateSettings", {"patch": {"enablePreviewTunnel": False}})["_tag"]
            try:
                ws.recv(timeout=10)
                out["closed when switched off"] = False
            except ConnectionClosed:
                out["closed when switched off"] = True
            except TimeoutError:
                out["closed when switched off"] = False
        out["while switched off"] = attempt(url, cookie, echo)
        rpc(url, cookie, "server.updateSettings", {"patch": {"enablePreviewTunnel": True}})
        out["switched on again"] = attempt(url, cookie, echo)
    return out


def observe_policy(command, root, echo):
    """A server under a managed policy that switches the tunnel off."""
    root.mkdir(parents=True)
    policy = root / "policy.json"
    policy.write_bytes(encode({"schemaVersion": 1, "organization": "Fixture Org", "limits": {"previewTunnel": False}}))
    out = {}
    with ExitStack() as stack:
        prefix = ["env", f"T3CODE_DEV_MANAGED_POLICY_PATH={policy}"] + command
        url, cookie = managed_server(stack, prefix, root / "home", secrets.token_hex(16), secrets.token_bytes(32))
        out["policy refusal"] = attempt(url, cookie, echo)
        config = rpc(url, cookie, "server.getConfig", {})
        out["policy summary"] = (config.get("value") or {}).get("managedPolicy", {}).get("limits")
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    outcomes = {}
    with ExitStack() as stack:
        echo = echo_server(stack)
        root = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="t3-tunnel-parity-")))
        for name, command in (("original", args.original_command), ("rust", args.rust_command)):
            command = shlex.split(command)
            outcomes[name] = {**observe(command, root / name, echo), **observe_policy(command, root / f"{name}-policy", echo)}
    failures = 0
    for check in outcomes["original"]:
        same = outcomes["original"][check] == outcomes["rust"].get(check)
        print(("PASS" if same else "FAIL"), check)
        if not same:
            failures += 1
            print(f"  original {json.dumps(outcomes['original'][check], sort_keys=True)}")
            print(f"  rust     {json.dumps(outcomes['rust'].get(check), sort_keys=True)}")
    # The echo checks must actually carry bytes, not merely agree.
    for check in ("short message", "300 KB message", "closed when switched off"):
        if outcomes["original"].get(check) is not True:
            failures += 1
            print(f"FAIL {check}: the reference did not succeed ({outcomes['original'].get(check)})")
    print(f"{len(outcomes['original'])} preview tunnel checks compared across the reference and the Rust server")
    return int(bool(failures))


if __name__ == "__main__":
    raise SystemExit(main())
