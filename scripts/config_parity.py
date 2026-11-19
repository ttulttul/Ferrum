#!/usr/bin/env python3
"""Check that both servers read and persist the shared userdata/settings.json file."""
import argparse
import json
from contextlib import ExitStack
from pathlib import Path
import secrets
import shlex
from tempfile import TemporaryDirectory

from websockets.sync.client import connect

from parity_harness import encode, managed_server


def call(ws, request_id, tag, payload):
    ws.send(encode({"_tag": "Request", "id": request_id, "tag": tag, "payload": payload, "headers": []}).decode())
    return ws.recv(timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    initial = {}
    patch = {"idleCompaction": "off"}
    credentials = secrets.token_urlsafe(32), secrets.token_bytes(32)
    with TemporaryDirectory() as directory, ExitStack() as stack:
        servers = []
        for name, executable in (("original", args.original_command), ("rust", args.rust_command)):
            root = Path(directory) / name
            state = root / "userdata"
            state.mkdir(parents=True)
            (state / "settings.json").write_text(json.dumps(initial, indent=2) + "\n")
            base, cookie = managed_server(stack, shlex.split(executable), root, *credentials)
            servers.append((base, cookie, root))
        for request_id, (base, cookie, _) in enumerate(servers):
            ws = stack.enter_context(connect(base.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2",
                                             additional_headers={"Cookie": cookie}, proxy=None))
            response = json.loads(call(ws, str(request_id), "server.updateSettings", {"patch": patch}))
            if response.get("exit", {}).get("_tag") != "Success":
                raise AssertionError(response)
        raw = [(root / "userdata" / "settings.json").read_bytes() for _, _, root in servers]
        if raw[0] != raw[1]:
            raise AssertionError(f"settings.json differs\noriginal={raw[0]!r}\nrust={raw[1]!r}")
        expected = b'{\n  "idleCompaction": "off"\n}\n'
        if raw[0] != expected:
            raise AssertionError(f"unexpected settings serialization: {raw[0]!r}")
    print("settings.json load, known settings serialization and atomic persistence matched")


if __name__ == "__main__":
    main()
