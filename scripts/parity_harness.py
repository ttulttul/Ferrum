#!/usr/bin/env python3
"""Run the Rust server and an original T3 server against identical HTTP fixtures.

The harness deliberately compares status and response bytes. It does not parse,
sort, or normalize JSON, so a passing fixture is evidence of wire-level parity.
The original server may be supplied as an existing URL or a command to launch.
"""
from __future__ import annotations

import argparse
import json
import os
import shlex
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


def request(base: str, fixture: dict) -> tuple[int, bytes]:
    body = fixture.get("body")
    data = None if body is None else json.dumps(body, separators=(",", ":")).encode()
    headers = {"content-type": "application/json"} if data is not None else {}
    req = urllib.request.Request(base.rstrip("/") + fixture["path"], data=data, method=fixture["method"], headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


def wait_for_health(base: str) -> None:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        try:
            request(base, {"method": "GET", "path": "/health", "body": None})
            return
        except (OSError, urllib.error.URLError):
            time.sleep(0.1)
    raise RuntimeError(f"server did not become reachable: {base}")


def launch(command: str | None, base: str) -> subprocess.Popen[str] | None:
    if not command:
        wait_for_health(base)
        return None
    process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, start_new_session=True)
    try:
        wait_for_health(base)
    except Exception:
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=5)
        raise
    return process


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-url", default="http://127.0.0.1:3773")
    parser.add_argument("--rust-url", default="http://127.0.0.1:43773")
    parser.add_argument("--original-command", help="command used to launch the original server")
    parser.add_argument("--rust-command", help="command used to launch the Rust server")
    parser.add_argument("--fixtures", default="tests/parity-fixtures.json")
    args = parser.parse_args()
    fixtures = json.loads(Path(args.fixtures).read_text())
    processes = []
    try:
        for command, url in ((args.original_command, args.original_url), (args.rust_command, args.rust_url)):
            process = launch(command, url)
            if process is not None:
                processes.append(process)
        failures = 0
        for fixture in fixtures:
            original = request(args.original_url, fixture)
            rust = request(args.rust_url, fixture)
            if original != rust:
                failures += 1
                print(f"FAIL {fixture['name']}: original={original[0]} rust={rust[0]}")
                print(f"  original bytes: {original[1].hex()}")
                print(f"  rust bytes:     {rust[1].hex()}")
            else:
                print(f"PASS {fixture['name']}: {original[0]} and {len(original[1])} bytes identical")
        return 1 if failures else 0
    finally:
        for process in processes:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)


if __name__ == "__main__":
    sys.exit(main())
