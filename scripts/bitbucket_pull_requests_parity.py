#!/usr/bin/env python3
"""List pull requests for a Bitbucket project on the reference and on the Rust server, with the
Bitbucket API a loopback fake, and compare how each reports what the API did.

`pullRequests.list` asks Bitbucket for the viewer (`GET /user`) before reading a repository.
The fake (`T3CODE_BITBUCKET_API_BASE_URL`) answers it as each case says: 401 (the credential is
rejected; Bitbucket answers an anonymous `/user` so), 404, 500, a dropped connection, or nothing
at all (a closed port). Both servers must give the same error, the same list result inside it,
and the same requests to the fake. Nothing may leave loopback: the harness's refusing proxy
must have seen nothing but the reference's model-manifest refresh. All data is fake.
"""
import argparse
from contextlib import ExitStack
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import secrets
import shlex
import socket
import subprocess
from tempfile import TemporaryDirectory
import threading
from urllib.parse import urlsplit

from websockets.sync.client import connect

import parity_harness
from parity_harness import encode, http, isolated_git_env, managed_server

CASES = ("401", "404", "500", "dropped connection", "closed port")
# The reference refreshes its model manifest from GitHub at start-up with no switch for it;
# the harness's proxy refuses it like anything else off loopback.
ALLOWED_REFUSALS = {"raw.githubusercontent.com"}


def fake_bitbucket(case, seen):
    class Bitbucket(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            seen.append({"path": self.path, "authorization": self.headers.get("Authorization")})
            if case == "dropped connection":
                self.connection.close()
                return
            body = encode({"type": "error", "error": {"message": "Fixture"}})
            self.send_response(int(case))
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
    return Bitbucket


def closed_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def git(*args, env):
    # parity-env: git builds the fixture checkout, not a T3 server
    subprocess.run(["git", *args], check=True, capture_output=True, env=env)


def list_pull_requests(url, cookie):
    with connect(url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2",
                 additional_headers={"Cookie": cookie}, proxy=None, compression=None) as ws:
        ws.send(encode({"_tag": "Request", "id": "list", "tag": "pullRequests.list",
                        "payload": {"state": "open", "projectId": "project:bitbucket"}, "headers": []}).decode())
        while True:
            frame = json.loads(ws.recv(timeout=90))
            if frame.get("_tag") == "Ping":
                ws.send(encode({"_tag": "Pong"}).decode())
            elif frame.get("requestId") == "list" and "exit" in frame:
                return frame["exit"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    answers = {}
    with TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        for side, command in (("original", args.original_command), ("rust", args.rust_command)):
            for case in CASES:
                with ExitStack() as stack:
                    seen = []
                    if case == "closed port":
                        port = closed_port()
                    else:
                        api = stack.enter_context(ThreadingHTTPServer(("127.0.0.1", 0), fake_bitbucket(case, seen)))
                        api.daemon_threads = True
                        threading.Thread(target=api.serve_forever, daemon=True).start()
                        stack.callback(api.shutdown)
                        port = api.server_port
                    base = f"http://127.0.0.1:{port}/2.0"
                    assert urlsplit(base).hostname == "127.0.0.1"
                    home = tmp / side / case.replace(" ", "-")
                    checkout = home / "checkout"
                    checkout.mkdir(parents=True)
                    env = {**isolated_git_env(tmp / "git"), "T3CODE_BITBUCKET_API_BASE_URL": base}
                    git_env = {**os.environ, **env}
                    git("-C", str(checkout), "init", "-qb", "main", env=git_env)
                    git("-C", str(checkout), "remote", "add", "origin",
                        "git@ssh.bitbucket.org:example-org/example-repo.git", env=git_env)
                    url, cookie = managed_server(stack, shlex.split(command), home,
                                                 secrets.token_urlsafe(32), secrets.token_bytes(32), env=env)
                    status, _, body = http(url, "POST", "/api/projects/mutate", encode({
                        "type": "project.create", "commandId": "create", "projectId": "project:bitbucket",
                        "title": "Bitbucket checkout", "workspaceRoot": str(checkout)}),
                        {"Cookie": cookie, "Content-Type": "application/json"})
                    assert status == 200, (status, body)
                    answer = list_pull_requests(url, cookie)
                    answers.setdefault(side, {})[case] = {"answer": answer, "requests": seen}
    refused = {urlsplit(f"//{entry.split(' ', 1)[1]}").hostname for entry in parity_harness.REFUSED}
    assert refused <= ALLOWED_REFUSALS, f"requests left loopback: {sorted(refused - ALLOWED_REFUSALS)}"
    failures = 0
    for case in CASES:
        original, rust = answers["original"][case], answers["rust"][case]
        for part in ("answer", "requests"):
            same = original[part] == rust[part]
            failures += not same
            print(f"{'PASS' if same else 'FAIL'} {case}: {part}")
            if not same:
                print(f"  original {json.dumps(original[part])[:2000]}\n  rust     {json.dumps(rust[part])[:2000]}")
    total = len(CASES) * 2
    print(f"{total - failures}/{total} Bitbucket pull request checks matched; nothing left loopback")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
