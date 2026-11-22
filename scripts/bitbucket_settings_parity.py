#!/usr/bin/env python3
"""Save Bitbucket credentials in Settings (`server.updateSettings`) and check that the reference
and the Rust server read them alike: Bitbucket's sign-in status in `server.discoverSourceControl`
and `pullRequests.list`, against a loopback Bitbucket API (`T3CODE_BITBUCKET_API_BASE_URL`).

Shared home: the servers take turns on one T3 home. The reference saves an access token, then
Rust reads it from the secret store without saving anything; Rust saves an Atlassian email and
API token in its place, then the reference reads those. Each turn's answers must match the turn
before it.

Side by side, each server on a fresh home: no credential; a token Bitbucket rejects; a token in
Settings and another in the environment (Settings wins); only the environment; and a secret
reference to a 1Password vault whose `op` CLI is not on PATH, which cannot be read.

The fake answers `/user` and lists one pull request for the fixture credentials only. Requests
are recorded by which fixture credential they carried, never the credential itself. All data is
fake. Nothing may leave loopback: every server runs behind the harness's loopback proxy, which forwards nothing (parity_harness.assert_nothing_escaped).
"""
import argparse
import base64
from contextlib import ExitStack, contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import secrets
import shlex
import subprocess
from tempfile import TemporaryDirectory
import threading
from urllib.parse import urlsplit

from websockets.sync.client import connect

import parity_harness
from parity_harness import encode, http, isolated_git_env, managed_server, path_without

TOKENS = {"settings-token": "fixture-settings-token", "env-token": "fixture-env-token",
          "rejected-token": "fixture-rejected-token"}
EMAIL, API_TOKEN = "fixture@example.invalid", "fixture-api-token"


def credential_label(header):
    """Which fixture credential a request carried, never the credential itself."""
    if header is None:
        return "none"
    for label, token in TOKENS.items():
        if header == f"Bearer {token}":
            return f"bearer {label}"
    if header == "Basic " + base64.b64encode(f"{EMAIL}:{API_TOKEN}".encode()).decode():
        return "basic api-token"
    return "other"


def fake_bitbucket(seen):
    accepted = {"bearer settings-token", "bearer env-token", "basic api-token"}

    class Bitbucket(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def reply(self, status, body):
            data = encode(body)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            label = credential_label(self.headers.get("Authorization"))
            seen.append(f"{urlsplit(self.path).path} [{label}]")
            if label not in accepted:
                return self.reply(401, {"type": "error"})
            path = urlsplit(self.path).path
            if path == "/2.0/user":
                return self.reply(200, {"username": "fixture-user", "display_name": "Fixture User", "nickname": "fixture-viewer"})
            if path == "/2.0/repositories/example-org/example-repo/pullrequests":
                return self.reply(200, {"values": [{
                    "id": 1, "title": "Change", "state": "OPEN", "author": {"display_name": "Fixture User"},
                    "source": {"branch": {"name": "feature"}}, "destination": {"branch": {"name": "main"}},
                    "created_on": "2026-11-20T09:00:00+00:00", "updated_on": "2026-11-20T10:00:00+00:00",
                    "links": {"html": {"href": "https://example.invalid/example-org/example-repo/pull-requests/1"}}}],
                    "next": None})
            self.reply(404, {"type": "error"})
    return Bitbucket


def git(*args, env):
    # parity-env: git builds the fixture checkout, not a T3 server
    subprocess.run(["git", *args], check=True, capture_output=True, env=env)


class Session:
    # The reference keeps a listing for 30 seconds by its input, through credential changes;
    # each observation asks for a different limit so that every one reads Bitbucket.
    reads = iter(range(90, 200))

    def __init__(self, url, cookie):
        self.url, self.cookie = url, cookie

    def rpc(self, tag, payload):
        with connect(self.url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2",
                     additional_headers={"Cookie": self.cookie}, proxy=None, compression=None) as ws:
            ws.send(encode({"_tag": "Request", "id": "r", "tag": tag, "payload": payload, "headers": []}).decode())
            while True:
                frame = json.loads(ws.recv(timeout=90))
                if frame.get("_tag") == "Ping":
                    ws.send(encode({"_tag": "Pong"}).decode())
                elif frame.get("requestId") == "r" and "exit" in frame:
                    return frame["exit"]

    def save(self, patch):
        # The reply carries the settings (secrets redacted); only whether it was saved is kept.
        return self.rpc("server.updateSettings", {"patch": patch}).get("_tag")

    def observe(self, seen):
        """Bitbucket's discovered sign-in, a listing, and what each asked the fake."""
        seen.clear()
        discovered = self.rpc("server.discoverSourceControl", {})
        providers = discovered.get("value", {}).get("sourceControlProviders", [])
        bitbucket = next((p for p in providers if p.get("kind") == "bitbucket"), None)
        auth = bitbucket and bitbucket.get("auth")
        asked_by_discovery = sorted(seen)
        seen.clear()
        listed = self.rpc("pullRequests.list", {"state": "open", "projectId": "project:bitbucket",
                                                "limit": next(Session.reads)})
        value = listed.get("value")
        summary = ({"entries": [e["number"] for e in value["entries"]], "viewers": value["viewers"]}
                   if listed.get("_tag") == "Success" else
                   {"failure": listed["cause"][0].get("error", {}).get("_tag"),
                    "detail": listed["cause"][0].get("error", {}).get("requirement")
                    or listed["cause"][0].get("error", {}).get("detail")})
        return {"discovered auth": auth, "discovery asked": asked_by_discovery, "listing": summary,
                "listing asked": sorted(seen)}


@contextmanager
def server(command, home, base, env=None):
    with ExitStack() as stack:
        server_env = {**isolated_git_env(home.parent / "git"), "T3CODE_BITBUCKET_API_BASE_URL": base, **(env or {})}
        url, cookie = managed_server(stack, shlex.split(command), home, "fixture-bootstrap-credential",
                                     b"k" * 32, env=server_env)
        yield Session(url, cookie)


def prepare_home(home):
    checkout = home / "checkout"
    checkout.mkdir(parents=True)
    env = {**os.environ, **isolated_git_env(home.parent / "git")}
    git("-C", str(checkout), "init", "-qb", "main", env=env)
    git("-C", str(checkout), "remote", "add", "origin", "git@bitbucket.org:example-org/example-repo.git", env=env)
    return checkout


def create_project(session, checkout):
    status, _, body = http(session.url, "POST", "/api/projects/mutate", encode({
        "type": "project.create", "commandId": "create", "projectId": "project:bitbucket",
        "title": "Bitbucket checkout", "workspaceRoot": str(checkout)}),
        {"Cookie": session.cookie, "Content-Type": "application/json"})
    assert status == 200, (status, body)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    commands = {"original": args.original_command, "rust": args.rust_command}
    checks = []
    with TemporaryDirectory() as tmp, ExitStack() as stack:
        tmp = Path(tmp)
        seen = []
        api = stack.enter_context(ThreadingHTTPServer(("127.0.0.1", 0), fake_bitbucket(seen)))
        api.daemon_threads = True
        threading.Thread(target=api.serve_forever, daemon=True).start()
        stack.callback(api.shutdown)
        base = f"http://127.0.0.1:{api.server_port}/2.0"
        assert urlsplit(base).hostname == "127.0.0.1"

        # Shared home: each server reads what the other saved.
        home = tmp / "shared" / "home"
        checkout = prepare_home(home)
        with server(commands["original"], home, base) as session:
            create_project(session, checkout)
            saved = session.save({"sourceControlHosts": {"bitbucket": {"accessToken": TOKENS["settings-token"]}}})
            reference_saved = {"saved": saved, **session.observe(seen)}
        with server(commands["rust"], home, base) as session:
            rust_read = {"saved": saved, **session.observe(seen)}
            saved = session.save({"sourceControlHosts": {"bitbucket": {"accessToken": "", "email": EMAIL, "apiToken": API_TOKEN}}})
        # Each turn reads in a fresh process: a server keeps a viewer it read for ten minutes.
        with server(commands["rust"], home, base) as session:
            rust_saved = {"saved": saved, **session.observe(seen)}
        with server(commands["original"], home, base) as session:
            reference_read = {"saved": saved, **session.observe(seen)}
        checks.append(("shared home: Rust reads the reference's access token", reference_saved, rust_read))
        checks.append(("shared home: the reference reads Rust's API token", rust_saved, reference_read))

        # Side by side: each server on a fresh home, the same steps.
        answers = {}
        for side, command in commands.items():
            record = answers[side] = {}
            fresh = tmp / side / "home"
            checkout = prepare_home(fresh)
            with server(command, fresh, base) as session:
                create_project(session, checkout)
                record["no credential"] = session.observe(seen)
                session.save({"sourceControlHosts": {"bitbucket": {"accessToken": TOKENS["rejected-token"]}}})
                record["a token Bitbucket rejects"] = session.observe(seen)
            with server(command, fresh, base, {"T3CODE_BITBUCKET_ACCESS_TOKEN": TOKENS["env-token"]}) as session:
                record["a rejected token in Settings beats one in the environment"] = session.observe(seen)
                session.save({"sourceControlHosts": {"bitbucket": {"accessToken": TOKENS["settings-token"]}}})
                record["the token in Settings beats the environment's"] = session.observe(seen)
                session.save({"sourceControlHosts": {"bitbucket": {"accessToken": ""}}})
                record["only the environment"] = session.observe(seen)
            # A 1Password reference with no `op` on PATH cannot be read; nothing else is tried.
            no_op = path_without({"op"}, tmp / f"{side}-path")
            with server(command, fresh, base, {"T3CODE_BITBUCKET_ACCESS_TOKEN": TOKENS["env-token"], "PATH": no_op}) as session:
                record["saving a secret reference"] = session.save({
                    "secretProviders": {"vault": {"kind": "one_password", "label": "Fixture Vault",
                                                  "config": {"account": "fixture.example.invalid"}}},
                    "sourceControlHosts": {"bitbucket": {"accessToken": {"provider": "vault",
                                                                         "reference": "op://Fixture/bitbucket/token"}}}})
                record["a secret reference that cannot be read"] = session.observe(seen)
        for name in answers["original"]:
            checks.append((f"side by side: {name}", answers["original"][name], answers["rust"].get(name)))
    # Every server ran behind a loopback proxy that forwards nothing; what it refused never left.
    parity_harness.assert_nothing_escaped()
    failures = 0
    for name, left, right in checks:
        same = left == right
        failures += not same
        print(f"{'PASS' if same else 'FAIL'} {name}")
        if not same:
            print(f"  first  {json.dumps(left)[:2500]}\n  second {json.dumps(right)[:2500]}")
    print(f"{len(checks) - failures}/{len(checks)} Bitbucket settings checks matched; nothing left loopback")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
