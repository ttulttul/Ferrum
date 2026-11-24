#!/usr/bin/env python3
"""Clone repositories with `sourceControl.cloneRepository` on the reference and
on the Rust server, and compare every answer and every clone.

Every remote is local: bare repositories reached over `file://` or as plain
paths, a credential-bearing URL to a closed loopback port, and an SSH remote
whose `ssh` (GIT_SSH_COMMAND, set for both servers) always refuses the key, so
the clone falls back to HTTPS on a closed loopback port. Nothing contacts a
real host. Covered: destination rules (empty, `~`, missing parents, existing
files and folders), a clone that succeeds and the commit it checks out, a
repository that is not there, redacted credentials, and the SSH-to-HTTPS
fallback and its combined failure, and the same clone run by an agent with
`t3_project_clone`. All data is fake.
"""
import argparse
from contextlib import ExitStack
import json
import os
import re
from pathlib import Path
import secrets
import shlex
import socket
import subprocess
from tempfile import TemporaryDirectory

from websockets.sync.client import connect

from parity_harness import encode, http, isolated_git_env, managed_server, mask_port
from peer_forwarding_parity import McpClient, mcp_token

FAKE_SSH = """#!/bin/sh
echo "git@127.0.0.1: Permission denied (publickey)." >&2
exit 255
"""


def git(*args, cwd=None, env=None):
    # parity-env: git builds the fixture repositories, not a T3 server
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True, env=env).stdout.strip()


def closed_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def bare_repository(root, env):
    work = root / "work"
    work.mkdir(parents=True)
    git("init", "-q", "-b", "main", cwd=work, env=env)
    (work / "README.md").write_text("Fixture repository\n")
    git("add", "README.md", cwd=work, env=env)
    git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
        "commit", "-q", "-m", "Initial commit", cwd=work, env=env)
    bare = root / "remote.git"
    git("clone", "-q", "--bare", str(work), str(bare), env=env)
    return bare, git("rev-parse", "HEAD", cwd=work, env=env)


class Server:
    def __init__(self, stack, command, home, secret, key, env):
        self.url, cookie = managed_server(stack, shlex.split(command), home, secret, key, env=env)
        self.cookie = cookie
        self.home = home
        self.socket = stack.enter_context(connect(
            self.url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2",
            additional_headers={"Cookie": cookie}, compression=None, proxy=None))
        self.sequence = 0

    def pairing_code(self):
        status, _, body = http(self.url, "POST", "/api/auth/pairing-token", encode({}),
                               {"Cookie": self.cookie, "Content-Type": "application/json"})
        assert status == 200, (status, body)
        return json.loads(body)["credential"]

    def rpc(self, tag, payload, timeout=150):
        self.sequence += 1
        request = f"clone-{self.sequence}"
        self.socket.send(encode(dict(_tag="Request", id=request, tag=tag, payload=payload, headers=[])).decode())
        while True:
            frame = json.loads(self.socket.recv(timeout=timeout))
            if frame.get("_tag") == "Ping":
                self.socket.send(encode({"_tag": "Pong"}).decode())
                continue
            if frame.get("requestId") == request and "exit" in frame:
                return frame["exit"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--show", action="store_true", help="print each answer as the reference gave it")
    args = parser.parse_args()
    answers = {}
    with TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        git_env = {**os.environ, **isolated_git_env(tmp / "git")}
        bare, commit = bare_repository(tmp / "fixture", git_env)
        fake_ssh = tmp / "fake-ssh"
        fake_ssh.write_text(FAKE_SSH)
        fake_ssh.chmod(0o755)
        port = closed_port()
        for side, command in (("original", args.original_command), ("rust", args.rust_command)):
            with ExitStack() as stack:
                home = tmp / side
                env = {**isolated_git_env(tmp / "git"), "GIT_SSH_COMMAND": str(fake_ssh)}
                server = Server(stack, command, home, secrets.token_urlsafe(32), secrets.token_bytes(32), env)
                work = home / "clones"
                (work / "full").mkdir(parents=True)
                (work / "full" / "notes.txt").write_text("keep\n")
                (work / "a-file").write_text("not a folder\n")
                (work / "empty").mkdir()
                record = {}

                def clone(name, **payload):
                    record[name] = server.rpc("sourceControl.cloneRepository", payload)

                clone("over file://", remoteUrl=f"file://{bare}", destinationPath=str(work / "over-file"))
                clone("from a plain path", remoteUrl=str(bare), destinationPath=str(work / "plain"))
                clone("into an empty folder", remoteUrl=str(bare), destinationPath=str(work / "empty"))
                clone("missing parents are made", remoteUrl=str(bare), destinationPath=str(work / "a" / "b" / "c"))
                clone("a destination under ~", remoteUrl=str(bare), destinationPath="~/clones/tilde")
                clone("padded inputs are trimmed", remoteUrl=f"  {bare}  ", destinationPath=f"  {work / 'padded'}  ")
                record["checked out"] = [
                    (work / name / ".git").exists() and git("rev-parse", "HEAD", cwd=work / name, env=git_env) == commit
                    for name in ("over-file", "plain", "empty", "a/b/c", "tilde", "padded")]
                record["origin of a clone"] = (work / "plain" / ".git").exists() and git(
                    "remote", "get-url", "origin", cwd=work / "plain", env=git_env).replace(str(bare), "<bare>")
                clone("a folder that is not empty", remoteUrl=str(bare), destinationPath=str(work / "full"))
                clone("a file in the way", remoteUrl=str(bare), destinationPath=str(work / "a-file"))
                clone("an empty destination", remoteUrl=str(bare), destinationPath="   ")
                clone("no repository", destinationPath=str(work / "none"))
                clone("a repository that is not there", remoteUrl=f"file://{tmp}/missing.git",
                      destinationPath=str(work / "missing"))
                record["nothing left by a failed clone"] = sorted(
                    p.name for p in (work / "missing").iterdir()) if (work / "missing").exists() else None
                clone("credentials are redacted", remoteUrl=f"http://fixture:s3cret@127.0.0.1:{port}/repo.git?token=abc",
                      destinationPath=str(work / "redacted"))
                clone("ssh refused, https tried too", remoteUrl="git@127.0.0.1:fixture/repo.git",
                      destinationPath=str(work / "ssh"))
                clone("ssh asked for explicitly", remoteUrl="git@127.0.0.1:fixture/repo.git", protocol="ssh",
                      destinationPath=str(work / "ssh-only"))
                clone("a custom ssh port is not rewritten", remoteUrl=f"ssh://git@127.0.0.1:{port}/fixture/repo.git",
                      destinationPath=str(work / "ssh-port"))
                # An agent clones with t3_project_clone: the same clone, its failure as the tool's.
                agent = McpClient(server.url, mcp_token(server, "full-access"))
                record["t3_project_clone"] = agent.call("t3_project_clone", {
                    "remoteUrl": f"file://{bare}", "destinationPath": str(work / "agent")})
                record["t3_project_clone checked out"] = (work / "agent" / ".git").exists() and git(
                    "rev-parse", "HEAD", cwd=work / "agent", env=git_env) == commit
                record["t3_project_clone, a repository that is not there"] = agent.call("t3_project_clone", {
                    "remoteUrl": f"file://{tmp}/missing.git", "destinationPath": str(work / "agent-missing")})
                record["t3_project_clone, a folder that is not empty"] = agent.call("t3_project_clone", {
                    "remoteUrl": str(bare), "destinationPath": str(work / "full")})
                text = re.sub(r"after \d+ ms", "after <n> ms", json.dumps(record))
                for path, mark in ((str(home), "<home>"), (str(bare), "<bare>"), (str(tmp), "<tmp>")):
                    text = text.replace(path, mark)
                text = mask_port(text, port)
                answers[side] = json.loads(text)
    failures = 0
    for name in answers["original"]:
        same = answers["original"][name] == answers["rust"].get(name)
        failures += not same
        if args.show:
            print(f"{name}: {json.dumps(answers['original'][name])[:1500]}")
        print(f"{'PASS' if same else 'FAIL'} {name}")
        if not same:
            print(f"  original {json.dumps(answers['original'][name])[:2500]}\n  rust     {json.dumps(answers['rust'].get(name))[:2500]}")
    total = len(answers["original"])
    print(f"{total - failures}/{total} clone checks matched across the reference and the Rust server")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
