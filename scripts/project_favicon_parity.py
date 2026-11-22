#!/usr/bin/env python3
"""Ask both servers for project favicons (`assets.createUrl` with a `project-favicon`
resource), fetch each URL, and compare what the reference and the Rust server give.

Covered: a project with no icon (and the miss each server caches for a minute), the
well-known icon files, an icon an HTML page links, `t3.json`'s `iconPath`, a `faviconPath`
override outside the workspace and one that is not an image, an `.icns` icon (served as its
embedded PNG), a folder no project owns, and a project a clone is creating: no icon while the
clone runs, its icon once the checkout lands. Clones come from a local bare repository over an
SSH URL to 127.0.0.1 whose fake `ssh` runs `git-upload-pack` here after a delay. Nothing may
leave loopback: the harness's refusing proxy must have seen nothing but the reference's
model-manifest refresh. All data is fake.
"""
import argparse
import base64
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import secrets
import shlex
import struct
import subprocess
from tempfile import TemporaryDirectory
import time
from urllib.parse import unquote, urlsplit

from websockets.sync.client import connect

import parity_harness
from parity_harness import encode, http, isolated_git_env, managed_server

ALLOWED_REFUSALS = {"raw.githubusercontent.com"}
SVG = b'<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16"><rect width="16" height="16"/></svg>\n'
FAKE_SSH = """#!/usr/bin/env python3
import os, sys, time
rest, host = list(sys.argv[1:]), None
while rest:
    arg = rest.pop(0)
    if arg in ("-o", "-p", "-i", "-l"):
        rest.pop(0)
    elif not arg.startswith("-"):
        host = arg
        break
with open(os.environ["FAKE_SSH_LOG"], "a") as out:
    out.write(f"{host}\\n")
parent = os.getppid()
deadline = time.time() + float(os.environ.get("FAKE_SSH_DELAY", "0"))
while time.time() < deadline:
    if os.getppid() != parent:
        sys.exit(1)
    time.sleep(0.2)
os.execvp("sh", ["sh", "-c", sys.argv[-1]])
"""


def png(width):
    """A PNG signature and IHDR naming `width` (enough for both servers' size checks)."""
    return b"\x89PNG\r\n\x1a\n" + struct.pack(">I", 13) + b"IHDR" + struct.pack(">II", width, width) + b"\x08\x06\x00\x00\x00"


def icns(*widths):
    entries = [(f"ic{index:02d}".encode(), png(width)) for index, width in enumerate(widths, start=7)]
    body = b"".join(kind + struct.pack(">I", len(data) + 8) + data for kind, data in entries)
    return b"icns" + struct.pack(">I", len(body) + 8) + body


def git(*args, cwd=None, env=None):
    # parity-env: git builds the fixture repositories, not a T3 server
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True, env=env).stdout.strip()


def fixture_repository(root, env):
    work = root / "work"
    (work / "public").mkdir(parents=True)
    (work / "public" / "favicon.svg").write_bytes(SVG)
    git("init", "-q", "-b", "main", cwd=work, env=env)
    git("add", ".", cwd=work, env=env)
    git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-q", "-m", "Initial", cwd=work, env=env)
    bare = root / "cloned.git"
    git("clone", "-q", "--bare", str(work), str(bare), env=env)
    return bare


def make_projects(home):
    """The fixture workspaces: {project id: (folder, files)}."""
    workspaces = {
        "p-none": {},
        "p-public": {"public/favicon.png": png(32), "favicon-ignored.txt": b"x"},
        "p-linked": {"index.html": b'<html><head><link rel="icon" href="/brand/mark.svg?v=3"></head></html>',
                     "public/brand/mark.svg": SVG},
        "p-t3json": {"t3.json": b'{\n  // the project icon\n  "iconPath": " assets/brand.svg ",\n}\n',
                     "assets/brand.svg": SVG, "favicon.ico": b"\x00\x00\x01\x00"},
        "p-external": {},
        "p-not-image": {"README.md": b"# Fixture\n"},
        "p-icns": {"Resources/AppIcon.icns": icns(64, 128, 256)},
    }
    folders = {}
    for project_id, files in workspaces.items():
        folder = home / "workspaces" / project_id
        folder.mkdir(parents=True)
        for name, data in files.items():
            (folder / name).parent.mkdir(parents=True, exist_ok=True)
            (folder / name).write_bytes(data)
        folders[project_id] = folder
    (home / "outside").mkdir()
    (home / "outside" / "brand.png").write_bytes(png(48))
    return folders


class Server:
    def __init__(self, stack, command, home, env):
        self.home = home
        self.url, self.cookie = managed_server(stack, shlex.split(command), home, secrets.token_urlsafe(32),
                                               secrets.token_bytes(32), env=env)
        self.socket = stack.enter_context(connect(self.url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2",
                                                  additional_headers={"Cookie": self.cookie}, compression=None, proxy=None))
        self.sequence = 0

    def rpc(self, tag, payload):
        self.sequence += 1
        request = f"favicon-{self.sequence}"
        self.socket.send(encode(dict(_tag="Request", id=request, tag=tag, payload=payload, headers=[])).decode())
        while True:
            frame = json.loads(self.socket.recv(timeout=120))
            if frame.get("_tag") == "Ping":
                self.socket.send(encode({"_tag": "Pong"}).decode())
            elif frame.get("requestId") == request and "exit" in frame:
                return frame["exit"]

    def mutate(self, payload):
        status, _, body = http(self.url, "POST", "/api/projects/mutate", encode(payload),
                               {"Cookie": self.cookie, "Content-Type": "application/json"})
        assert status == 200, (status, body)

    def favicon(self, cwd):
        """What createUrl answers, with its token decoded, and what fetching the URL gives."""
        reply = self.rpc("assets.createUrl", {"resource": {"_tag": "project-favicon", "cwd": str(cwd)}})
        if reply.get("_tag") != "Success":
            return {"reply": reply}
        value = dict(reply["value"])
        token, name = value["relativeUrl"].removeprefix("/api/assets/").split("/", 1)
        payload = token.split(".")[0]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        status, headers, body = http(self.url, "GET", value["relativeUrl"], None, {"Cookie": self.cookie})
        headers = {k.lower(): v for k, v in dict(headers).items()}
        value.pop("relativeUrl")
        return {"reply": {**value, "expiresAt": "<bucket>" if value["expiresAt"] % (30 * 60 * 1000) == 0 else value["expiresAt"]},
                "claims": {**claims, "expiresAt": "<bucket>"}, "fileName": unquote(name),
                "served": {"status": status, "contentType": headers.get("content-type") if status == 200 else None,
                           "sha256": hashlib.sha256(body).hexdigest() if status == 200 else None}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    answers = {}
    with TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        git_env = {**os.environ, **isolated_git_env(tmp / "git")}
        bare = fixture_repository(tmp / "fixture", git_env)
        fake_ssh = tmp / "fake-ssh"
        fake_ssh.write_text(FAKE_SSH)
        fake_ssh.chmod(0o755)
        for side, command in (("original", args.original_command), ("rust", args.rust_command)):
            with ExitStack() as stack:
                home = tmp / side
                folders = make_projects(home)
                ssh_log = tmp / f"{side}-ssh.log"
                env = {**isolated_git_env(tmp / "git"), "GIT_SSH_COMMAND": str(fake_ssh), "FAKE_SSH_LOG": str(ssh_log),
                       "FAKE_SSH_DELAY": "10"}
                server = Server(stack, command, home, env)
                for project_id, folder in folders.items():
                    server.mutate({"type": "project.create", "commandId": f"create-{project_id}", "projectId": project_id,
                                   "title": project_id, "workspaceRoot": str(folder)})
                server.mutate({"type": "project.update", "commandId": "override-external", "projectId": "p-external",
                               "faviconPath": str(home / "outside" / "brand.png")})
                server.mutate({"type": "project.update", "commandId": "override-not-image", "projectId": "p-not-image",
                               "faviconPath": "README.md"})
                record = {}
                record["no icon"] = server.favicon(folders["p-none"])
                (folders["p-none"] / "favicon.svg").write_bytes(SVG)
                record["no icon, one added since (the miss is cached)"] = server.favicon(folders["p-none"])
                for project_id in ("p-public", "p-linked", "p-t3json", "p-external", "p-not-image", "p-icns"):
                    record[project_id] = server.favicon(folders[project_id])
                record["a folder no project owns"] = server.favicon(home / "outside")
                # A project a clone is creating: no icon until the checkout lands.
                destination = home / "clones" / "cloned"
                start = server.rpc("projectClone.start", {
                    "projectId": "p-cloned", "title": "Cloned", "createdAt": "2026-11-22T00:00:00.000Z",
                    "remoteUrl": f"ssh://fixture@127.0.0.1{bare}", "destinationPath": str(destination)})
                record["clone started"] = start["_tag"]
                record["while cloning"] = server.favicon(destination)
                deadline = time.time() + 90
                while not (destination / "public" / "favicon.svg").exists() and time.time() < deadline:
                    time.sleep(0.5)
                time.sleep(2)
                record["once cloned"] = server.favicon(destination)
                hosts = ssh_log.read_text().split() if ssh_log.exists() else []
                assert hosts and all(urlsplit(f"//{h}").hostname == "127.0.0.1" for h in hosts), hosts
                text = json.dumps(record)
                for path, mark in ((str(home), "<home>"), (str(tmp), "<tmp>")):
                    text = text.replace(path, mark)
                answers[side] = json.loads(text)
    refused = {urlsplit(f"//{entry.split(' ', 1)[1]}").hostname for entry in parity_harness.REFUSED}
    assert refused <= ALLOWED_REFUSALS, f"requests left loopback: {sorted(refused - ALLOWED_REFUSALS)}"
    failures = 0
    for name in answers["original"]:
        same = answers["original"][name] == answers["rust"].get(name)
        failures += not same
        print(f"{'PASS' if same else 'FAIL'} {name}")
        if not same:
            print(f"  original {json.dumps(answers['original'][name])[:2500]}\n  rust     {json.dumps(answers['rust'].get(name))[:2500]}")
    total = len(answers["original"])
    print(f"{total - failures}/{total} project favicon checks matched; nothing left loopback")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
