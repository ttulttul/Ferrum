#!/usr/bin/env python3
"""Clone repositories into new projects with `projectClone.start`, follow them
with `subscribeProjectClones`, and cancel and retry them, on the reference and
on the Rust server, and compare what each does.

Every remote is local: bare repositories over `file://`, or over an SSH URL to
127.0.0.1 whose `ssh` (GIT_SSH_COMMAND) is a fake that runs `git-upload-pack`
on this machine, after a scripted delay when a case needs git to go quiet. So a
clone can stall (twenty seconds without progress), recover, or be cancelled
while it waits. Both servers send any HTTP through a loopback recorder
(HTTP_PROXY and friends) that forwards nothing, and the fake `ssh` logs every
host it is asked for; the suite fails if anything was aimed off loopback other
than the reference's model-manifest refresh, which the recorder refuses.
Telemetry is off for both servers. All data is fake.

Covered: a clone that finishes (its progress stages, project, checkout and
retention), a destination git has started writing, a destination or project
already being cloned (a cancelled clone's destination, a running clone's
project), a destination that is not empty, a clone that stalls and recovers, a
clone stalled and cancelled then retried, a failed clone and a retry refused
for foreign files, a thread launch refused while a clone runs, a clone onto a
folder another project owns, and a project deleted while its clone runs.
"""
import argparse
from contextlib import ExitStack
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import secrets
import shlex
import subprocess
from tempfile import TemporaryDirectory
import threading
import time
from urllib.parse import urlsplit

from websockets.sync.client import connect

from parity_harness import encode, http, isolated_git_env, managed_server

# Logs the host git asks for, waits as scripted for the repository, then runs
# git's remote command here. It gives up if git (its parent) goes away.
FAKE_SSH = """#!/usr/bin/env python3
import json, os, pathlib, sys, time
args = sys.argv[1:]
host, rest = None, list(args)
while rest:
    arg = rest.pop(0)
    if arg in ("-o", "-p", "-i", "-l"):
        rest.pop(0)
    elif not arg.startswith("-"):
        host = arg
        break
with open(os.environ["FAKE_SSH_LOG"], "a") as out:
    out.write(f"{host}\\n")
command = args[-1]
delays = json.loads(pathlib.Path(os.environ["FAKE_SSH_DELAYS"]).read_text())
delay = next((float(v) for k, v in delays.items() if k in command), 0.0)
parent = os.getppid()
deadline = time.time() + delay
while time.time() < deadline:
    if os.getppid() != parent:
        sys.exit(1)
    time.sleep(0.2)
os.execvp("sh", ["sh", "-c", command])
"""

LOOPBACK = {"127.0.0.1", "localhost", "::1"}
# The reference refreshes its model manifest from GitHub at start-up and has no
# switch for it; the recorder refuses that request like any other.
REFUSED_BACKGROUND = {"raw.githubusercontent.com"}


def git(*args, cwd=None, env=None):
    # parity-env: git builds the fixture repositories, not a T3 server
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True, env=env).stdout.strip()


def bare_repository(root, name, env):
    work = root / f"{name}-work"
    work.mkdir(parents=True)
    git("init", "-q", "-b", "main", cwd=work, env=env)
    for index in range(40):
        (work / f"file-{index}.txt").write_text(f"Fixture file {index}\n" * 50)
    git("add", ".", cwd=work, env=env)
    git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-q", "-m", "Initial", cwd=work, env=env)
    bare = root / f"{name}.git"
    git("clone", "-q", "--bare", str(work), str(bare), env=env)
    return bare, git("rev-parse", "HEAD", cwd=work, env=env)


class Recorder(BaseHTTPRequestHandler):
    """A proxy that forwards nothing: it records each request's target and refuses it."""
    def log_message(self, *args):
        pass

    def refuse(self):
        self.server.seen.append(self.path if self.command != "CONNECT" else f"connect://{self.path}")
        self.send_response(403)
        self.send_header("Content-Length", "0")
        self.end_headers()

    do_GET = do_POST = do_PUT = do_HEAD = do_CONNECT = refuse


def off_loopback(targets):
    hosts = []
    for target in targets:
        host = urlsplit(target if "://" in target else f"//{target}").hostname
        if host not in LOOPBACK and host not in REFUSED_BACKGROUND:
            hosts.append(target)
    return hosts


class Server:
    def __init__(self, stack, command, home, secret, key, env):
        self.url, self.cookie = managed_server(stack, shlex.split(command), home, secret, key, env=env)
        ws = self.url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2"
        options = dict(additional_headers={"Cookie": self.cookie}, compression=None, proxy=None)
        self.socket = stack.enter_context(connect(ws, **options))
        self.lock = threading.Lock()
        self.sequence = 0
        # The clone list as subscribeProjectClones streams it, each list as it came.
        self.lists = []
        self.changed = threading.Condition()
        watcher = stack.enter_context(connect(ws, **options))
        watcher.send(encode(dict(_tag="Request", id="clones", tag="subscribeProjectClones", payload={}, headers=[])).decode())
        threading.Thread(target=self.follow, args=(watcher,), daemon=True).start()

    def follow(self, watcher):
        try:
            while True:
                frame = json.loads(watcher.recv())
                if frame.get("_tag") == "Ping":
                    watcher.send(encode({"_tag": "Pong"}).decode())
                elif frame.get("_tag") == "Chunk":
                    watcher.send(encode({"_tag": "Ack", "requestId": "clones"}).decode())
                    with self.changed:
                        self.lists.extend(frame["values"])
                        self.changed.notify_all()
        except Exception:
            pass

    def rpc(self, tag, payload):
        with self.lock:
            self.sequence += 1
            request = f"clone-{self.sequence}"
            self.socket.send(encode(dict(_tag="Request", id=request, tag=tag, payload=payload, headers=[])).decode())
            while True:
                frame = json.loads(self.socket.recv(timeout=120))
                if frame.get("_tag") == "Ping":
                    self.socket.send(encode({"_tag": "Pong"}).decode())
                    continue
                if frame.get("requestId") == request and "exit" in frame:
                    return frame["exit"]

    def clone(self, project_id):
        with self.changed:
            for snapshots in reversed(self.lists):
                return next((s for s in snapshots if s["projectId"] == project_id), None)
        return None

    def history(self, project_id):
        with self.changed:
            return [s for snapshots in self.lists for s in snapshots if s["projectId"] == project_id]

    def wait(self, project_id, test, timeout=90):
        """The first streamed snapshot of `project_id` from now on that passes `test`."""
        deadline = time.time() + timeout
        with self.changed:
            while time.time() < deadline:
                current = next((s for s in (self.lists[-1] if self.lists else []) if s["projectId"] == project_id), None)
                if test(current):
                    return current
                self.changed.wait(timeout=0.5)
        raise AssertionError(f"{project_id}: no snapshot passed the test; last {self.clone(project_id)}")

    def projects(self):
        status, _, body = http(self.url, "GET", "/api/projects", None, {"Cookie": self.cookie})
        assert status == 200, (status, body)
        return {p["id"]: p for p in json.loads(body)["projects"]}


def shape(snapshot):
    """A snapshot with its times, sequence and transfer detail masked."""
    if snapshot is None:
        return None
    masked = dict(snapshot)
    for key in ("startedAt", "endedAt"):
        masked[key] = None if masked.get(key) is None else "<time>"
    masked["sequence"] = "<n>"
    if masked.get("detail") is not None:
        masked["detail"] = "<detail>"
    return masked


def trail(history, key):
    """The values of `key` a clone went through, repeats collapsed."""
    seen = []
    for snapshot in history:
        if not seen or seen[-1] != snapshot[key]:
            seen.append(snapshot[key])
    return seen


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
        fixtures = tmp / "fixtures"
        repos = {name: bare_repository(fixtures, name, git_env) for name in ("plain", "recover", "stall", "doomed")}
        fake_ssh = tmp / "fake-ssh"
        fake_ssh.write_text(FAKE_SSH)
        fake_ssh.chmod(0o755)
        ssh_url = lambda name: f"ssh://fixture@127.0.0.1{repos[name][0]}"
        for side, command in (("original", args.original_command), ("rust", args.rust_command)):
            with ExitStack() as stack:
                home = tmp / side
                delays = tmp / f"{side}-delays.json"
                delays.write_text(json.dumps({"recover.git": 23, "stall.git": 600, "doomed.git": 600}))
                ssh_log = tmp / f"{side}-ssh.log"
                proxy = stack.enter_context(ThreadingHTTPServer(("127.0.0.1", 0), Recorder))
                proxy.seen = []
                threading.Thread(target=proxy.serve_forever, daemon=True).start()
                stack.callback(proxy.shutdown)
                proxy_url = f"http://127.0.0.1:{proxy.server_port}"
                env = {
                    **isolated_git_env(tmp / "git"),
                    "GIT_SSH_COMMAND": str(fake_ssh), "FAKE_SSH_LOG": str(ssh_log), "FAKE_SSH_DELAYS": str(delays),
                    # Any HTTP either server sends goes to the recorder, which forwards nothing.
                    **{name: proxy_url for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")},
                    "NO_PROXY": "", "no_proxy": "", "NODE_USE_ENV_PROXY": "1",
                    "T3CODE_TELEMETRY_ENABLED": "false",
                }
                server = Server(stack, command, home, secrets.token_urlsafe(32), secrets.token_bytes(32), env)
                work = home / "clones"
                work.mkdir(parents=True)
                record = {}
                now = "2026-11-22T00:00:00.000Z"

                def start(project_id, destination, **source):
                    return server.rpc("projectClone.start", {"projectId": project_id, "title": f"Clone {project_id}",
                                                              "createdAt": now, "destinationPath": str(destination), **source})

                def launch_thread(project_id):
                    return server.rpc("orchestration.launchThread", {
                        "commandId": f"launch-{project_id}", "projectId": project_id, "title": "Draft",
                        "modelSelection": {"instanceId": "codex", "model": "fixture"}, "runtimeMode": "full-access",
                        "interactionMode": "default", "workspaceStrategy": {"type": "root"}})

                def listing(path):
                    return sorted(p.name for p in path.iterdir()) if path.exists() else None

                # A clone that finishes.
                record["start"] = start("p-plain", work / "plain", remoteUrl=f"file://{repos['plain'][0]}")
                done = server.wait("p-plain", lambda s: s and s["phase"] != "running")
                record["finished"] = shape(done)
                # The reference's stream keeps only the latest list when a client lags, so
                # which stages in between a client sees depends on timing.
                stages = trail(server.history("p-plain"), "stage")
                record["finished stages"] = [stages[0], stages[-1], set(stages) <= {
                    "connecting", "counting", "receiving", "resolving", "checkout"}]
                record["finished phases"] = trail(server.history("p-plain"), "phase")
                record["finished checkout"] = git("rev-parse", "HEAD", cwd=work / "plain", env=git_env) == repos["plain"][1]
                project = server.projects().get("p-plain") or {}
                record["finished project"] = {k: project.get(k) for k in ("title", "workspaceRoot", "deletedAt")}
                record["retry a finished clone"] = server.rpc("projectClone.retry", {"projectId": "p-plain"})
                record["cancel a finished clone"] = server.rpc("projectClone.cancel", {"projectId": "p-plain"})
                record["cancel an unknown clone"] = server.rpc("projectClone.cancel", {"projectId": "p-none"})
                record["retry an unknown clone"] = server.rpc("projectClone.retry", {"projectId": "p-none"})
                (work / "full").mkdir()
                (work / "full" / "notes.txt").write_text("keep\n")
                record["a destination that is not empty"] = start("p-full", work / "full", remoteUrl=f"file://{repos['plain'][0]}")

                # A clone stalled and cancelled, while others try its destination and project.
                record["stalling start"] = start("p-stall", work / "stall", remoteUrl=ssh_url("stall"))
                # Both servers check the destination before the clones in progress, and git writes
                # .git into it before it even calls ssh, so wait for that: then a second clone
                # into it is refused for the files, on both, however fast the runner.
                git_started = time.time() + 30
                while not (work / "stall" / ".git").exists() and time.time() < git_started:
                    time.sleep(0.1)
                record["same destination, once git has started"] = start(
                    "p-other", work / "stall", remoteUrl=f"file://{repos['plain'][0]}")
                record["same project"] = start("p-stall", work / "elsewhere", remoteUrl=f"file://{repos['plain'][0]}")
                record["launch while cloning"] = launch_thread("p-stall")
                record["stalled"] = shape(server.wait("p-stall", lambda s: s and s.get("stalled")))
                record["cancel"] = server.rpc("projectClone.cancel", {"projectId": "p-stall"})
                record["cancelled"] = shape(server.wait("p-stall", lambda s: s and s["phase"] == "cancelled"))
                record["cancelled folder"] = listing(work / "stall")
                record["cancel again"] = server.rpc("projectClone.cancel", {"projectId": "p-stall"})
                # The cancelled clone is still tracked and its destination is empty again, so a
                # clone of another project into it is refused as one in progress.
                record["same destination as a cancelled clone"] = start(
                    "p-other", work / "stall", remoteUrl=f"file://{repos['plain'][0]}")
                record["launch after a cancel"] = launch_thread("p-stall")
                delays.write_text(json.dumps({"recover.git": 23, "doomed.git": 600}))
                record["retry"] = server.rpc("projectClone.retry", {"projectId": "p-stall"})
                record["retried"] = shape(server.wait("p-stall", lambda s: s and s["phase"] == "done"))
                record["retried checkout"] = git("rev-parse", "HEAD", cwd=work / "stall", env=git_env) == repos["stall"][1]

                # A clone that stalls and recovers.
                record["recovering start"] = start("p-recover", work / "recover", remoteUrl=ssh_url("recover"))
                server.wait("p-recover", lambda s: s and s.get("stalled"))
                record["recovered"] = shape(server.wait("p-recover", lambda s: s and s["phase"] != "running"))
                record["recovered stalls"] = trail(server.history("p-recover"), "stalled")

                # A failed clone, and a retry refused for files that are not the clone's.
                record["failing start"] = start("p-missing", work / "missing", remoteUrl=f"file://{tmp}/missing.git")
                record["failed"] = shape(server.wait("p-missing", lambda s: s and s["phase"] == "failed"))
                record["failed folder"] = listing(work / "missing")
                record["launch after a failure"] = launch_thread("p-missing")
                (work / "missing" / "notes.txt").write_text("mine\n")
                record["retry refused"] = server.rpc("projectClone.retry", {"projectId": "p-missing"})
                record["refused folder"] = listing(work / "missing")

                # A clone onto a folder another project owns clones for that project.
                record["owner project"] = server.rpc("projects.mutate", {
                    "type": "project.create", "commandId": "owner-create", "projectId": "p-owner", "title": "Owner",
                    "workspaceRoot": str(work / "owned"), "createWorkspaceRootIfMissing": True})
                record["owner project"] = record["owner project"].get("_tag")
                record["clone onto an owned folder"] = start("p-claim", work / "owned", remoteUrl=f"file://{repos['plain'][0]}")
                record["owned clone"] = shape(server.wait("p-owner", lambda s: s and s["phase"] != "running"))
                record["no claim left"] = server.clone("p-claim")

                # A project deleted while its clone runs.
                record["doomed start"] = start("p-doomed", work / "doomed", remoteUrl=ssh_url("doomed"))
                server.wait("p-doomed", lambda s: s and s["phase"] == "running")
                time.sleep(1)
                record["delete"] = server.rpc("projects.mutate", {
                    "type": "project.delete", "commandId": "doomed-delete", "projectId": "p-doomed"}).get("_tag")
                record["deleted clone"] = server.wait("p-doomed", lambda s: s is None, timeout=30)
                record["deleted folder"] = listing(work / "doomed")

                # A finished clone is forgotten thirty seconds after it ends.
                record["finished clone kept a while"] = server.clone("p-owner") is not None
                server.wait("p-plain", lambda s: s is None, timeout=60)
                record["finished clone forgotten"] = True
                record["listed now"] = sorted(s["projectId"] for s in (server.lists[-1] if server.lists else []))

                # Nothing aimed off loopback.
                hosts = ssh_log.read_text().split() if ssh_log.exists() else []
                assert hosts and not off_loopback(hosts), f"{side}: ssh asked for {hosts}"
                assert not off_loopback(proxy.seen), f"{side}: requests left loopback: {proxy.seen}"
                record["ssh hosts"] = sorted(set(hosts))
                text = json.dumps(record)
                for path, mark in ((str(home), "<home>"), (str(fixtures), "<fixtures>"), (str(tmp), "<tmp>")):
                    text = text.replace(path, mark)
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
    print(f"{total - failures}/{total} project clone checks matched across the reference and the Rust server")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
