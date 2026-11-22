#!/usr/bin/env python3
"""Run the reference and the Rust server in turn on one T3 home and check that
they share its orchestration database, `userdata/statev2.sqlite`
(docs/shared-store.md).

Checks:
- a database the Rust server creates is the reference's schema at the pinned
  migration head, and the reference opens it without migrating;
- projects and threads created on the reference are seen identically by the
  Rust server (shell, thread projection);
- a database at a migration head the Rust server does not support is refused,
  clearly and without being changed.

All data is fake: temporary homes, generated keys and fixture text. No
provider CLI is installed, so a turn ends as soon as it starts.
"""
import argparse
from contextlib import ExitStack, contextmanager
import hashlib
import json
import os
from pathlib import Path
import secrets
import shlex
import shutil
import sqlite3
import subprocess
from tempfile import TemporaryDirectory
import time

from client_initial_sync_parity import rpc
from parity_harness import PROVIDER_CLIS, encode, free_port, http, managed_server, path_without

PROJECT = "project:shared-store"
THREAD = "thread:shared-store-reference"
HEADERS = {"x-t3-orchestration-protocol": "2"}


@contextmanager
def running(command, home, credentials):
    with ExitStack() as stack:
        yield managed_server(stack, shlex.split(command), home, *credentials)


def database(home):
    return home / "userdata" / "statev2.sqlite"


def schema(path):
    """Every schema object with its stored text, in creation order, and the ledger."""
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as conn:
        objects = conn.execute("SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY rowid").fetchall()
        ledger = conn.execute("SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id").fetchall()
    return objects, ledger


def digest(path):
    """The database's bytes, with its WAL folded in by whoever closed it last."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def seed(url, cookie, root):
    """A project and a thread with one message, created as the web client does."""
    workspace = root / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    status, _, body = http(url, "POST", "/api/projects/mutate", encode({
        "type": "project.create", "commandId": "create-project", "projectId": PROJECT,
        "title": "Shared store", "workspaceRoot": str(workspace)}),
        {"Cookie": cookie, "Content-Type": "application/json"})
    assert status == 200, (status, body[:400])
    rpc(url, cookie, "orchestration.dispatchCommand", {
        "type": "thread.create", "commandId": "create-thread", "threadId": THREAD, "projectId": PROJECT,
        "title": "Made on the reference", "modelSelection": {"instanceId": "codex", "model": "fixture"},
        "runtimeMode": "full-access", "interactionMode": "default", "branch": None, "worktreePath": None,
        "createdBy": "user", "creationSource": "web"})
    rpc(url, cookie, "orchestration.dispatchCommand", {
        "type": "message.dispatch", "commandId": "send-message", "threadId": THREAD,
        "messageId": "message:shared-store", "text": "Hello from the shared store fixture", "attachments": [],
        "createdBy": "user", "creationSource": "web", "dispatchMode": {"type": "start_immediately"}})


def settle(url, cookie, seconds=60):
    """Waits until the seeded thread has no run in flight (no provider CLI is installed)."""
    deadline = time.monotonic() + seconds
    while True:
        projection = rpc(url, cookie, "orchestration.getThreadProjection", {"threadId": THREAD})
        runs = projection.get("runs", [])
        if runs and not any(run["status"] in ("preparing", "starting", "running", "waiting") for run in runs):
            return
        assert time.monotonic() < deadline, f"the seeded turn did not settle: {[run['status'] for run in runs]}"
        time.sleep(0.5)


def views(url, cookie):
    """What a client sees of the seeded state: the shell's projects and threads, and the thread."""
    def get(path):
        status, _, body = http(url, "GET", path, headers={"Cookie": cookie, **HEADERS})
        assert status == 200, (path, status, body[:400])
        return json.loads(body)

    shell = get("/api/orchestration/shell")
    return {
        "projects": [project for project in shell.get("projects", []) if project.get("id") == PROJECT],
        "threads": [thread for thread in shell.get("threads", []) if thread.get("id") == THREAD],
        "projection": rpc(url, cookie, "orchestration.getThreadProjection", {"threadId": THREAD}),
    }


def comparable(view):
    """Drops what is computed per process rather than read from the database."""
    view = json.loads(json.dumps(view))
    for project in view["projects"]:
        # Each server derives it from the workspace's Git remote on read.
        project.pop("repositoryIdentity", None)
    return view


def refused(command, home, credentials, root):
    """Starts the Rust server on `home` and returns its exit status and output."""
    server = shlex.split(command)
    scratch = root / "refusal"
    scratch.mkdir(exist_ok=True)
    env = {k: os.environ[k] for k in ("LANG", "LC_ALL") if k in os.environ}
    env.update(PATH=path_without(PROVIDER_CLIS, scratch / ".parity-path"), HOME=str(home), T3CODE_HOME=str(home))
    result = subprocess.run(server + ["--host", "127.0.0.1", "--port", str(free_port())],
                            capture_output=True, text=True, env=env, cwd=home, timeout=60)
    return result.returncode, result.stdout + result.stderr


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    reference, rust = args.original_command, args.rust_command
    credentials = secrets.token_urlsafe(32), secrets.token_bytes(32)
    checks = []

    def same(name, expected, actual):
        assert expected == actual, f"{name}\n reference {json.dumps(expected)[:4000]}\n rust      {json.dumps(actual)[:4000]}"
        checks.append(name)

    with TemporaryDirectory() as tmp:
        root = Path(tmp)

        # 1. The schema a fresh home gets from each server.
        reference_home, rust_home = root / "reference-fresh", root / "rust-fresh"
        with running(reference, reference_home, credentials):
            pass
        with running(rust, rust_home, credentials):
            pass
        reference_schema, rust_schema = schema(database(reference_home)), schema(database(rust_home))
        same("fresh database schema", reference_schema[0], rust_schema[0])
        same("fresh database migration ledger", reference_schema[1], rust_schema[1])
        with running(reference, rust_home, credentials):
            pass
        same("reference opens the Rust server's database without migrating", rust_schema, schema(database(rust_home)))

        # 2. A project and a thread made on the reference, read by both.
        home = root / "shared"
        with running(reference, home, credentials) as (url, cookie):
            seed(url, cookie, root)
            settle(url, cookie)
            reference_view = views(url, cookie)
        with running(rust, home, credentials) as (url, cookie):
            rust_view = views(url, cookie)
        assert reference_view["threads"], "the reference's shell lists the seeded thread"
        same("project made on the reference", comparable(reference_view)["projects"], comparable(rust_view)["projects"])
        same("thread shell made on the reference", reference_view["threads"], rust_view["threads"])
        same("thread projection made on the reference", reference_view["projection"], rust_view["projection"])
        with running(reference, home, credentials) as (url, cookie):
            same("the reference still reads the home after the Rust server", comparable(reference_view),
                 comparable(views(url, cookie)))

        # 3. Migration heads the Rust server does not support.
        for name, statement, expected in [
            ("newer", "INSERT INTO effect_sql_migrations (migration_id, name) VALUES (99, 'FixtureFuture')", "newer"),
            ("older", "DELETE FROM effect_sql_migrations WHERE migration_id = (SELECT MAX(migration_id) FROM effect_sql_migrations)", "older"),
        ]:
            copy = root / f"unsupported-{name}"
            shutil.copytree(home, copy, symlinks=True)
            conn = sqlite3.connect(database(copy), isolation_level=None)
            conn.execute(statement)
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            conn.close()
            before = digest(database(copy))
            status, output = refused(rust, copy, credentials, root)
            assert status != 0, (name, status, output[-2000:])
            assert expected in output and "was not changed" in output, (name, output[-2000:])
            same(f"{name} migration head left unchanged", before, digest(database(copy)))

    print(f"{len(checks)}/{len(checks)} shared store checks matched across the reference and the Rust server on one home")


if __name__ == "__main__":
    main()
