#!/usr/bin/env python3
"""Run the reference and the Rust server in turn on one T3 home and check that
they share its orchestration database, `userdata/statev2.sqlite`
(docs/shared-store.md).

Checks:
- a database the Rust server creates is the reference's schema at the pinned
  migration head, and the reference opens it without migrating;
- projects and threads created on either server are seen identically by the
  other (shell, thread projection), and so is a thread the Rust server
  renamed after reading it from the reference;
- both write event, message and thread times as ISO 8601 with milliseconds;
- a Rust home from before the shared store is imported once, keeping its log,
  and the reference reads its threads as the Rust server does;
- a database at a migration head the Rust server does not support is refused,
  clearly and without being changed;
- provider turns across a switch (CROSS_SCENARIOS): a turn one server cut off
  (stopped as a service manager stops it) is continued by the other, a new
  turn is started on the other server's thread, and queued messages and
  pending approvals carry over. Each scenario runs reference -> reference,
  reference -> Rust and Rust -> reference on one home each, with fake provider
  CLIs, and the switched runs must end as the reference's own restart does
  (client_thread_parity.py's comparison). Known differences are in
  shared_store_parity_xfail.json; an entry that starts matching fails.

All data is fake: temporary homes, generated keys and fixture text. No
provider CLI is installed, so a turn ends as soon as it starts.
"""
import argparse
from contextlib import ExitStack, contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import shutil
import sqlite3
import subprocess
from tempfile import TemporaryDirectory
import time
from types import SimpleNamespace

import client_thread_parity as turns
from client_rpc_parity import run_driver
from client_initial_sync_parity import rpc
from parity_harness import PROVIDER_CLIS, encode, free_port, http, managed_server, path_without, run_concurrently, server_env

PROJECT = "project:shared-store"
THREAD = "thread:shared-store-reference"
RUST_PROJECT = "project:shared-store-rust"
RUST_THREAD = "thread:shared-store-rust"
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


def seed(url, cookie, root, project=PROJECT, thread=THREAD, made_on="the reference"):
    """A project and a thread with one message, created as the web client does."""
    workspace = root / f"workspace-{project.split(':')[1]}"
    workspace.mkdir(parents=True, exist_ok=True)
    status, _, body = http(url, "POST", "/api/projects/mutate", encode({
        "type": "project.create", "commandId": f"create-{project}", "projectId": project,
        "title": f"Made on {made_on}", "workspaceRoot": str(workspace)}),
        {"Cookie": cookie, "Content-Type": "application/json"})
    assert status == 200, (status, body[:400])
    rpc(url, cookie, "orchestration.dispatchCommand", {
        "type": "thread.create", "commandId": f"create-{thread}", "threadId": thread, "projectId": project,
        "title": f"Made on {made_on}", "modelSelection": {"instanceId": "codex", "model": "fixture"},
        "runtimeMode": "full-access", "interactionMode": "default", "branch": None, "worktreePath": None,
        "createdBy": "user", "creationSource": "web"})
    citation = ("[Assistant quote](t3-citation://v1/env/thread/message?"
                "text=Kestrel&start=0&end=7&prefix=&suffix=&comment=Orchard)")
    text = ("Hello from the **shared store** fixture. Hello again! Café 部署 " + citation
            if thread == THREAD else "A shared storehouse fixture also says hello.")
    rpc(url, cookie, "orchestration.dispatchCommand", {
        "type": "message.dispatch", "commandId": f"send-{thread}", "threadId": thread,
        "messageId": f"message:{thread.split(':')[1]}", "text": text, "attachments": [],
        "createdBy": "user", "creationSource": "web", "dispatchMode": {"type": "start_immediately"}})


def settle(url, cookie, thread=THREAD, seconds=60):
    """Waits until a seeded thread has no run in flight (no provider CLI is installed)."""
    deadline = time.monotonic() + seconds
    while True:
        projection = rpc(url, cookie, "orchestration.getThreadProjection", {"threadId": thread})
        runs = projection.get("runs", [])
        if runs and not any(run["status"] in ("preparing", "starting", "running", "waiting") for run in runs):
            return
        assert time.monotonic() < deadline, f"the seeded turn did not settle: {[run['status'] for run in runs]}"
        time.sleep(0.5)


def views(url, cookie, project=PROJECT, thread=THREAD):
    """What a client sees of a seeded project and thread: shell rows and the thread's projection."""
    def get(path):
        status, _, body = http(url, "GET", path, headers={"Cookie": cookie, **HEADERS})
        assert status == 200, (path, status, body[:400])
        return json.loads(body)

    shell = get("/api/orchestration/shell")
    return {
        "projects": [row for row in shell.get("projects", []) if row.get("id") == project],
        "threads": [row for row in shell.get("threads", []) if row.get("id") == thread],
        "projection": rpc(url, cookie, "orchestration.getThreadProjection", {"threadId": thread}),
    }


def search_views(args, root, url, credential):
    """Compare decoded results on precisely the same rows, including IDs, dates and revision."""
    entry = "message:shared-store-reference"
    find = {"threadId": THREAD, "query": "hello"}
    plan = [
        ("rank", "searchThreads", {"query": "shared store"}, False),
        ("phrase", "searchThreads", {"query": "hello", "limit": 1}, False),
        ("accent", "searchThreads", {"query": "cafe"}, False),
        ("cjk", "searchThreads", {"query": "部署"}, False),
        ("quote", "searchThreads", {"query": "kestrel"}, False),
        ("settled", "searchThreads", {"query": "Renamed", "titlesOnly": True,
                                     "olderSettledBefore": "2100", "projectIds": [PROJECT]}, False),
        ("scoped", "searchThreads", {"query": "hello", "olderSettledBefore": "2100", "projectIds": []}, False),
        ("skipQuote", "searchThread", {**find, "query": "Kestrel"}, False),
        ("comment", "searchThread", {**find, "query": "orchard"}, False),
        ("stream", "searchThreadStream", find, True),
        ("warm", "searchThreadStream", find, True),
        ("next", "searchThread", {**find, "start": {"entryId": entry, "occurrence": 0}, "offset": 1}, False),
        ("wrap", "searchThread", {**find, "start": {"entryId": entry, "occurrence": 0}, "offset": -1}, False),
        ("clamp", "searchThread", {**find, "index": 99}, False),
        ("rendered", "searchThread", {**find, "query": "the shared store"}, False),
        ("miss", "searchThreadStream", {**find, "query": "unmatched-needle"}, True),
    ]
    cases = root / "search-cases.json"
    cases.write_text(json.dumps({"steps": [
        {"name": name, "rpc": "orchestration." + rpc, "input": payload,
         **({"stream": {"take": 3}} if stream else {})}
        for name, rpc, payload, stream in plan]}))
    result = run_driver(SimpleNamespace(**vars(args), cases=cases), url, credential, {})
    found = {s["name"]: s.get("value") for s in result["steps"] if s["outcome"] == "reply"}
    assert all(name in found for name, *_ in plan), "search RPC did not decode or finish"
    assert [hit["threadId"] for hit in found["rank"]["matches"]] == [THREAD, RUST_THREAD]
    assert all(len(found[name]["matches"]) == 1 for name in ["accent", "cjk", "quote"])
    assert found["skipQuote"]["totalMatches"] == 0 and found["comment"]["totalMatches"] == 1
    assert [row["id"] for row in found["settled"]["threads"]] == [THREAD]
    assert found["scoped"]["threads"] == [] and len(found["scoped"]["matches"]) == 2
    assert [item.get("complete") for item in found["stream"]["items"]] == [False, None]
    assert len(found["warm"]["items"]) == 1
    assert found["next"]["totalMatches"] == 2 and found["next"]["match"]["occurrence"] == 1
    assert found["rendered"]["totalMatches"] == 1
    return {name: found[name] for name, *_ in plan}


ISO_MS = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z")


def written_times(home, thread):
    """How a server wrote a thread's event, message and thread times: ISO 8601 with three
    digits of milliseconds and `Z` (Effect's `DateTime.formatIso`), and its events' times
    not whole seconds."""
    with sqlite3.connect(f"file:{database(home)}?mode=ro", uri=True) as conn:
        occurred = [row[0] for row in conn.execute(
            "SELECT occurred_at FROM orchestration_events WHERE stream_id = ?1", (thread,))]
        others = [row[0] for row in conn.execute(
            "SELECT created_at FROM orchestration_v2_projection_messages WHERE thread_id = ?1 "
            "UNION ALL SELECT created_at FROM orchestration_v2_projection_threads WHERE thread_id = ?1 "
            "UNION ALL SELECT updated_at FROM orchestration_v2_projection_threads WHERE thread_id = ?1",
            (thread,))]
    assert len(occurred) > 3 and others, (thread, occurred, others)
    # One in a thousand times falls on a whole second; a server keeping whole seconds writes most.
    distinct = set(occurred)
    whole = {t for t in distinct if t.endswith(".000Z")}
    return {"not ISO with milliseconds": [t for t in occurred + others if not ISO_MS.fullmatch(t or "")],
            "event times mostly with milliseconds": 2 * len(whole) < len(distinct)}


def comparable(view):
    """Drops what is computed per process rather than read from the database."""
    view = json.loads(json.dumps(view))
    for project in view["projects"]:
        # Each server derives it from the workspace's Git remote on read.
        project.pop("repositoryIdentity", None)
    return view


# Restart scenarios run with the two halves on different servers. A name is a
# client_thread_parity.py scenario, or one defined here; `follow_up` sends the
# restarted server a new turn.
CROSS_SCENARIOS = [
    {"name": "codex restart mid-turn"},
    {"name": "claude restart mid-turn"},
    {"name": "codex restart queued"},
    {"name": "codex restart unaccepted"},
    {"name": "codex restart continuation off", "follow_up": "Now append a line"},
    {"name": "claude restart continuation off", "follow_up": "Now append a line"},
    # A thread made on one server whose first turn runs on the other, calling T3's MCP
    # tools with the thread credential that server gives the CLI. Each server's provider check
    # settles before the turn, so orchestrator_capabilities reads a settled catalogue.
    {"name": "codex agent mcp on the other server", "follow_up": "Run echo hi and write notes.txt",
     "scenario": {"instance": "codex", "model": "fixture", "scenario": "codex_mcp.json",
                  "awaitStatus": True, "restart": {"after": "created"}}},
    {"name": "claude agent mcp on the other server", "follow_up": "Run echo hi and write notes.txt",
     "scenario": {"instance": "claudeAgent", "model": "claude-opus-5-5", "scenario": "claude_mcp.json",
                  "awaitStatus": True, "restart": {"after": "created"}}},
    # A thread made on one server whose turn the other runs, steers (Codex `turn/steer`) and
    # stops.
    {"name": "codex steer then stop on the other server", "scenario": {
        "instance": "codex", "model": "fixture", "scenario": "codex_steer.json",
        "restart": {"after": "created", "liveTurn": True},
        "steer": {"text": "Also print the date", "ack": "Steered: printing the date too"},
        "interrupt": "steer"}},
    # A first title still being generated when its server stops: the other server takes the
    # outbox's title request over and titles the thread.
    {"name": "codex title request on the other server", "scenario": {
        "instance": "codex", "model": "fixture", "scenario": "codex_title_slow.json",
        "dispatch": {"titleSeed": "Run echo hi and write notes.txt"}, "title": "initial",
        "restart": {"after": "titling"}}},
    {"name": "codex approval waiting", "scenario": {
        "instance": "codex", "model": "fixture", "scenario": "codex_approval.json",
        "runtimeMode": "approval-required", "restart": {"after": "waiting"}}},
    {"name": "claude approval waiting", "scenario": {
        "instance": "claudeAgent", "model": "claude-opus-5-5", "scenario": "claude_approval.json",
        "runtimeMode": "approval-required", "restart": {"after": "waiting"}}},
]
ORDERS = {"reference -> reference": ("reference", "reference"), "reference -> rust": ("reference", "rust"),
          "rust -> reference": ("rust", "reference")}


def cross_turn(args, credentials, scenario, first, second, follow_up):
    """One restart scenario on one home, started on `first` and restarted on `second`."""
    with TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        root = tmp / "home"
        env, fake_bin, log = turns.scenario_env(tmp, "cross", root, scenario, turns.scenario_drivers(scenario))
        outcome = turns.restart_turn(args, shlex.split(first), root, credentials, env, tmp / "checkouts",
                                     fake_bin, scenario, second=shlex.split(second), follow_up=follow_up)
        outcome["launches"] = turns.turn_launches(log)
        outcome["appServers"] = turns.turn_app_servers(log)
        outcome["cli"] = turns.cli_approvals(log)
        outcome["launch"] = turns.cli_launch(log, tmp)
        outcome["mcp"] = turns.agent_mcp(log, tmp)
        return outcome


def cross_turns(args, credentials):
    """Each CROSS_SCENARIOS entry across a switch; the number of unexpected results."""
    xfail = json.loads((Path(__file__).parent / "shared_store_parity_xfail.json").read_text())
    commands = {"reference": args.original_command, "rust": args.rust_command}
    failures = 0
    for entry in CROSS_SCENARIOS:
        if args.cross and entry["name"] not in args.cross:
            continue
        scenario = ({"name": entry["name"], **entry["scenario"]} if "scenario" in entry else
                    next(s for s in turns.PROVIDER_SCENARIOS if s["name"] == entry["name"]))
        outcomes = dict(zip(ORDERS, run_concurrently([
            (lambda first=first, second=second: cross_turn(args, credentials, scenario, commands[first],
                                                           commands[second], entry.get("follow_up")))
            for first, second in ORDERS.values()], len(ORDERS))))
        if "agent mcp" in entry["name"]:
            # The comparison means something only if the CLI called the T3 MCP server.
            assert outcomes["reference -> reference"].get("mcp"), f"{entry['name']}: the CLI made no MCP call"
        if "title request" in entry["name"]:
            # Only a takeover means something: the first server's run was cut off, the
            # second's titled the thread.
            title = outcomes["reference -> reference"].get("title") or {}
            assert len(title.get("runs", [])) == 2 and title["initial"]["title"] != "New thread" \
                and not title["initial"]["pending"], f"{entry['name']}: no title takeover: {title}"
        label_base = entry["name"] + (" + follow-up" if entry.get("follow_up") and "scenario" not in entry else "")
        for order in ("reference -> rust", "rust -> reference"):
            label = f"{label_base} ({order})"
            problems = turns.compare_turn(outcomes["reference -> reference"], outcomes[order])
            expected = xfail.get(label, {})
            for check in sorted(set(problems) | set(expected)):
                if check in problems and check in expected:
                    print(f"XFAIL {label}: {check}: {expected[check]}")
                elif check in problems:
                    failures += 1
                    print(f"FAIL {label}: {check}")
                    for line in problems[check][:20]:
                        print(f"  {line}")
                else:
                    failures += 1
                    print(f"XPASS {label}: {check}: remove it from shared_store_parity_xfail.json")
            if not problems:
                print(f"PASS {label}")
    return failures


def differences(left, right, path="$"):
    """Where two JSON values differ, as `path: left != right` lines."""
    if isinstance(left, dict) and isinstance(right, dict):
        if list(left) != list(right) and set(left) == set(right):
            return [f"{path}: key order {list(left)} != {list(right)}"]
        out = []
        for key in dict.fromkeys([*left, *right]):
            out += differences(left.get(key, "<absent>"), right.get(key, "<absent>"), f"{path}.{key}")
        return out
    if isinstance(left, list) and isinstance(right, list) and len(left) == len(right):
        return [line for index, (a, b) in enumerate(zip(left, right)) for line in differences(a, b, f"{path}[{index}]")]
    return [] if left == right else [f"{path}: {json.dumps(left)[:300]} != {json.dumps(right)[:300]}"]


def refused(command, home, credentials, root):
    """Starts the Rust server on `home` and returns its exit status and output."""
    server = shlex.split(command)
    scratch = root / "refusal"
    scratch.mkdir(exist_ok=True)
    env = server_env(home, {"PATH": path_without(PROVIDER_CLIS, scratch / ".parity-path")})
    result = subprocess.run(server + ["--host", "127.0.0.1", "--port", str(free_port())],
                            capture_output=True, text=True, env=env, cwd=home, timeout=60)
    return result.returncode, result.stdout + result.stderr


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--reference-root", required=True, help="the pinned reference checkout (its client runtime)")
    parser.add_argument("--node", default="node")
    parser.add_argument("--reply-ms", type=int, default=8000)
    parser.add_argument("--skip-turns", action="store_true", help="skip the provider turns across a switch")
    parser.add_argument("--cross", action="append", help="run only this provider turn across a switch")
    args = parser.parse_args()
    args.faketime_lib, args.real_time, args.dump = None, False, None
    reference, rust = args.original_command, args.rust_command
    credentials = secrets.token_urlsafe(32), secrets.token_bytes(32)
    checks = []

    def same(name, expected, actual, labels=("reference", "rust")):
        if expected != actual:
            print(f"{name}: first differences ({labels[0]} vs {labels[1]}):")
            for line in differences(expected, actual)[:12]:
                print("  " + line)
        assert expected == actual, (f"{name}\n {labels[0]:<9} {json.dumps(expected)[:4000]}"
                                    f"\n {labels[1]:<9} {json.dumps(actual)[:4000]}")
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

        # Projects created within one second keep the order they were made in: both servers
        # store a project's creation time in milliseconds, so the shell lists them, and the
        # database orders them, by when they were made rather than by their (random) ids.
        def same_second(command, home):
            with running(command, home, credentials) as (url, cookie):
                for name in ("Zeta", "Alpha", "Mu"):
                    rpc(url, cookie, "projects.createNew", {"name": name})
                    time.sleep(0.01)
                status, _, body = http(url, "GET", "/api/orchestration/shell", headers={"Cookie": cookie, **HEADERS})
                assert status == 200, (status, body[:400])
                listed = [row["title"] for row in json.loads(body).get("projects", [])]
            with sqlite3.connect(f"file:{database(home)}?mode=ro", uri=True) as conn:
                rows = conn.execute("SELECT title, created_at FROM projection_projects "
                                    "ORDER BY created_at, project_id").fetchall()
            return {"shell order": listed, "database order": [row[0] for row in rows],
                    "distinct times": len({row[1] for row in rows}) == len(rows)}

        same("projects made within one second", same_second(reference, root / "reference-order"),
             same_second(rust, root / "rust-order"))

        # Read a disabled task across a server switch, edit it on Rust, then read
        # that exact row (including timestamps and run history) on the reference.
        task_input = dict(id="task:shared", title="Read the report", prompt="Read today's report",
                          projectId=PROJECT, enabled=False, schedule={"type":"interval","everyMs":60000},
                          workspaceStrategy={"type":"root"}, threadMode="fresh_session",
                          modelSelection={"instanceId":"codex","model":"fixture"},
                          runtimeMode="approval-required", interactionMode="default")
        unchanged = {"id": task_input["id"], "enabled": False}
        with running(reference, rust_home, credentials) as session:
            original = rpc(*session, "scheduledTasks.upsert", task_input)["task"]
        with running(rust, rust_home, credentials) as session:
            same("reference schedule read by Rust", original, rpc(*session, "scheduledTasks.setEnabled", unchanged)["task"])
            updated = rpc(*session, "scheduledTasks.upsert", {**task_input, "title":"Updated on Rust"})["task"]
        with running(reference, rust_home, credentials) as session:
            same("Rust schedule edit read by the reference", updated, rpc(*session, "scheduledTasks.setEnabled", unchanged)["task"])

        # 2. A project and a thread made on the reference, read by both.
        home = root / "shared"
        with running(reference, home, credentials) as (url, cookie):
            seed(url, cookie, root)
            settle(url, cookie)
            reference_view = views(url, cookie)
        reference_times = written_times(home, THREAD)
        with running(rust, home, credentials) as (url, cookie):
            rust_view = views(url, cookie)
        assert reference_view["threads"], "the reference's shell lists the seeded thread"
        same("project made on the reference", comparable(reference_view)["projects"], comparable(rust_view)["projects"])
        same("thread shell made on the reference", reference_view["threads"], rust_view["threads"])
        same("thread projection made on the reference", reference_view["projection"], rust_view["projection"])
        with running(reference, home, credentials) as (url, cookie):
            same("the reference still reads the home after the Rust server", comparable(reference_view),
                 comparable(views(url, cookie)))

        # 3. A project and a thread made on the Rust server, and a change it
        # makes to the reference's thread, read back by the reference.
        with running(rust, home, credentials) as (url, cookie):
            seed(url, cookie, root, RUST_PROJECT, RUST_THREAD, "the Rust server")
            settle(url, cookie, RUST_THREAD)
            rpc(url, cookie, "orchestration.dispatchCommand", {
                "type": "thread.metadata.update", "commandId": "rename-reference-thread", "threadId": THREAD,
                "title": "Renamed on the Rust server"})
            rpc(url, cookie, "orchestration.dispatchCommand", {
                "type": "thread.settle", "commandId": "settle-reference-thread", "threadId": THREAD})
            rust_made = views(url, cookie, RUST_PROJECT, RUST_THREAD)
            rust_renamed = views(url, cookie)
            rust_search = search_views(args, root, url, credentials[0])
        assert rust_made["threads"], "the Rust server's shell lists its thread"
        same("event times written with milliseconds", reference_times, written_times(home, RUST_THREAD))
        assert rust_renamed["projection"]["thread"]["title"] == "Renamed on the Rust server", rust_renamed["projection"]["thread"]
        with running(reference, home, credentials) as (url, cookie):
            reference_made = views(url, cookie, RUST_PROJECT, RUST_THREAD)
            reference_renamed = views(url, cookie)
            same("ranked message search and navigating visible occurrences",
                 search_views(args, root, url, credentials[0]), rust_search)
        same("project made on the Rust server", comparable(rust_made)["projects"], comparable(reference_made)["projects"], ("rust", "reference"))
        same("thread shell made on the Rust server", rust_made["threads"], reference_made["threads"], ("rust", "reference"))
        same("thread projection made on the Rust server", rust_made["projection"], reference_made["projection"], ("rust", "reference"))
        same("rename made on the Rust server", comparable(rust_renamed), comparable(reference_renamed), ("rust", "reference"))
        same("the reference opens the database the Rust server wrote without migrating",
             reference_schema, schema(database(home)))

        # 4. A Rust home from before the shared store (its own log, no database): the
        # Rust server imports its threads once, and the reference reads them as it does.
        legacy = root / "legacy"
        with running(rust, legacy, credentials) as (url, cookie):
            seed(url, cookie, root, "project:shared-store-legacy", "thread:shared-store-legacy", "the old Rust server")
            settle(url, cookie, "thread:shared-store-legacy")
        for suffix in ("", "-wal", "-shm"):
            Path(f"{database(legacy)}{suffix}").unlink(missing_ok=True)
        log = legacy / "userdata" / "events.jsonl"
        logged = log.read_bytes()
        with running(rust, legacy, credentials) as (url, cookie):
            rust_legacy = views(url, cookie, "project:shared-store-legacy", "thread:shared-store-legacy")
        assert log.read_bytes().startswith(logged), "the import keeps the log it imported"
        with running(reference, legacy, credentials) as (url, cookie):
            reference_legacy = views(url, cookie, "project:shared-store-legacy", "thread:shared-store-legacy")
        same("project imported from the Rust server's log", comparable(rust_legacy)["projects"],
             comparable(reference_legacy)["projects"], ("rust", "reference"))
        same("thread shell imported from the Rust server's log", rust_legacy["threads"],
             reference_legacy["threads"], ("rust", "reference"))
        same("thread projection imported from the Rust server's log", rust_legacy["projection"],
             reference_legacy["projection"], ("rust", "reference"))

        # 5. Migration heads the Rust server does not support.
        for name, statement, expected in [
            ("newer", "INSERT INTO effect_sql_migrations (migration_id, name) VALUES (9999, 'FixtureFuture')", "newer"),
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
    if not args.skip_turns:
        with TemporaryDirectory() as git:
            turns.GIT_ISOLATION.update(turns.isolated_git_env(Path(git)))
            failures = cross_turns(args, credentials)
        if failures:
            raise SystemExit(f"{failures} unexpected result(s) in provider turns across a switch")
        print(f"provider turns across a switch: {len(CROSS_SCENARIOS)} scenarios matched the reference's own restart")


if __name__ == "__main__":
    main()
