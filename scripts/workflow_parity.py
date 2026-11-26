#!/usr/bin/env python3
"""An outside agent works with durable workflow runs through t3_workflow, on the reference and on
the Rust server, and both must answer the same.

The Rust server runs no workflow definitions, so it is compared with the reference whose
workflows folder is empty. First the reference alone builds history from a fake workflow
(fixture.ts: steps, a question, a wait on a channel, a failing step, stop, skip and a child run;
no network and no agents), and that database is copied for every server below:

- workflows off (the default): every action is refused;
- on, with no run kept: nothing is there;
- on, with the copied runs: lists, reads, questions, the store, signals (to a run's wait, to a
  channel, to a question, kept for later, retried), store changes, resumes (nothing is
  registered), cancels (with a child) and a read-only client's change.

Compared: every answer (times and ids masked), whether tools/list offers the tool, and the
workflow tables each server leaves. All data is fake.
"""
import argparse
from contextlib import ExitStack
import json
import re
import secrets
import shutil
import sqlite3
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from peer_forwarding_parity import McpClient, mcp_token
from source_control_clone_parity import Server

FIXTURE = """
const fixture = T3.workflow(
  "fixture",
  { version: 1, payload: Schema.Struct({ mode: Schema.String }) },
  (payload) =>
    Effect.gen(function* () {
      yield* T3.title(`Fixture ${payload.mode}`);
      const counted = yield* T3.step("count", { returns: Schema.Number }, Effect.succeed(2));
      if (payload.mode === "ask") {
        yield* T3.subject({
          projectId: "project:fixture", tracker: "github", host: "github.com",
          scope: "fixture/example", key: "42", url: "https://example.invalid/tasks/42",
        });
        const answer = yield* T3.ask("approve", { title: "Ship it?", choices: ["yes", "no"] });
        return Option.getOrNull(answer);
      }
      if (["wait", "cancel-me", "child"].includes(payload.mode)) {
        const said = yield* T3.wait("go", { returns: Schema.String, channel: "go" });
        return Option.getOrNull(said);
      }
      if (payload.mode === "fail") {
        yield* T3.step("boom", { returns: Schema.Number }, Effect.fail("no luck"));
      }
      if (payload.mode === "stop") return yield* T3.stop("not today");
      if (payload.mode === "skip") return yield* T3.skip("not ours");
      if (payload.mode === "parent") {
        const child = yield* T3.invoke("child", fixture, { mode: "child" }, { key: "child-of-parent" });
        return child.outcome;
      }
      return counted;
    }),
);
"""
MODES = {"done": "completed", "ask": "waiting", "wait": "waiting", "fail": "paused", "stop": "stopped",
         "skip": "skipped", "cancel-me": "waiting", "parent": "waiting"}
TABLES = ("workflow_runs", "workflow_steps", "workflow_waits", "workflow_mail", "workflow_signals",
          "workflow_store", "workflow_triggers", "workflow_hooks", "workflow_run_references")
TIME = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z")
UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def mask(value):
    return json.loads(TIME.sub("<time>", UUID.sub("<uuid>", json.dumps(value, sort_keys=True))))


def start_server(stack, command, home, workflows=None, enabled=True, database=None):
    (home / "userdata").mkdir(parents=True, exist_ok=True)
    if enabled:
        (home / "userdata" / "settings.json").write_text(json.dumps({"workflowsEnabled": True}))
    if database is not None:
        shutil.copy(database, home / "userdata" / "statev2.sqlite")
    env = {"T3CODE_WORKFLOWS": str(workflows)} if workflows is not None else {}
    return Server(stack, command, home, secrets.token_urlsafe(32), secrets.token_bytes(32), env)


def tables(home):
    with sqlite3.connect(home / "userdata" / "statev2.sqlite") as db:
        db.row_factory = sqlite3.Row
        found = {}
        for table in TABLES:
            rows = [dict(row) for row in db.execute(f"SELECT * FROM {table}")]
            found[table] = sorted((mask(row) for row in rows), key=lambda row: json.dumps(row, sort_keys=True))
        return found


def listed(server, token):
    client = McpClient(server.url, token)
    status, _, body = client.post(dict(jsonrpc="2.0", id=2, method="tools/list", params={}))
    tools = json.loads(body)["result"]["tools"] if status == 200 else []
    return "t3_workflow" in [tool["name"] for tool in tools]


def build_history(command, tmp):
    """The reference alone, with fixture.ts: the runs every server below starts from."""
    home = tmp / "history"
    definitions = tmp / "definitions"
    definitions.mkdir()
    (definitions / "fixture.ts").write_text(FIXTURE)
    with ExitStack() as stack:
        server = start_server(stack, command, home, workflows=definitions)
        agent = McpClient(server.url, mcp_token(server, "full-access"))
        call = lambda **arguments: agent.call("t3_workflow", arguments)
        by_title = {}
        deadline = time.time() + 60
        # The folder loads in the background: a start before it does is not registered yet.
        while call(action="start", workflow="fixture", idempotencyKey="done", input={"mode": "done"}).get("isError") is not False:
            if time.time() > deadline:
                raise SystemExit("fixture.ts did not load")
            time.sleep(0.5)
        for mode in MODES:
            call(action="start", workflow="fixture", idempotencyKey=mode, input={"mode": mode})
        call(action="signal", workflow="fixture", channel="later", value="kept for later", signalId="kept-1")
        call(action="store_set", key="fixture/a", value=1)
        call(action="store_set", key="fixture/b", value={"x": [1, 2], "y": None})
        call(action="store_set", key="other", value="z")
        while time.time() < deadline:
            runs = call(action="list").get("text", {}).get("runs", [])
            by_title = {run["title"]: run["status"] for run in runs}
            if all(by_title.get(f"Fixture {mode}") == status for mode, status in MODES.items()):
                break
            time.sleep(0.5)
        else:
            raise SystemExit(f"history did not settle: {by_title}")
    copy = tmp / "history.sqlite"
    with sqlite3.connect(home / "userdata" / "statev2.sqlite") as source, sqlite3.connect(copy) as target:
        source.backup(target)
    return copy


def ids(agent):
    runs = agent.call("t3_workflow", {"action": "list"})["text"]["runs"]
    return {run["title"].removeprefix("Fixture "): run["runId"] for run in runs}


def no_engine(agent):
    """Every action, as a service with no engine answers it."""
    call = lambda **arguments: agent.call("t3_workflow", arguments)
    return {
        "list": call(action="list"),
        "read": call(action="read", runId="run-1"),
        "read without runId": call(action="read"),
        "questions": call(action="questions"),
        "store": call(action="store", key="fixture/"),
        "hooks": call(action="hooks"),
        "start": call(action="start", workflow="fixture", idempotencyKey="k", input={"mode": "done"}),
        "start without a key": call(action="start", workflow="fixture"),
        "signal": call(action="signal", workflow="fixture", channel="go", value="hi"),
        "resume": call(action="resume", runId="run-1"),
        "cancel": call(action="cancel", runId="run-1", reason="enough"),
        "store_set": call(action="store_set", key="fixture/a", value=2),
        "store_delete": call(action="store_delete", key="fixture/a"),
    }


def history(agent, reader):
    call = lambda **arguments: agent.call("t3_workflow", arguments)
    run = ids(agent)
    record = {
        "list": call(action="list"),
        "list waiting": call(action="list", status="waiting"),
        "list skipped": call(action="list", status="skipped"),
        "list cancelled": call(action="list", status="cancelled"),
    }
    for mode in [*MODES, "child"]:
        record[f"read {mode}"] = call(action="read", runId=run[mode])
    record.update({
        "read a run that is not there": call(action="read", runId="run-missing"),
        "read, runId trimmed": call(action="read", runId=f"  {run['done']} "),
        "read, runId blank": call(action="read", runId="  "),
        "questions": call(action="questions"),
        "store": call(action="store"),
        "store under fixture/": call(action="store", key="fixture/"),
        "hooks": call(action="hooks"),
        "start": call(action="start", workflow="fixture", idempotencyKey="again", input={"mode": "done"}),
        "start without a workflow": call(action="start", idempotencyKey="k"),
        "resume the paused run": call(action="resume", runId=run["fail"]),
        "resume a run that is not there": call(action="resume", runId="run-missing"),
        "signal to both addresses": call(action="signal", runId=run["wait"], wait="go", workflow="fixture",
                                         channel="go", value="x"),
        "signal to no address": call(action="signal", runId=run["wait"], value="x"),
        "signal without a value": call(action="signal", workflow="fixture", channel="go"),
        "answer that does not fit": call(action="signal", runId=run["ask"], wait="approve", value={"choice": "maybe"}),
        "answer": call(action="signal", runId=run["ask"], wait="approve", value={"choice": "yes", "reason": "ok"},
                       signalId="answer-1"),
        "answer again, same id": call(action="signal", runId=run["ask"], wait="approve", value={"choice": "no"},
                                      signalId="answer-1"),
        "signal kept for later": call(action="signal", workflow="fixture", channel="later2", value=[1],
                                      signalId="kept-2"),
        "signal on a channel": call(action="signal", workflow="fixture", channel="go", value="hello",
                                    signalId="go-1"),
        "signal null": call(action="signal", workflow="fixture", channel="nobody", value=None, signalId="null-1"),
        "store_set": call(action="store_set", key="fixture/c", value=True),
        "store_delete": call(action="store_delete", key="fixture/a"),
        "store_set without a value": call(action="store_set", key="fixture/d"),
        "store_delete without a key": call(action="store_delete"),
        "store_set, blank key": call(action="store_set", key=" ", value=1),
        "read-only client changes the store": reader.call("t3_workflow", {"action": "store_set", "key": "k", "value": 1}),
        "read-only client lists": reader.call("t3_workflow", {"action": "list", "status": "paused"}),
        "cancel": call(action="cancel", runId=run["cancel-me"], reason="no longer needed"),
        "cancel again": call(action="cancel", runId=run["cancel-me"]),
        "cancel the parent": call(action="cancel", runId=run["parent"]),
        "read the child after": call(action="read", runId=run["child"]),
        "cancel a run that is not there": call(action="cancel", runId="run-missing"),
        "questions after": call(action="questions"),
        "list after": call(action="list"),
    })
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--dump", type=Path, help="write both servers' answers here as JSON")
    args = parser.parse_args()
    results = {}
    with TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        database = build_history(args.original_command, tmp)
        for side, command in (("original", args.original_command), ("rust", args.rust_command)):
            record = {}
            for mode in ("off", "disabled", "history"):
                with ExitStack() as stack:
                    home = tmp / side / mode
                    empty = home / "no-workflows"
                    empty.mkdir(parents=True)
                    server = start_server(stack, command, home, workflows=empty, enabled=mode != "off",
                                          database=None if mode == "disabled" else database)
                    token = mcp_token(server, "full-access")
                    agent = McpClient(server.url, token)
                    record[f"{mode}: listed"] = listed(server, token)
                    answers = no_engine(agent) if mode != "history" else history(
                        agent, McpClient(server.url, mcp_token(server, "read-only", name="Reader")))
                    record.update({f"{mode}: {name}": answer for name, answer in answers.items()})
                if mode == "history":
                    record["history: tables"] = tables(home)
            results[side] = mask(record)
    if args.dump:
        args.dump.write_text(json.dumps(results, indent=1, sort_keys=True))
    failures = 0
    original, rust = results["original"], results["rust"]
    for name in original:
        same = original[name] == rust.get(name)
        failures += not same
        print(f"{'PASS' if same else 'FAIL'} {name}")
        if not same:
            print(f"  original {json.dumps(original[name])[:2500]}\n  rust     {json.dumps(rust.get(name))[:2500]}")
    print(f"{len(original) - failures}/{len(original)} workflow checks matched across the reference and the Rust server")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
