#!/usr/bin/env python3
"""An agent sends feedback to the T3 team with t3_feedback, on the reference and on the Rust
server, and both must answer the same.

With agent feedback on (`auto`), a thread's agent (the fake Claude Code playing
claude_feedback.json) lists its tools, then sends:
- unprompted reports whose text holds a secret, a file path and the thread's id (scrubbed),
  naming a tool, a prefixed tool, a `t3` command and `general`, until the turn's limit of three;
- one naming no T3 tool (refused);
- two reports the user asked for, which show cards to approve instead.

The user then answers the cards through agentFeedback.answerRequest: one sent with the
conversation, one declined, one answered twice, one that does not exist, and one on a thread that
does not exist. With feedback off, the tool is not listed.

Compared: every MCP answer (ids and times masked), whether tools/list lists t3_feedback, the
cards in the thread, every answer, and the rows the shared database keeps. Telemetry is off in
every parity server, so no report leaves the machine on either side. All data is fake.
"""
import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import secrets
import sqlite3
from tempfile import TemporaryDirectory

from client_thread_parity import FAKE_CLAUDE, FIXTURES, GIT_ISOLATION, THREAD, agent_mcp, comparable_mcp, rpc, seed_turn
from link_request_parity import TIME, UUID, Server, wait_runs
from parity_harness import PROVIDER_CLIS, RELAY_TOOLS, isolated_git_env, path_without

SCENARIO = {"name": "claude feedback", "instance": "claudeAgent", "model": "claude-opus-5-5"}


def mask(value, server, root):
    text = json.dumps(value, sort_keys=True)
    text = text.replace(server.environment_id, "<environment>")
    for side in ("original", "rust"):
        text = text.replace(f"{root}/{side}", "<home>/<server>")
    return json.loads(TIME.sub("<time>", UUID.sub("<uuid>", text)))


def cards(projection):
    keep = ("type", "status", "title", "feedbackStatus", "preview", "conversationTrace", "completedAt",
            "providerThreadId", "nativeItemRef", "parentItemId")
    items = [i for i in projection.get("turnItems", []) if i.get("type") == "feedback_request"]
    return [{k: item.get(k, "<absent>") for k in keep} for item in sorted(items, key=lambda i: i.get("ordinal", 0))]


def rows(root):
    path = root / "userdata" / "statev2.sqlite"
    with sqlite3.connect(path) as db:
        db.row_factory = sqlite3.Row
        found = db.execute("SELECT * FROM agent_feedback ORDER BY created_at, subject").fetchall()
    return [{k: (json.loads(row[k]) if k == "trace" and row[k] else row[k]) for k in row.keys()
             if k not in ("id", "created_at", "run_id")} | {"hasRun": row["run_id"] is not None} for row in found]


def listed(log):
    for line in Path(f"{log}.mcp").read_text().splitlines():
        record = json.loads(line)
        if (record.get("request") or {}).get("method") == "tools/list":
            tools = record["answer"].get("result", {}).get("tools", [])
            return "t3_feedback" in [tool["name"] for tool in tools]
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--dump", type=Path, help="write both servers' answers here as JSON")
    args = parser.parse_args()
    results = {}
    with TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        GIT_ISOLATION.update(isolated_git_env(tmp / "git"))
        for side, command in (("original", args.original_command), ("rust", args.rust_command)):
            for mode in ("off", "auto"):
                with ExitStack() as stack:
                    root = tmp / side / mode
                    fake_bin = tmp / f"{side}-{mode}-bin"
                    fake_bin.mkdir(parents=True)
                    (fake_bin / "claude").write_text(FAKE_CLAUDE)
                    (fake_bin / "claude").chmod(0o755)
                    log = tmp / f"{side}-{mode}-provider.log"
                    env = {**GIT_ISOLATION, "FAKE_PROVIDER_CLI": str(FIXTURES / "fake_cli.py"), "FAKE_PROVIDER_LOG": str(log),
                           "FAKE_PROVIDER_SCENARIO": str(FIXTURES / "claude_feedback.json"),
                           "PATH": os.pathsep.join([str(fake_bin), path_without(PROVIDER_CLIS | RELAY_TOOLS, tmp / f"{side}-{mode}-hidden")])}
                    server = Server(stack, command, root, secrets.token_urlsafe(32), secrets.token_bytes(32), env)
                    rpc(server.url, server.cookie, "server.updateSettings", {"patch": {"agentFeedback": {"mode": mode}}})
                    seeded = seed_turn(server.url, server.cookie, tmp / f"{side}-{mode}-checkouts", SCENARIO)
                    dispatched = server.rpc("orchestration.dispatchCommand", seeded["dispatch"])
                    assert dispatched["_tag"] == "Success", dispatched
                    projection = wait_runs(server, 1)
                    record = {f"{mode}: listed": listed(log), f"{mode}: agent mcp": agent_mcp(log, tmp)}
                    if mode == "auto":
                        record["cards"] = cards(projection)
                        ids = [i["id"] for i in projection.get("turnItems", []) if i.get("type") == "feedback_request"]
                        answer = lambda item, payload: server.rpc("agentFeedback.answerRequest", {
                            "threadId": THREAD, "turnItemId": item, **payload})
                        if len(ids) == 2:
                            record["send with the conversation"] = answer(ids[0], {"answer": "send", "includeConversation": True})
                            record["decline"] = answer(ids[1], {"answer": "decline"})
                            record["answered twice"] = answer(ids[0], {"answer": "send"})
                        record["no such card"] = answer("turn-item:feedback-request:missing", {"answer": "send"})
                        record["no such thread"] = server.rpc("agentFeedback.answerRequest", {
                            "threadId": "thread:missing", "turnItemId": "turn-item:missing", "answer": "decline"})
                        record["cards after the answers"] = cards(server.projection())
                        record["stored reports"] = rows(root)
                    results.setdefault(side, {}).update(mask(record, server, tmp))
    if args.dump:
        args.dump.write_text(json.dumps(results, indent=1, sort_keys=True))
    failures = 0
    original, rust = results["original"], results["rust"]
    for key in ("off: agent mcp", "auto: agent mcp"):
        original[key], rust[key] = comparable_mcp(original[key], rust[key])
    for name in original:
        same = original[name] == rust[name]
        failures += not same
        print(f"{'PASS' if same else 'FAIL'} {name}")
        if not same:
            print(f"  original {json.dumps(original[name])[:3000]}\n  rust     {json.dumps(rust[name])[:3000]}")
    print(f"{len(original) - failures}/{len(original)} feedback checks matched across the reference and the Rust server")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
