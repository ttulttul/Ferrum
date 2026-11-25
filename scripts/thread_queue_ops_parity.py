#!/usr/bin/env python3
"""Messages queued behind a long run, then reordered and cancelled, on the reference and on the
Rust server: how each run is numbered and timed, and where its items sit, must read the same.

The asking agent (the fake Claude Code playing claude_thread_queue_ops.json's driver), on a
second Claude thread:
- starts a long turn there, and queues QUEUE-A, QUEUE-B and QUEUE-C behind it;
- moves QUEUE-C before QUEUE-A and cancels QUEUE-B (t3_queue reorder, cancel), lists the queue
  and reads the thread while the long turn still runs;
- waits for QUEUE-A, which runs after QUEUE-C.

Compared: every MCP answer (the queue list and the mid-run thread read among them), the target's
runs as its projection lists them (the run id's ordinal, the run's ordinal, status and message,
and whether it started: no startedAt, startedAt at its request, or later), and the order of its
user and assistant items. All data is fake: temporary homes, loopback addresses, generated keys
and fake CLIs.
"""
import argparse
from contextlib import ExitStack
import datetime
import json
import re
import os
from pathlib import Path
import secrets
from tempfile import TemporaryDirectory

from client_thread_parity import (FIXTURES, GIT_ISOLATION, THREAD, agent_mcp, comparable_mcp, rpc, seed_turn,
                                  write_fake_clis)
from link_request_parity import Server
from parity_harness import PROVIDER_CLIS, RELAY_TOOLS, isolated_git_env, path_without
from thread_queue_parity import TARGET, mask, wait_settled

SCENARIO = {"name": "claude thread queue ops", "instance": "claudeAgent", "model": "claude-opus-5-5"}
FIXTURE = "claude_thread_queue_ops.json"


def when(run):
    """Whether the run started: never, at its request (a queued run that ran later says so
    wrongly), or later."""
    if run.get("startedAt") is None:
        return "not started"
    started = datetime.datetime.fromisoformat(run["startedAt"].replace("Z", "+00:00"))
    requested = datetime.datetime.fromisoformat(run["requestedAt"].replace("Z", "+00:00"))
    return "later" if (started - requested).total_seconds() >= 1 else "at its request"


def record(view):
    texts = {message.get("id"): message.get("text") for message in view.get("messages", [])}
    by_run = {run["id"]: texts.get(run.get("userMessageId")) for run in view.get("runs", [])}
    runs = [{"id ordinal": run["id"].rsplit(":", 1)[-1], "ordinal": run.get("ordinal"),
             "status": run.get("status"), "message": texts.get(run.get("userMessageId")),
             "started": when(run)} for run in view.get("runs", [])]
    items = sorted(view.get("turnItems", []), key=lambda item: (item.get("ordinal", 0), item.get("id")))
    order = [f'{item.get("type")}: {by_run.get(item.get("runId"), "?")}' for item in items
             if item.get("type") in ("user_message", "assistant_message")]
    return {"runs": runs, "items": order}


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
            with ExitStack() as stack:
                root = tmp / side
                fake_bin = tmp / f"{side}-bin"
                write_fake_clis(fake_bin, {"claude"})
                log = tmp / f"{side}-provider.log"
                env = {**GIT_ISOLATION, "FAKE_PROVIDER_CLI": str(FIXTURES / "fake_cli.py"), "FAKE_PROVIDER_LOG": str(log),
                       "FAKE_PROVIDER_SCENARIO": str(FIXTURES / FIXTURE),
                       "PATH": os.pathsep.join([str(fake_bin), path_without(PROVIDER_CLIS | RELAY_TOOLS, tmp / f"{side}-hidden")])}
                server = Server(stack, command, root, secrets.token_urlsafe(32), secrets.token_bytes(32), env)
                seeded = seed_turn(server.url, server.cookie, tmp / f"{side}-checkouts", SCENARIO)
                rpc(server.url, server.cookie, "orchestration.dispatchCommand", {
                    "type": "thread.create", "commandId": "create-target", "threadId": TARGET,
                    "projectId": "project:github", "title": "Target",
                    "modelSelection": {"instanceId": "claudeAgent", "model": "claude-opus-5-5"},
                    "runtimeMode": "full-access", "interactionMode": "default", "branch": None,
                    "worktreePath": None, "createdBy": "user", "creationSource": "web"})
                dispatched = server.rpc("orchestration.dispatchCommand", seeded["dispatch"])
                assert dispatched["_tag"] == "Success", dispatched
                # The long run, QUEUE-C and QUEUE-A run; QUEUE-B never does.
                views = wait_settled(server, {THREAD: 1, TARGET: 3}, seconds=90)
                record_ = record(views[TARGET])
                # A queue change answers with the event sequence it was committed at, which each
                # server numbers its own way.
                record_["mcp answers"] = sorted(re.sub(r'(\\"sequence\\": )\d+', r'\1<sequence>', json.dumps(line, sort_keys=True))
                                                for line in agent_mcp(log, tmp))
                results[side] = mask(record_, server, tmp)
    if args.dump:
        args.dump.write_text(json.dumps(results, indent=1, sort_keys=True))
    failures = 0
    original, rust = results["original"], results["rust"]
    original["mcp answers"], rust["mcp answers"] = (sorted(side) for side in comparable_mcp(
        original["mcp answers"], rust["mcp answers"]))
    for name in original:
        same = original[name] == rust[name]
        failures += not same
        print(f"{'PASS' if same else 'FAIL'} {name}")
        if not same:
            for left, right in zip(original[name], rust[name]):
                if left != right:
                    print(f"  original {json.dumps(left)[:1500]}\n  rust     {json.dumps(right)[:1500]}")
            if len(original[name]) != len(rust[name]):
                print(f"  lengths {len(original[name])} != {len(rust[name])}")
    print(f"{len(original) - failures}/{len(original)} queue operation checks matched across the reference and the Rust server")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
