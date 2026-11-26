#!/usr/bin/env python3
"""A thread's activity time and the thread list's order, on the reference and on the Rust
server, which both must answer the same.

The fake Claude Code agent (claude_thread_activity.json's driver):
- reads a second thread, runs a turn there and reads it again: a thread's updatedAt moves on
  with every event on it (its runs among them), not only with metadata changes, and a read
  without afterPosition gets the newest items;
- lists the threads, with and without subagents: newest activity first (updatedAt).

Compared: every MCP answer (times masked), and from the raw answers, for each read whether
updatedAt has moved past createdAt, and the order of each list. All data is fake: temporary
homes, loopback addresses, generated keys and fake CLIs.
"""
import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import secrets
from tempfile import TemporaryDirectory

from client_thread_parity import (FIXTURES, GIT_ISOLATION, THREAD, agent_mcp, comparable_mcp, rpc, seed_turn,
                                  write_fake_clis)
from link_request_parity import Server
from parity_harness import PROVIDER_CLIS, RELAY_TOOLS, isolated_git_env, path_without
from thread_queue_parity import TARGET, mask, wait_settled

SCENARIO = {"name": "claude thread activity", "instance": "claudeAgent", "model": "claude-opus-5-5"}
FIXTURE = "claude_thread_activity.json"


def activity(log):
    """From the raw answers: each read's updatedAt against createdAt, and each list's order."""
    out = []
    for line in Path(f"{log}.mcp").read_text().splitlines():
        record = json.loads(line)
        tool = record["request"].get("tool")
        result = (record.get("answer") or {}).get("result") or {}
        text = "".join(part.get("text", "") for part in result.get("content", []))
        try:
            content = json.loads(text)
        except ValueError:
            continue
        if tool == "t3_thread_read":
            thread = content.get("thread", {})
            out.append(f"read {thread.get('threadId')}: updatedAt "
                       + ("later than createdAt" if thread.get("updatedAt", "") > thread.get("createdAt", "")
                          else "at createdAt"))
        elif tool == "t3_thread_list":
            ids = [t["threadId"] for t in content.get("threads", [])]
            out.append(f"list {json.dumps(record['request']['arguments'], sort_keys=True)}: {ids}")
    return out


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
                # Run completion now precedes checkpoint capture. Observe an idle target
                # at the same boundary on both servers before reading it or starting a new turn.
                markers = tmp / f"{side}-checkpoints"
                markers.mkdir()
                env = {**GIT_ISOLATION, "FAKE_CHECKPOINT_MARKERS": str(markers), "FAKE_PROVIDER_CLI": str(FIXTURES / "fake_cli.py"), "FAKE_PROVIDER_LOG": str(log),
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
                wait_settled(server, {THREAD: 1, TARGET: 1}, seconds=90, checkpoint_markers=markers)
                results[side] = mask({"activity": activity(log),
                                      "mcp answers": sorted(line for line in agent_mcp(log, tmp) if line.startswith("{"))},
                                     server, tmp)
    if args.dump:
        args.dump.write_text(json.dumps(results, indent=1, sort_keys=True))
    original, rust = results["original"], results["rust"]
    original["mcp answers"], rust["mcp answers"] = (sorted(side) for side in comparable_mcp(
        original["mcp answers"], rust["mcp answers"]))
    failures = 0
    for name in original:
        same = original[name] == rust[name]
        failures += not same
        print(f"{'PASS' if same else 'FAIL'} {name}")
        if not same:
            for left, right in zip(original[name], rust[name]):
                if left != right:
                    print(f"  original {left[:1500]}\n  rust     {right[:1500]}")
            if len(original[name]) != len(rust[name]):
                print(f"  lengths {len(original[name])} != {len(rust[name])}")
    print(f"{len(original) - failures}/{len(original)} thread activity checks matched across the reference and the Rust server")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
