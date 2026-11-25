#!/usr/bin/env python3
"""A thread an agent started reads that agent's thread while the agent waits on it, on the
reference and on the Rust server, and both must answer the same.

The driver (the fake Claude Code playing claude_thread_blocked_wait.json's driver) starts a turn
on a second Claude thread with t3_thread_send, then waits on that run with t3_thread_wait (its
CLI reports the call as a running mcp__t3-code__t3_thread_wait tool, as Claude Code does). The
second thread's agent reads the driver's thread with t3_thread_read: the reference shows it
blockedOnThreadWait while the wait runs, so an agent can tell its creator is waiting on it before
it sends into that thread or waits on it in turn (each would then wait on the other). The read is
retried, briefly, until the wait shows.

Compared: every MCP answer, without retry counts. All data is fake: temporary homes, loopback
addresses, generated keys and fake CLIs.
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
from thread_send_modes_parity import without_retries

SCENARIO = {"name": "claude thread blocked wait", "instance": "claudeAgent", "model": "claude-opus-5-5"}
FIXTURE = "claude_thread_blocked_wait.json"


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
                wait_settled(server, {THREAD: 1, TARGET: 1}, seconds=90)
                results[side] = mask({"mcp answers": sorted(without_retries(agent_mcp(log, tmp)))}, server, tmp)
    if args.dump:
        args.dump.write_text(json.dumps(results, indent=1, sort_keys=True))
    original, rust = results["original"], results["rust"]
    original["mcp answers"], rust["mcp answers"] = (sorted(side) for side in comparable_mcp(
        original["mcp answers"], rust["mcp answers"]))
    same = original["mcp answers"] == rust["mcp answers"]
    print(f"{'PASS' if same else 'FAIL'} mcp answers")
    if not same:
        for left, right in zip(original["mcp answers"], rust["mcp answers"]):
            if left != right:
                print(f"  original {left[:1500]}\n  rust     {right[:1500]}")
    print(f"{int(same)}/1 blocked-wait checks matched across the reference and the Rust server")
    return int(not same)


if __name__ == "__main__":
    raise SystemExit(main())
