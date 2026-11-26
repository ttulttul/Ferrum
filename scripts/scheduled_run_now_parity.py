#!/usr/bin/env python3
"""An agent runs a scheduled task with no bound thread now, on the reference and on the Rust
server, and both must answer the same.

The fake Claude Code agent schedules an hourly task in its project without binding it to its
thread (bindToCurrentThread false), runs it now (run_scheduled_task_now), lists the project's
tasks and deletes the task. A task with no bound thread starts a thread of its own each run, and
run-now answers with that thread (threadId and its link), as the reference's runTask returns it.

Compared: every MCP answer. All data is fake: temporary homes, loopback addresses, generated keys
and fake CLIs.
"""
import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import secrets
from tempfile import TemporaryDirectory
import time

from client_thread_parity import FIXTURES, GIT_ISOLATION, agent_mcp, comparable_mcp, seed_turn, write_fake_clis
from link_request_parity import Server
from parity_harness import PROVIDER_CLIS, RELAY_TOOLS, isolated_git_env, path_without
from thread_queue_parity import mask

# The task runs on the model the agent's thread runs; one the fake CLI's version supports.
SCENARIO = {"name": "claude scheduled run now", "instance": "claudeAgent", "model": "claude-sonnet-5"}
FIXTURE = "claude_scheduled_run_now.json"
CALLS = 4


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
                dispatched = server.rpc("orchestration.dispatchCommand", seeded["dispatch"])
                assert dispatched["_tag"] == "Success", dispatched
                calls = Path(f"{log}.mcp")
                deadline = time.monotonic() + 90
                while time.monotonic() < deadline and sum(
                        '"answer"' in line for line in (calls.read_text().splitlines() if calls.exists() else [])) < CALLS:
                    time.sleep(0.25)
                # The tool calls (how each server hands Claude Code its MCP servers differs).
                results[side] = mask({"mcp answers": sorted(line for line in agent_mcp(log, tmp)
                                                            if line.startswith("{"))}, server, tmp)
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
    print(f"{int(same)}/1 scheduled run-now checks matched across the reference and the Rust server")
    return int(not same)


if __name__ == "__main__":
    raise SystemExit(main())
