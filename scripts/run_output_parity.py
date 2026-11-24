#!/usr/bin/env python3
"""An agent asks another thread for its result as data, and that thread's agent submits it with
t3_submit_output, on the reference and on the Rust server, and both must answer the same.

The asking agent (the fake Claude Code playing claude_output.json):
- calls t3_submit_output in a turn that was not asked for a result (refused);
- sends with an outputSchema the reference cannot use, and asks a command for a result (both
  refused), and launches a thread with an outputSchema but no message (refused);
- sends a message with an outputSchema to a second thread, whose fake Codex (codex_output.json)
  submits text that is not JSON, then a value that does not fit, then one that does;
- waits on that thread with untilOutput, which answers with the output.

Compared: every MCP answer, the target's runs (their outputSchema and output, times masked), and
the prompt its Codex was given (the output instructions after the message). All data is fake:
temporary homes, loopback addresses, generated keys and fake CLIs.
"""
import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import secrets
from tempfile import TemporaryDirectory
import time

from client_thread_parity import (FIXTURES, GIT_ISOLATION, agent_mcp, comparable_mcp, rpc, seed_turn,
                                  turn_inputs, write_fake_clis)
from link_request_parity import TIME, UUID, Server, wait_runs
from parity_harness import PROVIDER_CLIS, RELAY_TOOLS, isolated_git_env, path_without

SCENARIO = {"name": "claude run output", "instance": "claudeAgent", "model": "claude-opus-5-5"}
TARGET = "thread:target"


def mask(value, server, root):
    text = json.dumps(value, sort_keys=True)
    text = text.replace(server.url, "<origin>").replace(server.environment_id, "<environment>")
    for side in ("original", "rust"):
        text = text.replace(f"{root}/{side}", "<home>/<server>")
    return json.loads(TIME.sub("<time>", UUID.sub("<uuid>", text)))


def wait_log(log, count, seconds=90):
    path = Path(f"{log}.mcp")
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if path.exists() and sum('"answer"' in line for line in path.read_text().splitlines()) >= count:
            return
        time.sleep(0.2)


def runs(projection):
    keep = ("status", "outputSchema", "output")
    return [{k: run.get(k, "<absent>") for k in keep} for run in projection.get("runs", [])]


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
                write_fake_clis(fake_bin, {"claude", "codex"})
                log = tmp / f"{side}-provider.log"
                env = {**GIT_ISOLATION, "FAKE_PROVIDER_CLI": str(FIXTURES / "fake_cli.py"), "FAKE_PROVIDER_LOG": str(log),
                       "FAKE_PROVIDER_SCENARIO": str(FIXTURES / "claude_output.json"),
                       "PATH": os.pathsep.join([str(fake_bin), path_without(PROVIDER_CLIS | RELAY_TOOLS, tmp / f"{side}-hidden")])}
                server = Server(stack, command, root, secrets.token_urlsafe(32), secrets.token_bytes(32), env)
                seeded = seed_turn(server.url, server.cookie, tmp / f"{side}-checkouts", SCENARIO)
                rpc(server.url, server.cookie, "orchestration.dispatchCommand", {
                    "type": "thread.create", "commandId": "create-target", "threadId": TARGET,
                    "projectId": "project:github", "title": "Target",
                    "modelSelection": {"instanceId": "codex", "model": "fixture"},
                    "runtimeMode": "full-access", "interactionMode": "default", "branch": None,
                    "worktreePath": None, "createdBy": "user", "creationSource": "web"})
                dispatched = server.rpc("orchestration.dispatchCommand", seeded["dispatch"])
                assert dispatched["_tag"] == "Success", dispatched
                wait_log(log, 9)
                wait_runs(server, 1)
                target = server.rpc("orchestration.getThreadProjection", {"threadId": TARGET})["value"]
                record = {
                    "mcp answers": sorted(json.dumps(line, sort_keys=True) for line in agent_mcp(log, tmp)),
                    "target runs": runs(target),
                    "target prompt": turn_inputs(log, tmp),
                }
                results[side] = mask(record, server, tmp)
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
    print(f"{len(original) - failures}/{len(original)} run output checks matched across the reference and the Rust server")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
