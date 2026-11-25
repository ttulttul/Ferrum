#!/usr/bin/env python3
"""An agent steers, restarts and clears another thread's conversation with t3_thread_send, on the
reference and on the Rust server, and both must answer the same.

The asking agent (the fake Claude Code playing claude_thread_send_modes.json's driver), on a
second Claude thread:
- starts a turn, then steers it (mode "steer"): the steer reaches the running turn, whose CLI
  answers it in the same run;
- starts another turn, then restarts it (mode "restart"): the running turn is interrupted, and
  the restart runs as a new run;
- sends with freshContext: the message starts a new provider session, without the earlier
  conversation;
- steers and restarts the idle thread: refused, as nothing runs there.

Steers and restarts are retried while the target's turn has not started yet. Compared: every MCP
answer, the target's runs in order (status and message), the prompts its CLI was given (a steer
marked as one), and whether each of its CLIs resumed a session. All data is fake: temporary
homes, loopback addresses, generated keys and fake CLIs.
"""
import argparse
import contextlib
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
from thread_queue_parity import TARGET, mask, runs, wait_settled

SCENARIO = {"name": "claude thread send modes", "instance": "claudeAgent", "model": "claude-opus-5-5"}
FIXTURE = "claude_thread_send_modes.json"


def target_cli(log):
    """The target thread's CLIs: each one's prompts in order (a steer marked), and whether it
    was started on an earlier session."""
    pids = {}
    launches = Path(str(log) + ".launch")
    for line in launches.read_text().splitlines() if launches.exists() else []:
        launch = json.loads(line)
        if launch["env"].get("T3CODE_THREAD_ID") == TARGET:
            pids[launch["pid"]] = {"resumes": "--resume" in launch["argv"], "prompts": []}
    messages = Path(str(log) + ".messages")
    for line in messages.read_text().splitlines() if messages.exists() else []:
        with contextlib.suppress(ValueError):
            record = json.loads(line)
            message = json.loads(record["line"])
            if record["pid"] not in pids or message.get("type") != "user":
                continue
            content = (message.get("message") or {}).get("content")
            text = content if isinstance(content, str) else "".join(
                part.get("text", "") for part in content or [] if isinstance(part, dict))
            # The prompt's own words, without the note on where an agent's message came from.
            text = text.rsplit("\n\n", 1)[-1]
            pids[record["pid"]]["prompts"].append(f"steer: {text}" if "priority" in message else text)
    return list(pids.values())


def without_retries(lines):
    """MCP records without their retry counts: how long a turn takes to start differs by server."""
    out = []
    for line in lines:
        record = json.loads(line) if line.startswith("{") else None
        if isinstance(record, dict):
            record.pop("retried", None)
            line = json.dumps(record, sort_keys=True)
        out.append(line)
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
                views = wait_settled(server, {THREAD: 1, TARGET: 4}, seconds=90)
                record = {
                    "mcp answers": sorted(without_retries(agent_mcp(log, tmp))),
                    "target runs": runs(views[TARGET]),
                    "target cli": target_cli(log),
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
    print(f"{len(original) - failures}/{len(original)} thread send mode checks matched across the reference and the Rust server")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
