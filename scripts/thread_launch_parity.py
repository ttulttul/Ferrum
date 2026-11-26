#!/usr/bin/env python3
"""A thread launched with t3_thread_launch runs its first turn as any other run does, on the
reference and on the Rust server, and both must answer the same.

The launching agent (the fake Claude Code playing claude_thread_launch_run.json's driver)
launches a Claude thread with a message. The launched thread's first turn, while it runs:
- delegates a task (delegate_task), whose child (a third Claude thread) answers after a while;
- schedules a task bound to its own thread and runs it now (run_scheduled_task_now), so the
  task's run queues behind the running turn;
- waits, and the launching agent restarts it (t3_thread_send mode "restart"), retried until the
  turn runs: the turn is interrupted and the restart runs next as a run of its own.

Then the scheduled run runs there. The task's result wakes nobody, as the turn that delegated it was
stopped. Compared: every MCP
answer (without retry counts), the launched thread's runs in order (status and message), and the
prompts each of its CLIs was given, and the launched thread's branch (a root launch that names
none records the project checkout's) and worktree path. All data is fake: temporary homes, loopback addresses,
generated keys and fake CLIs.
"""
import argparse
import contextlib
from contextlib import ExitStack
import json
import os
from pathlib import Path
import secrets
from tempfile import TemporaryDirectory
import time

from client_thread_parity import (FIXTURES, GIT_ISOLATION, THREAD, agent_mcp, comparable_mcp, provider_status,
                                  seed_turn, write_fake_clis)
from link_request_parity import Server
from parity_harness import PROVIDER_CLIS, RELAY_TOOLS, isolated_git_env, path_without
from thread_queue_parity import LIVE, mask, projection, runs
from thread_send_modes_parity import without_retries

SCENARIO = {"name": "claude thread launch run", "instance": "claudeAgent", "model": "claude-opus-5-5"}
FIXTURE = "claude_thread_launch_run.json"


def launched_id(log, seconds=60):
    """The thread the driver launched, from its t3_thread_launch answer."""
    path = Path(f"{log}.mcp")
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        for line in path.read_text().splitlines() if path.exists() else []:
            record = json.loads(line)
            if (record.get("request") or {}).get("tool") != "t3_thread_launch":
                continue
            result = (record.get("answer") or {}).get("result") or {}
            text = "".join(part.get("text", "") for part in result.get("content", []))
            with contextlib.suppress(ValueError, KeyError, TypeError):
                return json.loads(text)["threadId"]
            return None
        time.sleep(0.25)
    return None


def settled(server, thread, runs_wanted, seconds):
    deadline = time.monotonic() + seconds
    while True:
        view = projection(server, thread)
        statuses = [run.get("status") for run in view.get("runs", [])]
        if (len(statuses) >= runs_wanted and not any(s in LIVE for s in statuses)) or time.monotonic() > deadline:
            return view
        time.sleep(0.25)


def cli_prompts(log, thread):
    """Each CLI of `thread`: its prompts in order, and whether it resumed a session."""
    pids = {}
    launches = Path(str(log) + ".launch")
    for line in launches.read_text().splitlines() if launches.exists() else []:
        launch = json.loads(line)
        if launch["env"].get("T3CODE_THREAD_ID") == thread:
            pids[launch["pid"]] = {"resumes": any(a.startswith("--resume") for a in launch["argv"]), "prompts": []}
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
            text = text.rsplit("\n\n", 1)[-1]
            pids[record["pid"]]["prompts"].append(f"steer: {text}" if "priority" in message else text)
    return list(pids.values())


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
                # A delegated task's target must be a checked, installed provider.
                provider_status(server.url, server.cookie, "claudeAgent", tmp)
                seeded = seed_turn(server.url, server.cookie, tmp / f"{side}-checkouts", SCENARIO)
                dispatched = server.rpc("orchestration.dispatchCommand", seeded["dispatch"])
                assert dispatched["_tag"] == "Success", dispatched
                settled(server, THREAD, 1, 90)
                launched = launched_id(log)
                # The interrupted turn, the restart and the scheduled run. The task's result wakes
                # nobody: the restart stopped the turn that delegated it.
                view = settled(server, launched, 3, 60) if launched else {}
                record = {
                    "mcp answers": sorted(without_retries(agent_mcp(log, tmp))),
                    # In order; their ordinals themselves are compared elsewhere.
                    "launched runs": [{k: v for k, v in run.items() if k != "ordinal"} for run in runs(view)],
                    "launched cli": cli_prompts(log, launched),
                    # A root launch naming no branch records the project checkout's.
                    "launched thread": {key: (view.get("thread") or {}).get(key) for key in ("branch", "worktreePath")},
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
    print(f"{len(original) - failures}/{len(original)} thread launch checks matched across the reference and the Rust server")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
