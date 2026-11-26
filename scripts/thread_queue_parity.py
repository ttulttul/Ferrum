#!/usr/bin/env python3
"""Messages an agent queues on another thread start in order once the run before them ends, on the
reference and on the Rust server, and both must answer the same.

The asking agent (the fake Claude Code playing claude_thread_queue.json's driver):
- starts a turn on a second Claude thread with t3_thread_send, then queues QUEUE-A and QUEUE-B
  behind it (mode "queue") while it runs;
- waits for that first run to end (t3_thread_wait with its runId), then queues QUEUE-C;
- schedules a task bound to its own thread and runs it now (run_scheduled_task_now) while its
  own turn still runs, so the task's run queues behind that turn.

Each queued run must start once the run before it ends, in the order it was queued: the second
thread's CLI is asked the long job, QUEUE-A, QUEUE-B and QUEUE-C, in that order, and the driver's
CLI then answers the scheduled task's prompt. Compared: every MCP answer, both threads' runs in
order (status and message), and the prompts each CLI was given. All data is fake: temporary homes,
loopback addresses, generated keys and fake CLIs.
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

from client_thread_parity import (FIXTURES, GIT_ISOLATION, THREAD, agent_mcp, comparable_mcp, rpc, seed_turn,
                                  write_fake_clis)
from link_request_parity import TIME, UUID, Server
from parity_harness import PROVIDER_CLIS, RELAY_TOOLS, isolated_git_env, path_without

SCENARIO = {"name": "claude thread queue", "instance": "claudeAgent", "model": "claude-opus-5-5"}
TARGET = "thread:target"
FIXTURE = "claude_thread_queue.json"
LIVE = ("preparing", "starting", "running", "waiting", "queued")


def mask(value, server, root):
    text = json.dumps(value, sort_keys=True)
    text = text.replace(server.url, "<origin>").replace(server.environment_id, "<environment>")
    for side in ("original", "rust"):
        text = text.replace(f"{root}/{side}", "<home>/<server>")
    return json.loads(TIME.sub("<time>", UUID.sub("<uuid>", text)))


def projection(server, thread):
    exit = server.rpc("orchestration.getThreadProjection", {"threadId": thread})
    assert exit["_tag"] == "Success", exit
    return exit["value"]


def wait_settled(server, counts, seconds=60, checkpoint_markers=None):
    """Until each thread has its count of runs and none is live (or the time is up)."""
    deadline = time.monotonic() + seconds
    while True:
        views = {thread: projection(server, thread) for thread in counts}
        if checkpoint_markers is not None:
            for run in views[TARGET].get("runs", []):
                if run.get("checkpointId") is not None:
                    (checkpoint_markers / str(run["ordinal"])).touch()
        done = all(len(views[thread].get("runs", [])) >= count
                   and all(run.get("status") not in LIVE for run in views[thread].get("runs", []))
                   for thread, count in counts.items())
        if done or time.monotonic() > deadline:
            return views
        time.sleep(0.25)


def runs(view):
    """Each run in order, with the message it carried."""
    texts = {message.get("id"): message.get("text") for message in view.get("messages", [])}
    ordered = sorted(view.get("runs", []), key=lambda run: run.get("ordinal", 0))
    return [{"ordinal": run.get("ordinal"), "status": run.get("status"),
             "message": texts.get(run.get("userMessageId"), "<absent>")} for run in ordered]


def prompts(log):
    """Each Claude CLI's user messages in the order it was given them, by the thread it runs."""
    threads = {}
    launches = Path(str(log) + ".launch")
    for line in launches.read_text().splitlines() if launches.exists() else []:
        launch = json.loads(line)
        threads[launch["pid"]] = launch["env"].get("T3CODE_THREAD_ID", "<none>")
    out = {}
    messages = Path(str(log) + ".messages")
    for line in messages.read_text().splitlines() if messages.exists() else []:
        with contextlib.suppress(ValueError):
            record = json.loads(line)
            message = json.loads(record["line"])
            if message.get("type") != "user" or "priority" in message:
                continue
            content = (message.get("message") or {}).get("content")
            text = content if isinstance(content, str) else "".join(
                part.get("text", "") for part in content or [] if isinstance(part, dict))
            # The prompt's own words, without the note on where an agent's message came from.
            out.setdefault(threads.get(record["pid"], "<unknown>"), []).append(text.rsplit("\n\n", 1)[-1])
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
                views = wait_settled(server, {THREAD: 2, TARGET: 4})
                record = {
                    "mcp answers": sorted(json.dumps(line, sort_keys=True) for line in agent_mcp(log, tmp)),
                    "driver runs": runs(views[THREAD]),
                    "target runs": runs(views[TARGET]),
                    "prompts": prompts(log),
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
            left_side, right_side = original[name], rust[name]
            if isinstance(left_side, dict):
                left_side, right_side = sorted(left_side.items()), sorted(right_side.items())
            for left, right in zip(left_side, right_side):
                if left != right:
                    print(f"  original {json.dumps(left)[:1500]}\n  rust     {json.dumps(right)[:1500]}")
            if len(left_side) != len(right_side):
                print(f"  lengths {len(left_side)} != {len(right_side)}")
    print(f"{len(original) - failures}/{len(original)} thread queue checks matched across the reference and the Rust server")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
