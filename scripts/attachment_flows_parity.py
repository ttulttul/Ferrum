#!/usr/bin/env python3
"""Attachments on a queued message's edit and on a launched thread's first message, on the
reference and on the Rust server, and both must answer the same.

Each side runs a fake Codex (tests/fixtures/providers):
- codex_queued_edit.json: while the first turn runs, a second message queues behind it, and its
  queued run is edited: to uploaded files (claimed for the thread), to a file the thread does not
  have (refused), to new text alone (the files stay), and the files edit again (a retry). The
  queued run then starts with them (after `queue.resume`, which Rust's held queue needs).
- codex_turn.json: orchestration.launchThread with uploaded files in its first message, and again
  with no thread id (refused: an upload needs the thread it goes to).

Compared: every reply, the messages and their turn items, what the provider CLI was asked
(images by hash), and the files each thread keeps. Homes, ids and times are masked. All data is
fake: temporary homes, fixture files and a fake CLI.
"""
import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import re
import secrets
from tempfile import TemporaryDirectory
import time

from client_thread_parity import (FIXTURES, GIT_ISOLATION, THREAD, _UPLOADS, seed_turn, turn_inputs,
                                  upload_attachments, write_fake_clis)
from link_request_parity import TIME, UUID, Server
from parity_harness import PROVIDER_CLIS, RELAY_TOOLS, isolated_git_env, path_without

LAUNCHED = "thread:launched"


def projection(server, thread):
    exit = server.rpc("orchestration.getThreadProjection", {"threadId": thread})
    return exit.get("value", {})


def wait_for(server, thread, done, seconds=60):
    deadline = time.monotonic() + seconds
    while True:
        current = projection(server, thread)
        if done(current) or time.monotonic() > deadline:
            return current
        time.sleep(0.25)


def runs_settled(count):
    def done(current):
        statuses = [run.get("status") for run in current.get("runs", [])]
        return len(statuses) >= count and all(
            s not in ("preparing", "starting", "running", "waiting", "queued") for s in statuses)
    return done


def messages(current):
    keep = ("role", "text", "attachments")
    return [{k: m.get(k) for k in keep} for m in current.get("messages", []) if m.get("role") == "user"]


def user_items(current):
    return [{k: i.get(k) for k in ("type", "text", "attachments")}
            for i in sorted(current.get("turnItems", []), key=lambda i: i.get("ordinal", 0))
            if i.get("type") == "user_message"]


def kept_files(root, thread):
    folder = root / "userdata" / "attachments"
    names = sorted(p.name for p in folder.rglob("*") if p.is_file()) if folder.exists() else []
    return sorted(UUID.sub("<uuid>", name) for name in names if "pending" not in name)


def mask(value, root):
    text = re.sub(r'"sequence": \d+', '"sequence": "<n>"', json.dumps(value, sort_keys=True))
    for side in ("original", "rust"):
        text = text.replace(f"{root}/{side}", "<home>/<server>")
    return json.loads(TIME.sub("<time>", UUID.sub("<uuid>", text)))


def start(stack, side, flow, command, tmp, scenario):
    fake_bin = tmp / f"{side}-{flow}-bin"
    write_fake_clis(fake_bin, {"codex"})
    log = tmp / f"{side}-{flow}-provider.log"
    env = {**GIT_ISOLATION, "FAKE_PROVIDER_CLI": str(FIXTURES / "fake_cli.py"), "FAKE_PROVIDER_LOG": str(log),
           "FAKE_PROVIDER_SCENARIO": str(FIXTURES / scenario),
           "PATH": os.pathsep.join([str(fake_bin), path_without(PROVIDER_CLIS | RELAY_TOOLS, tmp / f"{side}-{flow}-hidden")])}
    root = tmp / side / flow
    return Server(stack, command, root, secrets.token_urlsafe(32), secrets.token_bytes(32), env), root, log


def queued_edit(server, root, log, tmp):
    record = {}
    scenario = {"name": "queued edit", "instance": "codex", "model": "fixture"}
    seeded = seed_turn(server.url, server.cookie, root / "checkouts", scenario)
    record["first message"] = server.rpc("orchestration.dispatchCommand", seeded["dispatch"])
    wait_for(server, THREAD, lambda p: any(r.get("status") == "running" for r in p.get("runs", [])))
    record["queued message"] = server.rpc("orchestration.dispatchCommand", {
        "type": "message.dispatch", "commandId": "queued-message", "threadId": THREAD,
        "messageId": "message:queued", "text": "Second message", "attachments": [],
        "createdBy": "user", "creationSource": "web", "dispatchMode": {"type": "queue_after_active"}})
    queued = wait_for(server, THREAD, lambda p: any(r.get("status") == "queued" for r in p.get("runs", [])))
    run_id = next(r["id"] for r in queued.get("runs", []) if r.get("status") == "queued")
    edit = lambda command_id, **fields: server.rpc("orchestration.dispatchCommand", {
        "type": "queued-run.edit", "commandId": command_id, "threadId": THREAD, "runId": run_id, **fields})
    uploads = upload_attachments(server.url, server.cookie, _UPLOADS)
    record["edit to uploaded files"] = edit("edit-files", text="Second message, with files", attachments=uploads)
    claimed = next(m for m in projection(server, THREAD)["messages"] if m["id"] == "message:queued")["attachments"]
    record["queued message after the edit"] = messages({"messages": [
        m for m in projection(server, THREAD)["messages"] if m["id"] == "message:queued"]})
    missing = [{**claimed[0], "id": claimed[0]["id"][:-4] + "beef"}] if claimed else []
    record["edit to a file the thread does not have"] = edit(
        "edit-missing", text="Second message, missing file", attachments=missing)
    record["edit to text alone"] = edit("edit-text", text="Second message, edited again")
    record["retry of the files edit"] = edit("edit-files", text="Second message, with files", attachments=uploads)
    # Rust holds a queued run until a client starts it (docs/agent-tools.md, 4b); the reference
    # starts it once the first run ends. Resuming the queue then starts it on both.
    wait_for(server, THREAD, lambda p: p.get("runs", [{}])[0].get("status") == "completed")
    server.rpc("orchestration.dispatchCommand", {"type": "queue.resume", "commandId": "resume-queue", "threadId": THREAD})
    final = wait_for(server, THREAD, runs_settled(2))
    record["runs"] = [r.get("status") for r in final.get("runs", [])]
    record["messages"] = messages(final)
    record["user turn items"] = user_items(final)
    record["provider was asked"] = turn_inputs(log, tmp)
    record["files kept"] = kept_files(root, THREAD)
    return record


def launch(server, root, log, tmp):
    record = {}
    scenario = {"name": "launch", "instance": "codex", "model": "fixture"}
    seed_turn(server.url, server.cookie, root / "checkouts", scenario)
    payload = {"commandId": "launch", "projectId": "project:github", "title": "Launched",
               "modelSelection": {"instanceId": "codex", "model": "fixture"}, "runtimeMode": "full-access",
               "interactionMode": "default", "workspaceStrategy": {"type": "root"}}
    launched = server.rpc("orchestration.launchThread", {
        **payload, "threadId": LAUNCHED,
        "initialMessage": {"text": "Look at these", "attachments": upload_attachments(server.url, server.cookie, _UPLOADS)}})
    record["launch"] = {"_tag": launched.get("_tag"), "threadId": launched.get("value", {}).get("threadId"),
                        "messages": messages(launched.get("value", {}).get("projection", {})),
                        "error": launched.get("cause") if launched.get("_tag") != "Success" else None}
    record["launch without a thread id"] = server.rpc("orchestration.launchThread", {
        **payload, "commandId": "launch-no-thread",
        "initialMessage": {"text": "No thread", "attachments": upload_attachments(server.url, server.cookie, _UPLOADS[:1])}})
    final = wait_for(server, LAUNCHED, runs_settled(1))
    record["messages"] = messages(final)
    record["user turn items"] = user_items(final)
    record["provider was asked"] = turn_inputs(log, tmp)
    record["files kept"] = kept_files(root, LAUNCHED)
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
        GIT_ISOLATION.update(isolated_git_env(tmp / "git"))
        for side, command in (("original", args.original_command), ("rust", args.rust_command)):
            record = {}
            for flow, scenario, run in (("queued edit", "codex_queued_edit.json", queued_edit),
                                        ("launch", "codex_turn.json", launch)):
                with ExitStack() as stack:
                    server, root, log = start(stack, side, flow.replace(" ", "-"), command, tmp, scenario)
                    record.update({f"{flow}: {name}": value for name, value in run(server, root, log, tmp).items()})
            results[side] = mask(record, tmp)
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
    print(f"{len(original) - failures}/{len(original)} attachment flow checks matched across the reference and the Rust server")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
