#!/usr/bin/env python3
"""An agent uploads files and sends them to another thread with the attachment tools, on the
reference and on the Rust server, and both must answer the same.

A thread's provider CLI (the fake Claude Code in tests/fixtures/providers, playing
claude_attachments.json) calls T3's tools on the MCP server its server gave it:
- t3_attachment_prepare_upload for an image, a Markdown file, a file it then discards, a file it
  never uploads, and an image type uploads do not take;
- while it waits, this harness posts the image's and the Markdown file's exact bytes to their
  upload URLs, as the agent would;
- t3_attachment_discard of its upload (twice: the second is refused) and of one it never
  prepared;
- t3_thread_send_attachments of the image and the file to another thread, whose fake Codex then
  runs the message; then sends refused: an upload never posted, an attachment the target does
  not hold, an archived target, and no attachments.

Compared: every MCP answer (upload tokens, ids and times by form), the target thread's messages
and their attachments, what the target's Codex was asked (images by their hash), and what the
attachments directory keeps. All data is fake: temporary homes, loopback addresses, fixture
files, generated keys and fake CLIs.
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

from client_thread_parity import (_ATTACHMENT_PNG, FIXTURES, GIT_ISOLATION, THREAD, agent_mcp, comparable_mcp,
                                  rpc, seed_turn, turn_inputs, write_fake_clis)
from link_request_parity import TIME, UUID, Server, wait_runs
from parity_harness import PROVIDER_CLIS, RELAY_TOOLS, http, isolated_git_env, path_without

SCENARIO = {"name": "claude attachment tools", "instance": "claudeAgent", "model": "claude-opus-5-5"}
TARGET = "thread:target"
ARCHIVED = "thread:archived"
UPLOADS = {"pixel.png": _ATTACHMENT_PNG, "notes.md": b"# Notes\nFixture file.\n"}
TOKEN = re.compile(r"/api/attachments/upload/[A-Za-z0-9_\-.]+")


def mask(value, server, root):
    text = json.dumps(value, sort_keys=True)
    text = text.replace(server.url, "<origin>").replace(server.environment_id, "<environment>")
    for side in ("original", "rust"):
        text = text.replace(f"{root}/{side}", "<home>/<server>")
    text = TOKEN.sub("/api/attachments/upload/<token>", text)
    text = re.sub(r'(expiresAt\\?": )\d+', r'\g<1>0', text)
    text = TIME.sub("<time>", UUID.sub("<uuid>", text))
    return json.loads(text)


def post_uploads(server, log, seconds=60):
    """Posts each fixture file to the upload URL its prepare answer gave."""
    calls = Path(f"{log}.mcp")
    posted = set()
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline and len(posted) < len(UPLOADS):
        for line in (calls.read_text().splitlines() if calls.exists() else []):
            record = json.loads(line)
            request = record.get("request") or {}
            name = ((request.get("arguments") or {}).get("upload") or {}).get("name")
            if request.get("tool") != "t3_attachment_prepare_upload" or name not in UPLOADS or name in posted:
                continue
            content = record["answer"]["result"]["structuredContent"]
            status, _, body = http(server.url, "POST", content["relativeUrl"], UPLOADS[name],
                                   {"Content-Type": "application/octet-stream"})
            if status >= 300:
                raise RuntimeError(f"upload of {name} failed: HTTP {status} {body[:200]!r}")
            posted.add(name)
        time.sleep(0.1)
    if len(posted) < len(UPLOADS):
        raise RuntimeError(f"only {sorted(posted)} were prepared in time")


def wait_log(log, count, seconds=60):
    path = Path(f"{log}.mcp")
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if path.exists() and sum('"answer"' in line for line in path.read_text().splitlines()) >= count:
            return
        time.sleep(0.1)


def messages(projection):
    return [{k: m.get(k) for k in ("role", "text", "createdBy", "creationSource", "senderThreadId", "attachments")}
            for m in projection.get("messages", [])]


def attachment_files(root):
    return sorted(UUID.sub("<uuid>", p.name) for p in (root / "userdata/attachments").glob("*") if p.is_file())


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
                       "FAKE_PROVIDER_SCENARIO": str(FIXTURES / "claude_attachments.json"),
                       "PATH": os.pathsep.join([str(fake_bin), path_without(PROVIDER_CLIS | RELAY_TOOLS, tmp / f"{side}-hidden")])}
                server = Server(stack, command, root, secrets.token_urlsafe(32), secrets.token_bytes(32), env)
                seeded = seed_turn(server.url, server.cookie, tmp / f"{side}-checkouts", SCENARIO)
                for thread, title in ((TARGET, "Target"), (ARCHIVED, "Archived")):
                    rpc(server.url, server.cookie, "orchestration.dispatchCommand", {
                        "type": "thread.create", "commandId": f"create-{thread}", "threadId": thread,
                        "projectId": "project:github", "title": title,
                        "modelSelection": {"instanceId": "codex", "model": "fixture"},
                        "runtimeMode": "full-access", "interactionMode": "default", "branch": None,
                        "worktreePath": None, "createdBy": "user", "creationSource": "web"})
                rpc(server.url, server.cookie, "orchestration.dispatchCommand",
                    {"type": "thread.archive", "commandId": "archive-thread", "threadId": ARCHIVED})
                dispatched = server.rpc("orchestration.dispatchCommand", seeded["dispatch"])
                assert dispatched["_tag"] == "Success", dispatched
                post_uploads(server, log)
                wait_log(log, 13)
                wait_runs(server, 1)
                target = server.rpc("orchestration.getThreadProjection", {"threadId": TARGET})["value"]
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline and any(
                        r.get("status") in ("preparing", "starting", "running", "queued") for r in target.get("runs", [])):
                    time.sleep(0.25)
                    target = server.rpc("orchestration.getThreadProjection", {"threadId": TARGET})["value"]
                record = {
                    "agent mcp": agent_mcp(log, tmp),
                    "target messages": messages(target),
                    "target runs": [run.get("status") for run in target.get("runs", [])],
                    "target turn input": turn_inputs(log, tmp),
                    "attachment files": attachment_files(root),
                }
                results[side] = mask(record, server, tmp)
    if args.dump:
        args.dump.write_text(json.dumps(results, indent=1, sort_keys=True))
    failures = 0
    original, rust = results["original"], results["rust"]
    original["agent mcp"], rust["agent mcp"] = comparable_mcp(original["agent mcp"], rust["agent mcp"])
    for name in original:
        same = original[name] == rust[name]
        failures += not same
        print(f"{'PASS' if same else 'FAIL'} {name}")
        if not same:
            print(f"  original {json.dumps(original[name])[:4000]}\n  rust     {json.dumps(rust[name])[:4000]}")
    print(f"{len(original) - failures}/{len(original)} attachment tool checks matched across the reference and the Rust server")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
