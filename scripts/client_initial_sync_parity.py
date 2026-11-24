#!/usr/bin/env python3
"""Run the pinned reference client's initial sync against both servers.

Each server runs in its own temporary home. The Node driver
(client_initial_sync.mjs) builds the reference client-runtime RPC session and
waits until the environment counts as connected, as ConnectionDriver does. It
then issues the RPCs and HTTP reads a freshly connected 0.1.10 client sends:
first on an empty server, then with a project and thread so the sidebar row
subscriptions run too. Each step must end the same way on both servers, with a
reply, an error or silence, and the same event discriminator.

A raw protocol section compares exact frames for the Effect RPC envelope
details the client relies on: numeric request ids, acknowledgements,
interrupts, invalid ids and frame parts.
"""
import argparse
import contextlib
import json
import os
from pathlib import Path
import secrets
import shlex
import subprocess
import tempfile

from websockets.sync.client import connect

from parity_harness import encode, http, managed_server, volatile_masked, recv_frame

ROOT = Path(__file__).resolve().parents[1]
PROJECT = "project:initial-sync"
THREAD = "thread:initial-sync"
# What RpcSession.connect sends with the web client's options.
CLIENT_CONFIG_INPUT = {"environmentThemes": True, "usageLimitSources": True, "usageLimitsCommand": True,
                       "workspaceSnapshotsOnDemand": True}


def socket(url, cookie, query=""):
    return connect(url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2" + query,
                   additional_headers={"Cookie": cookie}, proxy=None, compression=None, max_size=None)


def rpc(url, cookie, tag, payload):
    with socket(url, cookie) as ws:
        ws.send(encode({"_tag": "Request", "id": "seed", "tag": tag, "payload": payload, "headers": []}).decode())
        while True:
            frame = json.loads(recv_frame(ws, timeout=20))
            if frame.get("_tag") == "Exit":
                if frame["exit"]["_tag"] != "Success":
                    raise RuntimeError(f"{tag} failed: {frame}")
                return frame["exit"]["value"]


def seed(url, cookie, repo):
    """A project on a git checkout with one thread, made the way the client makes them."""
    status, _, body = http(url, "POST", "/api/projects/mutate", encode({
        "type": "project.create", "commandId": "seed-project", "projectId": PROJECT,
        "title": "Initial sync", "workspaceRoot": str(repo)}), {"Cookie": cookie, "Content-Type": "application/json"})
    if status != 200:
        raise RuntimeError(f"project.create failed on {url}: HTTP {status} {body[:400]!r}")
    rpc(url, cookie, "orchestration.dispatchCommand", {
        "type": "thread.create", "commandId": "seed-thread", "threadId": THREAD, "projectId": PROJECT,
        "title": "Draft", "modelSelection": {"instanceId": "codex", "model": "fixture"},
        "runtimeMode": "full-access", "interactionMode": "default", "branch": None, "worktreePath": None,
        "createdBy": "user", "creationSource": "web"})
    return {"cwd": str(repo), "threadId": THREAD}


def run_client(args, url, cookie, seeded=None):
    env = {**os.environ, "T3_CLIENT_SYNC_COOKIE": cookie,
           "T3_CLIENT_SYNC_TIMEOUT_MS": str(args.timeout_ms), "T3_CLIENT_SYNC_STREAM_MS": str(args.reply_ms)}
    if seeded is not None:
        env["T3_CLIENT_SYNC_SEED"] = json.dumps(seeded)
    # parity-env: the Node client driver, not a T3 server
    result = subprocess.run([args.node, str(ROOT / "scripts/client_initial_sync.mjs"), args.reference_root, url],
                            capture_output=True, text=True, env=env, timeout=args.timeout_ms / 1000 + 120)
    lines = [line for line in result.stdout.splitlines() if line.startswith("{")]
    if not lines:
        raise RuntimeError(f"client driver produced no result (exit {result.returncode}):\n{result.stderr[-4000:]}")
    return json.loads(lines[-1])


NO_REPLY = "no reply within 10 s"


def protocol_frames(url, cookie):
    """Exact replies to the envelope details the 0.1.10 client depends on.

    Each check uses its own socket, so a server that ignores one frame still
    gets the others, and silence is reported instead of raised.
    """
    replies = []
    for request in [
        {"_tag": "Request", "id": 0, "tag": "server.probe", "payload": {}, "headers": [],
         "traceId": "0123456789abcdef0123456789abcdef", "spanId": "0123456789abcdef", "sampled": True},
        {"_tag": "Request", "id": "0", "tag": "server.probe", "payload": {}, "headers": []},
        {"_tag": "Request", "tag": "server.probe", "payload": {}, "headers": []},
        {"_tag": "Request", "id": None, "tag": "server.probe", "payload": {}, "headers": []},
        {"_tag": "Request", "id": False, "tag": "server.probe", "payload": {}, "headers": []},
        {"_tag": "Request", "id": 3, "tag": "no.suchMethod", "payload": {}, "headers": []},
    ]:
        with socket(url, cookie) as ws:
            ws.send(encode(request).decode())
            try:
                replies.append(recv_frame(ws, timeout=10))
            except TimeoutError:
                replies.append(NO_REPLY)
    # A stream interrupted by its numeric id ends with an interrupt exit.
    with socket(url, cookie) as ws:
        try:
            ws.send(encode({"_tag": "Request", "id": 4, "tag": "subscribeServerLifecycle", "payload": {},
                            "headers": []}).decode())
            first = json.loads(recv_frame(ws, timeout=10))
            replies.append(f"first frame for id 4: {first['_tag']} {first.get('requestId')!r}")
            ws.send(encode({"_tag": "Ack", "requestId": 4}).decode())
            ws.send(encode({"_tag": "Interrupt", "requestId": 4}).decode())
            while True:
                frame = json.loads(recv_frame(ws, timeout=10))
                if frame["_tag"] == "Exit":
                    # The reference names its runtime's interrupting fiber; the number is not part of the contract.
                    for reason in frame["exit"].get("cause", []):
                        reason.pop("fiberId", None)
                    replies.append(encode(frame).decode())
                    break
        except TimeoutError:
            replies.append(NO_REPLY)
    # With frame parts, a large reply arrives as parts the client joins back.
    with socket(url, cookie, "&frameParts=1") as ws:
        ws.send(encode({"_tag": "Request", "id": 5, "tag": "subscribeServerConfig", "payload": CLIENT_CONFIG_INPUT,
                        "headers": []}).decode())
        parts, pending = 0, ""
        while True:
            try:
                text = recv_frame(ws, timeout=10)
            except TimeoutError:
                replies.append(NO_REPLY)
                break
            parts += 1
            if text.startswith("\u001e"):
                if len(text.encode()) > 48 * 1024:
                    replies.append(f"part over 48 KiB: {len(text.encode())} bytes")
                pending += text[1:]
                continue
            whole = json.loads(pending + text)
            # splitSocketFrame splits once the UTF-8 frame and its one-byte part marker pass 48 KiB.
            expected_split = len((pending + text).encode()) + 1 > 48 * 1024
            replies.append(f"snapshot over frame parts: {whole['_tag']} {whole['requestId']!r} "
                           f"{whole['values'][0]['type']} split as expected: {expected_split == (parts > 1)}")
            break
    return replies


def compare_steps(label, outcomes):
    failures = 0
    names = sorted({step["name"] for result in outcomes.values() for step in result["steps"]})
    by_name = {server: {step["name"]: step for step in result["steps"]} for server, result in outcomes.items()}
    for name in names:
        original, rust = by_name["original"].get(name), by_name["rust"].get(name)
        same = (original is not None and rust is not None
                and (original["outcome"], original.get("signature")) == (rust["outcome"], rust.get("signature")))
        # The client must be connected; silence there is the reported 90 s hang.
        if name == "environment.initialSync" and (rust or {}).get("outcome") != "reply":
            same = False
        if same:
            print(f"PASS {label} {name}: {rust['outcome']} {rust.get('signature', '')}"
                  f" (original {original['atMs']} ms, rust {rust['atMs']} ms)")
        else:
            failures += 1
            print(f"FAIL {label} {name}\n  original {json.dumps(original)}\n  rust     {json.dumps(rust)}")
    return failures, len(names)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--reference-root", required=True,
                        help="checkout of the pinned upstream with dependencies installed")
    parser.add_argument("--node", default="node")
    parser.add_argument("--timeout-ms", type=int, default=20000, help="limit for the initial sync")
    parser.add_argument("--reply-ms", type=int, default=5000,
                        help="wait for each later reply or first stream value before counting it silent")
    args = parser.parse_args()
    credentials = secrets.token_urlsafe(32), secrets.token_bytes(32)
    failures = total = 0
    with tempfile.TemporaryDirectory() as temporary, contextlib.ExitStack() as stack:
        root = Path(temporary)
        servers = {}
        for name, command in (("original", args.original_command), ("rust", args.rust_command)):
            servers[name] = managed_server(stack, shlex.split(command), root / name, *credentials)

        frames = {name: protocol_frames(url, cookie) for name, (url, cookie) in servers.items()}
        for index, (original, rust) in enumerate(zip(frames["original"], frames["rust"])):
            original, rust = volatile_masked(original), volatile_masked(rust)
            total += 1
            if original == rust:
                print(f"PASS protocol frame {index}: {rust[:160]}")
            else:
                failures += 1
                print(f"FAIL protocol frame {index}\n  original {original[:1600]}\n  rust     {rust[:1600]}")

        empty = {name: run_client(args, url, cookie) for name, (url, cookie) in servers.items()}
        step_failures, step_total = compare_steps("empty", empty)
        failures, total = failures + step_failures, total + step_total

        seeded = {}
        for name, (url, cookie) in servers.items():
            repo = root / f"{name}-repo"
            # parity-env: git, not a T3 server
            subprocess.run(["git", "init", "-qb", "main", str(repo)], check=True)
            # parity-env: git, not a T3 server
            subprocess.run(["git", "-C", str(repo), "-c", "user.email=test@example.test", "-c", "user.name=Parity",
                            "commit", "--allow-empty", "-qm", "Fixture"], check=True)
            seeded[name] = run_client(args, url, cookie, seed(url, cookie, repo))
        step_failures, step_total = compare_steps("seeded", seeded)
        failures, total = failures + step_failures, total + step_total
    print(f"{total - failures}/{total} client initial sync checks matched")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
