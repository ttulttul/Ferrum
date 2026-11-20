#!/usr/bin/env python3
"""Open a seeded thread with the pinned reference client against both servers.

Each server runs in its own temporary home with two projects on git checkouts
whose remotes are on Bitbucket and GitHub, and one thread that has had a user
message dispatched. The Node driver (client_thread.mjs) uses the reference
client-runtime, so every reply is decoded with the client's own schemas, and
records:

- `orchestration.subscribeThread` up to `synchronized`, with the options the
  0.1.10 client sends and without them, and for a missing thread;
- `subscribeWorktreeSetup` and `worktreeSetupCancel` for the thread;
- each project's repository identity on the shell subscription;
- `pullRequests.list` for each project, without host credentials.

Each step must end the same way on both servers. Decoded values are compared
by shape (every key path and value type) and, where the value does not depend
on time or generated ids, exactly.
"""
import argparse
import contextlib
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import subprocess
import tempfile
import time

from client_initial_sync_parity import rpc
from parity_harness import RELAY_TOOLS, encode, http, isolated_git_env, managed_server, path_without

ROOT = Path(__file__).resolve().parents[1]
THREAD = "thread:client-thread"
REMOTES = {
    "project:bitbucket": ("Bitbucket checkout", "git@ssh.bitbucket.org:example-org/example-repo.git"),
    "project:github": ("GitHub checkout", "https://github.com/Example-Org/Example.Repo.git"),
}
# Agent CLIs a server would start for the dispatched message. Hidden so both servers see a host
# without them, as on CI, and the turn never reaches a provider.
PROVIDER_CLIS = {"codex", "claude", "cursor-agent", "opencode", "gemini", "grok", "kiro-cli", "droid", "amp",
                 "copilot", "qwen"}
GENERATED = re.compile(r"^(\d{4}-\d\d-\d\dT[\d:.]+Z|[0-9a-f]{40}|[0-9a-f-]{36}|.*:[0-9a-f-]{36}.*)$")


# Set in main: the Git configuration both servers and the seeded checkouts use.
GIT_ISOLATION = {}


def git(*args, cwd=None):
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, env={**os.environ, **GIT_ISOLATION})


def make_checkout(path, remote):
    git("init", "-qb", "main", str(path))
    git("-C", str(path), "remote", "add", "origin", remote)
    (path / "README.md").write_text("fixture\n")
    git("-C", str(path), "add", "README.md")
    git("-C", str(path), "-c", "user.email=test@example.test", "-c", "user.name=Parity", "commit", "-qm", "Fixture")


def seed(url, cookie, root):
    headers = {"Cookie": cookie, "Content-Type": "application/json"}
    for project_id, (title, remote) in REMOTES.items():
        checkout = root / project_id.split(":")[1]
        make_checkout(checkout, remote)
        status, _, body = http(url, "POST", "/api/projects/mutate", encode({
            "type": "project.create", "commandId": f"create-{project_id}", "projectId": project_id,
            "title": title, "workspaceRoot": str(checkout)}), headers)
        if status != 200:
            raise RuntimeError(f"project.create failed on {url}: HTTP {status} {body[:400]!r}")
    rpc(url, cookie, "orchestration.dispatchCommand", {
        "type": "thread.create", "commandId": "seed-thread", "threadId": THREAD, "projectId": "project:bitbucket",
        "title": "Thread", "modelSelection": {"instanceId": "codex", "model": "fixture"},
        "runtimeMode": "full-access", "interactionMode": "default", "branch": None, "worktreePath": None,
        "createdBy": "user", "creationSource": "web"})
    # No provider is installed in the sandbox, so the turn ends as soon as it starts.
    rpc(url, cookie, "orchestration.dispatchCommand", {
        "type": "message.dispatch", "commandId": "seed-message", "threadId": THREAD,
        "messageId": "message:seed", "text": "Hello from the parity fixture", "attachments": [],
        "createdBy": "user", "creationSource": "web", "dispatchMode": {"type": "start_immediately"}})
    return {"threadId": THREAD, "projectIds": list(REMOTES), "hosts": ["ssh.bitbucket.org", "github.com"]}


def run_client(args, url, cookie, seeded):
    env = {**os.environ, "T3_CLIENT_SYNC_COOKIE": cookie, "T3_CLIENT_THREAD_SEED": json.dumps(seeded),
           "T3_CLIENT_THREAD_WAIT_MS": str(args.reply_ms)}
    result = subprocess.run([args.node, str(ROOT / "scripts/client_thread.mjs"), args.reference_root, url],
                            capture_output=True, text=True, env=env, timeout=args.reply_ms / 1000 + 120)
    lines = [line for line in result.stdout.splitlines() if line.startswith("{")]
    if not lines:
        raise RuntimeError(f"client driver produced no result (exit {result.returncode}):\n{result.stderr[-4000:]}")
    return json.loads(lines[-1])


def shape(value, path="$", out=None):
    """Every key path with its value type; array elements share one path."""
    out = {} if out is None else out
    if isinstance(value, dict):
        out.setdefault(path, set()).add("object")
        for key, item in value.items():
            shape(item, f"{path}.{key}", out)
    elif isinstance(value, list):
        out.setdefault(path, set()).add("array")
        for item in value:
            shape(item, f"{path}[]", out)
    else:
        out.setdefault(path, set()).add(type(value).__name__)
    return out


def stable(value, root):
    """The value with times, generated ids and the temporary home masked."""
    if isinstance(value, dict):
        return {key: stable(item, root) for key, item in value.items()}
    if isinstance(value, list):
        return [stable(item, root) for item in value]
    if isinstance(value, str):
        value = value.replace(str(root), "<home>")
        return "<generated>" if GENERATED.match(value) else value
    return value


def variants(projection):
    """Each projection record by collection and, for turn items, by type."""
    out = {"thread": [projection.get("thread")]}
    for key, value in projection.items():
        if not isinstance(value, list):
            continue
        for record in value:
            if key == "turnItems":
                out.setdefault(f"turnItems:{record.get('type')}", []).append(record)
            elif key == "visibleTurnItems":
                item = record.get("item", {})
                row = {k: v for k, v in record.items() if k != "item"}
                out.setdefault(f"visibleTurnItems:{item.get('type')}", []).append(row)
                out.setdefault(f"visibleTurnItems.item:{item.get('type')}", []).append(item)
            else:
                out.setdefault(key, []).append(record)
    return out


def comparable_shape(records):
    """Key paths with value types, where a null matches any type."""
    merged = {}
    for record in records:
        for path, types in shape(record).items():
            merged.setdefault(path, set()).update(types)
    return merged


# Fields a client shows that do not depend on time, generated ids or how far the run got.
STABLE_FIELDS = {
    "messages": ["role", "text", "attachments", "streaming", "createdBy", "creationSource"],
    "turnItems:user_message": ["text", "textOmitted", "inputIntent", "attachments", "createdBy",
                               "creationSource", "status", "title"],
    "visibleTurnItems:user_message": ["position", "visibility"],
    "visibleTurnItems:error": ["position", "visibility"],
    "turnItems:error": ["status"],
    "runs": ["status", "ordinal", "queuePosition"],
    "thread": ["title", "projectId", "runtimeMode", "interactionMode", "modelSelection", "branch", "worktreePath"],
}
STABLE_FIELDS["visibleTurnItems.item:user_message"] = STABLE_FIELDS["turnItems:user_message"]


def compare_thread(original, rust):
    """Problems with Rust's thread stream as the reference client decodes it.

    The reference records its provider runtime's work in the projection (attempts, nodes,
    provider threads), which Rust does not model. Each kind of record both servers send must
    have the same fields and types; values are compared where they do not depend on timing.
    """
    problems = []
    kinds = [[item.get("kind") for item in side] for side in (original, rust)]
    if kinds[0] != kinds[1]:
        return [f"items {kinds[0]} != {kinds[1]}"]
    for left, right in zip(original, rust):
        if left.get("kind") != "snapshot":
            continue
        outer = [sorted(k for k in item if k != "projection") for item in (left, right)]
        if outer[0] != outer[1]:
            problems.append(f"snapshot fields {outer[0]} != {outer[1]}")
        for field in ("historyCursor", "hasMoreHistory", "turnItemsOmitLocalVisible", "idTable"):
            if left.get(field) != right.get(field):
                problems.append(f"snapshot {field}: original {left.get(field)!r} rust {right.get(field)!r}")
        projections = left["projection"], right["projection"]
        if sorted(projections[0]) != sorted(projections[1]):
            problems.append(f"projection fields {sorted(projections[0])} != {sorted(projections[1])}")
        ours, theirs = variants(projections[0]), variants(projections[1])
        for name in sorted(set(ours) & set(theirs)):
            a, b = comparable_shape(ours[name]), comparable_shape(theirs[name])
            for path in sorted(set(a) | set(b)):
                left_types, right_types = a.get(path, set()) - {"NoneType"}, b.get(path, set()) - {"NoneType"}
                if path not in a or path not in b or (left_types and right_types and left_types != right_types):
                    problems.append(f"{name} {path}: original {sorted(a.get(path, []))} rust {sorted(b.get(path, []))}")
            for field in STABLE_FIELDS.get(name, []):
                values = [[record.get(field) for record in side[name]] for side in (ours, theirs)]
                if values[0] != values[1]:
                    problems.append(f"{name}.{field}: original {values[0]!r} rust {values[1]!r}")
        for name in sorted(set(ours) - set(theirs)):
            if name.endswith(("user_message", "error")) or name in ("messages", "runs", "thread"):
                problems.append(f"rust sends no {name}")
    return problems


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--reference-root", required=True,
                        help="checkout of the pinned upstream with dependencies installed")
    parser.add_argument("--node", default="node")
    parser.add_argument("--reply-ms", type=int, default=8000)
    parser.add_argument("--dump", type=Path, help="write each server's decoded results here")
    args = parser.parse_args()
    credentials = secrets.token_urlsafe(32), secrets.token_bytes(32)
    outcomes, roots = {}, {}
    with tempfile.TemporaryDirectory() as temporary, contextlib.ExitStack() as stack:
        os.environ["PATH"] = path_without(PROVIDER_CLIS | RELAY_TOOLS, Path(temporary) / "bin")
        GIT_ISOLATION.update(isolated_git_env(Path(temporary) / "git"))
        # Rust runs its provider command for a turn; one that fails ends the turn as the
        # reference's does without a provider CLI.
        commands = {"original": shlex.split(args.original_command),
                    "rust": shlex.split(args.rust_command) + ["--provider-command", "false"]}
        for name, command in commands.items():
            root = Path(temporary) / name
            url, cookie = managed_server(stack, command, root, *credentials, env=GIT_ISOLATION)
            seeded = seed(url, cookie, Path(temporary) / f"{name}-checkouts")
            roots[name] = Path(temporary) / f"{name}-checkouts"
            time.sleep(2)
            outcomes[name] = run_client(args, url, cookie, seeded)
    if args.dump:
        args.dump.mkdir(parents=True, exist_ok=True)
        for name, outcome in outcomes.items():
            (args.dump / f"{name}.json").write_text(json.dumps(outcome, indent=2, sort_keys=True))
    failures = total = 0
    names = sorted(set(outcomes["original"]["steps"]) | set(outcomes["rust"]["steps"]))
    for name in names:
        total += 1
        original = outcomes["original"]["steps"].get(name)
        rust = outcomes["rust"]["steps"].get(name)
        problems = []
        if original is None or rust is None:
            problems.append("missing on one server")
        elif original["outcome"] != rust["outcome"] or original.get("tag") != rust.get("tag"):
            problems.append(f"outcome {original['outcome']} {original.get('tag', '')} != "
                            f"{rust['outcome']} {rust.get('tag', '')}: {rust.get('message', '')[:600]}")
        elif name.startswith("orchestration.subscribeThread") and original["outcome"] == "reply":
            problems = compare_thread(original["value"], rust["value"])
        else:
            left, right = shape(original.get("value")), shape(rust.get("value"))
            for key in sorted(set(left) | set(right)):
                if left.get(key) != right.get(key):
                    problems.append(f"{key}: original {sorted(left.get(key, []))} rust {sorted(right.get(key, []))}")
            if not problems:
                comparable = [stable(original.get("value", original.get("error")), roots["original"]),
                              stable(rust.get("value", rust.get("error")), roots["rust"])]
                if comparable[0] != comparable[1]:
                    problems.append("values differ:\n    original " + json.dumps(comparable[0], sort_keys=True)[:3000]
                                    + "\n    rust     " + json.dumps(comparable[1], sort_keys=True)[:3000])
        if problems:
            failures += 1
            print(f"FAIL {name}")
            for problem in problems[:40]:
                print(f"  {problem}")
        else:
            print(f"PASS {name}: {rust['outcome']} {rust.get('tag', '')}")
    print(f"{total - failures}/{total} client thread checks matched")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
