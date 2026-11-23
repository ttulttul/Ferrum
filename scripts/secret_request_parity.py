#!/usr/bin/env python3
"""An agent asks the user for secrets, on the reference and on the Rust
server, and both must answer the same.

A thread's provider CLI (the fake Claude Code in tests/fixtures/providers,
playing claude_secret.json) calls `request_secret` on the MCP server its
server gave it:
- three cards it waits on in the background: one saved for a `secretRef`, one
  saved for a file (`delivery: "file"`), and one the user declines;
- one that times out;
- a pasted `value`, which goes straight to a file, then again with the same
  key and another value, then with `delivery: "secretRef"` (refused);
- a retry of the saved cards with the same keys once they are answered;
- one more asked mid-turn and left open, which closes when the run ends.

The user answers through `secrets.answerRequest`:
- a secret provider that cannot read (no secret provider is configured, so
  nothing is read);
- a save, a save for the file, and a decline;
- an empty secret, a card answered twice, requests naming no card or no
  thread, and an answer after its run ended.

Compared: every MCP answer and `answerRequest` reply, the cards in the
thread's projection and where each sits in the timeline, the files the agent
was given (contents and mode), and what the secrets directory keeps. Refs and
file names are compared by form, as each server's salt differs. All data is
fake: temporary homes, loopback addresses, fixture labels and values,
generated keys and a fake CLI.
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

from client_thread_parity import FAKE_CLAUDE, FIXTURES, GIT_ISOLATION, THREAD, agent_mcp, comparable_mcp, seed_turn
from link_request_parity import Server, TIME, UUID, wait_runs
from parity_harness import PROVIDER_CLIS, RELAY_TOOLS, isolated_git_env, path_without

SCENARIO = {"name": "claude secret request", "instance": "claudeAgent", "model": "claude-opus-5-5"}
REF = re.compile(r"secret-ref:[0-9a-f]{32}")
HEX = re.compile(r"(?<![0-9a-f])[0-9a-f]{32}(?![0-9a-f])")


def mask(value, server, root):
    text = json.dumps(value, sort_keys=True)
    text = text.replace(server.environment_id, "<environment>")
    for side in ("original", "rust"):
        text = text.replace(f"{root}/{side}", "<home>/<server>")
    text = REF.sub("secret-ref:<hex>", text)
    text = HEX.sub("<hex>", text)
    text = TIME.sub("<time>", UUID.sub("<uuid>", text))
    text = re.sub(r"native-thread:pending%3A[^\"]*", "native-thread:pending%3A<run>", text)
    return json.loads(text)


def secret_cards(projection):
    return [item for item in projection.get("turnItems", []) if item.get("type") == "secret_request"]


def cards(projection):
    """The secret cards as a client shows them."""
    keep = ("id", "type", "status", "title", "label", "reason", "url", "placeholder", "delivery", "secretStatus",
            "nativeItemRef", "parentItemId", "completedAt", "providerThreadId", "providerTurnId")
    return [{key: item.get(key, "<absent>") for key in keep}
            for item in sorted(secret_cards(projection), key=lambda i: i.get("ordinal", 0))]


def timeline(projection):
    """The thread's turn items in order: where each card sits among its run's items."""
    items = sorted(projection.get("turnItems", []), key=lambda item: (item.get("ordinal", 0), item.get("id")))
    return [f"{item.get('type')} {re.sub(r'.*%3A', '', item['id']) if item.get('type') == 'secret_request' else ''}"
            .strip() for item in items]


def wait_cards(server, keys, status, seconds=60):
    """The projection once each card in `keys` has `status`."""
    deadline = time.monotonic() + seconds
    while True:
        projection = server.projection()
        found = {item["id"].rsplit(":", 1)[-1]: item.get("secretStatus") for item in secret_cards(projection)}
        if all(found.get(key) == status for key in keys) or time.monotonic() > deadline:
            return projection
        time.sleep(0.1)


def wait_log(log, count, seconds=30):
    """Waits until the fake CLI has logged `count` answers from T3's MCP server."""
    deadline = time.monotonic() + seconds
    path = Path(f"{log}.mcp")
    while time.monotonic() < deadline:
        if path.exists() and sum('"answer"' in line for line in path.read_text().splitlines()) >= count:
            return
        time.sleep(0.1)


def kept(root):
    """What each server's secrets directory holds, by form: the saved values (their project and
    value; the time each was saved left out) and the files given to the agent, with their modes."""
    out = []
    for path in sorted(root.rglob("*")):
        relative = HEX.sub("<hex>", str(path.relative_to(root)))
        if not path.is_file() or not ("secret-request" in path.name or path.parent.name == "files"):
            continue
        entry = {"path": relative, "mode": oct(path.stat().st_mode & 0o777)}
        if path.name.startswith("secret-request-") and path.name != "secret-request-salt.bin":
            stored = json.loads(path.read_bytes())
            entry["stored"] = {"value": stored.get("value"), "projectId": stored.get("projectId"),
                               "savedAt": type(stored.get("savedAt")).__name__}
        elif path.parent.name == "files":
            entry["contents"] = path.read_text()
        out.append(entry)
    return sorted(out, key=lambda e: json.dumps(e, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--show", action="store_true", help="print each answer as the reference gave it")
    parser.add_argument("--dump", type=Path, help="write both servers' answers here as JSON")
    args = parser.parse_args()
    results, raws = {}, {}
    with TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        GIT_ISOLATION.update(isolated_git_env(tmp / "git"))
        for side, command in (("original", args.original_command), ("rust", args.rust_command)):
            with ExitStack() as stack:
                root = tmp / side
                fake_bin = tmp / f"{side}-bin"
                fake_bin.mkdir()
                (fake_bin / "claude").write_text(FAKE_CLAUDE)
                (fake_bin / "claude").chmod(0o755)
                log = tmp / f"{side}-provider.log"
                env = {**GIT_ISOLATION, "FAKE_PROVIDER_CLI": str(FIXTURES / "fake_cli.py"), "FAKE_PROVIDER_LOG": str(log),
                       "FAKE_PROVIDER_SCENARIO": str(FIXTURES / "claude_secret.json"),
                       "PATH": os.pathsep.join([str(fake_bin), path_without(PROVIDER_CLIS | RELAY_TOOLS, tmp / f"{side}-hidden")])}
                server = Server(stack, command, root, secrets.token_urlsafe(32), secrets.token_bytes(32), env)
                record = {}
                seeded = seed_turn(server.url, server.cookie, tmp / f"{side}-checkouts", SCENARIO)
                dispatched = server.rpc("orchestration.dispatchCommand", seeded["dispatch"])
                assert dispatched["_tag"] == "Success", dispatched
                asked = wait_cards(server, ("card-save", "card-file", "card-decline"), "pending")
                record["cards while asked"] = cards(asked)
                by_key = {item["id"].rsplit(":", 1)[-1]: item["id"] for item in secret_cards(asked)}
                answer = lambda key, payload: server.rpc("secrets.answerRequest", dict(
                    threadId=THREAD, turnItemId=by_key.get(key, f"turn-item:secret-request:missing:{key}"),
                    answer=payload))
                record["unreadable provider"] = answer("card-decline", {
                    "type": "reference", "reference": {"provider": "vault", "reference": "op://Vault/Item/field"}})
                record["empty secret"] = answer("card-save", {"type": "save", "secret": "   "})
                record["save"] = answer("card-save", {"type": "save", "secret": "  fixture-secret-save  "})
                record["answered twice"] = answer("card-save", {"type": "save", "secret": "other"})
                record["save for a file"] = answer("card-file", {"type": "save", "secret": "fixture-secret-file"})
                record["decline"] = answer("card-decline", {"type": "decline"})
                record["no such card"] = answer("card-none", {"type": "decline"})
                record["no such thread"] = server.rpc("secrets.answerRequest", dict(
                    threadId="thread:missing", turnItemId="turn-item:missing", answer={"type": "decline"}))
                final = wait_runs(server, 1)
                final = wait_cards(server, ("card-late",), "cancelled")
                by_key = {item["id"].rsplit(":", 1)[-1]: item["id"] for item in secret_cards(final)}
                record["answer after the run"] = answer("card-late", {"type": "save", "secret": "late"})
                # The late card's call answers once its run has ended.
                wait_log(log, 10)
                record["agent mcp"] = agent_mcp(log, tmp)
                record["cards after the turn"] = cards(final)
                record["timeline"] = timeline(final)
                record["secrets kept"] = kept(root)
                results[side] = mask(record, server, tmp)
                raws[side] = mask({k: final.get(k) for k in ("turnItems", "runs")}, server, tmp)
    if args.dump:
        args.dump.write_text(json.dumps({"answers": results, "projections": raws}, indent=1, sort_keys=True))
    failures = 0
    original, rust = results["original"], results["rust"]
    original["agent mcp"], rust["agent mcp"] = comparable_mcp(original["agent mcp"], rust["agent mcp"])
    for name in original:
        same = original[name] == rust[name]
        failures += not same
        if args.show:
            print(f"{name}: {json.dumps(original[name])[:3000]}")
        print(f"{'PASS' if same else 'FAIL'} {name}")
        if not same:
            print(f"  original {json.dumps(original[name])[:4000]}\n  rust     {json.dumps(rust[name])[:4000]}")
    print(f"{len(original) - failures}/{len(original)} secret request checks matched across the reference and the Rust server")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
