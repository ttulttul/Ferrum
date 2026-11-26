#!/usr/bin/env python3
"""An agent asks the user to link another machine, on the reference and on the
Rust server, and both must answer the same.

A thread's provider CLI (the fake Claude Code in tests/fixtures/providers,
playing claude_link.json) calls T3's tools on the MCP server its server gave
it: it lists the links, names a machine already linked (answered `linked` at
once), forwards a call there (which carries `T3-Sender-Thread`), asks for three
links the user must make (`pending` cards), retries one with the same key, asks for one more mid-turn,
and makes two calls the server refuses. Then the user answers the cards
through `peerLinks.answerRequest`: one declined, one linked to the fake peer
(which wakes the agent with a notification, so a resumed turn runs, and
wakes the thread from its snooze), a card
answered twice, requests naming no card or no thread, and a card archiving
the thread closed.

Compared: every MCP answer, every `answerRequest` reply, the cards and wake
messages in the thread's projection, where each card sits in the timeline, the links each server then keeps, and
what the fake peer received. All data is fake: temporary homes, loopback
addresses, fixture labels, generated keys and a fake CLI.
"""
import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import re
import secrets
import shlex
from tempfile import TemporaryDirectory
import time

from websockets.sync.client import connect

from client_thread_parity import (FAKE_CLAUDE, FIXTURES, GIT_ISOLATION, THREAD, agent_mcp, comparable_mcp,
                                  seed_turn)
from parity_harness import PROVIDER_CLIS, RELAY_TOOLS, encode, http, isolated_git_env, managed_server, path_without, recv_frame
from peer_forwarding_parity import FAKE_PEER_ID, FakePeer
from peer_link_outgoing_parity import set_linked_environments

UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
TIME = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z")
SCENARIO = {"name": "claude link request", "instance": "claudeAgent", "model": "claude-opus-5-5"}


class Server:
    """One server with the fake Claude Code on its PATH, and an owner socket."""

    def __init__(self, stack, command, home, secret, key, env):
        self.url, self.cookie = managed_server(stack, shlex.split(command), home, secret, key, env=env)
        self.socket = stack.enter_context(connect(
            self.url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2",
            additional_headers={"Cookie": self.cookie}, compression=None, proxy=None))
        self.sequence = 0
        status, _, body = http(self.url, "GET", "/.well-known/t3/environment")
        self.environment_id = json.loads(body)["environmentId"]

    def rpc(self, tag, payload, timeout=60):
        self.sequence += 1
        request = f"link-{self.sequence}"
        self.socket.send(encode(dict(_tag="Request", id=request, tag=tag, payload=payload, headers=[])).decode())
        while True:
            frame = json.loads(recv_frame(self.socket, timeout=timeout))
            if frame.get("_tag") == "Ping":
                self.socket.send(encode({"_tag": "Pong"}).decode())
                continue
            if frame.get("requestId") == request and "exit" in frame:
                return frame["exit"]

    def projection(self):
        exit = self.rpc("orchestration.getThreadProjection", {"threadId": THREAD})
        assert exit["_tag"] == "Success", exit
        return exit["value"]


def settled(projection, runs):
    statuses = [run.get("status") for run in projection.get("runs", [])]
    return len(statuses) >= runs and all(s not in ("preparing", "starting", "running", "waiting", "queued")
                                         for s in statuses)


def wait_runs(server, runs, seconds=60, *, checkpoint=False):
    deadline = time.monotonic() + seconds
    while True:
        projection = server.projection()
        if settled(projection, runs) and (not checkpoint or all(
                run.get("checkpointId") is not None for run in projection["runs"])):
            return projection
        if time.monotonic() > deadline:
            raise TimeoutError(f"Expected {runs} settled runs (checkpoint={checkpoint})")
        time.sleep(0.25)


def mask(value, server, root, peer):
    text = json.dumps(value, sort_keys=True)
    text = text.replace(server.environment_id, "<environment>").replace(peer.url, "<peer-url>")
    for side in ("original", "rust"):
        text = text.replace(f"{root}/{side}", "<home>/<server>")
    text = TIME.sub("<time>", UUID.sub("<uuid>", text))
    # A provider thread named before its native one carries the run id, whose form differs (the
    # shared store gives Rust the reference's run ids).
    text = re.sub(r"native-thread:pending%3A[^\"]*", "native-thread:pending%3A<run>", text)
    return json.loads(text)


def cards(projection):
    """The link cards and the messages that answer them, as a client shows them."""
    items = [item for item in projection.get("turnItems", []) if item.get("type") == "link_request"]
    keep = ("id", "type", "status", "title", "url", "reason", "environmentId", "label", "hint", "requestedAccess",
            "linkStatus", "linkedEnvironmentId", "linkedLabel", "linkedAccess", "failure", "nativeItemRef",
            "parentItemId", "completedAt", "providerThreadId")
    out = [{key: item.get(key, "<absent>") for key in keep} for item in sorted(items, key=lambda i: i.get("ordinal", 0))]
    notices = [item for item in projection.get("turnItems", []) if item.get("type") == "notification"]
    messages = [{k: m.get(k, "<absent>") for k in ("id", "role", "text", "createdBy", "creationSource", "notification")}
                for m in projection.get("messages", []) if "link-request" in str(m.get("id"))]
    return {"cards": out, "notifications": [{k: n.get(k) for k in ("source", "outcome", "summary", "status")} for n in notices],
            "messages": messages, "runs": [run.get("status") for run in projection.get("runs", [])]}


def timeline(projection):
    """The thread's turn items in order, as a client lays them out: where each card sits among
    the items its run had when it was asked."""
    items = sorted(projection.get("turnItems", []), key=lambda item: (item.get("ordinal", 0), item.get("id")))
    return [f"{item.get('type')} {re.sub(r'.*:', '', item['id']) if item.get('type') == 'link_request' else ''}".strip()
            for item in items]


def snooze_state(projection):
    thread = projection.get("thread") or {}
    return {"snoozedUntil": thread.get("snoozedUntil"), "snoozed": thread.get("snoozedAt") is not None}


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
                peer = FakePeer()
                stack.callback(peer.close)
                root = tmp / side
                set_linked_environments(root, True)
                fake_bin = tmp / f"{side}-bin"
                fake_bin.mkdir()
                (fake_bin / "claude").write_text(FAKE_CLAUDE)
                (fake_bin / "claude").chmod(0o755)
                log = tmp / f"{side}-provider.log"
                env = {**GIT_ISOLATION, "FAKE_PROVIDER_CLI": str(FIXTURES / "fake_cli.py"), "FAKE_PROVIDER_LOG": str(log),
                       "FAKE_PROVIDER_SCENARIO": str(FIXTURES / "claude_link.json"),
                       "PATH": os.pathsep.join([str(fake_bin), path_without(PROVIDER_CLIS | RELAY_TOOLS, tmp / f"{side}-hidden")])}
                server = Server(stack, command, root, secrets.token_urlsafe(32), secrets.token_bytes(32), env)
                record = {}
                linked = server.rpc("peerLinks.link", dict(url=peer.url, pairingCode="fixture-code", access="auto"))
                assert linked["_tag"] == "Success", linked
                seeded = seed_turn(server.url, server.cookie, tmp / f"{side}-checkouts", SCENARIO)
                peer.take_log()
                dispatched = server.rpc("orchestration.dispatchCommand", seeded["dispatch"])
                assert dispatched["_tag"] == "Success", dispatched
                # The timeline includes the checkpoint, which is attached after the run ends.
                after_turn = wait_runs(server, 1, checkpoint=True)
                record["agent mcp"] = agent_mcp(log, tmp)
                record["what the peer received"] = [
                    {k: e[k] for k in ("method", "modeLimit", "sender", "params")} for e in peer.take_log()
                    if e["method"] == "tools/call"]
                record["cards after the turn"] = cards(after_turn)
                record["timeline after the turn"] = timeline(after_turn)
                by_key = {re.sub(r".*:", "", item["id"]): item["id"] for item in after_turn.get("turnItems", [])
                          if item.get("type") == "link_request"}
                answer = lambda key, payload: server.rpc("peerLinks.answerRequest", dict(
                    threadId=THREAD, turnItemId=by_key.get(key, f"turn-item:missing:{key}"), answer=payload))
                record["decline"] = answer("card-vps", {"type": "decline"})
                record["answered twice"] = answer("card-vps", {"type": "decline"})
                wait_runs(server, 2)
                # A snoozed thread wakes when a card links.
                snoozed = server.rpc("orchestration.dispatchCommand", {
                    "type": "thread.snooze", "commandId": "snooze-thread", "threadId": THREAD,
                    "snoozedUntil": "2099-01-01T00:00:00.000Z"})
                assert snoozed["_tag"] == "Success", snoozed
                record["snoozed before the link"] = snooze_state(server.projection())
                record["link"] = answer("card-url", {"type": "link", "urls": [peer.url], "access": "auto",
                                                     "pairingCode": "fixture-code"})
                record["snooze after the link"] = snooze_state(server.projection())
                record["no such card"] = answer("card-none", {"type": "decline"})
                record["no such thread"] = server.rpc("peerLinks.answerRequest", dict(
                    threadId="thread:missing", turnItemId="turn-item:missing", answer={"type": "decline"}))
                final = wait_runs(server, 3)
                record["cards after the answers"] = cards(final)
                raw = {k: final.get(k) for k in ("turnItems", "messages", "runs")}
                # Archiving the thread closes the card still waiting, waking nobody.
                archived = server.rpc("orchestration.dispatchCommand", {
                    "type": "thread.archive", "commandId": "archive-thread", "threadId": THREAD})
                assert archived["_tag"] == "Success", archived
                record["cards after archiving"] = cards(server.projection())
                record["answer after archiving"] = answer("card-left", {"type": "decline"})
                listed = server.rpc("peerLinks.list", {})
                record["links kept"] = [(row["environmentId"], row["access"], row["status"])
                                        for row in listed["value"]["links"]]
                results[side] = mask(record, server, tmp, peer)
                raws[side] = mask(raw, server, tmp, peer)
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
    print(f"{len(original) - failures}/{len(original)} link request checks matched across the reference and the Rust server")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
