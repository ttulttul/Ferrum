#!/usr/bin/env python3
"""Compare how the reference and the Rust server page a long thread's history to an environment
linked to them: the bounded snapshot on `GET /api/links/thread/stream` keeps only the newest ten
user turns and hands out a `historyCursor`, and `GET /api/links/thread/history` reads the turns
before a cursor, twenty at a time, until no more history is left.

A second server of the same kind then links to the first, and its client reads the same
snapshot and pages through the link (`peerLinks.subscribeThread`, `peerLinks.threadHistoryPage`).

Each server runs on its own fresh home with the same steps. A thread gets 34 user turns, each sent
once the one before it has settled. No provider CLI is on PATH, so each turn is the user's
message and the error its failed run records. The answers are compared by row (item id, position
and type) and by decoded cursor, with the snapshot sequence masked. All data is fake. Nothing may
leave loopback: every server runs behind the harness's loopback proxy, which forwards nothing
(parity_harness.assert_nothing_escaped).
"""
import argparse
import base64
from contextlib import ExitStack
import http.client as http_client
import json
from pathlib import Path
import secrets
import shlex
import time
from tempfile import TemporaryDirectory
from urllib.parse import quote, urlsplit

from websockets.sync.client import connect

import parity_harness
from parity_harness import encode, http, managed_server, recv_frame
from peer_link_outgoing_parity import Server
from peer_link_relay_parity import stream as relay_stream
from peer_link_routes_parity import dispatch, link, post, set_linked_environments

TURNS = 34
THREAD = "thread:history"


def projection(url, cookie):
    with connect(url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2", additional_headers={"Cookie": cookie},
                 proxy=None, compression=None, max_size=None) as ws:
        ws.send(encode({"_tag": "Request", "id": "read", "tag": "orchestration.getThreadProjection",
                        "payload": {"threadId": THREAD}, "headers": []}).decode())
        while True:
            frame = json.loads(recv_frame(ws, timeout=20))
            if frame.get("_tag") == "Exit":
                return frame["exit"].get("value") or {}


def settled(url, cookie, turn):
    """Whether turn `turn` has recorded its failed run's error after its message. The newest turn
    is always in the projection's window, which a long thread bounds."""
    found = projection(url, cookie)
    items = (found.get("projection") or found).get("turnItems", [])
    ids = [item.get("id") for item in items]
    message = f"turn-item:message:message%3A{turn:02d}"
    return message in ids and any(item.get("type") == "error" for item in items[ids.index(message) + 1:])


def seed(url, cookie, workspace):
    post(url, cookie, "/api/projects/mutate", {
        "type": "project.create", "commandId": "seed-project", "projectId": "project:history",
        "title": "History", "workspaceRoot": str(workspace)})
    dispatch(url, cookie, {
        "type": "thread.create", "commandId": "seed-thread", "threadId": THREAD, "projectId": "project:history",
        "title": "Long thread", "modelSelection": {"instanceId": "codex", "model": "fixture"},
        "runtimeMode": "full-access", "interactionMode": "default", "branch": None, "worktreePath": None,
        "createdBy": "user", "creationSource": "web"})
    for turn in range(TURNS):
        dispatch(url, cookie, {
            "type": "message.dispatch", "commandId": f"turn-{turn}", "threadId": THREAD,
            "messageId": f"message:{turn:02d}", "text": f"Turn {turn}", "attachments": [], "createdBy": "user",
            "creationSource": "web", "dispatchMode": {"type": "start_immediately"}})
        deadline = time.monotonic() + 30
        while not settled(url, cookie, turn):
            assert time.monotonic() < deadline, f"turn {turn} did not settle on {url}"
            time.sleep(0.2)


def cursor(value):
    """A cursor by what it names, with its snapshot sequence masked."""
    if value is None:
        return None
    payload = json.loads(base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)))
    return {**payload, "seq": "<sequence>"}


def rows(items):
    return [[row["sourceItemId"], row["position"], row["visibility"], row["item"]["type"]] for row in items]


def snapshot(url, token):
    parts = urlsplit(url)
    connection = http_client.HTTPConnection(parts.hostname, parts.port, timeout=20)
    try:
        connection.request("GET", f"/api/links/thread/stream?thread={quote(THREAD)}",
                           headers={"authorization": f"Bearer {token}"})
        response = connection.getresponse()
        assert response.status == 200, response.status
        while True:
            line = response.fp.readline().decode()
            assert line, "the stream ended before its snapshot"
            if line.startswith("data: "):
                return json.loads(line[len("data: "):])
    finally:
        connection.close()


def history(url, token, value):
    status, headers, body = http(url, "GET", f"/api/links/thread/history?thread={quote(THREAD)}&cursor={quote(value)}",
                                 None, {"authorization": f"Bearer {token}"})
    parsed = json.loads(body)
    return status, headers.get("cache-control"), parsed


def page_summary(page):
    return {"items": rows(page["items"]), "nextCursor": cursor(page["nextCursor"]),
            "hasMoreHistory": page["hasMoreHistory"], "subagents": page.get("subagents")}


def run(command, home):
    record = {}
    set_linked_environments(home, True)
    with ExitStack() as stack:
        url, cookie = managed_server(stack, shlex.split(command), home, secrets.token_urlsafe(32),
                                     secrets.token_bytes(32))
        workspace = home.parent / "workspace"
        workspace.mkdir()
        seed(url, cookie, workspace)
        token = link(url, cookie, "auto")
        found = snapshot(url, token)
        bounded = found["projection"]
        record["snapshot: window"] = rows(bounded["visibleTurnItems"])
        record["snapshot: turn items"] = [item["id"] for item in bounded["turnItems"]]
        record["snapshot: messages"] = [message["id"] for message in bounded["messages"]]
        # The records a window keeps: those its rows name, and the live ones.
        for key in ("runs", "attempts", "nodes", "subagents", "providerSessions", "providerThreads",
                    "providerTurns", "plans", "checkpoints", "contextHandoffs", "runtimeRequests"):
            record[f"snapshot: {key}"] = [entry.get("id") for entry in bounded.get(key, [])]
        record["snapshot: cursor"] = cursor(found["historyCursor"])
        record["snapshot: more history"] = found["hasMoreHistory"]
        record["snapshot: latest local turn ordinal"] = found["latestLocalTurnOrdinal"]
        next_cursor, number = found["historyCursor"], 0
        while next_cursor is not None:
            number += 1
            status, cache, page = history(url, token, next_cursor)
            assert status == 200, (status, page)
            record[f"page {number}"] = {"status": status, "cache-control": cache, **page_summary(page)}
            next_cursor = page["nextCursor"]
            assert number < 10, "paging did not end"
        # A stale cursor names a row that is still there: the same page again.
        status, _, page = history(url, token, found["historyCursor"])
        record["the snapshot's cursor again"] = page_summary(page) == record["page 1"] if number else page
        record["a cursor the store cannot read"] = history(url, token, "not-a-cursor")[::2]
        record["no cursor"] = http(url, "GET", f"/api/links/thread/history?thread={quote(THREAD)}", None,
                                   {"authorization": f"Bearer {token}"})[0]

        # Through a link: a server of the same kind links here, and its client pages the thread
        # (`peerLinks.subscribeThread`, then `peerLinks.threadHistoryPage`).
        local_home = home.parent / "local"
        set_linked_environments(local_home, True)
        local = Server(stack, command, local_home, secrets.token_urlsafe(32), secrets.token_bytes(32))
        code = post(url, cookie, "/api/auth/pairing-token", {})["credential"]
        made = local.rpc("peerLinks.link", dict(url=url, pairingCode=code, access="auto"))
        assert made["_tag"] == "Success", made
        target = dict(environmentId=made["value"]["environmentId"], threadId=THREAD)
        relayed = relay_stream(local, target)["items"][0]
        record["through a link: snapshot cursor"] = cursor(relayed.get("historyCursor"))
        record["through a link: snapshot window"] = rows(relayed["projection"]["visibleTurnItems"])
        next_cursor, number = relayed.get("historyCursor"), 0
        while next_cursor is not None:
            number += 1
            exit = local.rpc("peerLinks.threadHistoryPage", dict(target, cursor=next_cursor))
            assert exit["_tag"] == "Success", exit
            record[f"through a link: page {number}"] = page_summary(exit["value"])
            next_cursor = exit["value"]["nextCursor"]
            assert number < 10, "paging did not end"
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    with TemporaryDirectory() as tmp:
        reference = run(args.original_command, Path(tmp) / "reference" / "home")
        rust = run(args.rust_command, Path(tmp) / "rust" / "home")
    # Every server ran behind a loopback proxy that forwards nothing; what it refused never left.
    parity_harness.assert_nothing_escaped()
    assert reference["snapshot: more history"] and "page 2" in reference, \
        "the fixture thread must span more than one history page"
    failures = 0
    for name in reference.keys() | rust.keys():
        expected, actual = reference.get(name), rust.get(name)
        same = expected == actual
        failures += not same
        print(f"{'PASS' if same else 'FAIL'} {name}")
        if not same:
            print(f"  reference {json.dumps(expected)[:2500]}\n  rust      {json.dumps(actual)[:2500]}")
    total = len(reference.keys() | rust.keys())
    print(f"{total - failures}/{total} link history paging checks matched; nothing left loopback")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
