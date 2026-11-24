#!/usr/bin/env python3
"""Compare how the reference and the Rust server window and page a long thread for its owner's
client:

- the bounded snapshot of `orchestration.subscribeThread`, with the web client's options;
- the bounded snapshot of `GET /api/orchestration/threads/:id/bounded`;
- the older pages of `GET /api/orchestration/threads/:id/history`.

Pages are read the way the client reads them: older pages until no history is left, and a jump
to an older entry (`throughEntryId`), first with `view=conversation` and then without a view.

Each server runs on its own fresh home with the same steps. A thread gets 34 user turns, each sent
once the one before it has settled. No provider CLI is on PATH, so each turn is the user's message
and the error its failed run records.

The answers are compared by row (item id, position and type), by decoded cursor with the snapshot
sequence masked, and by the ids of each kind of record the snapshot keeps. The client's compact
id encoding (`compactIds`) is not asked for, because Rust does not compact ids.

All data is fake. Nothing may leave loopback: every server runs behind the harness's loopback
proxy, which forwards nothing (parity_harness.assert_nothing_escaped).
"""
import argparse
from contextlib import ExitStack
import json
from pathlib import Path
import secrets
import shlex
from tempfile import TemporaryDirectory
from urllib.parse import quote, urlencode

from websockets.sync.client import connect

import parity_harness
from parity_harness import encode, http, managed_server, recv_frame
from peer_link_history_parity import THREAD, cursor, rows, seed

RECORDS = ("runs", "attempts", "nodes", "subagents", "providerSessions", "providerThreads", "providerTurns",
           "plans", "checkpoints", "contextHandoffs", "runtimeRequests", "messages")
# What the web client asks of subscribeThread, less compact ids.
CLIENT_OPTIONS = {"requestCompletionMarker": True, "acceptBoundedSnapshot": True, "acceptDeferredWork": True,
                  "acceptCompactTurnItems": True, "acceptDedupedBodies": True}


def snapshot_summary(found):
    projection = found["projection"]
    summary = {"window": rows(projection["visibleTurnItems"]),
               "turn items": [item["id"] for item in projection["turnItems"]],
               "cursor": cursor(found.get("historyCursor")), "more history": found.get("hasMoreHistory"),
               "latest local turn ordinal": found.get("latestLocalTurnOrdinal"),
               "turn items omit local visible": found.get("turnItemsOmitLocalVisible")}
    for key in RECORDS:
        summary[key] = [entry.get("id") for entry in projection.get(key, [])]
    return summary


def subscribe(url, cookie, options):
    """The first snapshot `orchestration.subscribeThread` sends with `options`."""
    with connect(url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2", additional_headers={"Cookie": cookie},
                 proxy=None, compression=None, max_size=None) as ws:
        ws.send(encode({"_tag": "Request", "id": "thread", "tag": "orchestration.subscribeThread",
                        "payload": {"threadId": THREAD, **options}, "headers": []}).decode())
        while True:
            frame = json.loads(recv_frame(ws, timeout=30))
            if frame.get("_tag") == "Chunk":
                ws.send(encode({"_tag": "Ack", "requestId": "thread"}).decode())
                for value in frame["values"]:
                    if value.get("kind") == "snapshot":
                        return value
            elif frame.get("_tag") == "Exit":
                raise AssertionError(f"subscribeThread ended: {frame}")


def owner_get(url, cookie, path, query):
    status, _, body = http(url, "GET", f"/api/orchestration/threads/{quote(THREAD)}/{path}?{urlencode(query)}", None,
                           {"Cookie": cookie, "x-t3-orchestration-protocol": "2"})
    try:
        parsed = json.loads(body)
    except ValueError:
        parsed = body.decode(errors="replace")[:200]
    return status, parsed


def page_summary(status, page):
    if status != 200:
        body = {k: v for k, v in page.items() if k != "traceId"} if isinstance(page, dict) else page
        return {"status": status, "body": body}
    return {"status": status, "items": rows(page["items"]), "nextCursor": cursor(page["nextCursor"]),
            "hasMoreHistory": page["hasMoreHistory"]}


def run(command, home):
    record = {}
    with ExitStack() as stack:
        url, cookie = managed_server(stack, shlex.split(command), home, secrets.token_urlsafe(32),
                                     secrets.token_bytes(32))
        workspace = home.parent / "workspace"
        workspace.mkdir()
        seed(url, cookie, workspace)

        for name, options in (("client options", CLIENT_OPTIONS), ("bounded only", {"acceptBoundedSnapshot": True})):
            for key, value in snapshot_summary(subscribe(url, cookie, options)).items():
                record[f"subscribeThread ({name}): {key}"] = value
        status, bounded = owner_get(url, cookie, "bounded",
                                    {"compactTurnItems": "1", "dedupeBodies": "1", "deferWork": "1"})
        assert status == 200, (status, bounded)
        for key, value in snapshot_summary(bounded).items():
            record[f"/bounded: {key}"] = value

        # Older pages, as the client reads them with deferred work, until no history is left.
        first = bounded["historyCursor"]
        next_cursor, number = first, 0
        while next_cursor is not None:
            number += 1
            status, page = owner_get(url, cookie, "history", {"cursor": next_cursor, "deferWork": "1"})
            record[f"/history page {number}"] = page_summary(status, page)
            assert status == 200, (status, page)
            next_cursor = page["nextCursor"]
            assert number < 10, "paging did not end"
        # A jump to an older entry: the conversation first, then the whole page.
        for target in ("message:02", "message:15", "turn-item:message:message%3A20"):
            for view in ("conversation", None):
                query = {"cursor": first, "throughEntryId": target, **({"view": view} if view else {})}
                record[f"/history through {target} ({view or 'no view'})"] = page_summary(
                    *owner_get(url, cookie, "history", query))
        record["/history: an unreadable cursor"] = page_summary(*owner_get(url, cookie, "history", {"cursor": "x"}))
        record["/history: no cursor"] = owner_get(url, cookie, "history", {})[0]
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
    assert reference["/bounded: more history"] and "/history page 2" in reference, \
        "the fixture thread must span more than one history page"
    failures = 0
    names = list(reference) + [name for name in rust if name not in reference]
    for name in names:
        expected, actual = reference.get(name), rust.get(name)
        same = expected == actual
        failures += not same
        print(f"{'PASS' if same else 'FAIL'} {name}")
        if not same:
            print(f"  reference {json.dumps(expected)[:2500]}\n  rust      {json.dumps(actual)[:2500]}")
    print(f"{len(names) - failures}/{len(names)} owner thread history checks matched; nothing left loopback")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
