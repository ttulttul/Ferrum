#!/usr/bin/env python3
"""Read a linked environment's thread and ask it for pairings through the reference and through
the Rust server, and check that a client of either hears the same.

The linking side of environment links (`PeerThreadRelay` and `PeerClientPairing` in the
reference): a client of the local server calls `peerLinks.names`, `peerLinks.subscribeThread`,
`peerLinks.threadHistoryPage`, `peerLinks.threadItem`, `peerLinks.pairClient` and
`peerLinks.pairLink`, and the local server asks the peer over its link. The peer is a real server
on its own home: the reference in one round and the Rust server in the other. In each round the
reference and then the Rust server run on the local home, which starts fresh for each, links to
the peer and takes the same steps.

The round also checks the peer's `t3-link-expires-at` header on `/mcp` and the link routes. A
link's session renews when it is used and is a day or more short of thirty days. The linking side
records the new expiry, so `peerLinks.list` shows it.

All data is fake: temporary homes, generated keys, loopback addresses and fixture labels.
Nothing may leave loopback: every server runs behind the harness's loopback proxy, which forwards
nothing (parity_harness.assert_nothing_escaped).
"""
import argparse
import base64
from contextlib import ExitStack
from datetime import datetime, timezone
import json
from pathlib import Path
import secrets
import sqlite3
from tempfile import TemporaryDirectory

from websockets.sync.client import connect

import parity_harness
from parity_harness import encode, http
from peer_link_outgoing_parity import Server, running, set_linked_environments
from peer_link_routes_parity import PROJECT, THREAD, dispatch, seed, shape

THIRTY_DAYS_MS = 30 * 86_400_000


def now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def ms(text):
    return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)


def stream(server, payload, then=None, items=1):
    """The first `items` chunks of `peerLinks.subscribeThread`, or how it ended; with `then`,
    also the item after doing it."""
    with connect(server.url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2",
                 additional_headers={"Cookie": server.cookie}, compression=None, proxy=None,
                 max_size=None) as ws:
        ws.send(encode(dict(_tag="Request", id="relay", tag="peerLinks.subscribeThread", payload=payload,
                            headers=[])).decode())
        found = []
        while True:
            frame = json.loads(ws.recv(timeout=40))
            if frame.get("_tag") == "Ping":
                ws.send(encode({"_tag": "Pong"}).decode())
            elif frame.get("_tag") == "Chunk":
                ws.send(encode({"_tag": "Ack", "requestId": "relay"}).decode())
                found.extend(shape(value) for value in frame["values"])
                if len(found) >= items and then is not None:
                    then()
                    then, items = None, items + 1
                elif len(found) >= items:
                    ws.send(encode({"_tag": "Interrupt", "requestId": "relay"}).decode())
                    return {"items": found}
            elif frame.get("_tag") == "Exit":
                return {"items": found, "exit": frame["exit"]}


def normal(value, peer, local_label=None, thread=None):
    """An answer with the run's own ids, addresses, credentials and times replaced by what they
    stand for."""
    text = json.dumps(value).replace(peer.descriptor["environmentId"], "<peer>").replace(peer.url, "<peer-url>")
    if thread:
        text = text.replace(thread, "<thread>")
    if local_label:
        text = text.replace(local_label, "<local>")
    value = json.loads(text)

    def walk(node, key=None):
        if isinstance(node, dict):
            return {k: walk(v, k) for k, v in node.items()}
        if isinstance(node, list):
            return [walk(v) for v in node]
        if key == "credential" and isinstance(node, str):
            return "<credential>"
        if key == "expiresAt" and isinstance(node, str):
            return f"in {round((ms(node) - now_ms()) / 60000)} minutes"
        if key == "snapshotSequence" and isinstance(node, int):
            return "<number>"
        return node
    return walk(value)


def session_id(token):
    payload = token.split(".")[0]
    return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))["sid"]


def peer_session_ends(peer_home, token):
    """When the peer's session for `token` now ends, in days (from its own store, read-only)."""
    with sqlite3.connect(f"file:{peer_home / 'userdata' / 'statev2.sqlite'}?mode=ro", uri=True) as db:
        (ends,) = db.execute("SELECT expires_at FROM auth_sessions WHERE session_id = ?",
                             (session_id(token),)).fetchone()
    return round((ms(ends) - now_ms()) / 86_400_000)


def shorten_link(peer_home, days):
    """Moves every link session on the peer to end in `days` days, as if made long ago."""
    ends = datetime.fromtimestamp((now_ms() + days * 86_400_000) / 1000, timezone.utc)
    with sqlite3.connect(peer_home / "userdata" / "statev2.sqlite") as db:
        db.execute("UPDATE auth_sessions SET expires_at = ? WHERE subject = 'mcp-client'",
                   (ends.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ends.microsecond // 1000:03d}Z",))


def link_token(home):
    found = sorted((home / "userdata" / "secrets").glob("peer-link-*.bin"))
    return found[0].read_bytes().decode()


def renewal_header(url, token, path="/mcp"):
    """When the peer says the link now ends, in days from now, as `path` answers it."""
    if path == "/mcp":
        status, headers, _ = http(url, "POST", "/mcp", encode(dict(
            jsonrpc="2.0", id=0, method="initialize", params=dict(
                protocolVersion="2025-06-18", capabilities={}, clientInfo=dict(name="Fixture", version="1")))),
            {"authorization": f"Bearer {token}", "content-type": "application/json",
             "accept": "application/json, text/event-stream"})
    else:
        status, headers, _ = http(url, "GET", path, None, {"authorization": f"Bearer {token}"})
    found = headers.get("t3-link-expires-at")
    return status, None if found is None else round((ms(found) - now_ms()) / 86_400_000)


def stored_expiry(local):
    links = local.rpc("peerLinks.list", {})["value"]["links"]
    return [round((ms(link["expiresAt"]) - now_ms()) / 86_400_000) for link in links]


def steps(local, peer, peer_home, thread_id):
    """The same steps on one local server, reading `thread_id` on the peer; every answer, by name."""
    target = dict(environmentId=peer.descriptor["environmentId"])
    thread = dict(target, threadId=thread_id)
    label = local.descriptor["label"]
    record = {}

    def norm(value):
        return normal(value, peer, label, thread_id)

    # An auto link: it reads, but may not pair a client.
    made = local.rpc("peerLinks.link", dict(url=peer.url, pairingCode=peer.pairing_code(), access="auto"))
    assert made["_tag"] == "Success", made
    record["names"] = norm(local.rpc("peerLinks.names", {}))
    record["stream: snapshot, then a rename there"] = norm(stream(local, thread, lambda: dispatch(
        peer.url, peer.cookie, {"type": "thread.metadata.update", "commandId": f"rename-{secrets.token_hex(4)}",
                                "threadId": thread_id, "title": "Renamed there"})))
    record["stream: a thread not there"] = norm(stream(local, dict(target, threadId="thread:missing")))
    record["stream: an environment not linked"] = norm(stream(local, dict(environmentId="environment-elsewhere",
                                                                          threadId=THREAD)))
    record["item: not there"] = norm(local.rpc("peerLinks.threadItem", dict(thread, itemId="item:missing")))
    record["history: a cursor the peer never gave"] = norm(local.rpc("peerLinks.threadHistoryPage",
                                                                     dict(thread, cursor="fixture-cursor")))
    record["pair a client over an auto link"] = norm(local.rpc("peerLinks.pairClient", target))
    record["pair a link over an auto link"] = norm(local.rpc("peerLinks.pairLink", dict(target, access="auto")))
    record["pair: an environment not linked"] = norm(local.rpc("peerLinks.pairClient",
                                                               dict(environmentId="environment-elsewhere")))

    # Relinked at full access: it may pair the client, and mint a link code there.
    made = local.rpc("peerLinks.link", dict(url=peer.url, pairingCode=peer.pairing_code(), access="full-access"))
    assert made["_tag"] == "Success", made
    record["pair a client over a full-access link"] = norm(local.rpc("peerLinks.pairClient", target))
    record["pair a link over a full-access link"] = norm(local.rpc("peerLinks.pairLink",
                                                                   dict(target, access="read-only")))

    # Renewal: a link session a day or more short of thirty days is renewed by its next use.
    token = link_token(local.home)
    record["/mcp: renewal header, fresh link"] = renewal_header(peer.url, token)
    shorten_link(peer_home, 10)
    record["link route: renewal header, ten days left"] = renewal_header(
        peer.url, token, f"/api/links/thread/item?thread={THREAD}&item=item:missing")
    shorten_link(peer_home, 10)
    record["/mcp: renewal header, ten days left"] = renewal_header(peer.url, token)
    record["peer: link session ends in days"] = peer_session_ends(peer_home, token)
    # The linking side records what a read through it hears.
    shorten_link(peer_home, 10)
    local_db = local.home / "userdata" / "statev2.sqlite"
    with sqlite3.connect(local_db) as db:
        ends = datetime.fromtimestamp((now_ms() + 10 * 86_400_000) / 1000, timezone.utc)
        db.execute("UPDATE peer_environment_links SET expires_at = ?",
                   (ends.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ends.microsecond // 1000:03d}Z",))
    record["stored link expiry before a read, in days"] = stored_expiry(local)
    record["item through the link after a renewal"] = norm(local.rpc("peerLinks.threadItem",
                                                                     dict(thread, itemId="item:missing")))
    record["stored link expiry after a read, in days"] = stored_expiry(local)

    # Revoked on the peer: the read is refused there, and the client hears why.
    status, _, body = http(peer.url, "POST", "/api/auth/clients/revoke", encode(dict(sessionId=session_id(token))),
                           {"Cookie": peer.cookie, "Content-Type": "application/json"})
    assert status == 200 and json.loads(body) == {"revoked": True}, (status, body)
    record["stream: revoked there"] = norm(stream(local, thread))
    record["pair a client: revoked there"] = norm(local.rpc("peerLinks.pairClient", target))
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    servers = {"reference": args.original_command, "rust": args.rust_command}
    checks = []
    for peer_kind in ("reference", "rust"):
        with TemporaryDirectory() as tmp, ExitStack() as outer:
            peer_home = Path(tmp) / "peer"
            secret, key = secrets.token_urlsafe(32), secrets.token_bytes(32)
            set_linked_environments(peer_home, True)
            # The peer's home is the reference's, so its link sessions live in auth_sessions.
            with running(servers["reference"], peer_home, secret, key):
                pass
            peer = Server(outer, servers[peer_kind], peer_home, secret, key)
            workspace = Path(tmp) / "workspace"
            workspace.mkdir()
            seed(peer.url, peer.cookie, workspace)
            answers = {}
            for local_kind in ("reference", "rust"):
                # A thread of its own on the peer for each local server, so neither reads the other's rename.
                thread_id = f"thread:relay-{local_kind}"
                dispatch(peer.url, peer.cookie, {
                    "type": "thread.create", "commandId": f"create-{local_kind}", "threadId": thread_id,
                    "projectId": PROJECT, "title": "Linked read", "modelSelection": {"instanceId": "codex", "model": "fixture"},
                    "runtimeMode": "full-access", "interactionMode": "default", "branch": None, "worktreePath": None,
                    "createdBy": "user", "creationSource": "web"})
                local_home = Path(tmp) / f"local-{local_kind}"
                set_linked_environments(local_home, True)
                with running(servers[local_kind], local_home, secret, key) as local:
                    answers[local_kind] = steps(local, peer, peer_home, thread_id)
            # Linked environments turned off here: nothing is asked of the peer.
            for local_kind in ("reference", "rust"):
                local_home = Path(tmp) / f"local-{local_kind}"
                set_linked_environments(local_home, False)
                with running(servers[local_kind], local_home, secret, key) as local:
                    target = dict(environmentId=peer.descriptor["environmentId"])
                    answers[local_kind]["off here: stream"] = normal(stream(local, dict(target, threadId=THREAD)), peer)
                    answers[local_kind]["off here: pair a client"] = normal(
                        local.rpc("peerLinks.pairClient", target), peer)
            for name, expected in answers["reference"].items():
                checks.append((f"{name} (peer on {peer_kind})", expected, answers["rust"].get(name)))
    # Every server ran behind a loopback proxy that forwards nothing; what it refused never left.
    parity_harness.assert_nothing_escaped()
    failures = 0
    for name, left, right in checks:
        same = left == right
        failures += not same
        print(f"{'PASS' if same else 'FAIL'} {name}")
        if not same:
            print(f"  reference {json.dumps(left)[:2500]}\n  rust      {json.dumps(right)[:2500]}")
    print(f"{len(checks) - failures}/{len(checks)} link relay checks matched; nothing left loopback")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
