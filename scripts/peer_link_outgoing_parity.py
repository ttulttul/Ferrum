#!/usr/bin/env python3
"""Link one T3 home to another from the reference and from the Rust server, and
check that each server reads, uses and forgets the links the other made.

Two homes: `local`, which links out, and `peer`, which is linked to. The peer
runs throughout a round (the reference in one round, the Rust server in the
other) and hands out pairing codes. On the local home the reference and the
Rust server take turns: a link made by either is stored in the same
`peer_environment_links` row and `secrets/peer-link-*.bin` token, so the other
lists it, reaches the peer with it and can forget it. Every answer of the
`peerLinks.*` RPCs is compared between the two, with the times and ids that
differ from run to run normalised. All data is fake: temporary homes,
generated keys, loopback addresses and fixture labels.
"""
import argparse
from contextlib import ExitStack, contextmanager
from datetime import datetime
import itertools
import json
from pathlib import Path
import secrets
import shlex
from tempfile import TemporaryDirectory

from websockets.sync.client import connect

from parity_gaps import Gaps
import parity_harness
from parity_harness import encode, free_port, http, managed_server

THIRTY_DAYS_MS = 30 * 86_400_000
_ids = itertools.count()


def set_linked_environments(home, enabled):
    path = home / "userdata" / "settings.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    settings = json.loads(path.read_text()) if path.exists() else {}
    settings["linkedEnvironmentsEnabled"] = enabled
    path.write_text(json.dumps(settings))


class Server:
    """One running server and an owner socket to it."""

    def __init__(self, stack, command, home, secret, key):
        self.url, self.cookie = managed_server(stack, shlex.split(command), home, secret, key)
        self.home = home
        self.socket = stack.enter_context(connect(
            self.url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2",
            additional_headers={"Cookie": self.cookie}, compression=None, proxy=None))
        status, _, body = http(self.url, "GET", "/.well-known/t3/environment")
        assert status == 200, (status, body)
        self.descriptor = json.loads(body)

    def rpc(self, tag, payload, timeout=40):
        request = f"peer-{next(_ids)}"
        self.socket.send(encode(dict(_tag="Request", id=request, tag=tag, payload=payload, headers=[])).decode())
        while True:
            frame = json.loads(self.socket.recv(timeout=timeout))
            if frame.get("_tag") == "Ping":
                self.socket.send(encode({"_tag": "Pong"}).decode())
                continue
            if frame.get("requestId") == request and "exit" in frame:
                return frame["exit"]

    def pairing_code(self):
        status, _, body = http(self.url, "POST", "/api/auth/pairing-token", encode({}),
                               {"Cookie": self.cookie, "Content-Type": "application/json"})
        assert status in (200, 201), (status, body)
        return json.loads(body)["credential"]


@contextmanager
def running(command, home, secret, key):
    with ExitStack() as stack:
        yield Server(stack, command, home, secret, key)


def ms(text):
    return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)


def normal(value, peer):
    """An RPC exit with the run's own ids, addresses and times replaced by what they stand for."""
    text = json.dumps(value)
    text = text.replace(peer.descriptor["environmentId"], "<peer>").replace(peer.url, "<peer-url>")
    value = json.loads(text)

    def link(row):
        row = dict(row)
        linked = ms(row.pop("linkedAt"))
        row["expiresIn"] = "30 days" if ms(row.pop("expiresAt")) - linked == THIRTY_DAYS_MS else "other"
        reached = row.pop("lastReachedAt")
        row["lastReached"] = None if reached is None else ("at link" if ms(reached) >= linked else "before")
        return row

    if value.get("_tag") == "Success" and isinstance(value.get("value"), dict):
        inner = value["value"]
        if "links" in inner:
            value = {**value, "value": {"links": [link(row) for row in inner["links"]]}}
        elif "linkedAt" in inner:
            value = {**value, "value": link(inner)}
    return value


def tokens(home):
    return sorted(path.name for path in (home / "userdata" / "secrets").glob("peer-link-*.bin"))


def link_token(home):
    """The access token the link holds for the peer (never printed)."""
    found = sorted((home / "userdata" / "secrets").glob("peer-link-*.bin"))
    return found[0].read_bytes().decode() if found else None


def peer_accepts(peer_url, token):
    """Whether the peer's /mcp still accepts `token`: the session the link signed in with."""
    status, _, _ = http(peer_url, "POST", "/mcp", encode(dict(
        jsonrpc="2.0", id=0, method="initialize", params=dict(
            protocolVersion="2025-06-18", capabilities={}, clientInfo=dict(name="Fixture", version="1")))),
        {"authorization": f"Bearer {token}", "content-type": "application/json",
         "accept": "application/json, text/event-stream"})
    return status == 200


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--show", action="store_true", help="print each answer as the reference gave it")
    args = parser.parse_args()
    servers = {"reference": args.original_command, "rust": args.rust_command}
    checks = []
    gaps = Gaps("peer_link_outgoing_parity")

    def same(name, expected, actual):
        checks.append(name)
        if expected != actual and gaps.expected(name):
            gaps.differs(name)
            return
        assert expected == actual, f"{name}\n  reference: {expected}\n  rust:      {actual}"

    for peer_kind in ("reference", "rust"):
        with TemporaryDirectory() as tmp, ExitStack() as round_stack:
            local_home, peer_home = Path(tmp) / "local", Path(tmp) / "peer"
            secret, key = secrets.token_urlsafe(32), secrets.token_bytes(32)
            set_linked_environments(local_home, True)
            set_linked_environments(peer_home, True)
            peer = Server(round_stack, servers[peer_kind], peer_home, secret, key)
            where = f"peer on {peer_kind}"
            # What each local server answers, in the same order, keyed by check.
            answers = {}

            def ask(kind, name, local, tag, payload):
                answers.setdefault(name, {})[kind] = normal(local.rpc(tag, payload), peer)

            def link_input(code, **extra):
                return {"url": peer.url, "pairingCode": code, "access": "auto", **extra}

            dead = f"http://127.0.0.1:{free_port()}"
            for kind in ("reference", "rust"):
                # Each local server starts from a home the reference created, so
                # `statev2.sqlite` is there; the reference makes it on first start.
                for path in (local_home / "userdata").glob("*.sqlite*"):
                    path.unlink()
                if kind == "rust":
                    with running(servers["reference"], local_home, secret, key):
                        pass
                with running(servers[kind], local_home, secret, key) as local:
                    ask(kind, "empty list", local, "peerLinks.list", {})
                    ask(kind, "wrong pairing code", local, "peerLinks.link", link_input("NOT-A-CODE"))
                    ask(kind, "nothing listening", local, "peerLinks.link", dict(url=dead, pairingCode="x", access="auto"))
                    ask(kind, "plain http off loopback", local, "peerLinks.link",
                        dict(url="http://peer.example.com", pairingCode="x", access="auto"))
                    ask(kind, "not an address", local, "peerLinks.link", dict(url="ftp://peer", pairingCode="x", access="auto"))
                    ask(kind, "this environment", local, "peerLinks.link",
                        dict(url=local.url, pairingCode=local.pairing_code(), access="auto"))
                    ask(kind, "expected id answered by none", local, "peerLinks.link",
                        link_input(peer.pairing_code(), expectedEnvironmentId="env-elsewhere", expectedLabel="Elsewhere"))
                    ask(kind, "link", local, "peerLinks.link", link_input(peer.pairing_code()))
                    ask(kind, "list after linking", local, "peerLinks.list", {})
                    answers.setdefault("one token", {})[kind] = len(tokens(local_home))
                    ask(kind, "link again replaces it", local, "peerLinks.link",
                        link_input(peer.pairing_code(), access="read-only"))
                    answers.setdefault("old token removed", {})[kind] = len(tokens(local_home))
                made_by = kind
                # The other server on the same home reads, then forgets, the link.
                other = "rust" if kind == "reference" else "reference"
                token = link_token(local_home)
                answers.setdefault("the peer accepts the link's token", {})[made_by] = peer_accepts(peer.url, token)
                with running(servers[other], local_home, secret, key) as local:
                    ask(made_by, "list from the other server", local, "peerLinks.list", {})
                    ask(made_by, "unlink on the other server", local, "peerLinks.unlink",
                        dict(environmentId=peer.descriptor["environmentId"]))
                    answers.setdefault("token removed by the other server", {})[made_by] = tokens(local_home)
                    # #41005: unlinking ends the link's session at the peer.
                    answers.setdefault("the peer refuses the token after unlinking", {})[made_by] = not peer_accepts(peer.url, token)
                    ask(made_by, "unlink again", local, "peerLinks.unlink",
                        dict(environmentId=peer.descriptor["environmentId"]))
                with running(servers[kind], local_home, secret, key) as local:
                    ask(kind, "list after the other server forgot it", local, "peerLinks.list", {})
                # Linked environments off: nothing is linked.
                set_linked_environments(local_home, False)
                with running(servers[kind], local_home, secret, key) as local:
                    ask(kind, "link while Linked environments is off", local, "peerLinks.link", link_input(peer.pairing_code()))
                set_linked_environments(local_home, True)

            for name, both in answers.items():
                if args.show:
                    print(f"{name} ({where}): {json.dumps(both['reference'])}")
                same(f"{name} ({where})", both["reference"], both["rust"])
            linked = answers["link"]["reference"]
            assert linked["_tag"] == "Success" and linked["value"]["environmentId"] == "<peer>", linked
            listed = answers["list from the other server"]["reference"]["value"]["links"]
            assert [row["status"] for row in listed] == ["reachable"], listed
    parity_harness.assert_nothing_escaped()
    if gaps.finish(ran=set(checks)):
        raise SystemExit(1)
    print(f"{len(checks)}/{len(checks)} outgoing peer link checks ran across the reference and the Rust server")


if __name__ == "__main__":
    main()
