#!/usr/bin/env python3
"""Show the reference's client sessions in the Rust server's connected-clients
list, and revoke them from it, as the reference does on the same home.

The reference keeps its client sessions in `auth_sessions` in
`statev2.sqlite`: browsers and the desktop app, outside MCP agents and linked
environments. On a home it ran on, the Rust server lists them in
`subscribeAuthAccess` (Settings → Connections) as the reference would, in
its order, and revoking one there (or every other client) marks it revoked
for both servers, with a `clientRemoved` event to clients watching. All data
is fake: a temporary home, generated keys and fixture labels.
"""
import argparse
from contextlib import ExitStack, contextmanager
import json
from pathlib import Path
import secrets
import shlex
from tempfile import TemporaryDirectory

from websockets.sync.client import connect

from parity_harness import encode, http, managed_server, recv_frame
from peer_forwarding_parity import mcp_token

# Fields of a listed session that only say who is looking: the server's own
# socket and the caller's own session.
OBSERVER_FIELDS = ("connected", "current")


@contextmanager
def running(command, home, secret, key):
    with ExitStack() as stack:
        url, cookie = managed_server(stack, shlex.split(command), home, secret, key)
        yield Server(stack, url, cookie)


class Server:
    def __init__(self, stack, url, cookie):
        self.url, self.cookie, self.stack = url, cookie, stack

    def post(self, path, data, cookie=None):
        status, _, body = http(self.url, "POST", path, encode(data),
                               {"Cookie": cookie or self.cookie, "Content-Type": "application/json"})
        return status, json.loads(body) if body else None

    def pairing_code(self):
        status, body = self.post("/api/auth/pairing-token", {})
        assert status == 200, (status, body)
        return body["credential"]

    def access(self):
        """An owner socket watching `subscribeAuthAccess`, and its first snapshot."""
        socket = self.stack.enter_context(connect(
            self.url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2",
            additional_headers={"Cookie": self.cookie}, compression=None, proxy=None))
        socket.send(encode({"_tag": "Request", "id": "access", "tag": "subscribeAuthAccess",
                            "payload": {}, "headers": []}).decode())
        return socket, next_change(socket)

    def browser(self, label):
        """A second browser signed in with a pairing code, with its own user agent."""
        status, body = self.post("/api/auth/pairing-token", {"label": label})
        assert status == 200, (status, body)
        status, headers, _ = http(self.url, "POST", "/api/auth/browser-session",
                                  encode({"credential": body["credential"]}),
                                  {"Content-Type": "application/json",
                                   "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Fixture/1.0"})
        assert status == 200, status
        return headers["set-cookie"].split(";", 1)[0]


def initialize_status(url, token):
    """The HTTP status an MCP client's first request gets with `token`."""
    status, _, _ = http(url, "POST", "/mcp", encode(dict(jsonrpc="2.0", id=0, method="initialize", params=dict(
        protocolVersion="2025-06-18", capabilities={}, clientInfo=dict(name="Fixture", version="1")))),
        {"authorization": f"Bearer {token}", "content-type": "application/json",
         "accept": "application/json, text/event-stream"})
    return status


def next_change(socket):
    while True:
        frame = json.loads(recv_frame(socket, timeout=10))
        if frame.get("_tag") == "Chunk":
            socket.send(encode({"_tag": "Ack", "requestId": "access"}).decode())
            return frame["values"][0]


def sessions(snapshot, ids):
    """The listed sessions among `ids`, in the order listed, without the observer's fields."""
    return [{k: v for k, v in row.items() if k not in OBSERVER_FIELDS}
            for row in snapshot["payload"]["clientSessions"] if row["sessionId"] in ids]


def reference_sessions(snapshot):
    """The sessions the reference lists, but its own owner session (the observer)."""
    return {row["sessionId"] for row in snapshot["payload"]["clientSessions"] if not row["current"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    reference, rust = args.original_command, args.rust_command
    checks = []

    def same(name, expected, actual):
        assert expected == actual, f"{name}\n  reference: {expected}\n  rust:      {actual}"
        checks.append(name)

    with TemporaryDirectory() as tmp:
        home = Path(tmp) / "home"
        settings = home / "userdata" / "settings.json"
        settings.parent.mkdir(parents=True)
        settings.write_text(json.dumps({"linkedEnvironmentsEnabled": True}))
        secret, key = secrets.token_urlsafe(32), secrets.token_bytes(32)
        # The reference signs in a second browser, an outside agent and a linked environment.
        with running(reference, home, secret, key) as server:
            server.browser("Fixture laptop")
            agent = mcp_token(server, "auto", name="Fixture agent")
            link = mcp_token(server, "full-access", peer_link=True, name="T3 Code · fixture-peer")
        with running(reference, home, secret, key) as server:
            _, listed = server.access()
            ids = reference_sessions(listed)
            observer = {row["sessionId"] for row in listed["payload"]["clientSessions"]} - ids
            expected = sessions(listed, ids)
        assert len(ids) >= 4, listed  # the first run's owner, the browser, the agent and the link
        with running(rust, home, secret, key) as server:
            socket, listed = server.access()
            same("the reference's sessions, listed in its order", expected, sessions(listed, ids))
            # The reference's own session of its last run is listed too, as any other.
            same("the reference's last session too", [False],
                 [row["current"] for row in listed["payload"]["clientSessions"] if row["sessionId"] in observer])
            rust_own = {row["sessionId"] for row in listed["payload"]["clientSessions"]} - ids - observer
            same("this server's own session is the current one",
                 [True], [row["current"] for row in listed["payload"]["clientSessions"] if row["sessionId"] in rust_own])
            # Revoked here: the browser the reference signed in, and the agent.
            browser = next(row["sessionId"] for row in expected if row["client"].get("label") == "Fixture laptop"
                           or "Fixture" in row["client"].get("userAgent", ""))
            agent_id = next(row["sessionId"] for row in expected if row["client"].get("label") == "Fixture agent")
            for revoked in (browser, agent_id):
                status, body = server.post("/api/auth/clients/revoke", {"sessionId": revoked})
                same(f"revoking a reference session answers ({revoked == agent_id and 'agent' or 'browser'})",
                     (200, {"revoked": True}), (status, body))
                same("clients watching hear it removed", {"type": "clientRemoved", "payload": {"sessionId": revoked}},
                     {k: v for k, v in next_change(socket).items() if k in ("type", "payload")})
            _, after = server.access()
            remaining = sessions(after, ids)
        with running(reference, home, secret, key) as server:
            _, listed = server.access()
            same("the reference lists what is left", sessions(listed, ids), remaining)
            code = initialize_status(server.url, agent)
            same("the reference refuses the agent revoked on the Rust server", 401, code)
        # Every other client, revoked on the Rust server: the reference keeps none of them.
        with running(rust, home, secret, key) as server:
            status, body = server.post("/api/auth/clients/revoke-others", {})
            assert status == 200, (status, body)
            _, after = server.access()
            same("none of the reference's sessions listed after revoking the others", [], sessions(after, ids))
        with running(reference, home, secret, key) as server:
            _, listed = server.access()
            same("nor on the reference", [], sessions(listed, ids))
            code = initialize_status(server.url, link)
            same("and it refuses the link", 401, code)
    print(f"{len(checks)}/{len(checks)} client session checks matched across the reference and the Rust server")


if __name__ == "__main__":
    main()
