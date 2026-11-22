#!/usr/bin/env python3
"""Forward MCP tool calls over environment links from the reference and from
the Rust server, and check that both send and answer the same.

An outside MCP client signed in to the local server names a linked
environment as `environmentId`; the local server calls that environment's
`/mcp` as the link's session (`PeerMcpClient`, `PeerForwarding`). Two kinds
of peer:

* A fake peer on loopback that answers the descriptor, the MCP sign-in and
  `/mcp`, records what each forwarded request carried, and can forget its
  sessions (a restart: 404, then a new session), turn Linked environments off
  (403), revoke the link (401), refuse a call, or stop answering.
* Real peers: the reference and the Rust server on a home of their own. A
  link made by either server on the local home is used by the other, and
  revoking it on either side is refused the same way.

All data is fake: temporary homes, generated keys, loopback addresses,
fixture labels and tokens.
"""
import argparse
import base64
from contextlib import ExitStack, contextmanager
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import secrets
import os
import shlex
import sqlite3
import subprocess
import sys
from tempfile import TemporaryDirectory
import threading
from urllib.parse import parse_qs, urlencode, urlsplit

from parity_harness import encode, http
from peer_link_outgoing_parity import Server, set_linked_environments

LINK_REDIRECT = "http://127.0.0.1/t3code-peer-link"
FAKE_PEER_ID = "env-fixture-peer"
ANSWERS = {
    "t3_thread_list": dict(projectId=None, currentThreadId=None, threads=[], nextCursor=None, total=0),
    "t3_project_list": dict(projects=[], nextCursor=None),
    "t3_thread_wait": dict(threadId="thread-there", runId=None, status="idle", timedOut=False),
    "t3_thread_send": dict(threadId="thread-there", messageId="message-there", runId="run-there",
                           status="queued", delivery="queued"),
    "t3_thread_launch": dict(threadId="thread-new", projectId="p", modelSelection=dict(instanceId="codex", model="fixture-model"),
                             runId=None, status=None),
}


class FakePeer:
    """A T3 environment on loopback that records what it is asked."""

    def __init__(self):
        self.lock = threading.Lock()
        self.reset()
        peer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def reply(self, status, body=None, headers=()):
                data = b"" if body is None else (body if isinstance(body, bytes) else json.dumps(body).encode())
                self.send_response(status)
                for name, value in headers:
                    self.send_header(name, value)
                if body is not None and not any(name == "content-type" for name, _ in headers):
                    self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                if self.path == "/.well-known/t3/environment":
                    if peer.mode == "down":
                        return self.reply(503, {"error": "down"})
                    return self.reply(200, dict(
                        environmentId=FAKE_PEER_ID, label="Fixture peer",
                        platform=dict(os="linux", arch="x64"), serverVersion="0.0.0-fixture",
                        capabilities=dict(peerLinks=True, mcpModeLimitHeader=True, linkFence=True)))
                self.reply(404, {})

            def do_POST(self):
                length = int(self.headers.get("content-length") or 0)
                raw = self.rfile.read(length).decode()
                if self.path == "/oauth/mcp/register":
                    return self.reply(201, dict(client_id="fixture-client", client_name=json.loads(raw)["client_name"],
                                                redirect_uris=[LINK_REDIRECT], grant_types=["authorization_code"],
                                                response_types=["code"], token_endpoint_auth_method="none"))
                if self.path == "/oauth/mcp/decision":
                    state = json.loads(raw)["authorization"]["state"]
                    return self.reply(200, dict(redirectTo=f"{LINK_REDIRECT}?{urlencode(dict(code='fixture-code', state=state))}"))
                if self.path == "/oauth/mcp/token":
                    return self.reply(200, dict(access_token="fixture-link-token", token_type="Bearer",
                                                expires_in=2592000, scope="orchestration:read orchestration:operate"))
                if self.path == "/mcp":
                    return self.mcp(json.loads(raw))
                self.reply(404, {})

            def mcp(self, body):
                session = self.headers.get("mcp-session-id")
                with peer.lock:
                    peer.log.append(dict(
                        method=body.get("method"), session=session,
                        token=self.headers.get("authorization"),
                        modeLimit=self.headers.get("t3-mode-limit"),
                        protocol=self.headers.get("mcp-protocol-version"),
                        sender=self.headers.get("t3-sender-thread"),
                        params=body.get("params")))
                    mode = peer.mode
                if mode == "off":
                    return self.reply(403, {"error": "linked_environments_off"})
                if mode == "revoked":
                    return self.reply(401, {"error": "invalid_mcp_credential"})
                if body.get("method") == "initialize":
                    with peer.lock:
                        peer.counter += 1
                        session = f"fixture-session-{peer.counter}"
                        peer.sessions.add(session)
                    return self.reply(200, dict(jsonrpc="2.0", id=body["id"], result=dict(
                        protocolVersion="2025-06-18", capabilities={}, serverInfo=dict(name="T3 Code", version="0"))),
                        [("mcp-session-id", session)])
                if session not in peer.sessions:
                    return self.reply(404)
                if body.get("method", "").startswith("notifications/"):
                    return self.reply(202)
                if mode == "http-500":
                    return self.reply(500, {})
                if mode == "rpc-error":
                    return self.reply(200, dict(jsonrpc="2.0", id=body["id"], error=dict(code=-32602, message="Invalid tool arguments")))
                if mode == "refuse":
                    text = json.dumps(dict(_tag="OrchestratorMcpFailure", code="thread_not_found", message="The thread was not found."))
                    return self.reply(200, dict(jsonrpc="2.0", id=body["id"], result=dict(content=[dict(type="text", text=text)], isError=True)))
                if mode == "garbled":
                    return self.reply(200, dict(jsonrpc="2.0", id=body["id"], result=dict(content=[dict(type="text", text="not json")], isError=False)))
                # What each tool answers there, in the shape the reference decodes; other tools echo.
                answer = ANSWERS.get(body["params"]["name"]) or dict(
                    answeredBy="fixture peer", tool=body["params"]["name"], arguments=body["params"]["arguments"])
                payload = dict(jsonrpc="2.0", id=body["id"], result=dict(
                    content=[dict(type="text", text=json.dumps(answer))], structuredContent=answer, isError=False))
                if mode == "sse":
                    return self.reply(200, f"event: message\ndata: {json.dumps(payload)}\n\n".encode(),
                                      [("content-type", "text/event-stream")])
                self.reply(200, payload)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def reset(self):
        with self.lock:
            self.mode, self.sessions, self.counter, self.log = "echo", set(), 0, []

    def forget_sessions(self):
        with self.lock:
            self.sessions.clear()

    def take_log(self):
        with self.lock:
            log, self.log = self.log, []
        return log

    def close(self):
        self.server.shutdown()


def post(url, cookie, path, data):
    status, _, body = http(url, "POST", path, encode(data), {"Cookie": cookie, "Content-Type": "application/json"})
    assert status in (200, 201), (path, status, body)
    return json.loads(body)


def mcp_token(server, access, peer_link=False, name="Fixture agent"):
    """Signs an outside agent (or, with `peer_link`, another environment) in to `server`'s /mcp."""
    url, cookie = server.url, server.cookie
    redirect = LINK_REDIRECT if peer_link else "http://127.0.0.1/fixture-agent/callback"
    registration = dict(client_name=name, redirect_uris=[redirect])
    if peer_link:
        registration["software_id"] = "t3code-peer-link"
    client = post(url, cookie, "/oauth/mcp/register", registration)["client_id"]
    verifier = secrets.token_urlsafe(48)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    authorization = dict(response_type="code", client_id=client, redirect_uri=redirect, code_challenge=challenge,
                         code_challenge_method="S256", state="fixture-state", resource=f"{url}/mcp")
    code = server.pairing_code()
    decided = post(url, cookie, "/oauth/mcp/decision",
                   dict(authorization=authorization, decision={"_tag": "pairing-code", "access": access, "code": code}))
    grant = parse_qs(urlsplit(decided["redirectTo"]).query)["code"][0]
    form = urlencode(dict(grant_type="authorization_code", code=grant, redirect_uri=redirect, client_id=client,
                          code_verifier=verifier, resource=f"{url}/mcp"))
    status, _, body = http(url, "POST", "/oauth/mcp/token", form, {"content-type": "application/x-www-form-urlencoded"})
    assert status == 200, (status, body)
    return json.loads(body)["access_token"]


class McpClient:
    """An outside agent's MCP session with one server."""

    def __init__(self, url, token, mode_limit=None):
        self.url, self.token, self.mode_limit = url, token, mode_limit
        status, headers, body = self.post(dict(jsonrpc="2.0", id=0, method="initialize", params=dict(
            protocolVersion="2025-06-18", capabilities={}, clientInfo=dict(name="Fixture agent", version="1"))))
        assert status == 200, (status, body)
        self.session = headers["mcp-session-id"]
        self.post(dict(jsonrpc="2.0", method="notifications/initialized"))

    def post(self, body):
        headers = {"authorization": f"Bearer {self.token}", "content-type": "application/json",
                   "accept": "application/json, text/event-stream"}
        if getattr(self, "session", None):
            headers["mcp-session-id"] = self.session
            headers["mcp-protocol-version"] = "2025-06-18"
        if self.mode_limit:
            headers["t3-mode-limit"] = self.mode_limit
        return http(self.url, "POST", "/mcp", encode(body), headers)

    def call(self, tool, arguments):
        """The tool call's answer: its result as the agent reads it, or the JSON-RPC error."""
        status, _, body = self.post(dict(jsonrpc="2.0", id=1, method="tools/call", params=dict(name=tool, arguments=arguments)))
        if status != 200:
            return dict(status=status)
        answer = json.loads(body)
        if "error" in answer:
            return dict(error=answer["error"])
        result = answer["result"]
        text = "".join(part.get("text", "") for part in result["content"])
        return dict(isError=result.get("isError", False), text=json.loads(text) if text[:1] in "{[" else text)


def link_session(home):
    """The peer session id of the one link stored on `home`, from its token."""
    token = next((home / "userdata" / "secrets").glob("peer-link-*.bin")).read_text()
    payload = token.split(".")[0]
    return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))["sid"]


def stored_links(home):
    with sqlite3.connect(home / "userdata" / "statev2.sqlite") as db:
        return db.execute("SELECT environment_id, last_error IS NOT NULL FROM peer_environment_links ORDER BY 1").fetchall()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--show", action="store_true", help="print each answer as the reference gave it")
    parser.add_argument("--reference-root", type=Path,
                        help="checkout of the pinned reference with its client runtime installed: also link to "
                             "each server with the reference's own sign-in and MCP client, whose schemas decode "
                             "every answer (scripts/peer_link_sign_in.mjs)")
    parser.add_argument("--node", default="node")
    args = parser.parse_args()
    servers = {"reference": args.original_command, "rust": args.rust_command}
    checks = []
    answers = {}

    def same(name, both):
        if args.show:
            print(f"{name}: {json.dumps(both['reference'])[:600]}")
        assert both["reference"] == both["rust"], f"{name}\n  reference: {both['reference']}\n  rust:      {both['rust']}"
        checks.append(name)

    @contextmanager
    def running(kind, home, secret, key):
        with ExitStack() as stack:
            yield Server(stack, servers[kind], home, secret, key)

    # --- Against the fake peer ---------------------------------------------------
    with TemporaryDirectory() as tmp, ExitStack() as outer:
        fake = FakePeer()
        outer.callback(fake.close)
        home = Path(tmp) / "local"
        secret, key = secrets.token_urlsafe(32), secrets.token_bytes(32)
        set_linked_environments(home, True)
        # The reference makes the home, the link and the agents' sessions; both servers use them.
        with running("reference", home, secret, key) as local:
            linked = local.rpc("peerLinks.link", dict(url=fake.url, pairingCode="fixture-code", access="auto"))
            assert linked["_tag"] == "Success", linked
            full = mcp_token(local, "full-access")
            read_only = mcp_token(local, "read-only", name="Fixture reader")
            approval = mcp_token(local, "approval-required", name="Fixture careful agent")
            fenced = mcp_token(local, "auto", peer_link=True, name="T3 Code · fixture-peer")
        fake.take_log()
        target = dict(environmentId=FAKE_PEER_ID)

        for kind in ("reference", "rust"):
            def record(name, value):
                answers.setdefault(name, {})[kind] = value

            fake.reset()
            with running(kind, home, secret, key) as local:
                agent = McpClient(local.url, full)
                record("read forwarded", agent.call("t3_environment_read", target))
                record("first call opens a session", fake.take_log())
                record("wait bounded", agent.call("t3_thread_wait", dict(
                    target, threadId="thread-there", timeoutMs=600000)))
                record("unknown argument refused here", agent.call("t3_thread_wait", dict(
                    target, threadId="thread-there", clientRequestId="retry-1")))
                record("send carries a scoped retry key", agent.call("t3_thread_send", dict(
                    target, threadId="thread-there", message="hello", clientRequestId="retry-1")))
                record("session reused", fake.take_log())
                fake.forget_sessions()
                record("stale session renewed", agent.call("t3_thread_list", target))
                record("404 then initialize then the call again", fake.take_log())
                fake.mode = "sse"
                record("event-stream answer", agent.call("t3_project_list", target))
                fake.mode = "refuse"
                record("refusal keeps the peer's code", agent.call("t3_thread_read", dict(target, threadId="gone")))
                fake.mode = "rpc-error"
                record("peer's JSON-RPC error", agent.call("t3_thread_read", dict(target, threadId="x")))
                fake.mode = "garbled"
                record("answer of another version", agent.call("t3_thread_read", dict(target, threadId="x")))
                fake.mode = "http-500"
                record("peer HTTP error", agent.call("t3_thread_read", dict(target, threadId="x")))
                fake.mode = "echo"
                fake.take_log()
                record("access cap", agent.call("t3_thread_launch", dict(
                    target, projectId="p", title="t", message="m", runtimeMode="full-access")))
                record("within the cap goes", agent.call("t3_thread_launch", dict(
                    target, projectId="p", title="t", message="m", runtimeMode="auto")))
                record("attachments stay here", agent.call("t3_thread_launch", dict(
                    target, projectId="p", title="t", message="m", attachments=[dict(type="file", id="upload-1", name="notes.txt", mimeType="text/plain", sizeBytes=5)])))
                record("launch arguments sent", [entry["params"] for entry in fake.take_log() if entry["method"] == "tools/call"])
                narrowed = McpClient(local.url, full, mode_limit="auto/plan")
                narrowed.call("t3_environment_read", target)
                record("caller's own limits travel", [e["modeLimit"] for e in fake.take_log()])
                careful = McpClient(local.url, approval)
                record("caller's mode cap", careful.call("t3_thread_launch", dict(
                    target, projectId="p", title="t", message="m", runtimeMode="auto")))
                record("project create needs full access", careful.call("t3_project_create", dict(target, title="x")))
                reader = McpClient(local.url, read_only)
                record("read-only caller may read there", reader.call("t3_thread_list", target))
                record("read-only caller may not send there", reader.call("t3_thread_send", dict(
                    target, threadId="t", message="m")))
                linked_caller = McpClient(local.url, fenced)
                record("linked caller below full access", linked_caller.call("t3_thread_list", target))
                record("unknown environment", agent.call("t3_thread_list", dict(environmentId="env-unknown")))
                record("this environment's own id runs here",
                       agent.call("t3_environment_read", dict(environmentId=local.descriptor["environmentId"]))["isError"])
                record("not an id", agent.call("t3_thread_list", dict(environmentId=7)))
                fake.take_log()
                fake.forget_sessions()
                fake.mode = "off"
                record("Linked environments off there", agent.call("t3_thread_list", target))
                fake.mode = "revoked"
                record("revoked there", agent.call("t3_thread_list", target))
                listed = local.rpc("peerLinks.list", {})
                record("listing shows the last error", [(row["lastError"], row["status"]) for row in listed["value"]["links"]])
                fake.mode = "down"
                record("peer not answering", agent.call("t3_thread_list", target))
                fake.mode = "echo"
                record("answers again", agent.call("t3_thread_list", target)["isError"])
                record("listing clears the error", stored_links(home))
                fake.take_log()
            # The link expired: refused here, before anything reaches the peer.
            with sqlite3.connect(home / "userdata" / "statev2.sqlite") as db:
                real_expiry = db.execute("SELECT expires_at FROM peer_environment_links").fetchone()[0]
                db.execute("UPDATE peer_environment_links SET expires_at = '2020-01-01T00:00:00.000Z'")
            with running(kind, home, secret, key) as local:
                record("expired link", McpClient(local.url, full).call("t3_thread_list", target))
            with sqlite3.connect(home / "userdata" / "statev2.sqlite") as db:
                db.execute("UPDATE peer_environment_links SET expires_at = ?", (real_expiry,))
            set_linked_environments(home, False)
            with running(kind, home, secret, key) as local:
                record("Linked environments off here", McpClient(local.url, full).call("t3_thread_list", target))
                record("off here: nothing reached the peer", fake.take_log())
            set_linked_environments(home, True)
        for name, both in answers.items():
            same(f"{name} (fake peer)", both)

    # --- Against real peers ------------------------------------------------------------
    for peer_kind in ("reference", "rust"):
        with TemporaryDirectory() as tmp, ExitStack() as outer:
            local_home, peer_home = Path(tmp) / "local", Path(tmp) / "peer"
            secret, key = secrets.token_urlsafe(32), secrets.token_bytes(32)
            set_linked_environments(local_home, True)
            set_linked_environments(peer_home, True)
            # The peer's home is the reference's, so either server issues the
            # link's session into its auth_sessions and either revokes it.
            with running("reference", peer_home, secret, key):
                pass
            peer = Server(outer, servers[peer_kind], peer_home, secret, key)
            target = dict(environmentId=peer.descriptor["environmentId"])
            real = {}
            with running("reference", local_home, secret, key) as local:
                agent_token = mcp_token(local, "full-access")
            for maker in ("reference", "rust"):
                user = "rust" if maker == "reference" else "reference"
                with running(maker, local_home, secret, key) as local:
                    made = local.rpc("peerLinks.link", dict(url=peer.url, pairingCode=peer.pairing_code(), access="auto"))
                    assert made["_tag"] == "Success", made
                with running(user, local_home, secret, key) as local:
                    agent = McpClient(local.url, agent_token)
                    there = agent.call("t3_environment_read", target)
                    real.setdefault("environment read there", {})[maker] = (
                        there["isError"], there["text"]["environmentId"] == peer.descriptor["environmentId"])
                    real.setdefault("projects there", {})[maker] = agent.call("t3_project_list", target)
                # Revoked on the peer, as its Settings → Connections does.
                assert post(peer.url, peer.cookie, "/api/auth/clients/revoke", dict(sessionId=link_session(local_home))) == {"revoked": True}
                with running(maker, local_home, secret, key) as local:
                    real.setdefault("revoked on the peer", {})[maker] = McpClient(local.url, agent_token).call("t3_thread_list", target)
                # Forgotten here (Settings → Linked environments): refused before anything goes out.
                with running(user, local_home, secret, key) as local:
                    local.rpc("peerLinks.unlink", target)
                    real.setdefault("forgotten here", {})[maker] = McpClient(local.url, agent_token).call(
                        "t3_thread_list", target)["text"]["code"]
            for name, both in real.items():
                normal = {("reference" if k == "reference" else "rust"): v for k, v in both.items()}
                same(f"{name} (link made on one server, used on the other; peer on {peer_kind})", normal)

    if args.reference_root is not None:
        decoded = reference_client_steps(args, servers)
        for index, (expected, actual) in enumerate(zip(decoded["reference"], decoded["rust"])):
            name = expected.get("name")
            assert expected.get("ok"), f"the reference's own client failed on the reference: {expected}"
            same(f"reference client: {name}", {"reference": expected, "rust": actual})
        assert len(decoded["reference"]) == len(decoded["rust"]), decoded
    print(f"{len(checks)}/{len(checks)} link forwarding checks matched across the reference and the Rust server")


def reference_client_steps(args, servers):
    """Each server's answers to the reference's own "link another machine" client, which decodes
    every one with the reference's schemas: its sign-in, its MCP handshake, and tool calls made
    as a linked environment (a client, not a thread), launching and reading a thread in a project.
    Linked with full access, which registering the project needs."""
    steps = {}
    for kind, command in servers.items():
        with TemporaryDirectory() as tmp, ExitStack() as stack:
            home = Path(tmp) / "home"
            set_linked_environments(home, True)
            secret, key = secrets.token_urlsafe(32), secrets.token_bytes(32)
            server = Server(stack, command, home, secret, key)
            project = Path(tmp) / "project"
            project.mkdir()
            status, _, body = http(server.url, "POST", "/api/auth/pairing-token",
                                   encode({"purpose": "peer-link", "access": "full-access"}),
                                   {"Cookie": server.cookie, "Content-Type": "application/json"})
            assert status == 200, (status, body)
            env = {**os.environ, "T3_PEER_LINK_CODE": json.loads(body)["credential"], "T3_PEER_LINK_ACCESS": "full-access",
                   "T3_PEER_LINK_ROOT": str(project)}
            # parity-env: the Node sign-in driver, a client of the server, not a T3 server
            done = subprocess.run([args.node, str(Path(__file__).with_name("peer_link_sign_in.mjs")),
                                   str(args.reference_root), server.url],
                                  env=env, capture_output=True, text=True, timeout=180)
            if done.stderr:
                print(done.stderr[-4000:], file=sys.stderr)
            steps[kind] = json.loads(done.stdout.strip().splitlines()[-1])["steps"]
    return steps


if __name__ == "__main__":
    main()
