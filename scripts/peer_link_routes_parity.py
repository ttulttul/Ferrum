#!/usr/bin/env python3
"""Compare what the reference and the Rust server serve to an environment linked to them,
outside `/mcp`, on the descriptor's `linkThreadStream`, `linkClientPairing` and
`linkPairingForLink` capabilities:

- `GET /api/links/thread/stream`, `/history` and `/item`: a thread read over a link, with the
  link's own MCP token, and every refusal (a browser origin, no token, the owner's cookie, a
  missing or unknown thread, the beta off, a revoked link);
- `POST /api/peer-link/client-pairing`: a pairing for a client of the linking environment
  (full-access links only, never more than a pairing code grants, three minutes), a link
  pairing code for a link from another environment, and every refusal.

Each server runs on its own fresh home with the same steps; the answers must match, with
credentials, times and ids reduced to their shape. All data is fake. Nothing may leave loopback:
every server runs behind the harness's loopback proxy, which forwards nothing
(parity_harness.assert_nothing_escaped).
"""
import argparse
import base64
from contextlib import ExitStack
from datetime import datetime
import hashlib
import http.client as http_client
import json
from pathlib import Path
import secrets
import shlex
from tempfile import TemporaryDirectory
from urllib.parse import parse_qs, urlencode, urlsplit

from websockets.sync.client import connect

import parity_harness
from parity_harness import encode, http, managed_server

LINK_REDIRECT = "http://127.0.0.1/t3code-peer-link"
PROJECT, THREAD = "project:link-routes", "thread:link-routes"
CLIENT_PAIRING = "/api/peer-link/client-pairing"


def set_linked_environments(home, enabled):
    path = home / "userdata" / "settings.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    settings = json.loads(path.read_text()) if path.exists() else {}
    settings["linkedEnvironmentsEnabled"] = enabled
    path.write_text(json.dumps(settings))


def post(url, cookie, path, data):
    status, _, body = http(url, "POST", path, encode(data), {"Cookie": cookie, "Content-Type": "application/json"})
    assert status in (200, 201), (path, status, body)
    return json.loads(body)


def link(url, cookie, access, name="fixture-peer"):
    """Signs in as a linked environment does (`PeerLinks.signIn`) at `access`; returns its token."""
    client = post(url, cookie, "/oauth/mcp/register",
                  dict(client_name=f"T3 Code · {name}", redirect_uris=[LINK_REDIRECT],
                       software_id="t3code-peer-link"))["client_id"]
    verifier = secrets.token_urlsafe(48)
    authorization = dict(response_type="code", client_id=client, redirect_uri=LINK_REDIRECT,
                         code_challenge=base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode(),
                         code_challenge_method="S256", state="fixture-state", resource=f"{url}/mcp")
    code = post(url, cookie, "/api/auth/pairing-token", {})["credential"]
    decided = post(url, cookie, "/oauth/mcp/decision",
                   dict(authorization=authorization, decision={"_tag": "pairing-code", "access": access, "code": code}))
    grant = parse_qs(urlsplit(decided["redirectTo"]).query)["code"][0]
    form = urlencode(dict(grant_type="authorization_code", code=grant, redirect_uri=LINK_REDIRECT,
                          client_id=client, code_verifier=verifier, resource=f"{url}/mcp"))
    status, _, body = http(url, "POST", "/oauth/mcp/token", form, {"content-type": "application/x-www-form-urlencoded"})
    assert status == 200, (status, body)
    return json.loads(body)["access_token"]


def session_id(token):
    payload = token.split(".")[0]
    return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))["sid"]


def dispatch(url, cookie, command):
    """One orchestration command over the owner's socket, as the client sends it."""
    with connect(url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2", additional_headers={"Cookie": cookie},
                 proxy=None, compression=None, max_size=None) as ws:
        ws.send(encode({"_tag": "Request", "id": "command", "tag": "orchestration.dispatchCommand", "headers": [],
                        "payload": command}).decode())
        while True:
            frame = json.loads(ws.recv(timeout=20))
            if frame.get("_tag") == "Exit":
                assert frame["exit"]["_tag"] == "Success", frame
                return


def seed(url, cookie, workspace):
    """A project with one thread, made the way the client makes them."""
    post(url, cookie, "/api/projects/mutate", {
        "type": "project.create", "commandId": "seed-project", "projectId": PROJECT,
        "title": "Link routes", "workspaceRoot": str(workspace)})
    dispatch(url, cookie, {
        "type": "thread.create", "commandId": "seed-thread", "threadId": THREAD, "projectId": PROJECT,
        "title": "Linked read", "modelSelection": {"instanceId": "codex", "model": "fixture"},
        "runtimeMode": "full-access", "interactionMode": "default", "branch": None, "worktreePath": None,
        "createdBy": "user", "creationSource": "web"})


def answer(status, headers, body, keep=("content-type", "cache-control", "pragma")):
    """Status, the headers that matter, and the body as JSON when it is JSON."""
    try:
        parsed = json.loads(body) if body else None
    except ValueError:
        parsed = body.decode(errors="replace")[:200]
    kept = {name: headers.get(name) for name in keep}
    if kept["content-type"]:
        kept["content-type"] = kept["content-type"].split(";")[0]
    return {"status": status, "headers": kept, "body": parsed}


def get(url, path, token=None, extra=None):
    headers = {**({"authorization": f"Bearer {token}"} if token else {}), **(extra or {})}
    return answer(*http(url, "GET", path, None, headers))


def read_event(response):
    """The next data event on an open stream; heartbeats are skipped."""
    while True:
        line = response.fp.readline().decode()
        assert line, "the stream ended before its next event"
        if line.startswith("data: "):
            return shape(json.loads(line[len("data: "):]))


def first_event(url, token, then=None):
    """The stream's status and headers, then its first data event, as a linking environment reads
    it; with `then`, also the event that follows doing it."""
    parts = urlsplit(url)
    connection = http_client.HTTPConnection(parts.hostname, parts.port, timeout=20)
    try:
        connection.request("GET", f"/api/links/thread/stream?thread={THREAD}",
                           headers={"authorization": f"Bearer {token}", "accept": "text/event-stream"})
        response = connection.getresponse()
        head = {"status": response.status,
                "headers": {name: response.headers.get(name)
                            for name in ("content-type", "cache-control", "x-accel-buffering")}}
        if response.status != 200:
            return {**head, "body": json.loads(response.read())}
        found = {**head, "first event": read_event(response)}
        if then is not None:
            then()
            found["next event"] = read_event(response)
        return found
    finally:
        connection.close()


def shape(value, key=None):
    """`value` with times and generated ids reduced to their kind; the thread's own ids are kept."""
    if isinstance(value, dict):
        return {k: shape(v, k) for k, v in sorted(value.items())}
    if isinstance(value, list):
        return [shape(v) for v in value]
    if isinstance(value, str) and (key or "").endswith("At"):
        return "<time>"
    if isinstance(value, str) and key == "id" and value.startswith("event:"):
        return "<event id>"
    if isinstance(value, (int, float)) and not isinstance(value, bool) and key in ("sequence", "version"):
        return "<number>"
    return value


def pairing(url, token, payload, origin=None):
    headers = {"content-type": "application/json", **({"authorization": f"Bearer {token}"} if token else {}),
               **({"origin": origin} if origin else {})}
    status, response_headers, body = http(url, "POST", CLIENT_PAIRING, encode(payload), headers)
    result = answer(status, response_headers, body)
    if status == 200:
        issued = result["body"]
        expires = datetime.fromisoformat(issued["expiresAt"].replace("Z", "+00:00")).timestamp()
        result["body"] = {"credential": bool(issued["credential"]) and "<credential>",
                          "expires in (minutes)": round((expires - datetime.now().timestamp()) / 60)}
    elif status == 400:
        result["body"] = "<schema error>"
    return result


def listed_pairings(url, cookie):
    """The pairing codes Settings → Connections lists (`subscribeAuthAccess`), by label and scopes."""
    with connect(url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2", additional_headers={"Cookie": cookie},
                 proxy=None, compression=None, max_size=None) as ws:
        ws.send(encode({"_tag": "Request", "id": "access", "tag": "subscribeAuthAccess",
                        "payload": {}, "headers": []}).decode())
        while True:
            frame = json.loads(ws.recv(timeout=10))
            if frame.get("_tag") == "Chunk":
                rows = frame["values"][0]["payload"]["pairingLinks"]
                return sorted(json.dumps({k: row.get(k) for k in ("label", "scopes", "purpose", "access")},
                                         sort_keys=True) for row in rows)


def run(command, home):
    """The same steps on one server; every answer, by name."""
    record = {}
    secret, key = secrets.token_urlsafe(32), secrets.token_bytes(32)
    set_linked_environments(home, True)
    with ExitStack() as stack:
        url, cookie = managed_server(stack, shlex.split(command), home, secret, key)
        workspace = home.parent / "workspace"
        workspace.mkdir(exist_ok=True)
        seed(url, cookie, workspace)
        auto, full = link(url, cookie, "auto"), link(url, cookie, "full-access")
        descriptor = json.loads(http(url, "GET", "/.well-known/t3/environment")[2])["capabilities"]
        record["descriptor capabilities"] = {k: descriptor.get(k) for k in
                                             ("linkThreadStream", "linkClientPairing", "linkPairingForLink")}

        record["stream: an auto link"] = first_event(url, auto)
        record["stream: a full-access link, then a rename"] = first_event(url, full, lambda: dispatch(url, cookie, {
            "type": "thread.metadata.update", "commandId": "rename", "threadId": THREAD, "title": "Renamed"}))
        stream = f"/api/links/thread/stream?thread={THREAD}"
        record["stream: a browser origin"] = get(url, stream, auto, {"origin": "http://127.0.0.1"})
        record["stream: no token"] = get(url, stream)
        record["stream: a forged token"] = get(url, stream, "e30." + secrets.token_urlsafe(32))
        record["stream: the owner's cookie"] = get(url, stream, extra={"Cookie": cookie})
        record["stream: no thread"] = get(url, "/api/links/thread/stream", auto)
        record["stream: an unknown thread"] = get(url, "/api/links/thread/stream?thread=thread:missing", auto)
        record["history: no cursor"] = get(url, f"/api/links/thread/history?thread={THREAD}", auto)
        record["history: an unknown thread"] = get(url, "/api/links/thread/history?thread=thread:missing&cursor=c", auto)
        record["item: no item"] = get(url, f"/api/links/thread/item?thread={THREAD}", auto)
        record["item: no token"] = get(url, f"/api/links/thread/item?thread={THREAD}&item=i")

        client = {"scopes": ["orchestration:read", "terminal:operate", "review:write", "relay:configure"],
                  "clientLabel": "Fixture browser " + "x" * 90}
        record["pairing: a full-access link"] = pairing(url, full, client)
        record["pairing: an auto link"] = pairing(url, auto, client)
        record["pairing: no scope a pairing carries"] = pairing(url, full, {"scopes": ["relay:configure"],
                                                                            "clientLabel": "Fixture browser"})
        record["pairing: a link pairing code"] = pairing(url, full, {"scopes": [], "clientLabel": "Fixture browser",
                                                                     "link": {"access": "auto"}})
        record["pairing: a link pairing code at full access"] = pairing(
            url, full, {"scopes": [], "clientLabel": "Fixture browser", "link": {"access": "full-access"}})
        record["pairing: a browser origin"] = pairing(url, full, client, origin="http://127.0.0.1")
        record["pairing: no token"] = pairing(url, None, client)
        record["pairing: the owner's cookie"] = answer(*http(url, "POST", CLIENT_PAIRING, encode(client),
                                                             {"content-type": "application/json", "Cookie": cookie}))
        record["pairing: an unknown scope"] = pairing(url, full, {"scopes": ["fixture:unknown"], "clientLabel": "x"})
        record["pairing: an unknown access"] = pairing(url, full, {"scopes": [], "clientLabel": "x",
                                                                   "link": {"access": "everything"}})
        record["pairing: a blank label"] = pairing(url, full, {"scopes": ["orchestration:read"], "clientLabel": " "})
        record["pairings listed in Connections"] = listed_pairings(url, cookie)

        assert post(url, cookie, "/api/auth/clients/revoke", dict(sessionId=session_id(auto))) == {"revoked": True}
        record["stream: a revoked link"] = get(url, stream, auto)
    set_linked_environments(home, False)
    with ExitStack() as stack:
        url, cookie = managed_server(stack, shlex.split(command), home, secret, key)
        record["stream: Linked environments off"] = get(url, stream, full)
        record["item: Linked environments off"] = get(url, f"/api/links/thread/item?thread={THREAD}&item=i", full)
        record["pairing: Linked environments off"] = pairing(url, full, client)
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
    failures = 0
    for name, expected in reference.items():
        actual = rust.get(name)
        same = expected == actual
        failures += not same
        print(f"{'PASS' if same else 'FAIL'} {name}")
        if not same:
            print(f"  reference {json.dumps(expected)[:2500]}\n  rust      {json.dumps(actual)[:2500]}")
    print(f"{len(reference) - failures}/{len(reference)} link route checks matched; nothing left loopback")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
