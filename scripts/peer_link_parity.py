#!/usr/bin/env python3
"""Run the reference and the Rust server in turn on one T3 home and check that
each accepts the environment links the other issued.

Another environment links to this one by signing in to `/mcp` as a peer link
(`software_id` `t3code-peer-link`) with a pairing code; it then calls `/mcp`
with the token it got. A home moved from one server to the other must keep
those links, a link revoked on either must be refused by both with the same
answer, and `/.well-known/t3/environment` must answer the same. All data is
fake: a temporary home, generated keys and fixture labels.
"""
import argparse
import base64
from contextlib import ExitStack, contextmanager
import hashlib
import json
from pathlib import Path
import secrets
import shlex
from tempfile import TemporaryDirectory
from urllib.parse import parse_qs, urlencode, urlsplit

from parity_gaps import Gaps
from parity_harness import encode, http, managed_server

LINK_REDIRECT = "http://127.0.0.1/t3code-peer-link"
INITIALIZE = dict(jsonrpc="2.0", id=1, method="initialize",
                  params=dict(protocolVersion="2025-06-18", capabilities={},
                              clientInfo=dict(name="Fixture peer", version="1")))


@contextmanager
def running(command, home, secret, key):
    with ExitStack() as stack:
        yield managed_server(stack, shlex.split(command), home, secret, key)


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


def link(url, cookie):
    """Signs in as a linked environment does (`PeerLinks.signIn`) and returns its token."""
    client = post(url, cookie, "/oauth/mcp/register",
                  dict(client_name="T3 Code · fixture-peer", redirect_uris=[LINK_REDIRECT],
                       software_id="t3code-peer-link"))["client_id"]
    verifier = secrets.token_urlsafe(48)
    authorization = dict(response_type="code", client_id=client, redirect_uri=LINK_REDIRECT,
                         code_challenge=base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode(),
                         code_challenge_method="S256", state="fixture-state", resource=f"{url}/mcp")
    code = post(url, cookie, "/api/auth/pairing-token", {})["credential"]
    decided = post(url, cookie, "/oauth/mcp/decision",
                   dict(authorization=authorization, decision={"_tag": "pairing-code", "access": "auto", "code": code}))
    grant = parse_qs(urlsplit(decided["redirectTo"]).query)["code"][0]
    form = urlencode(dict(grant_type="authorization_code", code=grant, redirect_uri=LINK_REDIRECT,
                          client_id=client, code_verifier=verifier, resource=f"{url}/mcp"))
    status, _, body = http(url, "POST", "/oauth/mcp/token", form, {"content-type": "application/x-www-form-urlencoded"})
    assert status == 200, (status, body)
    issued = json.loads(body)
    return issued["access_token"], (issued["token_type"], issued["scope"], issued["expires_in"] > 29 * 86400)


def session_id(token):
    payload = token.split(".")[0]
    return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))["sid"]


def call_mcp(url, token):
    """What a linked environment's first call (`PeerMcpClient`'s initialize) gets back."""
    status, headers, body = http(url, "POST", "/mcp", encode(INITIALIZE), {
        "authorization": f"Bearer {token}", "content-type": "application/json",
        "accept": "application/json, text/event-stream"})
    if status == 200:
        result = json.loads(body)["result"]
        return status, result["protocolVersion"], sorted(result["capabilities"]), "mcp-session-id" in headers
    challenge = headers.get("www-authenticate", "").replace(url, "<origin>")
    return status, headers.get("cache-control"), challenge, json.loads(body)


def descriptor(url):
    status, headers, body = http(url, "GET", "/.well-known/t3/environment")
    return status, headers.get("content-type"), json.loads(body)


# Descriptor capabilities for features Rust does not have yet (each a known
# difference in parity_gaps.json): they are compared apart from the rest.
ADDED_CAPABILITIES = ("previewTunnelSetting", "linkThreadStream", "linkClientPairing",
                      "linkPairingForLink", "agentFeedback", "issueTrackerAccounts")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    reference, rust = args.original_command, args.rust_command
    with TemporaryDirectory() as tmp:
        home = Path(tmp) / "home"
        secret, key = secrets.token_urlsafe(32), secrets.token_bytes(32)
        set_linked_environments(home, True)
        forged = "e30." + base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode()
        checks = []
        gaps = Gaps("peer_link_parity")

        def same(name, expected, actual):
            checks.append(name)
            if expected != actual and gaps.expected(name):
                gaps.differs(name)
                return
            assert expected == actual, (name, expected, actual)

        def capabilities_added_upstream(found):
            """The descriptor's capabilities the reference gained upstream
            (named in parity_gaps.json), apart from the rest of it."""
            status, kind, body = json.loads(json.dumps(found))
            added = {key: body["capabilities"].pop(key, None) for key in ADDED_CAPABILITIES}
            return [status, kind, body], added

        with running(reference, home, secret, key) as (url, cookie):
            reference_descriptor = descriptor(url)
            reference_link, reference_grant = link(url, cookie)
            reference_live = call_mcp(url, reference_link)
            reference_forged = call_mcp(url, forged)
            assert reference_live[0] == 200, reference_live
        with running(rust, home, secret, key) as (url, cookie):
            reference_rest, reference_added = capabilities_added_upstream(reference_descriptor)
            rust_rest, rust_added = capabilities_added_upstream(descriptor(url))
            same("descriptor on the same home", reference_rest, rust_rest)
            same("descriptor capabilities added upstream", reference_added, rust_added)
            same("link issued by the reference", reference_live, call_mcp(url, reference_link))
            same("unknown credential", reference_forged, call_mcp(url, forged))
            rust_link, rust_grant = link(url, cookie)
            same("token response", reference_grant, rust_grant)
            rust_live = call_mcp(url, rust_link)
        with running(reference, home, secret, key) as (url, cookie):
            same("link issued by the Rust server", rust_live, call_mcp(url, rust_link))
            # Revoked on the reference, as its Settings → Connections does.
            assert post(url, cookie, "/api/auth/clients/revoke", dict(sessionId=session_id(reference_link))) == {"revoked": True}
            reference_revoked = call_mcp(url, reference_link)
            assert reference_revoked[0] == 401, reference_revoked
        with running(rust, home, secret, key) as (url, cookie):
            same("link revoked on the reference", reference_revoked, call_mcp(url, reference_link))
            same("other link still live", rust_live, call_mcp(url, rust_link))
            # Revoked on the Rust server: the reference must refuse it as well.
            assert post(url, cookie, "/api/auth/clients/revoke", dict(sessionId=session_id(rust_link))) == {"revoked": True}
            rust_revoked = call_mcp(url, rust_link)
        with running(reference, home, secret, key) as (url, cookie):
            same("link revoked on the Rust server", call_mcp(url, rust_link), rust_revoked)
        # A third link, then Linked environments turned off: both refuse it the same way.
        with running(reference, home, secret, key) as (url, cookie):
            third, _ = link(url, cookie)
        set_linked_environments(home, False)
        with running(reference, home, secret, key) as (url, cookie):
            reference_off = call_mcp(url, third)
            assert reference_off[0] == 403, reference_off
        with running(rust, home, secret, key) as (url, cookie):
            same("link while Linked environments is off", reference_off, call_mcp(url, third))
        if gaps.finish(ran=set(checks)):
            raise SystemExit(1)
        print(f"{len(checks)}/{len(checks)} peer link checks ran across the reference and the Rust server on one home")


if __name__ == "__main__":
    main()
