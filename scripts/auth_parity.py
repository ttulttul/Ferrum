#!/usr/bin/env python3
"""Exercise the same scoped pairing, RPC ticket, media and revocation flow on both servers."""
import argparse
import base64
from contextlib import ExitStack
import json
from pathlib import Path
import secrets
import shlex
import tempfile
import time
from urllib.parse import quote, urlsplit

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from websockets.sync.client import connect

import parity_harness as harness


def exercise(stack, url, owner_cookie, relay_key):
    observations = {}

    def post(path, data, cookie=owner_cookie):
        return harness.http(url, "POST", path, harness.encode(data), {
            "Content-Type": "application/json", "Cookie": cookie, "traceparent": harness.TRACE})

    def record(name, response):
        status, _, body = response
        observations[name] = status, body

    def issue():
        status, _, body = post("/api/auth/pairing-token", {"scopes": ["orchestration:read"]})
        assert status == 200, (status, body)
        return json.loads(body)["credential"]

    def link_state():
        return harness.http(url, "GET", "/api/connect/link-state", headers={"Cookie": owner_cookie})
    record("unlinked environment", link_state())
    record("persist activity preference", post("/api/connect/preferences", {"publishAgentActivity":True}))
    record("hold requires link", post("/api/connect/preferences",
           {"publishAgentActivity":False, "holdWebhooksWhileOffline":True}))
    record("rollback preference", link_state())
    record("unlink", post("/api/connect/unlink", {}))
    record("unlink clears preferences", link_state())
    for route in ("health", "mint-credential"):
        record("no key for " + route, post("/api/t3-connect/" + route, {"proof":"invalid"}, ""))

    relay = "https://relay.example"
    origin = {"localHttpHost":"127.0.0.1","localHttpPort":urlsplit(url).port}
    endpoint = {"httpBaseUrl":"https://environment.example","wsBaseUrl":"wss://environment.example","providerKind":"manual"}
    proof_input = {"relayIssuer":relay,"challenge":"test","origin":origin,"endpoint":endpoint}
    record("link origin port mismatch", post("/api/connect/link-proof", {
        **proof_input, "origin": {**origin, "localHttpPort":443}}))
    status, _, data = post("/api/connect/link-proof", proof_input)
    assert status == 200, data

    def jwt_parts(token):
        header, body, signature = token.split(".")
        unb64 = lambda s: base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))
        return json.loads(unb64(body)), (header + "." + body).encode(), unb64(signature)

    link, message, signature = jwt_parts(json.loads(data))
    environment_key = serialization.load_pem_public_key(link["environmentPublicKey"].encode())
    environment_key.verify(signature, message)
    assert link["challenge"] == "test" and link["aud"] == relay
    env_id = link["environmentId"]
    config = {"relayUrl":relay,"cloudUserId":"parity-user","environmentCredential":"fixture",
              "cloudMintPublicKey":relay_key.public_key().public_bytes(
                  serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode(),
              "endpointRuntime":None}
    record("apply manual relay config", post("/api/connect/relay-config",config))
    record("linked environment", link_state())
    record("different cloud user", post("/api/connect/relay-config",{**config,"cloudUserId":"other"}))
    record("linked invalid health proof", post("/api/t3-connect/health",{"proof":"invalid"},""))
    now = int(time.time())
    claims = {"iss":relay,"aud":"t3-env:" + env_id,"sub":"parity-user","jti":"health-test",
              "iat":now,"exp":now+90,"environmentId":env_id,"scope":["environment:status"],"nonce":"health-nonce"}
    b64 = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=")
    message = b".".join(b64(harness.encode(v)) for v in ({"alg":"EdDSA","typ":"t3-cloud-health+jwt"}, claims))
    proof = {"proof":(message + b"." + b64(relay_key.sign(message))).decode()}
    status, _, data = post("/api/t3-connect/health", proof, "")
    assert status == 200, data
    health = json.loads(data)
    result, message, signature = jwt_parts(health["proof"])
    environment_key.verify(signature, message)
    assert result["requestNonce"] == "health-nonce" and health["status"] == "online"
    assert result["environmentId"] == env_id and result["checkedAt"] == health["checkedAt"]
    record("reject replayed health proof", post("/api/t3-connect/health", proof, ""))
    record("unlink manual environment", post("/api/connect/unlink",{}))

    code = issue()
    status, headers, _ = post("/api/auth/browser-session", {"credential": code}, "")
    assert status == 200
    reader = headers["set-cookie"].split(";", 1)[0]
    record("spent pairing grant", post("/api/auth/browser-session", {"credential": code}, ""))
    record("reader cannot delegate", post("/api/auth/pairing-token", {}, reader))
    record("reader cannot write projects", post("/api/projects/mutate", {
        "type": "project.delete", "commandId": "delete", "projectId": "absent"}, reader))

    def ticket(kind):
        status, _, body = post("/api/auth/websocket-ticket/" + kind, None, reader)
        assert status == 200
        return json.loads(body)["ticket"]

    media = quote(ticket("media"), safe="")
    record("read permitted on media", harness.http(url, "GET",
           "/api/preview-stream/download?id=missing&wsTicket=" + media))
    record("media cannot change devices", harness.http(url, "POST",
           "/api/device-hub/vendor/serve-emu/api/fold?wsTicket=" + media,
           headers={"traceparent": harness.TRACE}))
    rpc = quote(ticket("rpc"), safe="")
    sock = stack.enter_context(connect(url.replace("http:", "ws:") +
        "/ws?orchestrationProtocol=2&wsTicket=" + rpc, compression=None, proxy=None))
    for name, payload in [("server.probe", {}), ("terminal.write", {
        "threadId": "test", "terminalId": "default", "data": "unused"})]:
        sock.send(harness.encode({"_tag":"Request","id":name,"tag":name,"payload":payload,"headers":[]}).decode())
        observations[name] = sock.recv(timeout=5)
    record("revoke other clients", post("/api/auth/clients/revoke-others", {}))
    record("revoked session", post("/api/auth/websocket-ticket/rpc", None, reader))
    record("revoked media ticket", harness.http(url, "GET",
           "/api/preview-stream/download?id=missing&wsTicket=" + media, headers={"traceparent": harness.TRACE}))
    return observations


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as tmp, ExitStack() as stack:
        seed = secrets.token_urlsafe(32)
        signing_key = secrets.token_bytes(32)
        relay_key = Ed25519PrivateKey.generate()
        outputs = []
        for i, cmd in enumerate((args.original_command, args.rust_command)):
            url, cookie = harness.managed_server(stack, shlex.split(cmd), Path(tmp)/str(i), seed, signing_key)
            outputs.append(exercise(stack, url, cookie, relay_key))
        failures = 0
        for name, original in outputs[0].items():
            rust = outputs[1][name]
            if original == rust:
                print("PASS", name)
            else:
                print("FAIL", name, repr(original), repr(rust), sep="\n")
                failures += 1
        raise SystemExit(bool(failures))


if __name__ == "__main__":
    main()
