#!/usr/bin/env python3
"""Exercise the same scoped pairing, RPC ticket, media and revocation flow on both servers."""
import argparse
import base64
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import secrets
import shlex
import sqlite3
import subprocess
import tempfile
import time
import uuid
from urllib.parse import quote, urlencode, urlsplit

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from websockets.sync.client import connect

import parity_harness as harness
from relay_parity import b64 as base64url, token as signed_jwt


# This persisted pairing record gives both databases the same public id and timestamps.
# Revoking it creates a live frame whose entire payload can be compared byte-for-byte.
FIXED_LINK = "00000000-0000-4000-8000-000000000007"
FIXED_CODE = "23456789ABCD"
FIXED_SCOPES = ["orchestration:read"]
FIXED_LABEL = "Parity fixture"

def seed_link(root, rust):
    if rust:
        userdata=root/"userdata"
        userdata.mkdir(parents=True,exist_ok=True)
        grant={"id":FIXED_LINK,"created":1735689600000,"expires":4102444800000,
               "label":FIXED_LABEL,"scopes":FIXED_SCOPES,"purpose":None,"access":None}
        key=base64url(hashlib.sha256(FIXED_CODE.encode()).digest())
        (userdata/"environment-auth.json").write_bytes(harness.encode({"sessions":{},"links":{key:grant}}))
    else:
        with sqlite3.connect(root/"userdata"/"statev2.sqlite") as db:
            db.execute("""insert into auth_pairing_links
                (id,credential,method,scopes,subject,label,created_at,expires_at)
                values (?,?,?,?,?,?,?,?)""",(FIXED_LINK,FIXED_CODE,"one-time-token",json.dumps(FIXED_SCOPES),
                "one-time-token",FIXED_LABEL,"2025-01-01T00:00:00.000Z","2100-01-01T00:00:00.000Z"))


def reference_cli(cli, root, *args):
    """Run the reference `t3` CLI against a home the way the desktop's SSH scripts do."""
    return subprocess.run([*cli, *args, "--base-dir", str(root)], env=harness.server_env(root),
                          cwd=root, check=True, capture_output=True, text=True, timeout=120).stdout


def migrate_reference_store(cli, root):
    # A home the desktop pairs with over SSH already holds the reference database:
    # `t3 auth pairing create` refuses to migrate it while another server owns the home.
    reference_cli(cli, root, "auth", "pairing", "list", "--json")


def exercise_cli_pairing(url, cli, root):
    """The desktop's SSH pairing: the reference CLI issues a one-time token out of process
    (REMOTE_PAIRING_SCRIPT in packages/ssh/src/tunnel.ts) and the desktop exchanges it for
    a bearer session (bootstrapRemoteBearerSession) on whichever server owns the home."""
    observations = {}
    issued_link = json.loads(reference_cli(cli, root, "auth", "pairing", "create", "--json"))
    credential = issued_link["credential"]
    def exchange(token, scope=None):
        return harness.http(url, "POST", "/oauth/token", urlencode({
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token": token,
            "subject_token_type": "urn:t3:params:oauth:token-type:environment-bootstrap",
            "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
            **({"scope": scope} if scope else {}),
        }).encode(), {"content-type": "application/x-www-form-urlencoded", "traceparent": harness.TRACE})
    status, _, body = exchange(credential)
    assert status == 200, (status, body)
    issued = json.loads(body)
    access = issued.pop("access_token")
    assert 0 < issued.pop("expires_in") <= 30 * 86400
    observations["CLI pairing token bearer exchange"] = status, issued
    status, _, body = harness.http(url, "GET", "/api/auth/session",
                                   headers={"authorization": "Bearer " + access, "traceparent": harness.TRACE})
    state = json.loads(body)
    assert state.pop("expiresAt")
    state["auth"].pop("sessionCookieName")  # derived from each server's port
    observations["CLI pairing bearer session"] = status, state
    status, _, body = harness.http(url, "POST", "/api/auth/websocket-ticket/rpc",
                                   headers={"authorization": "Bearer " + access, "traceparent": harness.TRACE})
    observations["CLI pairing bearer ticket"] = status, sorted(json.loads(body))
    observations["CLI pairing token is single use"] = exchange(credential)[::2]
    expired = json.loads(reference_cli(cli, root, "auth", "pairing", "create", "--ttl", "1s", "--json"))["credential"]
    time.sleep(1.5)
    observations["expired CLI pairing token"] = exchange(expired)[::2]
    scoped = json.loads(reference_cli(cli, root, "auth", "pairing", "create",
                                      "--scope", "orchestration:read", "--json"))["credential"]
    observations["CLI pairing token without the requested scope"] = exchange(scoped, "settings:write")[::2]
    status, _, body = exchange(scoped, "orchestration:read settings:write")
    narrowed = json.loads(body)
    narrowed.pop("access_token", None)
    narrowed.pop("expires_in", None)
    observations["CLI pairing token narrows to granted scopes"] = status, narrowed
    active = [link["id"] for link in json.loads(reference_cli(cli, root, "auth", "pairing", "list", "--json"))]
    observations["reference CLI no longer lists the spent token"] = issued_link["id"] not in active
    return observations


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

    def access_subscription():
        status, _, body = post("/api/auth/websocket-ticket/rpc",None)
        assert status==200
        ticket=json.loads(body)["ticket"]
        sock=connect(url.replace("http:","ws:")+"/ws?"+urlencode({
            "orchestrationProtocol":"2","wsTicket":ticket}),compression=None,proxy=None)
        sock.send(harness.encode({"_tag":"Request","id":"access","tag":"subscribeAuthAccess","payload":{},"headers":[]}).decode())
        return sock

    def read_change(sock):
        wire=sock.recv(timeout=5)
        frame=json.loads(wire)
        assert frame["_tag"]=="Chunk" and len(frame["values"])==1,wire
        sock.send(harness.encode({"_tag":"Ack","requestId":"access"}).decode())
        return frame["values"][0],wire

    with access_subscription() as sock:
        initial,_ = read_change(sock)
        assert initial["type"]=="snapshot" and initial["revision"]==1
        snapshot=initial["payload"]
        session=next(x for x in snapshot["clientSessions"] if x["current"])
        assert session["connected"] and session["lastConnectedAt"] and session["subject"]=="desktop-bootstrap"
        observations["access snapshot persisted grant"]=harness.encode(next(x for x in snapshot["pairingLinks"] if x["id"]==FIXED_LINK))
        observations["access current session metadata"]=harness.encode(session["client"])
        record("revoke fixture pairing",post("/api/auth/pairing-links/revoke",{"id":FIXED_LINK}))
        change,wire=read_change(sock)
        assert change["type"]=="pairingLinkRemoved" and change["revision"]==2 and change["payload"]=={"id":FIXED_LINK}
        observations["live access pairing removal"]=wire

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
    def jwt_parts(token):
        header, body, signature = token.split(".")
        unb64 = lambda s: base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))
        return json.loads(unb64(body)), (header + "." + body).encode(), unb64(signature)

    # The caller's origin is not checked (upstream #38892): the proof names
    # the server's own loopback origin whatever the request says.
    status, _, body = post("/api/connect/link-proof", {**proof_input, "origin": {**origin, "localHttpPort": 443}})
    claims = jwt_parts(json.loads(body))[0] if status == 200 else body
    observations["link origin port mismatch"] = (status, status == 200 and claims["origin"] == origin,
                                                 status == 200 and sorted(claims))
    status, _, data = post("/api/connect/link-proof", proof_input)
    assert status == 200, data

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
    device = ec.generate_private_key(ec.SECP256R1())
    coords = device.public_key().public_numbers()
    jwk = {"crv":"P-256","kty":"EC","x":base64url(coords.x.to_bytes(32,"big")),"y":base64url(coords.y.to_bytes(32,"big"))}
    thumb = base64url(hashlib.sha256(harness.encode(jwk)).digest())
    mint = signed_jwt(relay_key, {"alg":"EdDSA","typ":"t3-cloud-mint+jwt"}, {
        **claims, "scope":["environment:connect"],"clientProofKeyThumbprint":thumb,"cnf":{"jkt":thumb}})
    status, _, data = post("/api/t3-connect/mint-credential", {"proof":mint}, "")
    assert status == 200, data
    issued = json.loads(data)
    result, message, signature = jwt_parts(issued["proof"])
    environment_key.verify(signature, message)
    grant = issued["credential"]
    assert result["credential"] == grant and result["clientProofKeyThumbprint"] == thumb
    def device_proof(method, path, access=None):
        claims = {"htm":method,"htu":url+path,"jti":str(uuid.uuid4()),"iat":int(time.time())}
        if access:
            claims["ath"]=base64url(hashlib.sha256(access.encode()).digest())
        return signed_jwt(device, {"alg":"ES256","typ":"dpop+jwt","jwk":jwk}, claims)

    access_type = "urn:ietf:params:oauth:token-type:access_token"
    bootstrap_type = "urn:t3:params:oauth:token-type:environment-bootstrap"
    def exchange(credential, kind, proof=None):
        headers={"content-type":"application/x-www-form-urlencoded","traceparent":harness.TRACE}
        if proof:
            headers["dpop"]=proof
        return harness.http(url, "POST", "/oauth/token", urlencode({
            "grant_type":"urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token":credential, "subject_token_type":kind, "requested_token_type":access_type
        }).encode(), headers)
    record("bound Connect grant refuses browser bootstrap",post("/api/auth/browser-session",{"credential":grant},""))
    record("bound Connect grant refuses bearer exchange",exchange(grant,bootstrap_type))
    with access_subscription() as sock:
        event,_=read_change(sock)
        pending=next(link for link in event["payload"]["pairingLinks"] if "connectDeviceRequest" in link)
        assert pending["subject"]=="cloud-connect" and pending["label"]=="T3 Connect"
        waiting=exchange(grant,bootstrap_type,device_proof("POST","/oauth/token"))
        assert waiting[0]==403,waiting
        record("Connect credential waits for approval",waiting)
        event,_=read_change(sock)
        assert event["type"]=="pairingLinkUpserted" and event["payload"]["id"]==pending["id"]
        observations["pending device request metadata"]=harness.encode(event["payload"]["connectDeviceRequest"]["client"])
        record("approve pending Connect device",post("/api/auth/connect-devices/approve",{"pairingLinkId":pending["id"]}))
        observed=[]
        for _ in range(2):
            event,_=read_change(sock)
            observed.append(event["type"])
            if event["type"]=="snapshot":
                trusted=next(row for row in event["payload"]["connectDevices"] if row["proofKeyThumbprint"]==thumb)
                assert trusted["verificationCode"]==pending["connectDeviceRequest"]["verificationCode"]
                assert pending["id"] not in [link["id"] for link in event["payload"]["pairingLinks"]]
        assert sorted(observed)==["pairingLinkRemoved","snapshot"]
        observations["approval notification order"]=observed

    status, _, data = exchange(grant,bootstrap_type,device_proof("POST","/oauth/token"))
    assert status == 200 and json.loads(data)["token_type"]=="DPoP", data
    access = json.loads(data)["access_token"]
    def protected(method, path, proof, scheme="DPoP"):
        return harness.http(url,method,path,headers={"authorization":scheme+" "+access,
            "dpop":proof,"traceparent":harness.TRACE})
    path = "/api/auth/websocket-ticket/rpc"
    record("DPoP session refuses Bearer presentation",protected("POST",path,device_proof("POST",path,access),"Bearer"))
    record("DPoP token hash is mandatory",protected("POST",path,device_proof("POST",path)))
    record("DPoP proof URL must match",protected("POST",path,device_proof("POST","/different",access)))
    proof_header = device_proof("POST",path,access)
    status, _, data = protected("POST",path,proof_header)
    assert status == 200, data
    rpc_ticket = json.loads(data)["ticket"]
    ws_query = urlencode({"orchestrationProtocol":"2","wsTicket":rpc_ticket,"dpop":device_proof("GET","/ws",rpc_ticket)})
    with connect(url.replace("http:","ws:")+"/ws?"+ws_query, compression=None, proxy=None) as sock:
        sock.send(harness.encode({"_tag":"Request","id":"bound","tag":"server.probe","payload":{},"headers":[]}).decode())
        observations["bound DPoP RPC probe"] = sock.recv(timeout=5)
    record("refuse replay of authenticated HTTP proof",protected("POST",path,proof_header))
    record("Connect session cannot renew",exchange(access,access_type,device_proof("POST","/oauth/token")))
    with access_subscription() as sock:
        initial,_=read_change(sock)
        device_row=next(x for x in initial["payload"]["connectDevices"] if x["proofKeyThumbprint"]==thumb)
        assert device_row["approvedAt"] and device_row["verificationCode"]
        record("revoke trusted Connect device",post("/api/auth/connect-devices/revoke",{"proofKeyThumbprint":thumb}))
        for revision in range(2,6):
            change,_=read_change(sock)
            assert change["revision"]==revision
            if change["type"]=="snapshot":
                assert change["payload"]["connectDevices"]==[]
                break
        else:
            raise AssertionError("trusted device removal did not refresh the access snapshot")
    record("revoked Connect access token",protected("POST",path,device_proof("POST",path,access)))
    status, _, data = exchange(issue(),bootstrap_type,device_proof("POST","/oauth/token"))
    assert status == 200, data
    access = json.loads(data)["access_token"]
    renew_proof = device_proof("POST","/oauth/token")
    status, _, data = exchange(access,access_type,renew_proof)
    assert status == 200 and json.loads(data)["token_type"]=="DPoP", data
    record("refuse replay of DPoP renewal",exchange(access,access_type,renew_proof))

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
    for name, payload in [("server.probe", {}),("subscribeAuthAccess",{}), ("terminal.write", {
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
            root=Path(tmp)/str(i)
            (root/"userdata").mkdir(parents=True)
            (root/"userdata"/"settings.json").write_text('{"requireConnectDeviceApproval":true}')
            cli = shlex.split(args.original_command)
            if i==1:
                seed_link(root,True)
                migrate_reference_store(cli, root)
            url, cookie = harness.managed_server(stack, shlex.split(cmd), root, seed, signing_key)
            if i==0:
                seed_link(root,False)
            observations = exercise_cli_pairing(url, cli, root)
            observations.update(exercise(stack, url, cookie, relay_key))
            outputs.append(observations)
        failures = 0
        for name, original in outputs[0].items():
            rust = outputs[1][name]
            original, rust = harness.volatile_masked(original), harness.volatile_masked(rust)
            if original == rust:
                print("PASS", name)
            else:
                print("FAIL", name, repr(original), repr(rust), sep="\n")
                failures += 1
        raise SystemExit(bool(failures))


if __name__ == "__main__":
    main()
