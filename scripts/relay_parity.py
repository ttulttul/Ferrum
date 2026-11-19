#!/usr/bin/env python3
"""Compare the upstream relay handlers with Rust; exercise credential and device lifecycles with local keys."""
import argparse
import base64
import contextlib
from datetime import datetime, timezone
import hashlib
import json
import os
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from urllib.parse import urlencode

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519, padding, rsa, utils

from parity_harness import ROOT, TRACE, encode, free_port, http

ISSUER = "https://relay.example.test"


def b64(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def token(key, header, claims):
    data = (b64(encode(header)) + "." + b64(encode(claims))).encode()
    if isinstance(key, rsa.RSAPrivateKey):
        signature = key.sign(data, padding.PKCS1v15(), hashes.SHA256())
    elif isinstance(key, ec.EllipticCurvePrivateKey):
        r, s = utils.decode_dss_signature(key.sign(data, ec.ECDSA(hashes.SHA256())))
        # The reference verifier accepts low-S P-256 signatures.
        order = 0xffffffff00000000ffffffffffffffffbce6faada7179e84f3b9cac2fc632551
        signature = r.to_bytes(32, "big") + min(s, order-s).to_bytes(32, "big")
    else:
        signature = key.sign(data)
    return data.decode() + "." + b64(signature)


def pem(key):
    return key.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode()


@contextlib.contextmanager
def serve(cmd, port, env):
    with subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL) as process:
        try:
            base = f"http://127.0.0.1:{port}"
            for _ in range(300):
                if process.poll() is not None:
                    raise RuntimeError("relay exited before readiness")
                try:
                    if http(base, "GET", "/health")[0] == 200:
                        break
                except OSError:
                    pass
                time.sleep(0.05)
            else:
                raise RuntimeError("relay failed health check")
            yield base
        finally:
            process.terminate()
            try:
                process.wait(3)
            except subprocess.TimeoutExpired:
                process.kill()


def run(args, directory):
    clerk = rsa.generate_private_key(65537, 2048)
    mint = ed25519.Ed25519PrivateKey.generate()
    env = dict(PATH=os.environ["PATH"], HOME=directory, RELAY_ISSUER=ISSUER, CLERK_ISSUER="https://clerk.example.test",
               CLERK_JWT_KEY=pem(clerk), CLERK_JWT_AUDIENCE="fixture", CLOUD_MINT_PUBLIC_KEY=pem(mint),
               CLOUD_MINT_PRIVATE_KEY=mint.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                                       serialization.NoEncryption()).decode())
    def identity(subject):
        return token(clerk, {"alg": "RS256", "typ": "JWT"}, {"iss": env["CLERK_ISSUER"], "aud": "fixture",
            "sub": subject, "exp": int(time.time())+600, "iat": int(time.time())})

    def bearer(sub):
        return {"Authorization": "Bearer " + identity(sub), "traceparent": TRACE}

    dpop = ec.generate_private_key(ec.SECP256R1())
    p = dpop.public_key().public_numbers()
    jwk = {"crv": "P-256", "kty": "EC", "x": b64(p.x.to_bytes(32, "big")), "y": b64(p.y.to_bytes(32, "big"))}

    def proof(method, path, access=None):
        claims = dict(htm=method, htu=ISSUER + path, jti=str(uuid.uuid4()), iat=int(time.time()))
        if access:
            claims["ath"] = b64(hashlib.sha256(access.encode()).digest())
        return token(dpop, dict(alg="ES256", typ="dpop+jwt", jwk=jwk), claims)

    port, reference = free_port(), free_port()
    cmd = [args.rust, "--relay", "--listen", f"127.0.0.1:{port}", "--data-dir", directory]
    with serve([args.node, ROOT/"scripts/relay_reference.mjs", args.checkout, str(reference)], reference, env) as original, serve(cmd, port, env) as rust:
        paths = ["/health", "/.well-known/oauth-authorization-server", "/.well-known/oauth-protected-resource",
                 "/v1/environments", "/v1/client/devices", "/v2/client/devices", "/v1/client/t3chat-link"]
        for path in paths:
            headers = bearer("fixture")
            def read(url):
                status, h, body = http(url, "GET", path, headers=headers)
                return status, [h.get(k) for k in ("content-type", "cache-control", "pragma")], body
            expected, actual = read(original), read(rust)
            assert expected[0] == 200 and expected == actual, (path, expected, actual)
        print(f"{len(paths)}/{len(paths)} differential relay HTTP fixtures matched", flush=True)
        def call(method, path, headers=None, body=None):
            status, _, data = http(rust, method, path, body, headers)
            return status, json.loads(data) if data else None
        path = "/v1/client/dpop-token"
        form = urlencode(dict(grant_type="urn:ietf:params:oauth:grant-type:token-exchange", subject_token=identity("fixture"),
            subject_token_type="urn:ietf:params:oauth:token-type:jwt", requested_token_type="urn:ietf:params:oauth:token-type:access_token",
            resource=ISSUER, scope="mobile:registration", client_id="t3-mobile"))
        headers = {"content-type": "application/x-www-form-urlencoded", "dpop": proof("POST", path)}
        status, response = call("POST", path, headers, form)
        assert status == 200, response
        access = response["access_token"]
        h, c, sig = access.split(".")
        mint.public_key().verify(base64.urlsafe_b64decode(sig+"=="), (h+"."+c).encode())
        assert call("POST", path, headers, form)[1]["dpopFailureReason"] == "replay"
        snapshot_path = "/v1/mobile/agent-activity"
        signed_headers = {"authorization":"DPoP "+access, "dpop":proof("GET",snapshot_path,access), "traceparent":TRACE}
        for name, headers_to_send in [("signed mobile snapshot", signed_headers), ("replayed proof", signed_headers),
                ("wrong request binding", {**signed_headers, "dpop":proof("POST",snapshot_path,access)}),
                ("wrong access token hash", {**signed_headers, "dpop":proof("GET",snapshot_path,"different")}),
                ("missing proof", {"authorization":"DPoP "+access, "traceparent":TRACE})]:
            def read_mobile(url):
                status, h, data = http(url, "GET", snapshot_path, headers={**headers_to_send, "Host":"relay.example.test"})
                return status, h.get("www-authenticate"), data
            expected, actual = read_mobile(original), read_mobile(rust)
            assert expected == actual, (name, expected, actual)
        print("5/5 DPoP-bound mobile HTTP fixtures matched using the Rust-issued access token", flush=True)
        mobile = "/v1/mobile/devices"
        headers = {"authorization": "DPoP "+access, "dpop": proof("POST", mobile, access), "content-type":"application/json"}
        device = dict(deviceId="fixture-device", label="Phone", platform="android", androidApiLevel=36,
                      preferences=dict.fromkeys(["notificationsEnabled", "liveActivitiesEnabled", "notifyOnApproval",
                                                "notifyOnInput", "notifyOnCompletion", "notifyOnFailure"], True))
        assert call("POST", mobile, headers, encode(device)) == (200, {"ok": True})
        assert call("POST", mobile, headers, encode(device))[1]["dpopFailureReason"] == "replay"
        assert call("GET", "/v1/client/devices", bearer("fixture"))[1] == {"devices":[]}
        assert call("GET", "/v2/client/devices", bearer("other"))[1] == {"devices":[]}
        devices = call("GET", "/v2/client/devices", bearer("fixture"))[1]["devices"]
        assert devices[0]["deviceId"] == "fixture-device" and not devices[0]["notifications"]["enabled"]
        flags = dict(notificationsEnabled=True, liveActivitiesEnabled=True, managedTunnelsEnabled=False)
        user = {**bearer("fixture"), "content-type":"application/json"}
        status, response = call("POST", "/v1/client/environment-link-challenges", user, encode(flags))
        assert status == 200
        host = ed25519.Ed25519PrivateKey.generate()
        now = int(time.time())
        claims = dict(iss="t3-env:env", aud=ISSUER, sub="env", environmentId="env", iat=now, exp=now+120, jti=str(uuid.uuid4()),
            challenge=response["challenge"], environmentPublicKey=pem(host), scopes=["agent_activity_notifications"],
            endpoint=dict(httpBaseUrl="https://host.example",wsBaseUrl="wss://host.example",providerKind="manual"),
            origin=dict(localHttpHost="localhost",localHttpPort=3773),
            descriptor=dict(environmentId="env",label="Server",platform=dict(os="linux",arch="x64"),serverVersion="0.1.10",capabilities={}))
        body = encode({**flags, "proof":token(host, dict(alg="EdDSA",typ="t3-env-link+jwt"),claims)})
        status, response = call("POST", "/v1/client/environment-links", user, body)
        assert status == 200, response
        env_headers = {"authorization":"Bearer "+response["environmentCredential"], "content-type":"application/json"}
        assert call("POST", "/v1/client/environment-links", user, body)[1]["reason"] == "replayed_nonce"
    with serve(cmd, port, env) as rust:
        assert call("POST", mobile, headers, encode(device))[1]["dpopFailureReason"] == "replay"
        assert call("GET", "/v2/client/devices", bearer("fixture"))[1]["devices"] == devices
        pref = "/v1/environments/env/link-preferences"
        assert call("POST", pref, env_headers, encode({"holdWebhooksWhileOffline": True})) == (200, {"holdWebhooksWhileOffline":True})
        agent_activity = "/v1/environments/env/threads/thread/agent-activity"
        aggregate_path = "/v1/mobile/agent-activity"
        activity = dict(environmentId="env", threadId="thread", projectTitle="Project", threadTitle="Thread",
                        phase="running", headline="Agent working", modelTitle="Model",
                        updatedAt=datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00","Z"), deepLink="/thread")
        def publish(state):
            data = dict(iss="t3-env:env", aud=ISSUER, sub="env", iat=now, exp=now+120, jti=str(uuid.uuid4()),
                        environmentId="env", threadId="thread", state=state)
            return encode(dict(proof=token(host, dict(alg="EdDSA", typ="t3-env-activity+jwt"), data), state=state))
        def snapshot():
            return call("GET", aggregate_path, {"authorization":"DPoP "+access, "dpop":proof("GET", aggregate_path, access)})[1]["aggregate"]
        signed = publish(activity)
        assert call("POST", agent_activity, env_headers, signed) == (200, {"ok": True, "deliveries":[]})
        assert snapshot()["activities"][0]["status"] == "Working"
        assert call("POST", agent_activity, env_headers, signed)[0] == 401
        assert call("POST", agent_activity, env_headers, publish(None))[0] == 200
        assert snapshot() is None
        assert call("GET", "/v1/environments", env_headers)[0] == 401
        assert call("POST", pref, user, encode({"holdWebhooksWhileOffline": True}))[0] == 401
        assert call("DELETE", "/v1/client/environment-links/env", bearer("other")) == (200, {"ok":False})
        assert call("DELETE", "/v1/client/environment-links/env", user) == (200, {"ok":True})
        assert call("POST", pref, env_headers, encode({"holdWebhooksWhileOffline": True}))[0] == 401
    print("Rust signed token exchange, replay rejection, mobile registration, account isolation, environment linking, revocation and restart checks passed")


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkout",type=Path,required=True)
    parser.add_argument("--node",default="node")
    parser.add_argument("--rust",default=str(ROOT/"target/debug/t3code-server"))
    with tempfile.TemporaryDirectory() as directory:
        run(parser.parse_args(),directory)
