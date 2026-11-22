#!/usr/bin/env python3
"""Compare how two T3 servers get, register and release the T3 Connect managed tunnel.

A server whose T3 home has a CLI-desired link but no stored tunnel (the state the reference
leaves when it stops) asks T3 Connect for one at startup, and releases it again when it stops.
Each scenario seeds a scratch home and points both servers at a fake T3 Connect API and fake
Clerk OAuth endpoint (one local HTTPS server with a throwaway CA, trusted through
`NODE_EXTRA_CA_CERTS`) and at `scripts/fake_cloudflared.py`. Nothing reaches the real T3
Connect, Clerk or Cloudflare. It compares, with secrets, times and ids replaced by
placeholders:

- every request each server sends to the fake API: method, path, bearer credential, body,
  and the header and claims of every signed proof (whose signature the fake checks against
  the environment key);
- the connectors started and their environment;
- the T3 Connect secrets left on disk after the server stops.

Scenarios:

- `provision`: a CLI-desired managed link with a current CLI token and no stored tunnel;
- `refresh`: the same with an expired CLI token, refreshed through the fake Clerk endpoint;
- `register-stored`: a linked environment with a stored, unconfirmed tunnel;
- `update-restart`: as `register-stored`, stopped with the desktop's update restart marker,
  which keeps the tunnel;
- `publish-only-link`: a CLI-desired publish-only link, provisioned without a tunnel;
- `confirmed-restart`: as `register-stored`, but T3 Connect already confirmed the tunnel for
  this server's port (what a server leaves when it hands the tunnel over), stopped through
  `./install`'s handoff marker. The connector must start as soon as the server listens, with
  no first-registration jitter, and the tunnel must survive the stop.

Besides the requests, connectors and secrets, it compares when the first connector starts:
`immediate` (within a few seconds of the server answering HTTP) or `later`, for the
scenarios where that does not depend on the reference's random 0-30 s registration jitter.

Each scenario runs on each server with its own home and its own fake T3 Connect, so all of them
run side by side (--jobs at a time): most of a run is the reference's registration jitter.
"""
import argparse
import importlib.machinery
import importlib.util
import base64
import contextlib
import datetime
import ipaddress
import json
import os
from pathlib import Path
import secrets
import shlex
import shutil
import signal
import ssl
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from cryptography import x509
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519
from cryptography.x509.oid import NameOID

from relay_client_parity import FAKE, alive, diff, normalize_start, read_jsonl, server, wait_for
from parity_harness import run_concurrently

ENVIRONMENT_ID = "00000000-0000-4000-8000-0000000e0001"
CLOUD_USER_ID = "user_parity"
ENVIRONMENT_CREDENTIAL = "fake-environment-credential"
STORED_TOKEN = "eyJhIjoic3RvcmVkLWFjY291bnQiLCJ0Ijoic3RvcmVkLXR1bm5lbCIsInMiOiJzdG9yZWQifQ"
STORED_TUNNEL_ID = "00000000-0000-4000-8000-00000000d00d"
NEW_TOKEN = "eyJhIjoibmV3LWFjY291bnQiLCJ0IjoibmV3LXR1bm5lbCIsInMiOiJuZXcifQ"
NEW_TUNNEL_ID = "00000000-0000-4000-8000-00000000beef"
ACCESS_TOKEN = "fake-cli-access-token"
REFRESHED_ACCESS_TOKEN = "fake-cli-access-token-refreshed"
REFRESH_TOKEN = "fake-cli-refresh-token"
ROTATED_REFRESH_TOKEN = "fake-cli-refresh-token-rotated"
CLIENT_ID = "parity-fake-cli-client"
PLACEHOLDERS = {
    STORED_TOKEN: "<stored-connector-token>",
    NEW_TOKEN: "<new-connector-token>",
    ACCESS_TOKEN: "<cli-access-token>",
    REFRESHED_ACCESS_TOKEN: "<refreshed-cli-access-token>",
    REFRESH_TOKEN: "<cli-refresh-token>",
    ROTATED_REFRESH_TOKEN: "<rotated-cli-refresh-token>",
    ENVIRONMENT_CREDENTIAL: "<environment-credential>",
}
SCENARIOS = ["provision", "refresh", "register-stored", "update-restart", "publish-only-link", "confirmed-restart"]
# Scenarios whose connector must start without waiting for T3 Connect.
TIMED_SCENARIOS = {"confirmed-restart"}
# A connector started this soon after the server answers HTTP did not wait for jitter.
IMMEDIATE_START = 5.0


def load_installer():
    """`./install`, imported for the handoff marker it writes before stopping a server."""
    path = Path(__file__).resolve().parent.parent / "install"
    loader = importlib.machinery.SourceFileLoader("t3code_install", str(path))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


def b64url(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def b64url_decode(text):
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def make_certificates(directory):
    """A throwaway CA and a 127.0.0.1 leaf it signs; returns (ca path, cert path, key path)."""
    now = datetime.datetime.now(datetime.timezone.utc)
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "relay parity CA")])
    ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name)
          .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
          .not_valid_before(now - datetime.timedelta(hours=1)).not_valid_after(now + datetime.timedelta(days=1))
          .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
          .add_extension(x509.KeyUsage(digital_signature=True, key_cert_sign=True, crl_sign=True,
                                       content_commitment=False, key_encipherment=False, data_encipherment=False,
                                       key_agreement=False, encipher_only=False, decipher_only=False), critical=True)
          .sign(ca_key, hashes.SHA256()))
    leaf_key = ec.generate_private_key(ec.SECP256R1())
    leaf = (x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")]))
            .issuer_name(ca_name).public_key(leaf_key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(hours=1)).not_valid_after(now + datetime.timedelta(days=1))
            .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), critical=False)
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.ExtendedKeyUsage([x509.oid.ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
            .sign(ca_key, hashes.SHA256()))
    paths = directory / "ca.pem", directory / "leaf.pem", directory / "leaf-key.pem"
    paths[0].write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    paths[1].write_bytes(leaf.public_bytes(serialization.Encoding.PEM))
    paths[2].write_bytes(leaf_key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                                serialization.NoEncryption()))
    return paths


def spki_pem(public_key):
    return public_key.public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode()


class FakeConnect:
    """T3 Connect's environment API and Clerk's OAuth token endpoint, recording every request."""

    def __init__(self, certificates, mint_key, relay_key):
        self.requests = []
        self.lock = threading.Lock()
        # Every fake of a run signs with the same keys, so both servers store the same ones.
        self.mint_key = mint_key
        self.relay_key = relay_key
        fake = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def handle_any(self):
                length = int(self.headers.get("content-length") or 0)
                raw = self.rfile.read(length) if length else b""
                status, body = fake.answer(self.command, self.path, self.headers, raw)
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = do_POST = do_DELETE = do_PUT = handle_any

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(str(certificates[1]), str(certificates[2]))
        self.httpd.socket = context.wrap_socket(self.httpd.socket, server_side=True)
        self.port = self.httpd.server_address[1]
        self.url = f"https://127.0.0.1:{self.port}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()

    def reset(self):
        with self.lock:
            self.requests = []

    def challenge(self):
        header = b64url(json.dumps({"alg": "EdDSA", "typ": "t3-relay-link-challenge+jwt"}).encode())
        claims = b64url(json.dumps({"iat": int(time.time()) - 5, "nonce": secrets.token_hex(8)}).encode())
        message = f"{header}.{claims}"
        return f"{message}.{b64url(self.relay_key.sign(message.encode()))}"

    def answer(self, method, path, headers, raw):
        record = {"method": method, "path": path.replace(ENVIRONMENT_ID, "<environment-id>"),
                  "authorization": headers.get("authorization"), "contentType": headers.get("content-type")}
        body = None
        if raw:
            try:
                body = json.loads(raw)
            except ValueError:
                body = {key: values[0] for key, values in
                        __import__("urllib.parse").parse.parse_qs(raw.decode(), keep_blank_values=True).items()}
                record["form"] = list(body)
        record["body"] = body
        with self.lock:
            self.requests.append(record)
        if method == "POST" and path == "/oauth/token":
            return 200, {"access_token": REFRESHED_ACCESS_TOKEN, "refresh_token": ROTATED_REFRESH_TOKEN,
                         "expires_in": 3600, "token_type": "Bearer"}
        if method == "POST" and path == "/v1/client/environment-link-challenges":
            return 200, {"challenge": self.challenge(), "expiresAt": "2999-01-01T00:00:00.000Z"}
        if method == "POST" and path == "/v1/client/environment-links":
            claims = verify(body.get("proof"), lambda claims: claims.get("environmentPublicKey"))
            managed = bool(body.get("managedTunnelsEnabled"))
            return 200, {
                "ok": True, "cloudUserId": CLOUD_USER_ID, "environmentId": claims["environmentId"],
                "endpoint": {"httpBaseUrl": "https://prod-parity.relay.invalid", "wsBaseUrl": "wss://prod-parity.relay.invalid",
                             "providerKind": "cloudflare_tunnel" if managed else "manual"},
                "endpointRuntime": {"providerKind": "cloudflare_tunnel", "connectorToken": NEW_TOKEN, "tunnelId": NEW_TUNNEL_ID,
                                    "tunnelName": "t3coderelay-managedendpoint-prod-parity"} if managed else None,
                "relayIssuer": self.url, "environmentCredential": ENVIRONMENT_CREDENTIAL,
                "cloudMintPublicKey": spki_pem(self.mint_key.public_key()),
            }
        if method == "POST" and path == f"/v1/environments/{ENVIRONMENT_ID}/tunnel/recovery":
            return 200, {"status": "ready"}
        if method == "DELETE" and path == f"/v1/client/environment-links/{ENVIRONMENT_ID}/tunnel":
            return 200, {"ok": True}
        return 404, {"_tag": "RelayInternalError", "message": "Not found", "traceId": "parity"}


def verify(token, key_for):
    """Decodes a signed proof and checks its EdDSA signature against the key its claims name."""
    header_text, claims_text, signature = token.split(".")
    claims = json.loads(b64url_decode(claims_text))
    key = serialization.load_pem_public_key(key_for(claims).encode())
    try:
        key.verify(b64url_decode(signature), f"{header_text}.{claims_text}".encode())
    except InvalidSignature:
        raise ValueError("invalid proof signature")
    return claims


def normalize_text(value):
    if isinstance(value, str):
        # Longest first: some fake secrets extend others.
        for secret, placeholder in sorted(PLACEHOLDERS.items(), key=lambda item: -len(item[0])):
            value = value.replace(secret, placeholder)
        return value
    if isinstance(value, list):
        return [normalize_text(item) for item in value]
    if isinstance(value, dict):
        return {key: normalize_text(item) for key, item in value.items()}
    return value


def normalize_proof(token, public_key):
    header_text, claims_text, _ = token.split(".")
    claims = json.loads(b64url_decode(claims_text))
    key = serialization.load_pem_public_key(public_key.encode())
    try:
        key.verify(b64url_decode(token.split(".")[2]), f"{header_text}.{claims_text}".encode())
        valid = True
    except InvalidSignature:
        valid = False
    shape = dict(claims)
    shape["jti"] = "<uuid>" if len(str(claims.get("jti", ""))) == 36 else claims.get("jti")
    shape["iat"] = "<iat>"
    shape["exp"] = f"iat+{claims['exp'] - claims['iat']}"
    if "challenge" in shape:
        shape["challenge"] = "<challenge>"
    if isinstance(shape.get("descriptor"), dict):
        # The descriptor's own parity is checked by the initial sync comparison.
        shape["descriptor"] = {"environmentId": shape["descriptor"].get("environmentId")}
    if "environmentPublicKey" in shape:
        shape["environmentPublicKey"] = "<environment-public-key>" if claims["environmentPublicKey"] == public_key.strip() else claims["environmentPublicKey"]
    return {"header": json.loads(b64url_decode(header_text)), "claims": shape, "signatureValid": valid}


def replace_ports(value, url, server_port):
    text = json.dumps(value).replace(url, "<connect>").replace(f":{server_port}", ":<server-port>")
    text = text.replace(f'"localHttpPort": {server_port}', '"localHttpPort": "<server-port>"')
    return json.loads(text)


def normalize_request(request, url, public_key, server_port):
    request = json.loads(json.dumps(request))
    body = request.get("body")
    if isinstance(body, dict) and isinstance(body.get("proof"), str):
        body["proof"] = normalize_proof(body["proof"], public_key)
    return normalize_text(replace_ports(request, url, server_port))


SECRET_NAMES = [
    "cloud-cli-desired-link", "cloud-cli-oauth-token", "cloud-endpoint-runtime-config",
    "cloud-endpoint-confirmed-origin", "cloud-linked-user-id", "cloud-relay-url", "cloud-relay-issuer",
    "cloud-relay-environment-credential", "cloud-mint-ed25519-public-key", "cloud-link-revoked",
    "cloud-link-ed25519-key-pair",
]


def snapshot_secrets(root, url, server_port, mint_pem, key_pair):
    directory = root / "userdata" / "secrets"
    out = {}
    for name in SECRET_NAMES:
        path = directory / f"{name}.bin"
        if not path.exists():
            continue
        text = path.read_text()
        if name == "cloud-mint-ed25519-public-key":
            text = "<mint-public-key>" if text == mint_pem.strip() else text
        elif name == "cloud-link-ed25519-key-pair":
            text = "<seeded-key-pair>" if text == key_pair else "<other-key-pair>"
        elif name == "cloud-cli-oauth-token":
            token = json.loads(text)
            token["expiresAtEpochMs"] = "<future>" if token["expiresAtEpochMs"] > time.time() * 1000 else "<past>"
            text = {"keys": list(json.loads(text)), "value": token}
        if isinstance(text, str):
            # Compared as stored, so key order and spacing count too.
            text = text.replace(url, "<connect>").replace(f'"localHttpPort":{server_port}', '"localHttpPort":"<server-port>"')
        out[name] = normalize_text(text)
        out[name + ".mode"] = oct(path.stat().st_mode & 0o777)
    leftovers = sorted(p.name for p in directory.glob("*.tmp"))
    if leftovers:
        out["temporaryFiles"] = leftovers
    return out


def write_secret(root, name, text):
    directory = root / "userdata" / "secrets"
    directory.mkdir(parents=True, mode=0o700, exist_ok=True)
    (directory / f"{name}.bin").write_text(text)
    (directory / f"{name}.bin").chmod(0o600)


def seed(root, scenario, fake, key_pair, server_port):
    (root / "userdata").mkdir(parents=True, exist_ok=True)
    (root / "userdata" / "environment-id").write_text(ENVIRONMENT_ID + "\n")
    write_secret(root, "cloud-link-ed25519-key-pair", key_pair)
    expires = int(time.time() * 1000) + (-60_000 if scenario == "refresh" else 3_600_000)
    write_secret(root, "cloud-cli-oauth-token", json.dumps(
        {"accessToken": ACCESS_TOKEN, "refreshToken": REFRESH_TOKEN, "expiresAtEpochMs": expires}, separators=(",", ":")))
    write_secret(root, "cloud-cli-desired-link", "publish_only" if scenario == "publish-only-link" else "managed")
    if scenario in ("register-stored", "update-restart", "confirmed-restart"):
        config = {"providerKind": "cloudflare_tunnel", "connectorToken": STORED_TOKEN, "tunnelId": STORED_TUNNEL_ID,
                  "tunnelName": "t3coderelay-managedendpoint-prod-stored"}
        write_secret(root, "cloud-endpoint-runtime-config", json.dumps(config, separators=(",", ":")))
        write_secret(root, "cloud-relay-url", fake.url)
        write_secret(root, "cloud-relay-issuer", fake.url)
        write_secret(root, "cloud-linked-user-id", CLOUD_USER_ID)
        write_secret(root, "cloud-relay-environment-credential", ENVIRONMENT_CREDENTIAL)
        write_secret(root, "cloud-mint-ed25519-public-key", spki_pem(fake.mint_key.public_key()))
    if scenario == "confirmed-restart":
        write_secret(root, "cloud-endpoint-confirmed-origin", json.dumps(
            {"config": config, "origin": {"localHttpHost": "127.0.0.1", "localHttpPort": server_port}},
            separators=(",", ":")))


def run_scenario(name, prefix, root, credentials, fake, ca_path, key_pair, public_key):
    from parity_harness import free_port
    port = free_port()
    fake.reset()
    seed(root, name, fake, key_pair, port)
    binary = root / "tools-bin" / "cloudflared"
    binary.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(FAKE, binary)
    binary.chmod(0o755)
    record = binary.parent / "fake-cloudflared.jsonl"
    host = f"127.0.0.1:{fake.port}$"
    env = {
        "T3CODE_CLOUDFLARED_PATH": str(binary),
        "NODE_EXTRA_CA_CERTS": str(ca_path),
        "T3CODE_RELAY_URL": fake.url,
        "T3CODE_CLERK_PUBLISHABLE_KEY": "pk_test_" + base64.b64encode(host.encode()).decode(),
        "T3CODE_CLERK_CLI_OAUTH_CLIENT_ID": CLIENT_ID,
    }
    expected_paths = {
        "provision": "/v1/environments/<environment-id>/tunnel/recovery",
        "refresh": "/v1/environments/<environment-id>/tunnel/recovery",
        "register-stored": "/v1/environments/<environment-id>/tunnel/recovery",
        "update-restart": "/v1/environments/<environment-id>/tunnel/recovery",
        "publish-only-link": "/v1/client/environment-links",
        "confirmed-restart": "/v1/environments/<environment-id>/tunnel/recovery",
    }
    result = {}
    with server(prefix, root, port, *credentials, env) as (process, url):
        if name in TIMED_SCENARIOS:
            answered = time.monotonic()
            started = None
            while time.monotonic() - answered < 45 and process.poll() is None:
                with contextlib.suppress(ValueError):
                    if any(e["event"] == "start" for e in read_jsonl(record)):
                        started = time.monotonic() - answered
                if started is not None:
                    break
                time.sleep(0.05)
            result["firstConnectorStart"] = ("never" if started is None else
                                             "immediate" if started < IMMEDIATE_START else "later")
            print(f"  {name}: {prefix[0].rsplit('/', 1)[-1]} started its connector "
                  + ("never" if started is None else f"{started:.2f} s after answering HTTP"))
        # The reference waits up to 30 s (first-registration jitter) before its first call.
        def done():
            paths = [r["path"].replace(ENVIRONMENT_ID, "<environment-id>") for r in fake.requests]
            return expected_paths[name] in paths
        wait_for(done, 60)
        if name != "publish-only-link":
            wait_for(lambda: (root / "userdata" / "secrets" / "cloud-endpoint-confirmed-origin.bin").exists(), 30)
        time.sleep(1)
        starts = [e for e in read_jsonl(record) if e["event"] == "start"]
        running = [e["pid"] for e in starts if alive(e["pid"])]
        result["runningBeforeShutdown"] = len(running)
        result["secretsBeforeShutdown"] = snapshot_secrets(root, fake.url, port, spki_pem(fake.mint_key.public_key()), key_pair)
        if name == "update-restart":
            marker = root / "runtime" / "desktop-update-restart"
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text("")
        handoff = load_installer().mark_tunnel_handoff(str(root)) if name == "confirmed-restart" else None
        before = len(fake.requests)
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=30)
            result["serverExited"] = True
        except subprocess.TimeoutExpired:
            result["serverExited"] = False
        result["connectorsLeftRunning"] = sum(alive(pid) for pid in running)
        if handoff is not None:
            result["handoffMarkerConsumed"] = not handoff.exists()
            load_installer().clear_tunnel_handoff(handoff)
        requests = list(fake.requests)
    result["startupRequests"] = [normalize_request(r, fake.url, public_key, port) for r in requests[:before]]
    result["shutdownRequests"] = [normalize_request(r, fake.url, public_key, port) for r in requests[before:]]
    events = read_jsonl(record)
    result["starts"] = [normalize_text(normalize_start(e, root)) for e in events if e["event"] == "start"]
    result["secretsAfterShutdown"] = snapshot_secrets(root, fake.url, port, spki_pem(fake.mint_key.public_key()), key_pair)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--scenario", action="append", choices=SCENARIOS)
    parser.add_argument("--dump", help="write both results as JSON to this path")
    parser.add_argument("--jobs", type=int, default=12, help="scenario runs at once (1 runs them one by one)")
    args = parser.parse_args()
    if not sys.platform.startswith("linux"):
        print("relay provisioning parity reads /proc and runs on Linux only")
        return 0
    credentials = secrets.token_urlsafe(32), secrets.token_bytes(32)
    environment_key = ed25519.Ed25519PrivateKey.generate()
    private_pem = environment_key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                                serialization.NoEncryption()).decode()
    public_key = spki_pem(environment_key.public_key())
    key_pair = json.dumps({"privateKey": private_pem, "publicKey": public_key}, separators=(",", ":"))
    failures = 0
    dump = {}
    with tempfile.TemporaryDirectory(prefix="relay-provisioning-tls-") as tls:
        certificates = make_certificates(Path(tls))
        keys = ed25519.Ed25519PrivateKey.generate(), ed25519.Ed25519PrivateKey.generate()

        def run(name, label, command):
            fake = FakeConnect(certificates, *keys)
            try:
                with tempfile.TemporaryDirectory(prefix=f"relayprov-{label}-") as temporary:
                    return run_scenario(name, shlex.split(command), Path(temporary), credentials, fake,
                                        certificates[0], key_pair, public_key)
            finally:
                fake.close()

        names = args.scenario or SCENARIOS
        runs = [(name, label, command) for name in names
                for label, command in (("original", args.original_command), ("rust", args.rust_command))]
        done = iter(run_concurrently([lambda run_args=run_args: run(*run_args) for run_args in runs], args.jobs))
        for name in names:
            results = [next(done), next(done)]
            dump[name] = {"original": results[0], "rust": results[1]}
            differences = diff(*results)
            if differences:
                failures += 1
                print(f"FAIL {name}")
                for line in differences:
                    print(f"  {line}")
            else:
                print(f"PASS {name} ({len(results[0]['startupRequests'])} startup requests, "
                      f"{len(results[0]['shutdownRequests'])} on shutdown, {len(results[0]['starts'])} connector starts)")
    if args.dump:
        Path(args.dump).write_text(json.dumps(dump, indent=2, sort_keys=True))
    print(f"{len(dump) - failures}/{len(dump)} relay provisioning scenarios matched")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
