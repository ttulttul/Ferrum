#!/usr/bin/env python3
"""Compare Expo Simulators device hosts on the reference and Rust servers, against a fake of
Expo's API on loopback.

Expo's API is a fixed HTTPS URL (https://api.expo.dev/graphql). This suite generates a CA and a
certificate for api.expo.dev, serves a fake of the API with it on loopback, and gives both servers
its own proxy, which tunnels `api.expo.dev:443` to the fake and refuses everything else. The
reference trusts the CA through NODE_EXTRA_CA_CERTS; the Rust server through
T3CODE_TEST_EXTRA_CA_FILE, which only debug builds read.

Each server is checked for:
- `device.easAccount` with each kind of credential (a token, a robot's, `eas login`'s session)
  and each failure (refused, rejected by Expo, a server error, an unusable token, none at all);
- the Expo hosts it lists from `easDeviceHosts` (as `easHosts`), with and without a credential;
- turning device support on, which lists each host's catalog;
- every request either server sent to Expo.
Nothing may leave loopback: anything but api.expo.dev is refused and fails the suite.
"""
import argparse
from contextlib import ExitStack
import datetime
import json
import os
from pathlib import Path
import secrets
import shlex
import socket
import ssl
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from tempfile import TemporaryDirectory
from time import monotonic, sleep

from client_thread_parity import scenario_env
from device_ssh_parity import call
from parity_harness import assert_nothing_escaped, managed_server

EXPO_HOST = "api.expo.dev"


def make_certificates(directory: Path):
    """A test CA, and a certificate for api.expo.dev it signs: (ca.pem, cert.pem, key.pem)."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    now = datetime.datetime.now(datetime.timezone.utc)
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "T3 parity test CA")])
    ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name)
          .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
          .not_valid_before(now - datetime.timedelta(days=1)).not_valid_after(now + datetime.timedelta(days=2))
          .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
          .add_extension(x509.KeyUsage(digital_signature=True, key_cert_sign=True, crl_sign=True,
                                       content_commitment=False, key_encipherment=False, data_encipherment=False,
                                       key_agreement=False, encipher_only=False, decipher_only=False), critical=True)
          .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False)
          .sign(ca_key, hashes.SHA256()))
    key = ec.generate_private_key(ec.SECP256R1())
    cert = (x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, EXPO_HOST)]))
            .issuer_name(ca_name).public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(days=1)).not_valid_after(now + datetime.timedelta(days=2))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName(EXPO_HOST)]), critical=False)
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
            .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False)
            .sign(ca_key, hashes.SHA256()))
    paths = directory / "ca.pem", directory / "cert.pem", directory / "key.pem"
    paths[0].write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    paths[1].write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    paths[2].write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                           serialization.NoEncryption()))
    return paths


VIEWER = {"meActor": {"__typename": "User", "username": "tester", "accounts": [
    {"name": "tester", "appsPaginated": {"edges": [{"node": {"id": "app-1", "fullName": "@tester/app"}}]}},
    {"name": "team", "appsPaginated": {"edges": [{"node": {"id": "app-2", "fullName": "@team/shared"}}]}},
]}}


class FakeExpo:
    """Expo's GraphQL API over TLS on loopback, answering by the credential each request carries."""

    def __init__(self, cert, key, log):
        fake = self
        self.log, self.lock = log, threading.Lock()

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def answer(self, status, body, kind="application/graphql-response+json"):
                data = body if isinstance(body, bytes) else json.dumps(body).encode()
                self.send_response(status)
                self.send_header("content-type", kind)
                self.send_header("content-length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_POST(self):
                raw = self.rfile.read(int(self.headers.get("content-length") or 0))
                try:
                    body = json.loads(raw)
                except ValueError:
                    body = raw.decode(errors="replace")
                credential = self.headers.get("authorization") or self.headers.get("expo-session")
                entry = {"path": self.path, "accept": self.headers.get("accept"),
                         "content-type": self.headers.get("content-type"),
                         "authorization": self.headers.get("authorization"),
                         "expo-session": self.headers.get("expo-session"), "body": body}
                with fake.lock, open(fake.log, "a") as file:
                    file.write(json.dumps(entry) + "\n")
                if credential in ("Bearer tok-ok", "sess-ok"):
                    return self.answer(200, {"data": VIEWER})
                if credential == "Bearer tok-robot":
                    return self.answer(200, {"data": {"meActor": {"__typename": "Robot", "firstName": None, "accounts": []}}})
                if credential == "Bearer tok-user-error":
                    return self.answer(200, {"data": None, "errors": [
                        {"message": "This account has no access", "extensions": {"errorType": "USER"}}]})
                if credential == "Bearer tok-500":
                    return self.answer(500, b"upstream failed", "text/plain")
                return self.answer(401, {"errors": [{"message": "Unauthorized"}]}, "application/json")

        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert, key)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.socket = context.wrap_socket(self.server.socket, server_side=True)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()


class ExpoProxy:
    """The servers' HTTP(S) proxy: CONNECT api.expo.dev:443 reaches the fake; anything else is
    refused and recorded."""

    def __init__(self, expo_port):
        self.refused = []
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def refuse(self):
                proxy.refused.append(f"{self.command} {self.path}")
                self.send_response(403)
                self.send_header("content-length", "0")
                self.send_header("connection", "close")
                self.end_headers()

            def do_CONNECT(self):
                if self.path != f"{EXPO_HOST}:443":
                    return self.refuse()
                upstream = socket.create_connection(("127.0.0.1", expo_port))
                self.send_response(200, "Connection established")
                self.end_headers()
                client = self.connection

                def pipe(source, sink):
                    try:
                        while chunk := source.recv(65536):
                            sink.sendall(chunk)
                    except OSError:
                        pass
                    finally:
                        for end in (source, sink):
                            try:
                                end.shutdown(socket.SHUT_RDWR)
                            except OSError:
                                pass

                forward = threading.Thread(target=pipe, args=(client, upstream), daemon=True)
                forward.start()
                pipe(upstream, client)
                forward.join(timeout=5)
                self.close_connection = True

            do_GET = do_POST = do_PUT = do_DELETE = do_HEAD = refuse

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()


# The startup fetches both servers attempt, which the proxy refuses as the harness's would.
STARTUP_FETCHES = {"raw.githubusercontent.com:443", "registry.npmjs.org:443"}


def wait_state(url, cookie, check, seconds=30):
    """The device state once `check` holds and it has settled (a change publishes more than once)."""
    deadline = monotonic() + seconds
    previous = None
    while True:
        state = call(url, cookie, "device.list", {})
        if (check(state.get("value", {})) and state == previous) or monotonic() > deadline:
            return state
        previous = state if check(state.get("value", {})) else None
        sleep(0.5)


def run_server(name, command, directory, ca, proxy):
    root = Path(directory, name)
    root.mkdir()
    env, _, _ = scenario_env(root, "eas", root / "server", {"name": "eas", "scenario": None}, set())
    for variable in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
        env[variable] = env[variable.lower()] = proxy.url
    env["NODE_EXTRA_CA_CERTS"] = str(ca)
    env["T3CODE_TEST_EXTRA_CA_FILE"] = str(ca)
    env.pop("EXPO_TOKEN", None)
    server_root = root / "server"
    server_root.mkdir()
    answers = []
    with ExitStack() as stack:
        url, cookie = managed_server(stack, shlex.split(command), server_root, secrets.token_urlsafe(32),
                                     secrets.token_bytes(32), env=env)
        for label, payload in [
            ("account with a token", {"accessToken": "tok-ok"}),
            ("account of a robot", {"accessToken": "tok-robot"}),
            ("account refused", {"accessToken": "tok-bad"}),
            ("account a user error", {"accessToken": "tok-user-error"}),
            ("account server error", {"accessToken": "tok-500"}),
            ("account unusable token", {"accessToken": "two words"}),
            ("account without a credential", {}),
        ]:
            answers.append((label, call(url, cookie, "device.easAccount", payload)))
        call(url, cookie, "server.updateSettings", {"patch": {"easDeviceHosts": [
            {"kind": "eas", "id": "expo", "label": "Expo cloud", "accessToken": "tok-ok"},
            {"kind": "eas", "id": "unsigned", "label": "No credential"},
        ]}})
        answers.append(("hosts", wait_state(url, cookie, lambda s: len(s.get("easHosts", [])) == 2)))
        answers.append(("account of a saved host", call(url, cookie, "device.easAccount", {"hostId": "expo"})))
        # `eas login` keeps its session in ~/.expo/state.json.
        expo_state = server_root / ".expo" / "state.json"
        expo_state.parent.mkdir(parents=True, exist_ok=True)
        expo_state.write_text(json.dumps({"auth": {"sessionSecret": "sess-ok"}}))
        answers.append(("account from eas login", call(url, cookie, "device.easAccount", {})))
        answers.append(("enable lists the catalog", call(url, cookie, "device.configure", {"enabled": True})))
        answers.append(("list", call(url, cookie, "device.list", {})))
    text = json.dumps(answers).replace(str(root), "<root>")
    return json.loads(text)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--dump", type=Path, help="write both servers' results as JSON")
    args = parser.parse_args()
    results = {}
    with TemporaryDirectory() as directory:
        ca, cert, key = make_certificates(Path(directory))
        for name, command in (("original", args.original_command), ("rust", args.rust_command)):
            log = Path(directory, f"{name}-expo.jsonl")
            expo = FakeExpo(cert, key, log)
            proxy = ExpoProxy(expo.port)
            answers = run_server(name, command, directory, ca, proxy)
            expo.close()
            requests = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
            results[name] = {"answers": answers, "requests": requests,
                             "refused": sorted({entry.split(" ", 1)[1] for entry in proxy.refused})}
    assert_nothing_escaped()
    if args.dump:
        args.dump.write_text(json.dumps(results, indent=1, sort_keys=True))
    failures, total = 0, 0
    for (label, left), (_, right) in zip(results["original"]["answers"], results["rust"]["answers"], strict=True):
        total += 1
        same = left == right
        failures += not same
        print(f"{'PASS' if same else 'FAIL'} {label}")
        if not same:
            print(f"  original {json.dumps(left)[:3000]}\n  rust     {json.dumps(right)[:3000]}")
    total += 1
    same = results["original"]["requests"] == results["rust"]["requests"]
    failures += not same
    print(f"{'PASS' if same else 'FAIL'} requests to Expo ({len(results['original']['requests'])} vs {len(results['rust']['requests'])})")
    if not same:
        print(f"  original {json.dumps(results['original']['requests'], indent=1)[:6000]}")
        print(f"  rust     {json.dumps(results['rust']['requests'], indent=1)[:6000]}")
    for name in ("original", "rust"):
        escaped = sorted(set(results[name]["refused"]) - STARTUP_FETCHES)
        total += 1
        failures += bool(escaped)
        print(f"{'FAIL' if escaped else 'PASS'} {name} reached nothing but Expo's API{f': {escaped}' if escaped else ''}")
    print(f"{total - failures}/{total} Expo device host checks matched across the reference and the Rust server")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
