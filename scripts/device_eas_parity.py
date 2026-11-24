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

A second server per side runs Expo Simulators sessions against a stateful fake: Expo's session
API (create, poll, list, stop) with each session's preview host and agent-device daemon on
loopback HTTP. It is checked for reaping a session a previous server recorded and left running,
opening a session (the ledger, the poll, the preview host's device id), the concurrency limit,
listing, screenshots and requests through the device hub proxy (paths, `device` and the preview
token rewritten; a WebSocket upgrade carrying the `serve-emu.token.*` protocol), and closing with
shutdown, which stops the session and clears its record.
Nothing may leave loopback: anything but api.expo.dev is refused and fails the suite.
"""
import argparse
import base64
from contextlib import ExitStack
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import socket
import ssl
import struct
import threading
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from tempfile import TemporaryDirectory
from time import monotonic, sleep

from websockets.sync.client import connect

from client_thread_parity import scenario_env
from device_ssh_parity import call
from parity_harness import assert_nothing_escaped, http, managed_server

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

    def __init__(self, cert, key, log, preview_url=None, agent_url=None):
        fake = self
        self.log, self.lock = log, threading.Lock()
        # Simulator sessions by id: {status, name, appId}. "sess-old" is one a previous server left.
        self.sessions = {"sess-old": {"status": "IN_PROGRESS", "name": LEFTOVER, "appId": "app-1"}}
        self.created = 0

        def session_api(body):
            operation, variables = body.get("operationName"), body.get("variables") or {}
            with fake.lock:
                if operation == "T3CreateEasSimulatorSession":
                    fake.created += 1
                    sid = f"sess-{fake.created}"
                    fake.sessions[sid] = {"status": "IN_PROGRESS", "name": variables["input"]["name"],
                                          "appId": variables["input"]["appId"]}
                    return {"deviceRunSession": {"createDeviceRunSession": {"id": sid}}}
                if operation == "T3EasSimulatorSession":
                    sid = variables["id"]
                    session = fake.sessions.get(sid)
                    if session is None:
                        return None
                    return {"deviceRunSessions": {"byId": {"id": sid, "status": session["status"], "remoteConfig": {
                        "__typename": "AgentDeviceRunSessionRemoteConfig",
                        "agentDeviceRemoteSessionUrl": agent_url, "agentDeviceRemoteSessionToken": f"agent-{sid}",
                        "webPreviewToken": f"preview-{sid}", "previewApiUrl": f"{preview_url}/api/{sid}"}}}}
                if operation == "T3ActiveEasSimulatorSessions":
                    statuses = set((variables.get("filter") or {}).get("statuses") or ())
                    return {"app": {"byId": {"deviceRunSessionsPaginated": {"edges": [
                        {"node": {"id": sid, "status": session["status"], "name": session["name"]}}
                        for sid, session in fake.sessions.items()
                        if session["appId"] == variables["appId"] and (not statuses or session["status"] in statuses)
                    ]}}}}
                if operation == "T3StopEasSimulatorSession":
                    session = fake.sessions.get(variables["id"])
                    if session is None:
                        return None
                    session["status"] = "STOPPED"
                    return {"deviceRunSession": {"ensureDeviceRunSessionStopped": {
                        "id": variables["id"], "status": "STOPPED"}}}
            return None

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
                    if isinstance(body, dict) and body.get("operationName") != "T3ExpoViewer":
                        data = session_api(body)
                        if data is None:
                            return self.answer(200, {"data": None, "errors": [{"message": "Not found"}]})
                        return self.answer(200, {"data": data})
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


LEFTOVER = "T3 Code · Expo cloud · 0a0b0c"


def png_chunk(kind, data):
    return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data))


# A 1x1 RGBA PNG.
PNG = (b"\x89PNG\r\n\x1a\n" + png_chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 6, 0, 0, 0))
       + png_chunk(b"IDAT", zlib.compress(b"\x00\x00\x00\x00\x00")) + png_chunk(b"IEND", b""))


class FakeSessionHost:
    """Each session's preview host (serve-sim at its root, serve-emu under /vendor/serve-emu) and
    its agent-device daemon, on loopback HTTP. Requests are logged with the token each carried."""

    def __init__(self, log):
        fake = self
        self.log, self.lock = log, threading.Lock()

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def record(self, **extra):
                entry = {"method": self.command, "path": self.path,
                         "authorization": self.headers.get("authorization"), **extra}
                with fake.lock, open(fake.log, "a") as file:
                    file.write(json.dumps(entry) + "\n")

            def answer(self, status, body, kind="application/json"):
                data = body if isinstance(body, bytes) else json.dumps(body).encode()
                self.send_response(status)
                self.send_header("content-type", kind)
                self.send_header("content-length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                if self.headers.get("upgrade", "").lower() == "websocket":
                    protocol = self.headers.get("sec-websocket-protocol")
                    self.record(protocol=protocol)
                    accept = base64.b64encode(hashlib.sha1(
                        (self.headers["sec-websocket-key"] + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()
                    ).digest()).decode()
                    # A real preview host answers HTTP/1.1 (tungstenite refuses a 1.0 handshake).
                    self.protocol_version = "HTTP/1.1"
                    self.send_response(101)
                    self.send_header("upgrade", "websocket")
                    self.send_header("connection", "Upgrade")
                    self.send_header("sec-websocket-accept", accept)
                    if protocol:
                        self.send_header("sec-websocket-protocol", protocol.split(",")[0].strip())
                    self.end_headers()
                    message = f"frames for {self.path}".encode()
                    self.wfile.write(bytes([0x81, len(message)]) + message + bytes([0x88, 0x00]))
                    self.wfile.flush()
                    self.close_connection = True
                    return
                self.record()
                path = self.path.split("?", 1)[0]
                if path == "/api":
                    return self.answer(200, {"device": "SIM-UDID-1"})
                if path == "/vendor/serve-emu/api":
                    return self.answer(200, {"serial": "emulator-5554"})
                if path.endswith("/api/screenshot"):
                    return self.answer(200, PNG, "image/png")
                return self.answer(404, {"error": "not found"})

            def do_POST(self):
                raw = self.rfile.read(int(self.headers.get("content-length") or 0))
                try:
                    body = json.loads(raw)
                except ValueError:
                    body = raw.decode(errors="replace")
                self.record(body=body)
                if self.path.split("?", 1)[0].endswith("/api/screenshot"):
                    return self.answer(200, PNG, "image/png")
                return self.answer(200, {"jsonrpc": "2.0", "id": body.get("id") if isinstance(body, dict) else None,
                                         "result": {"ok": True}})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
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


def proxied(url, cookie, route):
    status, headers, body = http(url, "GET", route, headers={"Cookie": cookie}, timeout=60)
    kind = headers.get("content-type", "").split(";")[0]
    return {"status": status, "type": kind, "body": body.hex() if kind == "image/png" else body.decode(errors="replace")}


def proxied_socket(url, cookie, route):
    try:
        with connect(url.replace("http", "ws", 1) + route, additional_headers={"Cookie": cookie},
                     proxy=None, open_timeout=30) as ws:
            return {"received": ws.recv(timeout=30)}
    except Exception as error:  # noqa: BLE001 - the outcome is what is compared
        response = getattr(error, "response", None)
        return {"failed": type(error).__name__, "status": getattr(response, "status_code", None),
                "body": getattr(response, "body", b"").decode(errors="replace")[:300]}


def ledger(server_root):
    files = sorted(server_root.rglob("device/eas/*.json"))
    return {str(path.relative_to(server_root)): json.loads(path.read_text()) for path in files}


def wait_log(log, predicate, seconds=60):
    deadline = monotonic() + seconds
    while monotonic() < deadline:
        lines = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
        if any(predicate(entry) for entry in lines):
            return True
        sleep(0.2)
    return False


def run_sessions(name, command, directory, ca, proxy, expo_log):
    """Simulator sessions on a host whose previous server left a session running."""
    root = Path(directory, f"{name}-sessions")
    root.mkdir()
    env, _, _ = scenario_env(root, "eas-sessions", root / "server", {"name": "eas-sessions", "scenario": None}, set())
    for variable in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
        env[variable] = env[variable.lower()] = proxy.url
    env["NODE_EXTRA_CA_CERTS"] = str(ca)
    env["T3CODE_TEST_EXTRA_CA_FILE"] = str(ca)
    env.pop("EXPO_TOKEN", None)
    server_root = root / "server"
    # The previous server's ledger: a session Expo still runs, and one too old to be running.
    now = datetime.datetime.now().timestamp() * 1000
    recorded = server_root / "userdata" / "device" / "eas" / "expo.json"
    recorded.parent.mkdir(parents=True)
    recorded.write_text(json.dumps([
        {"name": LEFTOVER, "appId": "app-1", "recordedAt": now - 60_000},
        {"name": "T3 Code · Expo cloud · 0d0e0f", "appId": "app-1", "recordedAt": now - 4 * 3600_000},
    ]))
    answers = []
    thread = {"threadId": "thread-eas", "hostId": "expo"}
    with ExitStack() as stack:
        url, cookie = managed_server(stack, shlex.split(command), server_root, secrets.token_urlsafe(32),
                                     secrets.token_bytes(32), env=env)
        call(url, cookie, "server.updateSettings", {"patch": {"easDeviceHosts": [
            {"kind": "eas", "id": "expo", "label": "Expo cloud", "accessToken": "tok-ok", "appId": "app-1",
             "maxConcurrentSessions": 1},
        ]}})
        wait_state(url, cookie, lambda s: len(s.get("easHosts", [])) == 1)
        answers.append(("enable", call(url, cookie, "device.configure", {"enabled": True})))
        stopped = wait_log(expo_log, lambda e: isinstance(e["body"], dict)
                           and e["body"].get("operationName") == "T3StopEasSimulatorSession")
        sleep(1)
        answers.append(("leftover reaped", {"stopped": stopped, "ledger": ledger(server_root)}))
        answers.append(("open android", call(url, cookie, "device.open", {
            **thread, "deviceId": "eas-android", "platform": "android"})))
        answers.append(("ledger while running", ledger(server_root)))
        answers.append(("open over the limit", call(url, cookie, "device.open", {
            **thread, "deviceId": "eas-ios", "platform": "ios"})))
        answers.append(("list", call(url, cookie, "device.list", {})))
        for label, route in [
            ("proxy screenshot", "/api/device-hub/vendor/serve-emu/api/screenshot?hostId=expo&device=sess-1"),
            ("proxy api (the only session)", "/api/device-hub/vendor/serve-emu/api?hostId=expo"),
            ("proxy api by device", "/api/device-hub/vendor/serve-emu/api?hostId=expo&device=sess-1&x=1"),
            ("proxy other platform", "/api/device-hub/vendor/serve-sim/api?hostId=expo&device=sess-1"),
            ("proxy unknown device", "/api/device-hub/vendor/serve-emu/api?hostId=expo&device=sess-9"),
            ("proxy outside the hub", "/api/device-hub/api/devices?hostId=expo"),
        ]:
            answers.append((label, proxied(url, cookie, route)))
        answers.append(("proxy socket", proxied_socket(
            url, cookie, "/api/device-hub/vendor/serve-emu/ws?hostId=expo&device=sess-1")))
        answers.append(("proxy socket of the other platform", proxied_socket(
            url, cookie, "/api/device-hub/vendor/serve-sim/helper/ws?hostId=expo&device=sess-1")))
        answers.append(("close with shutdown", call(url, cookie, "device.close", {
            **thread, "deviceId": "sess-1", "shutdown": True})))
        answers.append(("list after close", call(url, cookie, "device.list", {})))
        answers.append(("ledger after close", ledger(server_root)))
    text = json.dumps(answers).replace(str(root), "<root>")
    return json.loads(text)


def normalized(value):
    """Session names carry a random suffix; records and open devices the time they were made."""
    text = re.sub(r"(T3 Code · [^\"]*? · )(?!0a0b0c|0d0e0f)[0-9a-f]{6}", r"\1<random>",
                  json.dumps(value, ensure_ascii=False))
    value = json.loads(text)

    def walk(node):
        if isinstance(node, dict):
            return {k: ("<time>" if k in ("recordedAt", "openedAt") else walk(v)) for k, v in node.items()}
        if isinstance(node, list):
            return [walk(v) for v in node]
        return node
    return walk(value)


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
            # Sessions: a fresh fake, with each session's preview host and daemon.
            hosts_log = Path(directory, f"{name}-session-hosts.jsonl")
            hosts = FakeSessionHost(hosts_log)
            sessions_log = Path(directory, f"{name}-expo-sessions.jsonl")
            expo = FakeExpo(cert, key, sessions_log, preview_url=hosts.url, agent_url=hosts.url)
            refused = list(proxy.refused)
            proxy = ExpoProxy(expo.port)
            answers += run_sessions(name, command, directory, ca, proxy, sessions_log)
            expo.close()
            hosts.close()
            read = lambda path: [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
            port = hosts.url.rsplit(":", 1)[1]
            results[name] = normalized({
                "answers": json.loads(json.dumps(answers).replace(port, "<port>")),
                "requests": requests,
                "session requests": json.loads(json.dumps(read(sessions_log)).replace(port, "<port>")),
                "session hosts": read(hosts_log),
                "refused": sorted({entry.split(" ", 1)[1] for entry in refused + proxy.refused})})
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
    for key in ("requests", "session requests", "session hosts"):
        total += 1
        left, right = results["original"][key], results["rust"][key]
        same = left == right
        failures += not same
        print(f"{'PASS' if same else 'FAIL'} {key} ({len(left)} vs {len(right)})")
        if not same:
            print(f"  original {json.dumps(left, indent=1)[:8000]}")
            print(f"  rust     {json.dumps(right, indent=1)[:8000]}")
    for name in ("original", "rust"):
        escaped = sorted(set(results[name]["refused"]) - STARTUP_FETCHES)
        total += 1
        failures += bool(escaped)
        print(f"{'FAIL' if escaped else 'PASS'} {name} reached nothing but Expo's API{f': {escaped}' if escaped else ''}")
    print(f"{total - failures}/{total} Expo device host checks matched across the reference and the Rust server")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
