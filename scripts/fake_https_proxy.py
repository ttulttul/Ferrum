"""An HTTPS proxy for parity tests that answers for any host itself.

Both servers honor it through their runtimes' own proxy settings: Node's `fetch` with
NODE_USE_ENV_PROXY=1 (which `parity_harness.offline_env()` already sets) and HTTPS_PROXY, trusting
NODE_EXTRA_CA_CERTS, and the Rust server under the same variables. It takes the place of the
harness's refusing proxy for HTTPS only. A client's CONNECT is answered by terminating TLS with a certificate for the
requested host, signed by a CA made for this proxy, and the request is answered from `routes`:

    {"GET registry.npmjs.org/@openai%2Fcodex/latest": {"status": 200, "body": {...}}}

`body` is sent as JSON, or as given when it is a string. Unknown routes answer 404, so nothing
reaches the network. Every request is recorded as "METHOD https://host/path", followed by its JSON
body when it has one (with a random `request_id` shown as "<request id>").
"""
from http.server import BaseHTTPRequestHandler
import json
import os
from pathlib import Path
import socket
import socketserver
import ssl
import subprocess
import tempfile
import threading


class FakeHttpsProxy:
    def __init__(self, routes, directory=None):
        self.routes = routes
        self.requests = []
        self.directory = Path(directory or tempfile.mkdtemp(prefix="t3-proxy-"))
        self.directory.mkdir(parents=True, exist_ok=True)
        self.ca_cert = self.directory / "ca.pem"
        self.ca_key = self.directory / "ca.key"
        self._contexts = {}
        self._lock = threading.Lock()
        self._make_ca()
        proxy = self

        class Tunnel(socketserver.StreamRequestHandler):
            def handle(self):
                proxy._tunnel(self.connection, self.rfile)

        class Server(socketserver.ThreadingTCPServer):
            daemon_threads = True
            allow_reuse_address = True

        self.server = Server(("127.0.0.1", 0), Tunnel)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def env(self):
        """The variables that send both servers' HTTPS to this proxy and trust its CA. Everything
        else stays as `parity_harness.offline_env()` sets it (NODE_USE_ENV_PROXY, NO_PROXY for
        loopback, and the refusing proxy for plain HTTP), so nothing leaves loopback either way:
        this proxy answers every host itself and forwards nothing."""
        return {"HTTPS_PROXY": self.url, "https_proxy": self.url, "NODE_EXTRA_CA_CERTS": str(self.ca_cert)}

    def close(self):
        self.server.shutdown()
        self.server.server_close()

    def _openssl(self, *args):
        subprocess.run(["openssl", *args], check=True, capture_output=True)

    def _make_ca(self):
        self._openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
                      "-subj", "/CN=T3 parity test CA", "-keyout", str(self.ca_key), "-out", str(self.ca_cert),
                      "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign")

    def _context(self, host):
        """A server TLS context with a certificate for `host`, made on first use."""
        with self._lock:
            if host in self._contexts:
                return self._contexts[host]
            key, csr, cert, ext = (self.directory / f"{host}.{suffix}" for suffix in ("key", "csr", "pem", "ext"))
            ext.write_text(f"subjectAltName=DNS:{host}\nextendedKeyUsage=serverAuth\n")
            self._openssl("req", "-newkey", "rsa:2048", "-nodes", "-subj", f"/CN={host}",
                          "-keyout", str(key), "-out", str(csr))
            self._openssl("x509", "-req", "-in", str(csr), "-CA", str(self.ca_cert), "-CAkey", str(self.ca_key),
                          "-CAcreateserial", "-days", "2", "-extfile", str(ext), "-out", str(cert))
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(str(cert), str(key))
            self._contexts[host] = context
            return context

    def _tunnel(self, connection, rfile):
        line = rfile.readline().decode("latin-1").strip()
        while rfile.readline() not in (b"\r\n", b"\n", b""):
            pass
        parts = line.split()
        if len(parts) < 2 or parts[0] != "CONNECT":
            connection.sendall(b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n")
            return
        host = parts[1].rsplit(":", 1)[0]
        connection.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        try:
            tls = self._context(host).wrap_socket(connection, server_side=True)
        except (ssl.SSLError, OSError):
            return
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def answer(self):
                length = int(self.headers.get("Content-Length") or 0)
                sent = self.rfile.read(length) if length else b""
                path = self.path.split("?", 1)[0]
                record = f"{self.command} https://{host}{path}"
                if sent:
                    try:
                        payload = json.loads(sent)
                    except ValueError:
                        payload = sent.decode("latin-1")
                    if isinstance(payload, dict) and "request_id" in payload:
                        payload["request_id"] = "<request id>"
                    record += " " + json.dumps(payload, sort_keys=True)
                proxy.requests.append(record)
                route = proxy.routes.get(f"{self.command} {host}{path}", {"status": 404, "body": {"error": "not found"}})
                body = route.get("body", {})
                data = body.encode() if isinstance(body, str) else json.dumps(body).encode()
                self.send_response(route.get("status", 200))
                self.send_header("Content-Type", route.get("contentType", "text/plain" if isinstance(body, str)
                                                         else "application/json"))
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(data)
                self.close_connection = True

            do_GET = do_POST = answer

            def log_message(self, *_):
                pass

        try:
            Handler(tls, ("127.0.0.1", 0), None)
        except (ssl.SSLError, OSError, ConnectionError):
            pass
        finally:
            try:
                tls.close()
            except OSError:
                pass
