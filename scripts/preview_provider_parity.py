#!/usr/bin/env python3
"""Compare live Chromium preview controls, JPEG frames and file transfers on a fixed page."""
import argparse
from contextlib import ExitStack
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import secrets
import shlex
from tempfile import TemporaryDirectory
from threading import Thread
from urllib.parse import urlencode

from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect

from parity_harness import http, managed_server


class PreviewPage(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        if self.path == "/file":
            self.send_header("Content-Disposition", 'attachment; filename="sample.txt"')
            self.end_headers()
            self.wfile.write(b"Preview download fixture\n")
        else:
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"""<!doctype html><title>Preview fixture</title>
                <style>body {background: #793} input,a {display:block;width:250px;height:60px}</style>
                <input type=file><a href="/file" download="sample.txt">Download</a>""")

    def log_message(self, *args):
        pass


def observe(server, page_url):
    url, cookie = server
    headers = {"Cookie": cookie}
    ws = url.replace("http", "ws", 1)
    result = {}
    with connect(ws + "/ws?orchestrationProtocol=2", additional_headers=headers, proxy=None) as rpc:
        def call(request_id, method, payload):
            rpc.send(json.dumps(dict(_tag="Request", id=request_id, tag=method, payload=payload, headers=[])))
            reply = json.loads(rpc.recv(timeout=10))["exit"]
            if reply["_tag"] != "Success":
                raise RuntimeError(f"{method} failed: {reply}")
            return reply["value"]

        opened = call("1", "preview.open", dict(threadId="preview-parity", url=page_url, runtime="server"))
        tab = {key: opened[key] for key in ("threadId", "tabId")}
        target = ws + "/api/preview-stream/ws?" + urlencode(tab)
        with connect(target, additional_headers=headers, proxy=None, compression=None) as viewer:
            def receive(kind):
                while True:
                    message = viewer.recv(timeout=10)
                    if isinstance(message, bytes):
                        viewer.send('{"type":"ack"}')
                        if "JPEG" not in result:
                            result["JPEG"] = message
                        elif message!=result["JPEG"]:
                            result.setdefault("page JPEG",message)
                        if kind == "jpeg":
                            return
                    else:
                        event = json.loads(message)
                        if event["type"] in ("control", "viewport"):
                            result.setdefault(event["type"], message.encode())
                        if event["type"] == kind:
                            return event

            def click(y):
                for action, buttons in (("down", 1), ("up", 0)):
                    viewer.send(json.dumps(dict(type="mouse", action=action, x=30, y=y,
                                                button="left", buttons=buttons, clickCount=1)))

            receive("jpeg")
            # The first frame can show the blank document preceding navigation.
            receive("jpeg")
            click(25)
            chooser = receive("fileChooser")
            multipart = (b'--T3Fixture\r\nContent-Disposition: form-data; name="file"; '
                         b'filename="input.txt"\r\nContent-Type: text/plain\r\n\r\nInput\r\n--T3Fixture--\r\n')
            status, _, body = http(url, "POST", "/api/preview-stream/upload?" +
                                   urlencode({**tab, "chooser": chooser["id"]}), multipart,
                                   {**headers, "Content-Type": "multipart/form-data; boundary=T3Fixture"})
            if status != 204:
                raise RuntimeError(f"Upload returned {status}")
            result["upload"] = (status, body)
            click(85)
            download = receive("download")
            status, fields, body = http(url, "GET", "/api/preview-stream/download?" +
                                        urlencode({**tab, "id": download["id"]}), headers=headers)
            if body != b"Preview download fixture\n":
                raise RuntimeError(f"Unexpected downloaded file, status {status}")
            result["download"] = (status, fields.get("content-disposition"), body)
            call("2", "preview.close", tab)
            try:
                while True:
                    viewer.recv(timeout=8)
            except ConnectionClosed as close:
                result["socket close"] = (close.rcvd.code, close.rcvd.reason)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--browser-cache", type=Path, required=True,
                        help="Installed Playwright browser cache shared by both servers")
    args = parser.parse_args()
    with TemporaryDirectory() as directory, ExitStack() as stack:
        page = stack.enter_context(ThreadingHTTPServer(("127.0.0.1", 0), PreviewPage))
        Thread(target=page.serve_forever, daemon=True).start()
        stack.callback(page.shutdown)
        url = f"http://127.0.0.1:{page.server_port}/"
        secret = secrets.token_urlsafe(32)
        signing_key = secrets.token_bytes(32)
        servers = []
        for name, cmd in (("original", args.original_command), ("rust", args.rust_command)):
            root = Path(directory, name)
            (root / ".cache").mkdir(parents=True)
            (root / ".cache/ms-playwright").symlink_to(args.browser_cache.resolve(), target_is_directory=True)
            servers.append(managed_server(stack, shlex.split(cmd), root, secret, signing_key))
        original, rust = [observe(server, url) for server in servers]
        failed = 0
        for key in ("control", "viewport", "JPEG", "page JPEG", "upload", "download", "socket close"):
            left, right = original[key], rust[key]
            if left == right:
                print("PASS", key)
            else:
                failed += 1
                if "JPEG" in key:
                    left, right = [f"{len(data)} bytes, SHA256 {sha256(data).hexdigest()}" for data in (left, right)]
                print(f"FAIL {key}\n  original {left}\n  rust     {right}")
        return bool(failed)


if __name__ == "__main__":
    raise SystemExit(main())
