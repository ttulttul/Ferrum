#!/usr/bin/env python3
"""Compare live Chromium preview controls, JPEG frames and file transfers on a fixed page."""
import argparse
from contextlib import ExitStack
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
import json
from pathlib import Path
import secrets
import shlex
from tempfile import TemporaryDirectory
from threading import Condition, Event, Thread
from time import monotonic
from urllib.parse import urlencode

from PIL import Image
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
        elif self.path == "/painted":
            self.end_headers()
            self.server.painted.set()
        elif self.path == "/clicked":
            self.end_headers()
            with self.server.clicked:
                self.server.clicks += 1
                self.server.clicked.notify_all()
        else:
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"""<!doctype html><title>Preview fixture</title>
                <style>body {background: #fff} input,a {display:block;width:250px;height:60px}</style>
                <input type=file><a href="/file" download="sample.txt">Download</a>
                <script>
                  const colors = ["#357", "#a53", "#248"];
                  document.addEventListener("click", event => {
                    if (event.clientY <= 150 || !colors.length) return;
                    document.body.style.background = colors.shift();
                    fetch("/clicked");
                    if (!colors.length)
                      requestAnimationFrame(() => requestAnimationFrame(() => fetch("/painted")));
                  });
                  // The ready colour comes from this script, after the listener is attached:
                  // Chromium can paint a stylesheet colour before the parser runs this block,
                  // and a click sent on that paint was lost on slow CI runners ("saw []").
                  document.body.style.background = "#793";
                </script>""")

    def log_message(self, *args):
        pass


def observe(name, server, page_url, page):
    painted = page.painted
    painted.clear()
    with page.clicked:
        page.clicks = 0
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
            def receive(kind, *, color=None, size=None, hold=False, wait=30):
                # Chromium may emit a blank document, live frames, and full-size stills
                # during navigation. Identify a paint before comparing its encoded bytes.
                # A shared CI runner can take well over 10 s to start Chromium and stream
                # its first frame; matching returns at once, so a long wait costs nothing.
                deadline = monotonic() + wait
                observed = []
                while (remaining := deadline - monotonic()) > 0:
                    try:
                        message = viewer.recv(timeout=remaining)
                    except TimeoutError:
                        break
                    if isinstance(message, bytes):
                        if color is not None:
                            with Image.open(BytesIO(message)) as frame:
                                if frame.format != "JPEG":
                                    raise ValueError(f"Unexpected preview encoding {frame.format}")
                                pixel = frame.getpixel((frame.width // 2, frame.height // 2))
                                observed.append((frame.size, pixel))
                                if (size is None or frame.size == size) and all(
                                    abs(a - b) <= 2 for a, b in zip(pixel, color, strict=True)
                                ):
                                    if not hold:
                                        viewer.send('{"type":"ack"}')
                                    return message
                        viewer.send('{"type":"ack"}')
                    else:
                        event = json.loads(message)
                        if event["type"] in ("control", "viewport"):
                            result.setdefault(event["type"], message.encode())
                        if event["type"] == kind:
                            return event
                raise RuntimeError(f"{name} timed out waiting for {kind}, {color=}, {size=}; saw {observed}")

            def click(y):
                for action, buttons in (("down", 1), ("up", 0)):
                    viewer.send(json.dumps(dict(type="mouse", action=action, x=30, y=y,
                                                button="left", buttons=buttons, clickCount=1)))

            def recolor():
                # Chromium can drop an input event that arrives before the page is ready
                # for input, and then no frame follows ("saw []"). The page reports each
                # click it handles, so a click is sent again only when the page never saw
                # it, which keeps the colour sequence intact.
                with page.clicked:
                    expected = page.clicks + 1
                    for _ in range(6):
                        click(200)
                        if page.clicked.wait_for(lambda: page.clicks >= expected, timeout=5):
                            return
                raise RuntimeError(f"{name}: the page did not register a click")

            receive("ready paint", color=(119, 153, 51), wait=60)
            recolor()
            result["first paint JPEG"] = receive("first paint", color=(51, 85, 119), size=(1280, 800))
            recolor()
            result["second paint JPEG"] = receive("second paint", color=(170, 85, 51),
                                                  size=(1280, 800), hold=True)
            # Keep one live frame in flight through the final paint. Both servers
            # should deliver a full-resolution still once the burst settles.
            recolor()
            if not painted.wait(30):
                raise RuntimeError("Page did not report its final paint")
            viewer.send('{"type":"ack"}')
            result["settled JPEG"] = receive("settled paint", color=(34, 68, 136), size=(2560, 1600))
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
        page.painted = Event()
        page.clicked = Condition()
        page.clicks = 0
        Thread(target=page.serve_forever, daemon=True).start()
        stack.callback(page.shutdown)
        url = f"http://127.0.0.1:{page.server_port}/"
        secret = secrets.token_urlsafe(32)
        signing_key = secrets.token_bytes(32)
        servers = {}
        for name, cmd in (("original", args.original_command), ("rust", args.rust_command)):
            root = Path(directory, name)
            (root / ".cache").mkdir(parents=True)
            (root / ".cache/ms-playwright").symlink_to(args.browser_cache.resolve(), target_is_directory=True)
            servers[name] = managed_server(stack, shlex.split(cmd), root, secret, signing_key)
        original = observe("original", servers["original"], url, page)
        rust = observe("rust", servers["rust"], url, page)
        failed = 0
        for key in ("control", "viewport", "first paint JPEG", "second paint JPEG",
                    "settled JPEG", "upload", "download", "socket close"):
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
