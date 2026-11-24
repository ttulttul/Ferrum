#!/usr/bin/env python3
"""Compare live Chromium preview controls, JPEG frames and file transfers on a fixed page."""
import argparse
from contextlib import ExitStack
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
import json
from pathlib import Path
import platform
import re
import secrets
import shlex
import sys
from tempfile import TemporaryDirectory
from threading import Condition, Event, Thread
from time import monotonic, sleep
from urllib.parse import urlencode

from PIL import Image
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect

from client_thread_parity import FIXTURES, THREAD, rpc, scenario_env, seed_turn
from parity_harness import http, managed_server


class FrameStall(RuntimeError):
    """No frame at all arrived after the page changed colour."""


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
        try:
            return stream(name, url, headers, target, tab, page, painted, result, call)
        except FrameStall:
            try:
                call("9", "preview.close", tab)
            except Exception:
                pass
            raise


def stream(name, url, headers, target, tab, page, painted, result, call):
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
            error = FrameStall if color is not None and not observed else RuntimeError
            raise error(f"{name} timed out waiting for {kind}, {color=}, {size=}; saw {observed}")

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


def observe_with_retry(name, server, page_url, page, attempts=2):
    # On shared CI runners the reference server's preview sometimes streams no frame at all
    # after a click the page did handle ("saw []" with the click confirmed). A fresh preview
    # session sometimes recovers; any other failure, or a frame that differs, still fails.
    for attempt in range(1, attempts + 1):
        try:
            return observe(name, server, page_url, page)
        except FrameStall as stall:
            if attempt == attempts:
                raise
            print(f"RETRY {name} preview session {attempt}: {stall}", file=sys.stderr)


def playwright_headless_shell(cache):
    """The headless shell both servers launch, from the Playwright cache: the newest
    `chromium*` folder holding `chrome-headless-shell-linux64/chrome-headless-shell`."""
    for folder in sorted((p for p in Path(cache).iterdir() if p.name.startswith("chromium")), reverse=True):
        shell = folder / "chrome-headless-shell-linux64"
        if (shell / "chrome-headless-shell").is_file():
            return shell
    raise SystemExit(f"no chrome-headless-shell-linux64 in the Playwright cache {cache}")


def seed_browser(root, reference_root, cache):
    """Install a headless shell where both servers look for their pinned one, so neither
    downloads chrome-headless-shell from storage.googleapis.com.

    Both use only `<T3 home>/tools/chrome-headless-shell/<platform>/<VERSION>/` and, once
    something executable is there, do not check its version. Linking the Playwright cache's
    headless shell there gives both servers the same browser binary."""
    if sys.platform != "linux" or platform.machine() not in ("x86_64", "AMD64"):
        raise SystemExit("the preview suite seeds the reference's linux64 headless shell only")
    source = (Path(reference_root) / "apps/server/src/preview/PreviewBrowser.ts").read_text()
    version = re.search(r'export const VERSION = "([^"]+)"', source).group(1)
    target = root / "tools/chrome-headless-shell/linux64" / version
    target.parent.mkdir(parents=True)
    target.symlink_to(playwright_headless_shell(cache).resolve(), target_is_directory=True)


def tool_turn(name, server, fixture, log, dispatch, cases):
    url, cookie = server
    turn = json.loads((FIXTURES / "codex_turn.json").read_text())
    started = next(i for i, step in enumerate(turn["steps"]) if step.get("send", {}).get("method") == "turn/started")
    turn["steps"][started + 1:started + 1] = [{"mcp": {"tool": tool, "arguments": args}} for tool, args, _ in cases]
    fixture.write_text(json.dumps(turn))
    calls = Path(str(log) + ".mcp")
    rpc(url, cookie, "orchestration.dispatchCommand", dispatch)
    deadline = monotonic() + 90
    while monotonic() < deadline:
        lines = calls.read_text().splitlines() if calls.exists() else []
        if len(lines) >= len(cases):
            break
        sleep(0.1)
    if len(lines) != len(cases):
        raise RuntimeError(f"{name}: fake CLI completed {len(lines)} of {len(cases)} preview calls")
    for line, (tool, _, expected) in zip(lines, cases, strict=True):
        answer = json.loads(line)["answer"]
        if "error" in answer:
            actual = answer["error"]["message"]
            if actual == expected and answer["error"]["code"] == -32602:
                continue
            raise RuntimeError(f"{name} {tool}: unexpected RPC error {actual!r}")
        result = answer["result"]
        content = "".join(item.get("text", "") for item in result.get("content", []))
        if result.get("isError"):
            actual = content
        else:
            try:
                actual = json.loads(content)
            except json.JSONDecodeError:
                if expected == {"__snapshot__": True}:
                    decoder = json.JSONDecoder()
                    offset = 0
                    values = []
                    while offset < len(content):
                        value, end = decoder.raw_decode(content, offset)
                        values.append(value)
                        offset = end
                    actual = values[-1] if values else {"_raw": content}
                else:
                    raise
        if expected == {"__status__": True}:
            if result.get("isError") or not isinstance(actual, dict) or not actual.get("available") or not actual.get("tabId"):
                raise RuntimeError(f"{name} {tool}: expected a live preview status, got {actual!r}")
            continue
        if expected == {"__snapshot__": True}:
            required = {"url", "title", "loading", "visibleText", "accessibilityTree", "screenshot"}
            if result.get("isError") or not isinstance(actual, dict) or not required <= actual.keys():
                raise RuntimeError(f"{name} {tool}: expected a snapshot, got {actual!r}")
            if "data" in actual["screenshot"]:
                raise RuntimeError(f"{name} {tool}: metadata snapshot unexpectedly included image data")
            continue
        if actual != expected or bool(result.get("isError")) != isinstance(expected, str):
            raise RuntimeError(f"{name} {tool}: expected {expected!r}, got {str(actual)[:500]!r}")


def expect_viewport(viewer, width, height, pointers=None):
    """Read the viewer's messages until it gets the viewport; the agent `pointer` messages before
    it go to `pointers`, their sequence numbers counted from the first."""
    deadline = monotonic() + 30
    while (remaining := deadline - monotonic()) > 0:
        event = viewer.recv(timeout=remaining)
        if isinstance(event, bytes):
            viewer.send('{"type":"ack"}')
            continue
        message = json.loads(event)
        if message.get("type") == "pointer" and pointers is not None:
            first = pointers[0]["sequence"] if pointers else message["sequence"]
            pointers.append({**message, "x": round(message["x"], 1), "y": round(message["y"], 1),
                             "sequence": message["sequence"] - first})
        if message == {"type": "viewport", "width": width, "height": height}:
            return
    raise RuntimeError(f"Viewer did not receive a {width} by {height} viewport")


def automation(name, server, page_url, directory, fixture, log, scenario):
    """Drive the thread's MCP tools using its provider credential, on actual Chromium tabs."""
    url, cookie = server
    seed = seed_turn(url, cookie, directory / f"{name}-checkouts", scenario)

    def open_tab(thread, **settings):
        return rpc(url, cookie, "preview.open", dict(threadId=thread, url=page_url, runtime="server", **settings))["tabId"]

    tab, human, foreign = open_tab(THREAD), open_tab(THREAD), open_tab("other-thread")
    zoomed = open_tab(THREAD, zoomFactor=2)
    viewer_url = url.replace("http", "ws", 1) + "/api/preview-stream/ws?" + urlencode(dict(threadId=THREAD, tabId=human))
    icon = {"_tag": "website", "pageUrl": page_url}
    missing = f"Preview tab {foreign} was not found for evaluate. Omit tabId to use the current tab, or call preview_open."
    held = "A human controls this browser tab. Wait until they release control."
    cases = [
        ("preview_evaluate", {"expression": " 1"}, "Invalid parameters for tool 'preview_evaluate': Expected a string with no leading or trailing whitespace\n  at [\"expression\"]"),
        ("preview_evaluate", {"tabId": foreign, "expression": "1"}, missing),
        ("preview_evaluate", {"tabId": human, "expression": "1"}, held),
        ("preview_set_appearance", {"tabId": human, "colorScheme": "dark"}, held),
        ("preview_set_appearance", {"tabId": tab, "colorScheme": "dark"}, {"tabId": tab, "colorScheme": "dark", "toolIcon": icon}),
        ("preview_evaluate", {"expression": "[document.title,matchMedia('(prefers-color-scheme: dark)').matches]"},
         {"value": ["Preview fixture", True], "toolIcon": icon}),
        ("preview_evaluate", {"expression": "await Promise.resolve(17)"}, {"value": 17, "toolIcon": icon}),
        ("preview_evaluate", {"expression": "[innerWidth, innerHeight, devicePixelRatio]"}, {"value": [1280, 800, 2], "toolIcon": icon}),
        ("preview_evaluate", {"expression": "new Promise(() => {})", "awaitPromise": False}, {"value": {}, "toolIcon": icon}),
        ("preview_evaluate", {"expression": "undefined"}, {"value": None, "toolIcon": icon}),
        ("preview_evaluate", {"expression": "'界'.repeat(22000)"}, "Preview automation evaluate produced a result larger than 64000 bytes."),
        ("preview_evaluate", {"expression": "const end = Date.now() + 25000; while (Date.now() < end) {}"},
         "Preview automation evaluate timed out after 15000ms."),
        ("preview_set_appearance", {"colorScheme": "system"}, {"tabId": tab, "colorScheme": "system", "toolIcon": icon}),
        ("preview_evaluate", {"expression": "matchMedia('(prefers-color-scheme: dark)').matches"}, {"value": False, "toolIcon": icon}),
    ]
    # Each successful resize must change the renderer as well as the session snapshot.
    sizes = [
        ({"mode": "freeform", "width": 701, "height": 393}, {"_tag": "freeform"}, 701, 393),
        ({"mode": "preset", "preset": "iphone-se", "orientation": "landscape"},
         {"_tag": "preset", "presetId": "iphone-se"}, 667, 375),
        ({"mode": "preset", "preset": "nest-hub", "orientation": "portrait"},
         {"_tag": "preset", "presetId": "nest-hub"}, 600, 1024),
        ({"mode": "fill", "timeoutMs": 5000}, {"_tag": "fill"}, 1280, 800),
    ]
    cases.append(("preview_resize", {"mode": "fill", "tabId": human}, held))
    missing_resize = f"Preview tab {foreign} was not found for resize. Omit tabId to use the current tab, or call preview_open."
    cases.append(("preview_resize", {"mode": "fill", "tabId": foreign}, missing_resize))
    for arguments, message in [
        ({"mode": "freeform", "width": 400}, "Custom dimensions require both width and height."),
        ({"mode": "fill", "orientation": "portrait"}, "Fill mode does not accept a preset, dimensions, or orientation."),
        ({"mode": "preset"}, "Preset mode requires a preset and does not accept custom dimensions."),
        ({"mode": "freeform"}, "Freeform mode requires width and height and does not accept a preset or orientation."),
        ({"mode": "freeform", "width": 3000, "height": 3000}, "Custom viewport area must not exceed 8294400 pixels."),
        ({"mode": "freeform", "width": 239, "height": 240}, 'Expected a value between 240 and 3840\n  at ["width"]'),
        ({"mode": "freeform", "width": 400.5, "height": 240}, 'Expected an integer\n  at ["width"]'),
        ({"mode": "fill", "timeoutMs": 0}, 'Expected a value greater than 0\n  at ["timeoutMs"]'),
    ]:
        cases.append(("preview_resize", arguments, f"Invalid parameters for tool 'preview_resize': {message}"))
    for arguments, setting, width, height in sizes:
        size = {"width": width, "height": height}
        if setting["_tag"] != "fill":
            setting = {**setting, **size}
        cases.extend([
            ("preview_resize", arguments, {"tabId": tab, "setting": setting, "viewport": size, "toolIcon": icon}),
            ("preview_evaluate", {"expression": "[innerWidth, innerHeight, devicePixelRatio, screen.width, screen.height]"},
             {"value": [width, height, 2, width, height], "toolIcon": icon}),
        ])
    cases.extend([
        ("preview_evaluate", {"tabId": zoomed, "expression": "(window.parityScreen = [screen.width, screen.height], true)"},
         {"value": True, "toolIcon": icon}),
        ("preview_resize", {"mode": "freeform", "width": 701, "height": 397, "tabId": zoomed},
         {"tabId": zoomed, "setting": {"_tag": "freeform", "width": 701, "height": 397},
          "viewport": {"width": 701, "height": 397}, "toolIcon": icon}),
        ("preview_evaluate", {"expression": "[innerWidth, innerHeight, devicePixelRatio, screen.width === parityScreen[0], screen.height === parityScreen[1]]"},
         {"value": [351, 199, 4, True, True], "toolIcon": icon}),
    ])
    cases.extend([
        # On the tab a passive viewer watches, which sees the agent's cursor.
        ("preview_act", {"tabId": tab, "action": "hover", "selector": "a"}, {"toolIcon": icon}),
        ("preview_act", {"tabId": tab, "action": "click", "selector": "a"}, {"toolIcon": icon}),
        ("preview_wait_for", {"urlIncludes": "#fixture"}, {"toolIcon": icon}),
        ("preview_open", {"url": page_url, "open": False, "reuseExistingTab": False}, {"__status__": True}),
        ("preview_navigate", {"url": page_url}, {"__status__": True}),
        ("preview_snapshot", {"includeImage": False}, {"__snapshot__": True}),
    ])
    # Leave the tab at a fixed size so the session list and a passive viewer can confirm it.
    fixed = {"_tag": "freeform", "width": 844, "height": 540}
    cases.append(("preview_resize", {"tabId": tab, "mode": "freeform", "width": 844, "height": 540},
                  {"tabId": tab, "setting": fixed, "viewport": {"width": 844, "height": 540}, "toolIcon": icon}))
    # The reference starts Chromium during the upgrade on a busy runner.
    with connect(viewer_url, additional_headers={"Cookie": cookie}, open_timeout=60,
                 proxy=None, compression=None) as viewer:
        while True:
            event = viewer.recv(timeout=30)
            if isinstance(event, bytes):
                viewer.send('{"type":"ack"}')
            elif json.loads(event).get("controller") == "you":
                break
        # Resizing a fill-mode viewer records its preferred size. A fixed setting
        # overrides it until the controller switches the tab back to fill.
        viewer.send(json.dumps({"type": "resize", "width": 820, "height": 512}))
        expect_viewport(viewer, 820, 512)
        rpc(url, cookie, "preview.resize", {"threadId": THREAD, "tabId": human, "viewport": fixed})
        expect_viewport(viewer, 844, 540)
        viewer.send(json.dumps({"type": "resize", "width": 610, "height": 420}))
        viewer.send(json.dumps({"type": "viewport", "setting": {"_tag": "fill"}}))
        expect_viewport(viewer, 610, 420)
        passive_url = url.replace("http", "ws", 1) + "/api/preview-stream/ws?" + urlencode(
            {"threadId": THREAD, "tabId": tab, "interactive": "false"})
        with connect(passive_url, additional_headers={"Cookie": cookie}, open_timeout=60,
                     proxy=None, compression=None) as passive:
            tool_turn(name, server, fixture, log, seed["dispatch"], cases)
            # The agent's cursor the viewer saw: the hover's move, then the click's move and ring.
            pointers = []
            expect_viewport(passive, 844, 540, pointers)
        sessions = rpc(url, cookie, "preview.list", {"threadId": THREAD})["sessions"]
        if next(item for item in sessions if item["tabId"] == tab)["viewport"] != fixed:
            raise RuntimeError(f"{name}: preview.resize did not persist the viewport setting")
    for thread, target in ((THREAD, human), (THREAD, tab), (THREAD, zoomed), ("other-thread", foreign)):
        rpc(url, cookie, "preview.close", dict(threadId=thread, tabId=target))
    print(f"PASS {name} Chromium MCP evaluation, appearance and resize ({len(cases)} calls)")
    return pointers


def disabled_automation(name, server, directory, fixture, log, scenario):
    url, cookie = server
    seed = seed_turn(url, cookie, directory / f"{name}-checkouts", scenario)
    rpc(url, cookie, "server.updateSettings", {"patch": {"projectSettingsOverrides": {
        "project:github": {"enableAgentBrowserAccess": False}}}})
    denied = ('MCP credential does not grant the preview capability: browser preview tools are off for this thread. '
              'Do not retry them. To check a page, use a headless browser from the shell, such as Playwright, or curl. '
              'The user can turn on "Agent browser access" in Settings; it applies when the agent session next starts.')
    tool_turn(name, server, fixture, log, seed["dispatch"], [
        ("preview_evaluate", {"expression": "1"}, denied),
        ("preview_set_appearance", {"colorScheme": "light"}, denied),
        ("preview_resize", {"mode": "fill"}, denied),
    ])
    print(f"PASS {name} project browser access refusals")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--browser-cache", type=Path, required=True,
                        help="Installed Playwright browser cache shared by both servers")
    parser.add_argument("--reference-root", type=Path, required=True,
                        help="The reference checkout, for the headless shell version it pins")
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
        pointers = {}
        scenario = {"name": "preview tools", "instance": "codex", "model": "fixture", "scenario": "codex_turn.json"}

        def start(lifetime, name, cmd):
            root = Path(directory, name)
            (root / ".cache").mkdir(parents=True)
            (root / ".cache/ms-playwright").symlink_to(args.browser_cache.resolve(), target_is_directory=True)
            seed_browser(root, args.reference_root, args.browser_cache)
            env, _, log = scenario_env(Path(directory), name, root, scenario, {"codex"})
            fixture = root / "preview-turn.json"
            fixture.write_bytes((FIXTURES / "codex_turn.json").read_bytes())
            env["FAKE_PROVIDER_SCENARIO"] = str(fixture)
            server = managed_server(lifetime, shlex.split(cmd), root, secret, signing_key, env=env)
            return server, fixture, log

        for name, cmd in (("original", args.original_command), ("rust", args.rust_command)):
            with ExitStack() as denied_stack:
                label = f"{name}-disabled"
                server, fixture, log = start(denied_stack, label, cmd)
                disabled_automation(label, server, Path(directory), fixture, log, scenario)
            servers[name], fixture, log = start(stack, name, cmd)
            pointers[name] = automation(name, servers[name], url + "#fixture", Path(directory), fixture, log, scenario)
        # The reference shows the agent's cursor for the hover and the click; Rust must show the same.
        phases = [event["phase"] for event in pointers["original"]]
        pointer_failed = phases != ["move", "move", "click"] or pointers["original"] != pointers["rust"]
        print(f"{'FAIL' if pointer_failed else 'PASS'} agent pointer")
        if pointer_failed:
            print(f"  original {json.dumps(pointers['original'])}\n  rust     {json.dumps(pointers['rust'])}")
        try:
            original = observe_with_retry("original", servers["original"], url, page)
        except FrameStall as stall:
            # Once the reference server's preview stalls, later sessions on it usually stall
            # too, even before the first paint. That is the reference failing, so the Rust
            # server is still exercised in full (its own colour, size, upload and download
            # checks) and only the byte comparison is skipped.
            print(f"SKIP comparison: the reference server's preview stalled: {stall}")
            original = None
        rust = observe_with_retry("rust", servers["rust"], url, page)
        if original is None:
            print("PASS rust preview session (colours, sizes, upload, download, close)")
            return pointer_failed
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
        return bool(failed) or pointer_failed


if __name__ == "__main__":
    raise SystemExit(main())
