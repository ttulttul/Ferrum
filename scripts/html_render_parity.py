#!/usr/bin/env python3
"""An agent checks and shows HTML pages with html_preview and html_render, on the reference and
on the Rust server, in a real headless Chromium, and both must answer the same.

A fake Codex turn calls the two tools on the MCP server its server gave it:
- previews of an inline page (console output, a thrown error, a local image inlined, a missing
  one, and an image on a loopback server the browser must not reach), of a light page at phone
  width, of a page file, at a width below the minimum, and with a width that is not a number;
- previews refused: a file that is not a page, both html and path, a viewport height unit;
- renders of an inline page, of a page file, with a blank title and a height below the minimum;
- renders refused: a missing local image and a viewport height unit.

Compared: every answer (screenshots by their pixels; attachment ids by form), and each page a
render stored. Both servers launch the same headless shell from the Playwright cache, each on a
throwaway profile, and serve the page to it from memory; the only page server is on loopback,
and it must see no request: the browser reaches only public addresses through its proxy. The
servers run behind the refusing proxy the harness gives every parity server.
"""
import argparse
import base64
from contextlib import ExitStack
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
import json
from pathlib import Path
import re
import secrets
import shlex
from tempfile import TemporaryDirectory
from threading import Thread
from time import monotonic, sleep

from PIL import Image

from client_thread_parity import FIXTURES, rpc, scenario_env, seed_turn
from parity_harness import managed_server
from preview_provider_parity import seed_reference_browser

UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


class Loopback(BaseHTTPRequestHandler):
    """A page server on loopback: the browser must never reach it."""

    def do_GET(self):
        self.server.hits.append(self.path)
        self.send_response(200)
        self.end_headers()

    def log_message(self, *_):
        pass


def cases(work, loopback):
    image = work / "fixture.png"
    page = work / "page.html"
    not_page = work / "notes.html"
    inline = (f'<!doctype html><html><head><title>Fixture</title></head><body>'
              f'<h1>Quarterly numbers</h1><p style="color: var(--muted-foreground)">Revenue grew.</p>'
              f'<img src="{image}" alt="dot"><img src="/missing/nowhere.png">'
              f'<img src="{loopback}/blocked.png">'
              f'<script>console.log("hello", 2); console.warn("careful"); '
              f'setTimeout(() => {{ throw new Error("boom"); }});</script></body></html>')
    light = ('<!doctype html><html><body><div style="height: 300px; background: var(--chart-2)">'
             'Chart</div><p>Below the chart.</p></body></html>')
    return [
        ("html_preview", {"html": inline}),
        ("html_preview", {"html": light, "width": 390, "appearance": "light"}),
        ("html_preview", {"path": str(page)}),
        ("html_preview", {"html": light, "width": 100}),
        ("html_preview", {"html": light, "width": "wide"}),
        ("html_preview", {"path": str(not_page)}),
        ("html_preview", {"html": light, "path": str(page)}),
        ("html_preview", {"html": '<div style="height: 50vh">x</div>'}),
        ("html_render", {"html": light, "title": "Revenue chart", "height": 420}),
        ("html_render", {"path": str(page), "title": "From a file", "height": 300}),
        ("html_render", {"html": "<p>tiny</p>", "title": "   ", "height": 5}),
        ("html_render", {"html": '<img src="/missing/nowhere.png">', "title": "Missing", "height": 200}),
        ("html_render", {"html": "<style>.a{min-height:10svh}</style>", "title": "Viewport", "height": 200}),
    ]


def write_fixtures(work):
    work.mkdir(parents=True, exist_ok=True)
    buffer = BytesIO()
    Image.new("RGB", (12, 12), (200, 40, 40)).save(buffer, format="PNG")
    (work / "fixture.png").write_bytes(buffer.getvalue())
    (work / "page.html").write_text(f'<!doctype html><html><body><h2>From a file</h2>'
                                    f'<img src="{work / "fixture.png"}"></body></html>')
    (work / "notes.html").write_text("API_KEY=not-a-page\n")


def answers(log, count, seconds=180):
    calls = Path(f"{log}.mcp")
    deadline = monotonic() + seconds
    lines = []
    while monotonic() < deadline:
        lines = calls.read_text().splitlines() if calls.exists() else []
        if len(lines) >= count:
            break
        sleep(0.2)
    if len(lines) < count:
        raise RuntimeError(f"the fake CLI completed {len(lines)} of {count} calls")
    return [json.loads(line)["answer"] for line in lines]


def comparable(answer, root):
    """An answer with its screenshot as size and pixel hash, ids by form, homes masked."""
    result = answer.get("result", answer)
    out = {"isError": result.get("isError")}
    content = []
    for part in result.get("content", []):
        if part.get("type") == "image":
            png = base64.b64decode(part["data"])
            image = Image.open(BytesIO(png))
            content.append({"type": "image", "mimeType": part.get("mimeType"), "size": image.size,
                            "pixels": sha256(image.convert("RGBA").tobytes()).hexdigest()})
        else:
            text = part.get("text", "")
            try:
                content.append({"type": "text", "json": json.loads(text)})
            except json.JSONDecodeError:
                content.append({"type": "text", "text": text})
    out["content"] = content
    if "structuredContent" in result:
        out["structuredContent"] = result["structuredContent"]
    if "error" in answer:
        out["rpcError"] = answer["error"]
    text = json.dumps(out, sort_keys=True).replace(str(root), "<home>")
    return json.loads(UUID.sub("<uuid>", text))


def stored_pages(root, results):
    """Each page a render stored, as the attachment holds it."""
    out = []
    for answer in results:
        text = json.dumps(answer)
        for attachment in re.findall(r'"attachmentId": "([^"]+)"', text):
            path = next(root.rglob(f"attachments/{attachment}.html"), None)
            out.append(path.read_text() if path else f"<missing {attachment}>")
    return out


def run(name, command, args, directory, work, loopback):
    root = Path(directory, name)
    (root / ".cache").mkdir(parents=True)
    (root / ".cache/ms-playwright").symlink_to(args.browser_cache.resolve(), target_is_directory=True)
    if name == "original":
        seed_reference_browser(root, args.reference_root, args.browser_cache)
    scenario = {"name": "html tools", "instance": "codex", "model": "fixture", "scenario": "codex_turn.json"}
    env, _, log = scenario_env(Path(directory), name, root, scenario, {"codex"})
    calls = cases(work, loopback)
    turn = json.loads((FIXTURES / "codex_turn.json").read_text())
    started = next(i for i, step in enumerate(turn["steps"]) if step.get("send", {}).get("method") == "turn/started")
    turn["steps"][started + 1:started + 1] = [{"mcp": {"tool": tool, "arguments": arguments}}
                                              for tool, arguments in calls]
    fixture = root / "html-turn.json"
    fixture.write_text(json.dumps(turn))
    env["FAKE_PROVIDER_SCENARIO"] = str(fixture)
    with ExitStack() as stack:
        url, cookie = managed_server(stack, shlex.split(command), root, secrets.token_urlsafe(32),
                                     secrets.token_bytes(32), env=env)
        seeded = seed_turn(url, cookie, Path(directory, f"{name}-checkouts"), scenario)
        rpc(url, cookie, "orchestration.dispatchCommand", seeded["dispatch"])
        results = answers(log, len(calls))
        return {
            "answers": [comparable(answer, Path(directory)) for answer in results],
            "stored pages": [UUID.sub("<uuid>", page.replace(str(work), "<work>"))
                             for page in stored_pages(root, results)],
        }, calls


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--reference-root", type=Path, required=True)
    parser.add_argument("--browser-cache", type=Path, required=True,
                        help="the Playwright browser cache both servers launch Chromium from")
    parser.add_argument("--dump", type=Path, help="write both servers' answers here as JSON")
    args = parser.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", 0), Loopback)
    server.hits = []
    Thread(target=server.serve_forever, daemon=True).start()
    loopback = f"http://127.0.0.1:{server.server_port}"
    results = {}
    try:
        with TemporaryDirectory() as directory:
            work = Path(directory, "work")
            write_fixtures(work)
            for name, command in (("original", args.original_command), ("rust", args.rust_command)):
                results[name], calls = run(name, command, args, directory, work, loopback)
    finally:
        server.shutdown()
    if args.dump:
        args.dump.write_text(json.dumps(results, indent=1, sort_keys=True))
    failures = 0
    labels = [f"{tool} {i + 1}" for i, (tool, _) in enumerate(calls)]
    original, rust = results["original"], results["rust"]
    for label, left, right in zip(labels, original["answers"], rust["answers"], strict=True):
        same = left == right
        failures += not same
        print(f"{'PASS' if same else 'FAIL'} {label}")
        if not same:
            print(f"  original {json.dumps(left)[:3000]}\n  rust     {json.dumps(right)[:3000]}")
    same = original["stored pages"] == rust["stored pages"]
    failures += not same
    print(f"{'PASS' if same else 'FAIL'} stored pages")
    if not same:
        for left, right in zip(original["stored pages"], rust["stored pages"]):
            if left != right:
                print(f"  original {left[:1500]}\n  rust     {right[:1500]}")
    reached = not server.hits
    failures += not reached
    print(f"{'PASS' if reached else 'FAIL'} the browsers never reached the loopback page server"
          + ("" if reached else f": {server.hits}"))
    total = len(labels) + 2
    print(f"{total - failures}/{total} HTML page checks matched across the reference and the Rust server")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
