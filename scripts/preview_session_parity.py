#!/usr/bin/env python3
"""An agent reads its browser tab's state, answers the page's JavaScript dialogs and records the
tab, with preview_status, preview_dialog and preview_recording_start/stop, on the reference and
on the Rust server, in a real headless Chromium, and both must answer the same.

A fake Codex turn calls the tools on the MCP server its server gave it:
- status and a dialog answer before any tab is open;
- a tab opened on a loopback fixture page, its status, and a dialog answer with none pending;
- a prompt, a confirm and an alert the page opens: the status that reports each, another tool
  refused while it is pending, the answer (with the prompt's text, or dismissed), and what the
  page received;
- a stop before any recording, a recording started (twice: the second finds it running), the
  page changed, the recording stopped, and a second stop;
- a status for a tab that does not exist.

Then, on fresh homes with no browser installed: a tab opened, a page previewed, the tab opened
again and a status. Each server tries to install its pinned headless shell, the refusing proxy
refuses the download, and both must answer with the same install error and leave no tab.

Last, with a fake headless shell that will not start, as a host missing a library, one blocking
Chrome's sandbox and one running it as root: a tab opened and a page previewed, which both
servers must explain with the same host hint (`PreviewBrowserHost`), each naming its own
`browser setup` command (masked).

Compared: every answer, with tab ids, times, ports and homes masked, and for a recording its
type, size range, the attachment it became and that the file is a playable video container.
Both servers launch the same headless shell from the Playwright cache, as the preview suite
does, each on a throwaway profile, behind the refusing proxy the harness gives every server.
"""
import argparse
from contextlib import ExitStack
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import re
import secrets
import shlex
from tempfile import TemporaryDirectory
from threading import Thread
from time import monotonic, sleep

from client_thread_parity import FIXTURES, rpc, scenario_env, seed_turn
from parity_harness import REFUSED, assert_nothing_escaped, managed_server
from preview_provider_parity import seed_browser

# What both servers fetch at start-up, which the harness's refusing proxy refuses (parity_harness).
STARTUP_FETCHES = {"raw.githubusercontent.com:443", "registry.npmjs.org:443"}
# The pinned headless shell's download, which the no-browser phase starts.
BROWSER_DOWNLOAD = "storage.googleapis.com:443"
# Each server's own `browser setup` command, which its hints name.
SETUP_COMMAND = re.compile(r'sudo (?:env \\"PATH=\$PATH\\" \S+|\S+) browser setup --base-dir [^`]*')
FAILING_BROWSERS = {
    "missing a library": 'echo "$0: error while loading shared libraries: libnss3.so: cannot open shared '
                         'object file: No such file or directory" >&2\nexit 127',
    "sandbox blocked": 'case "$*" in *--version*) echo "Chrome 158"; exit 0;; esac\necho "[0:FATAL:zygote_host_impl_'
                       'linux.cc(132)] No usable sandbox! Update your kernel or see https://chromium.googlesource.com" >&2\nexit 1',
    "running as root": 'case "$*" in *--version*) echo "Chrome 158"; exit 0;; esac\necho "[0:ERROR:zygote_host_impl_'
                       'linux.cc(105)] Running as root without --no-sandbox is not supported. See https://crbug.com/638180." >&2\nexit 1',
}
UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
TIME = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z")


class Fixture(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(b"""<!doctype html><title>Session fixture</title>
            <style>body{margin:0;background:#fff}#box{width:200px;height:120px;background:#357}</style>
            <div id=box></div><button onclick="document.body.style.background='#a53'">Paint</button>""")

    def log_message(self, *_):
        pass


def steps(page):
    dialog_after = lambda script: {"expression": f"setTimeout(() => {{ {script} }}, 50); true"}
    return [
        ("preview_status", {}),
        ("preview_dialog", {"accept": True}),
        ("preview_open", {"url": page, "open": False}),
        ("preview_status", {}),
        ("preview_dialog", {"accept": True}),
        ("preview_evaluate", dialog_after("window.answer = prompt('Your name?', 'Ada');")),
        ("sleep", 1),
        ("preview_status", {}),
        ("preview_evaluate", {"expression": "1"}),
        ("preview_dialog", {"accept": True, "promptText": "Grace"}),
        ("preview_evaluate", {"expression": "window.answer"}),
        ("preview_evaluate", dialog_after("window.confirmed = confirm('Delete it?');")),
        ("sleep", 1),
        ("preview_status", {}),
        ("preview_dialog", {"accept": False}),
        ("preview_evaluate", {"expression": "window.confirmed"}),
        ("preview_evaluate", dialog_after("alert('Saved');")),
        ("sleep", 1),
        ("preview_dialog", {"accept": True, "promptText": "ignored"}),
        ("preview_status", {}),
        ("preview_recording_stop", {}),
        ("preview_recording_start", {}),
        ("preview_recording_start", {}),
        ("preview_act", {"action": "click", "selector": "button"}),
        ("sleep", 1),
        ("preview_recording_stop", {}),
        ("preview_recording_stop", {}),
        ("preview_status", {"tabId": "tab-missing"}),
    ]


def no_browser_steps(page):
    return [
        ("preview_open", {"url": page, "open": False}),
        ("html_preview", {"html": "<!doctype html><p>No browser yet</p>"}),
        ("preview_open", {"url": page, "open": False}),
        ("preview_status", {}),
    ]


def failing_browser_steps(page):
    return [
        ("preview_open", {"url": page, "open": False}),
        ("html_preview", {"html": "<!doctype html><p>A browser that will not start</p>"}),
    ]


def seed_failing_browser(root, reference_root, script):
    """A headless shell at the pinned path that fails as `script` says."""
    source = (Path(reference_root) / "apps/server/src/preview/PreviewBrowser.ts").read_text()
    version = re.search(r'export const VERSION = "([^"]+)"', source).group(1)
    shell = root / "tools/chrome-headless-shell/linux64" / version / "chrome-headless-shell"
    shell.parent.mkdir(parents=True)
    shell.write_text(f"#!/bin/sh\n{script}\n")
    shell.chmod(0o755)


def answers(log, count, seconds=240):
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


def recording(value):
    """A recording by form: its size as a range, and its file as the container it holds."""
    if not (isinstance(value, dict) and "sizeBytes" in value and "mimeType" in value):
        return value
    path = Path(value.get("path", ""))
    head = path.read_bytes()[:12] if path.is_file() else b""
    container = ("webm" if head.startswith(b"\x1a\x45\xdf\xa3") else
                 "mp4" if head[4:8] == b"ftyp" else "missing" if not head else "unknown")
    size = value["sizeBytes"]
    return {**value, "sizeBytes": "over 1 KiB" if isinstance(size, int) and size > 1024 else size,
            "file": {"container": container, "size matches": path.is_file() and path.stat().st_size == size}}


def comparable(answer, home, port, tabs):
    result = answer.get("result", answer)
    out = {"isError": result.get("isError"), "rpcError": answer.get("error")}
    texts = []
    for part in result.get("content", []):
        text = part.get("text", "")
        try:
            texts.append(recording(json.loads(text)))
        except json.JSONDecodeError:
            texts.append(text)
    out["content"] = texts
    text = json.dumps(out, sort_keys=True)
    for side in ("original", "rust"):
        text = text.replace(f"{home}/{side}/", "<home>/<server>/")
    text = SETUP_COMMAND.sub("<setup>", text)
    text = text.replace(str(home), "<home>").replace(f":{port}/", ":<port>/")
    # Tab ids, in the order each first appears.
    for tab in re.findall(r'"tabId": "([^"<]+)"', text):
        if tab != "tab-missing" and tab not in tabs:
            tabs.append(tab)
    for index, tab in enumerate(tabs):
        text = text.replace(tab, f"<tab {index}>")
    return json.loads(TIME.sub("<time>", UUID.sub("<uuid>", text)))


def run(name, command, args, directory, page, port, plan=None, browser=True):
    root = Path(directory, name)
    (root / ".cache").mkdir(parents=True)
    if browser is True:
        (root / ".cache/ms-playwright").symlink_to(args.browser_cache.resolve(), target_is_directory=True)
        seed_browser(root, args.reference_root, args.browser_cache)
    elif browser:
        seed_failing_browser(root, args.reference_root, browser)
    scenario = {"name": "preview session tools", "instance": "codex", "model": "fixture", "scenario": "codex_turn.json"}
    env, _, log = scenario_env(Path(directory), name, root, scenario, {"codex"})
    plan = plan or steps(page)
    turn = json.loads((FIXTURES / "codex_turn.json").read_text())
    started = next(i for i, step in enumerate(turn["steps"]) if step.get("send", {}).get("method") == "turn/started")
    turn["steps"][started + 1:started + 1] = [
        {"sleep": arguments} if tool == "sleep" else {"mcp": {"tool": tool, "arguments": arguments}}
        for tool, arguments in plan]
    fixture = root / "session-turn.json"
    fixture.write_text(json.dumps(turn))
    env["FAKE_PROVIDER_SCENARIO"] = str(fixture)
    calls = [(tool, arguments) for tool, arguments in plan if tool != "sleep"]
    with ExitStack() as stack:
        url, cookie = managed_server(stack, shlex.split(command), root, secrets.token_urlsafe(32),
                                     secrets.token_bytes(32), env=env)
        seeded = seed_turn(url, cookie, Path(directory, f"{name}-checkouts"), scenario)
        rpc(url, cookie, "orchestration.dispatchCommand", seeded["dispatch"])
        tabs = []
        return [comparable(answer, Path(directory), port, tabs) for answer in answers(log, len(calls))], calls


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--reference-root", type=Path, required=True)
    parser.add_argument("--browser-cache", type=Path, required=True,
                        help="the Playwright browser cache both servers launch Chromium from")
    parser.add_argument("--dump", type=Path, help="write both servers' answers here as JSON")
    args = parser.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", 0), Fixture)
    Thread(target=server.serve_forever, daemon=True).start()
    page = f"http://127.0.0.1:{server.server_port}/"
    results = {}
    try:
        with TemporaryDirectory() as directory:
            for name, command in (("original", args.original_command), ("rust", args.rust_command)):
                results[name], calls = run(name, command, args, directory, page, server.server_port)
            for name, command in (("original", args.original_command), ("rust", args.rust_command)):
                results[f"{name} without a browser"], bare = run(
                    f"{name}-bare", command, args, directory, page, server.server_port,
                    plan=no_browser_steps(page), browser=False)
            failing = {}
            for kind, script in FAILING_BROWSERS.items():
                for name, command in (("original", args.original_command), ("rust", args.rust_command)):
                    failing[(kind, name)], hints = run(
                        f"{name}-{kind.replace(' ', '-')}", command, args, directory, page, server.server_port,
                        plan=failing_browser_steps(page), browser=script)
    finally:
        server.shutdown()
    # Every server (and the browsers they launched) sent its HTTP(S) through the loopback refusing
    # proxy; what it refused is only the servers' start-up fetches, never a page's request.
    assert_nothing_escaped()
    refused = sorted({entry.split(" ", 1)[1] for entry in REFUSED} - STARTUP_FETCHES - {BROWSER_DOWNLOAD})
    if refused:
        print(f"FAIL requests besides the servers' start-up fetches reached the refusing proxy: {refused}")
        return 1
    if args.dump:
        args.dump.write_text(json.dumps(results, indent=1, sort_keys=True))
    failures = 0
    checks = [(f"{index + 1} {tool}", left, right)
              for index, ((tool, _), left, right) in enumerate(zip(calls, results["original"], results["rust"], strict=True))]
    checks += [(f"without a browser {index + 1} {tool}", left, right)
               for index, ((tool, _), left, right) in enumerate(zip(
                   bare, results["original without a browser"], results["rust without a browser"], strict=True))]
    for kind in FAILING_BROWSERS:
        checks += [(f"browser {kind} {index + 1} {tool}", left, right)
                   for index, ((tool, _), left, right) in enumerate(zip(
                       hints, failing[(kind, "original")], failing[(kind, "rust")], strict=True))]
        # The hints must be the host's, not a generic launch failure.
        if not re.search(r"This host|running as root", json.dumps(failing[(kind, "original")])):
            failures += 1
            print(f"FAIL browser {kind}: the reference gave no host hint")
    for name, left, right in checks:
        same = left == right
        failures += not same
        print(f"{'PASS' if same else 'FAIL'} {name}")
        if not same:
            print(f"  original {json.dumps(left)[:3000]}\n  rust     {json.dumps(right)[:3000]}")
    # The reference must really have tried, and failed, to install its browser.
    if "could not install its headless browser" not in json.dumps(results["original without a browser"][0]):
        failures += 1
        print("FAIL without a browser: the reference did not report a failed install")
    print(f"{len(checks) - failures}/{len(checks)} preview session checks matched across the reference and the Rust server")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
