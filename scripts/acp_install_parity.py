#!/usr/bin/env python3
"""Install ACP Registry agents with `server.prepareAcpRegistryAgent`, remove managed
binaries with `server.uninstallAcpRegistryManagedBinary`, and compare the reference and
the Rust server.

Both servers read the same fixture registry index, seeded as the cached index in each home
(the registry's fixed HTTPS URL is refused, so both fall back to the cache), and install from
local stand-ins only:

- npx agents: the npm CLI, pointed by `npm_config_registry` at a loopback registry that serves
  a fixture package tarball, installs into `<T3 home>/tools/<agent>/<version>/npm`. A second
  prepare reuses the recorded receipt; a package the registry does not have fails.
- binary agents: one already in `<T3 home>/tools/<agent>/<version>/<platform>` is reused, one
  whose command path leaves its folder is refused, and one that must be downloaded fails,
  since its archive URL (HTTPS, as the registry requires) is refused.
- uvx agents: `uv` is not on the servers' PATH, so the runner is unavailable.
- uninstall: a binary just prepared is kept, as is one a provider instance uses; another is
  removed with its folders; an unknown agent and an invalid id are reported.

Nothing may leave loopback: every server runs behind the harness's loopback proxy, which forwards nothing (parity_harness.assert_nothing_escaped). All data is fake.
"""
import argparse
import base64
from contextlib import ExitStack
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import platform
import re
import secrets
import shlex
import shutil
import tarfile
from tempfile import TemporaryDirectory
import threading
from urllib.parse import unquote, urlsplit

from websockets.sync.client import connect

import parity_harness
from parity_harness import encode, managed_server, path_without, recv_frame

TARGET = {("Linux", "x86_64"): "linux-x86_64", ("Linux", "aarch64"): "linux-aarch64"}.get(
    (platform.system(), platform.machine()))


def binary(archive, cmd="./run"):
    return {"binary": {TARGET: {"archive": archive, "cmd": cmd}}}


REGISTRY = {"version": "1.0.0", "agents": [
    {"id": "npm-agent", "name": "Npm Agent", "version": "1.0.0", "description": "Installed with npm",
     "distribution": {"npx": {"package": "@fixture/acp-agent@1.0.0"}}},
    {"id": "npm-missing", "name": "Npm Missing", "version": "1.0.0", "description": "Not in the registry",
     "distribution": {"npx": {"package": "missing-agent@1.0.0"}}},
    {"id": "uv-agent", "name": "Uv Agent", "version": "2.0.0", "description": "Installed with uv",
     "distribution": {"uvx": {"package": "uv-agent==2.0.0"}}},
    {"id": "bin-cached", "name": "Cached Binary", "version": "1.0.0", "description": "Already downloaded",
     "distribution": binary("https://example.invalid/bin-cached.tar.gz")},
    {"id": "bin-download", "name": "Download Binary", "version": "1.0.0", "description": "Must be downloaded",
     "distribution": binary("https://example.invalid/bin-download.tar.gz")},
    {"id": "bin-unsafe", "name": "Unsafe Binary", "version": "1.0.0", "description": "Leaves its folder",
     "distribution": binary("https://example.invalid/bin-unsafe.tar.gz", cmd="../escape")},
]}
PACKAGE = "@fixture/acp-agent"


def package_tarball():
    """A fixture npm package with one command, as npm packs it (under `package/`)."""
    files = {"package/package.json": json.dumps({"name": PACKAGE, "version": "1.0.0", "bin": {"acp-agent": "bin/agent.js"}}),
             "package/bin/agent.js": "#!/usr/bin/env node\nconsole.log('fixture agent')\n"}
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w:gz") as archive:
        for name, text in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(text.encode())
            info.mode = 0o755
            archive.addfile(info, io.BytesIO(text.encode()))
    return data.getvalue()


def npm_registry(seen):
    tarball = package_tarball()

    class Registry(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            path = unquote(self.path.split("?")[0])
            seen.append(path)
            base = f"http://127.0.0.1:{self.server.server_port}"
            if path == f"/{PACKAGE}":
                body = json.dumps({"name": PACKAGE, "dist-tags": {"latest": "1.0.0"}, "versions": {"1.0.0": {
                    "name": PACKAGE, "version": "1.0.0", "bin": {"acp-agent": "bin/agent.js"},
                    "dist": {"tarball": f"{base}/{PACKAGE}/-/acp-agent-1.0.0.tgz",
                             "shasum": hashlib.sha1(tarball).hexdigest(),
                             "integrity": "sha512-" + base64.b64encode(hashlib.sha512(tarball).digest()).decode()}}}}).encode()
                return self.reply(200, "application/json", body)
            if path == f"/{PACKAGE}/-/acp-agent-1.0.0.tgz":
                return self.reply(200, "application/octet-stream", tarball)
            self.reply(404, "application/json", b'{"error":"Not found"}')

        def reply(self, status, kind, body):
            self.send_response(status)
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Registry)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def seed_home(home):
    cache = home / "caches" / "acp-registry"
    cache.mkdir(parents=True)
    (cache / "registry.json").write_text(json.dumps(REGISTRY))
    for agent in ("bin-cached", "bin-old", "bin-used"):
        run = home / "tools" / agent / "1.0.0" / TARGET / "run"
        run.parent.mkdir(parents=True)
        run.write_text("#!/bin/sh\necho fixture\n")
        run.chmod(0o755)
    settings = home / "userdata" / "settings.json"
    settings.parent.mkdir(parents=True)
    settings.write_text(json.dumps({"providerInstances": {
        "codex": {"driver": "codex", "enabled": False}, "claudeAgent": {"driver": "claudeAgent", "enabled": False},
        "acp-used": {"driver": "acpRegistry", "enabled": False, "config": {"source": "registry", "agentId": "bin-used"}}}}))


def call(url, cookie, tag, payload):
    with connect(url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2",
                 additional_headers={"Cookie": cookie}, proxy=None, compression=None) as socket:
        socket.send(encode({"_tag": "Request", "id": "acp", "tag": tag, "payload": payload, "headers": []}).decode())
        while True:
            frame = json.loads(recv_frame(socket, timeout=300))
            if frame.get("_tag") == "Ping":
                socket.send(encode({"_tag": "Pong"}).decode())
            elif frame.get("requestId") == "acp" and "exit" in frame:
                return frame["exit"]


def trim_transport(value):
    """Drops what lies under an HttpClientError: the reference's fetch adds Node's own errors."""
    if isinstance(value, dict):
        if value.get("name") == "HttpClientError":
            return {k: v for k, v in value.items() if k != "cause"}
        return {k: trim_transport(v) for k, v in value.items()}
    if isinstance(value, list):
        return [trim_transport(v) for v in value]
    return value


def listing(root):
    return sorted(str(p.relative_to(root)) for p in root.rglob("*")) if root.exists() else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    if TARGET is None:
        raise SystemExit("acp_install_parity runs on Linux x86_64 or aarch64")
    npm = shutil.which("npm")
    if npm is None:
        raise SystemExit("acp_install_parity needs the npm CLI on PATH: both servers install npx agents with it")
    answers = {}
    with TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        for side, command in (("original", args.original_command), ("rust", args.rust_command)):
            with ExitStack() as stack:
                seen = []
                registry = npm_registry(seen)
                stack.callback(registry.shutdown)
                registry_url = f"http://127.0.0.1:{registry.server_port}/"
                assert urlsplit(registry_url).hostname == "127.0.0.1"
                home = tmp / side
                seed_home(home)
                # npm from the harness's PATH; uv hidden, so a uvx agent has no runner.
                path = os.pathsep.join([str(Path(npm).parent), os.environ.get("PATH", "")])
                env = {"npm_config_registry": registry_url, "npm_config_update_notifier": "false",
                       "npm_config_audit": "false", "npm_config_fund": "false",
                       "PATH": path_without({"uv"}, tmp / f"{side}-path", path)}
                url, cookie = managed_server(stack, shlex.split(command), home, secrets.token_urlsafe(32),
                                             secrets.token_bytes(32), env=env)
                record = {}

                def prepare(name, agent_id):
                    record[name] = trim_transport(call(url, cookie, "server.prepareAcpRegistryAgent", {"agentId": agent_id}))

                def uninstall(name, agent_id):
                    record[name] = call(url, cookie, "server.uninstallAcpRegistryManagedBinary", {"agentId": agent_id})

                prepare("npm agent", "npm-agent")
                npm_root = home / "tools" / "npm-agent" / "1.0.0" / "npm"
                record["npm agent command installed"] = (npm_root / "bin" / "acp-agent").exists()
                receipts = sorted((home / "caches" / "acp-registry" / "package-installs").glob("npm-agent-*.json"))
                record["npm agent receipt"] = [json.loads(r.read_text()) for r in receipts]
                record["npm agent receipt name"] = [re.sub(r"[0-9a-f]{16}", "<digest>", r.name) for r in receipts]
                fetched = len(seen)
                prepare("npm agent again (the receipt answers)", "npm-agent")
                record["npm agent again fetched nothing"] = len(seen) == fetched
                prepare("npm package the registry does not have", "npm-missing")
                prepare("uvx agent without uv", "uv-agent")
                prepare("binary already installed", "bin-cached")
                prepare("binary that must be downloaded", "bin-download")
                record["nothing left by the failed download"] = listing(home / "tools" / "bin-download")
                prepare("binary whose command leaves its folder", "bin-unsafe")
                prepare("agent not in the registry", "missing")
                uninstall("uninstall a binary just prepared", "bin-cached")
                uninstall("uninstall a binary a provider instance uses", "bin-used")
                uninstall("uninstall an unused binary", "bin-old")
                record["unused binary's folders"] = listing(home / "tools" / "bin-old")
                uninstall("uninstall an agent with nothing installed", "nothing-here")
                uninstall("uninstall an invalid agent id", "Not Valid")
                for label, agent_id in (("blank", "   "), ("too long", "a" * 129), ("padded", "  bin-old  ")):
                    prepare(f"prepare, agent id {label}", agent_id)
                    uninstall(f"uninstall, agent id {label}", agent_id)
                record["tools left"] = listing(home / "tools")
                text = json.dumps(record)
                text = text.replace(str(Path(npm).parent), "<npm-bin>").replace(str(home), "<home>").replace(str(tmp), "<tmp>")
                text = re.sub(r"127\.0\.0\.1:\d+", "127.0.0.1:<port>", text)
                text = re.sub(r"\d{4}-\d\d-\d\dT\d\d_\d\d_\d\d_\d+Z", "<time>", text)
                answers[side] = json.loads(text)
    # Every server ran behind a loopback proxy that forwards nothing; what it refused never left.
    parity_harness.assert_nothing_escaped()
    failures = 0
    for name in answers["original"]:
        same = answers["original"][name] == answers["rust"].get(name)
        failures += not same
        print(f"{'PASS' if same else 'FAIL'} {name}")
        if not same:
            print(f"  original {json.dumps(answers['original'][name])[:2500]}\n  rust     {json.dumps(answers['rust'].get(name))[:2500]}")
    total = len(answers["original"])
    print(f"{total - failures}/{total} ACP agent install checks matched; nothing left loopback")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
