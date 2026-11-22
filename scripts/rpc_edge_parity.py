#!/usr/bin/env python3
"""Compare deterministic RPC edge contracts on fresh WebSocket sessions.

Both servers are kept off the network (parity_harness.offline_env), so what they ask a
registry for is answered locally and the same way for both:

- `server.getNpmRelease`: both read releases with the npm CLI, which honours
  `npm_config_registry`; it points at a loopback npm registry serving a fixture `t3` package.
- The ACP Registry (`server.searchAcpRegistry`, `server.prepareAcpRegistryAgent`): both fetch
  a fixed HTTPS URL with no override, so the fetch is refused and both fall back to the cached
  index, seeded with a fixture in each home (`caches/acp-registry/registry.json`). A second pair
  of servers has no cache, so both fail `registry_unavailable`.
"""
import argparse
import contextlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import secrets
import shlex
import shutil
import tempfile
import threading

from websockets.sync.client import connect

from parity_harness import encode, managed_server

# A fixture ACP Registry index: an agent with a sha256-pinned Linux binary, one installed with
# npx, one with uvx, one with only a macOS binary, one whose description mentions the query,
# and one that does not decode (its id has a space), which both servers drop.
ACP_REGISTRY = {"version": "1.0.0", "agents": [
    {"id": "claude-acp", "name": "Claude Agent", "version": "0.109.1",
     "description": "ACP wrapper for Anthropic's Claude", "authors": ["Anthropic", "Zed Industries"],
     "license": "proprietary", "repository": "https://github.com/agentclientprotocol/claude-agent-acp",
     "icon": "https://cdn.agentclientprotocol.com/registry/v1/latest/claude-acp.svg",
     "distribution": {"binary": {
         "linux-x86_64": {"archive": "https://example.invalid/claude-acp-linux-x64.tar.gz", "cmd": "./claude-acp",
                          "sha256": "a" * 64},
         "darwin-aarch64": {"archive": "https://example.invalid/claude-acp-darwin-arm64.tar.gz", "cmd": "./claude-acp"}}}},
    {"id": "claude-helper", "name": "  Claude Helper  ", "version": "1.2.3", "description": "Helps with code",
     "website": "https://example.invalid/helper", "distribution": {"npx": {"package": "@fixture/claude-helper@1.2.3"}}},
    {"id": "claudette", "name": "Claudette", "version": "2.0.0", "description": "A Python agent",
     "distribution": {"uvx": {"package": "claudette==2.0.0"}}},
    {"id": "claude-mac", "name": "Claude for Mac", "version": "3.0.0", "description": "macOS only",
     "distribution": {"binary": {"darwin-aarch64": {"archive": "https://example.invalid/mac.zip", "cmd": "./mac"}}}},
    {"id": "other-agent", "name": "Other", "version": "1.0.0", "description": "Works well alongside claude",
     "authors": ["Fixture"], "distribution": {"npx": {"package": "other-agent@1.0.0"}}},
    {"id": "Bad Id", "name": "Broken", "version": "1.0.0", "description": "", "distribution": {}},
]}

# The fixture `t3` package the loopback npm registry serves.
NPM_PACKAGES = {"/t3": {
    "name": "t3", "dist-tags": {"latest": "1.2.3", "nightly": "1.3.0-nightly.1"},
    "versions": {
        "1.2.3": {"name": "t3", "version": "1.2.3", "t3code": {"databaseMigration": 42}},
        "1.3.0-nightly.1": {"name": "t3", "version": "1.3.0-nightly.1"},
    }}}

ACP_FIXTURES = [
    ("server.searchAcpRegistry", {"query": "claude"}),
    ("server.searchAcpRegistry", {"query": "", "limit": 2}),
    ("server.prepareAcpRegistryAgent", {"agentId": "missing"}),
    ("server.prepareAcpRegistryAgent", {"agentId": "claude-mac"}),
]


def npm_registry():
    """A loopback npm registry serving NPM_PACKAGES; anything else is a 404."""
    class Registry(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            document = NPM_PACKAGES.get(self.path.split("?")[0])
            body = json.dumps(document if document is not None else {"error": "Not found"}).encode()
            self.send_response(200 if document is not None else 404)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Registry)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_port}/"
    assert url.startswith("http://127.0.0.1:")
    return server, url


def seed_acp_cache(root):
    cache = root / "caches" / "acp-registry"
    cache.mkdir(parents=True)
    (cache / "registry.json").write_text(json.dumps(ACP_REGISTRY))


def call(base, cookie, tag, payload):
    with connect(base.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2",
                 additional_headers={"Cookie": cookie}, proxy=None, compression=None) as socket:
        socket.send(encode({"_tag": "Request", "id": "edge", "tag": tag, "payload": payload, "headers": []}).decode())
        return socket.recv(timeout=60)


def without_transport_detail(frame):
    """A frame with what lies under an HttpClientError removed: the reference's fetch adds
    Node's own errors (undici's) there, which the Rust server cannot name."""
    def trim(value):
        if isinstance(value, dict):
            if value.get("name") == "HttpClientError":
                return {k: v for k, v in value.items() if k != "cause"}
            return {k: trim(v) for k, v in value.items()}
        if isinstance(value, list):
            return [trim(v) for v in value]
        return value
    return json.dumps(trim(json.loads(frame)), sort_keys=True)


FIXTURES = [
    ("mcpApps.toolInfo", {"threadId": "missing", "itemId": "missing", "name": "tool"}),
    ("mcpApps.readResource", {"threadId": "missing", "itemId": "missing", "uri": "resource"}),
    ("mcpApps.updateModelContext", {"threadId": "missing", "itemId": "missing", "conversationThreadId": "conversation"}),
    ("provider.auth.start", {"instanceId": "missing"}),
    ("provider.auth.cancel", {"instanceId": "missing", "flowId": "flow"}),
    ("provider.install.start", {"instanceId": "missing"}),
    ("provider.install.subscribe", {"instanceId": "missing"}),
    ("provider.uploadFeedback", {"threadId": "missing"}),
    ("attachments.delete", {"attachmentId": "unknown"}),
    ("assets.persistChatAttachments", {"threadId": "thread", "messageId": "message", "attachments": []}),
    ("assets.createUrl", {"resource": {}}),
    ("filesystem.getMetadata", {"paths": []}),
    ("sourceControl.signInWait", {"provider": "unknown", "host": "unknown"}),
    ("sourceControl.starred", {"provider": "unknown", "host": "unknown", "repository": "owner/repo"}),
    ("peerLinks.unlink", {"environmentId": "unknown"}),
    ("scheduledTasks.delete", {"id": "unknown"}),
    ("scheduledTasks.setEnabled", {"id": "unknown", "enabled": False}),
    ("scheduledTasks.listWebhookDeliveries", {"id": "unknown"}),
    ("secrets.answerRequest", {"threadId": "missing", "turnItemId": "missing", "answer": {"type": "decline"}}),
    ("server.getNpmRelease", {"spec": "latest"}),
    ("server.getNpmRelease", {"spec": "nightly"}),
    ("server.getNpmRelease", {"spec": "1.2.3"}),
    *ACP_FIXTURES,
    ("orchestration.searchThreadStream", {"threadId": "missing", "query": "needle"}),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    credentials = secrets.token_urlsafe(32), secrets.token_bytes(32)
    npm = shutil.which("npm")
    if npm is None:
        raise SystemExit("rpc_edge_parity needs the npm CLI on PATH: both servers read npm releases with it")
    failures = 0
    total = 0
    with tempfile.TemporaryDirectory() as temporary, contextlib.ExitStack() as stack:
        registry, registry_url = npm_registry()
        stack.callback(registry.shutdown)
        env = {"npm_config_registry": registry_url, "npm_config_update_notifier": "false",
               "PATH": os.pathsep.join([str(Path(npm).parent), os.environ.get("PATH", "")])}

        def pair(label, seeded):
            servers = []
            for name, command in (("original", args.original_command), ("rust", args.rust_command)):
                root = Path(temporary) / f"{label}-{name}"
                if seeded:
                    seed_acp_cache(root)
                servers.append(managed_server(stack, shlex.split(command), root, *credentials, env=env))
            return servers

        def compare(servers, fixtures, normalize=lambda frame: frame, suffix=""):
            nonlocal failures, total
            for tag, payload in fixtures:
                total += 1
                values = [normalize(call(base, cookie, tag, payload)) for base, cookie in servers]
                if values[0] != values[1]:
                    failures += 1
                    print(f"FAIL {tag}{suffix} {json.dumps(payload)}\n  original {values[0]}\n  rust     {values[1]}")
                else:
                    print(f"PASS {tag}{suffix} {json.dumps(payload)}")

        compare(pair("seeded", True), FIXTURES)
        compare(pair("uncached", False), ACP_FIXTURES[:1] + ACP_FIXTURES[2:3], without_transport_detail,
                " (no cached ACP Registry)")
    print(f"{total - failures}/{total} deterministic edge fixtures matched")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
