#!/usr/bin/env python3
"""Compare deterministic RPC edge contracts on fresh WebSocket sessions."""
import argparse
import contextlib
import json
from pathlib import Path
import secrets
import shlex
import tempfile

from websockets.sync.client import connect

from parity_harness import encode, managed_server


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
    ("server.searchAcpRegistry", {"query": "claude"}),
    ("server.prepareAcpRegistryAgent", {"agentId": "missing"}),
    ("orchestration.searchThreadStream", {"threadId": "missing", "query": "needle"}),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    credentials = secrets.token_urlsafe(32), secrets.token_bytes(32)
    failures = 0
    with tempfile.TemporaryDirectory() as temporary, contextlib.ExitStack() as stack:
        servers = [
            managed_server(stack, shlex.split(command), Path(temporary) / name, *credentials)
            for name, command in (("original", args.original_command), ("rust", args.rust_command))
        ]
        for tag, payload in FIXTURES:
            values = []
            for base, cookie in servers:
                with connect(
                    base.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2",
                    additional_headers={"Cookie": cookie},
                    proxy=None,
                    compression=None,
                ) as socket:
                    socket.send(encode({"_tag": "Request", "id": "edge", "tag": tag, "payload": payload, "headers": []}).decode())
                    values.append(socket.recv(timeout=10))
            if values[0] != values[1]:
                failures += 1
                print(f"FAIL {tag}\n  original {values[0]}\n  rust     {values[1]}")
            else:
                print(f"PASS {tag}")
    print(f"{len(FIXTURES) - failures}/{len(FIXTURES)} deterministic edge fixtures matched")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
