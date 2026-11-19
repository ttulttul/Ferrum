#!/usr/bin/env python3
"""Compare durable orchestration command replies across a restart of each server."""
import argparse
from contextlib import ExitStack
from pathlib import Path
import secrets
import shlex
from tempfile import TemporaryDirectory

from websockets.sync.client import connect
from parity_harness import encode, managed_server


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    credentials = secrets.token_urlsafe(32), secrets.token_bytes(32)
    first = "thread:opaque-a"
    other = "thread:opaque-b"

    def command(kind, cid, tid=first, **params):
        return "orchestration.dispatchCommand", dict(type=kind, commandId=cid, threadId=tid, **params)

    create = command("thread.create", "create-1", projectId="project:opaque",
                     title="Draft", modelSelection=dict(instanceId="codex", model="fixture"),
                     runtimeMode="full-access", interactionMode="default", branch=None,
                     worktreePath=None, createdBy="user", creationSource="web")
    titles = "orchestration.getThreadTitles", dict(threadIds=[first, other])
    metadata = "orchestration.getThreadLinkMetadata", dict(threadIds=[first])
    archive = command("thread.archive", "archive-1")
    sessions = [
        [create, titles, metadata, command("thread.mark-unread","no-run"),
         command("thread.unarchive","active"),archive, archive, command("thread.archive","archived"), command("thread.archive", "archive-1", other),
         command("thread.pin", "pin-archived")],
        [create, archive, titles, command("thread.unarchive", "unarchive-1"),
         command("thread.metadata.update", "update-1", title="Ready"), metadata,
         command("thread.delete", "delete-1"), titles, command("thread.delete", "delete-1"),
         (create[0], dict(create[1],commandId="reuse-deleted")),
         command("thread.archive", "missing-1", other), command("thread.guest-share.close", "missing-2", other)]
    ]
    count = 0
    with TemporaryDirectory() as temporary:
        for phase, requests in enumerate(sessions):
            with ExitStack() as stack:
                sockets = []
                for name, executable in [("ts", args.original_command), ("rust", args.rust_command)]:
                    base, cookie = managed_server(stack, shlex.split(executable), Path(temporary) / name,
                                                  *credentials)
                    sockets.append(stack.enter_context(connect(base.replace("http", "ws", 1)
                        + "/ws?orchestrationProtocol=2", additional_headers={"Cookie": cookie}, proxy=None)))
                for tag, payload in requests:
                    frame = encode(dict(_tag="Request", id=str(count), tag=tag, payload=payload, headers=[])).decode()
                    for ws in sockets:
                        ws.send(frame)
                    reference, rust = [ws.recv(timeout=10) for ws in sockets]
                    if reference != rust:
                        raise AssertionError(f"Phase {phase+1} {tag} {payload}\n TS {reference}\n RS {rust}")
                    count += 1
    print(f"{count}/{count} orchestration lifecycle frames matched across restart")


if __name__ == "__main__":
    main()
