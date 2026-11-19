#!/usr/bin/env python3
"""Compare durable orchestration command replies across a restart of each server."""
import argparse
from contextlib import ExitStack
from pathlib import Path
import secrets
import shlex
from tempfile import TemporaryDirectory

from websockets.sync.client import connect
from parity_harness import encode, http, managed_server


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--faketime-lib", type=Path, help="compare the full timestamped projection and shell")
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
    issue = dict(tracker="github",host="github.com",scope="Example/Repo",key="17")
    pr = dict(host="github.com",repository="Example/Repo",number=23)
    issue_threads = "issues.linkedThreads", dict(issue,scope="example/repo")
    pr_threads = "pullRequests.linkedThreads", dict(pr,projectId="project:opaque")
    link_issue = command("thread.issue.link","link-issue",**issue,projectId="project:opaque",url="https://github.com/Example/Repo/issues/17",displayKey="#17",title="Issue",source="manual")
    link_pr = command("thread.pull-request.link","link-pr",**pr,url="https://github.com/Example/Repo/pull/23",source="manual")
    snapshots = [] if not args.faketime_lib else [
        ("orchestration.getThreadProjection",{"threadId":first}),("orchestration.getThreadFamily",{"threadId":first}),
        *[("HTTP",path) for path in ["/api/orchestration/shell",f"/api/orchestration/threads/{first}",
          f"/api/orchestration/threads/{first}/bounded",f"/api/orchestration/threads/{first}/transcript"]]]
    edge_reads = [
        ("orchestration.getTurnDiff", {"threadId": first, "fromTurnCount": 0, "toTurnCount": 0}),
        ("orchestration.getFullThreadDiff", {"threadId": first, "toTurnCount": 0}),
        ("orchestration.getTurnDiff", {"threadId": "thread:missing", "fromTurnCount": 0, "toTurnCount": 1}),
        ("orchestration.getFullThreadDiff", {"threadId": "thread:missing", "toTurnCount": 1}),
        ("orchestration.getThreadProjection", {"threadId": "thread:missing"}),
        ("orchestration.getTurnItem", {"threadId": first, "itemId": "item:missing"}),
        ("orchestration.getNativeChildWork", {"threadId": first}),
        ("orchestration.getWorkflowScript", {"threadId": first, "scriptPath": "relative.js"}),
        ("orchestration.stopWorkflow", {"threadId": first, "subagentId": "subagent:missing"}),
        ("orchestration.forceStopThread", {"threadId": first}),
    ]
    dispatch_edge_reads = [
        command("prepared-run.release", "prepared-missing", runId="00000000-0000-4000-8000-000000000001"),
        command("provider-session.detach", "session-missing", providerSessionId="session:missing"),
        command("queued-run.cancel", "queued-missing", runId="00000000-0000-4000-8000-000000000001"),
        command("runtime-request.respond", "request-missing", requestId="request:missing"),
        command("thread.user-input.dismiss", "input-missing", requestId="request:missing"),
    ]
    sessions = [
        [create, *dispatch_edge_reads, *edge_reads, titles, metadata, *snapshots, issue_threads,pr_threads,link_issue,link_pr,issue_threads,pr_threads,*snapshots, command("thread.mark-unread","no-run"),
         command("thread.unarchive","active"),archive, archive, command("thread.archive","archived"), command("thread.archive", "archive-1", other),
         command("thread.pin", "pin-archived")],
        [create, archive, titles, *([issue_threads,pr_threads] if args.faketime_lib else []), command("thread.unarchive", "unarchive-1"),
         command("thread.metadata.update", "update-1", title="Ready"), metadata, issue_threads,pr_threads,
         command("thread.issue.unlink","unlink-issue",**issue),command("thread.pull-request.unlink","unlink-pr",**pr),issue_threads,pr_threads,
         command("thread.delete", "delete-1"), titles, command("thread.delete", "delete-1"),
         (create[0], dict(create[1],commandId="reuse-deleted")),
         command("thread.archive", "missing-1", other), command("thread.guest-share.close", "missing-2", other)]
    ]
    count = 0
    with TemporaryDirectory() as temporary:
        for phase, requests in enumerate(sessions):
            with ExitStack() as stack:
                sockets = []
                servers = []
                for name, executable in [("ts", args.original_command), ("rust", args.rust_command)]:
                    launcher = shlex.split(executable)
                    if args.faketime_lib:
                        launcher = ["env", f"LD_PRELOAD={args.faketime_lib.resolve(strict=True)}",
                                    "FAKETIME=2026-11-11 10:12:13", "TZ=UTC","FAKETIME_DONT_FAKE_MONOTONIC=1"] + launcher
                    base, cookie = managed_server(stack, launcher, Path(temporary) / name,
                                                  *credentials)
                    servers.append((base,cookie))
                    sockets.append(stack.enter_context(connect(base.replace("http", "ws", 1)
                        + "/ws?orchestrationProtocol=2", additional_headers={"Cookie": cookie}, proxy=None)))
                for tag, payload in requests:
                    if tag == "HTTP":
                        reads = [http(base,"GET",payload,headers={"Cookie":cookie,"x-t3-orchestration-protocol":"2"}) for base,cookie in servers]
                        if (reads[0][0],reads[0][2]) != (reads[1][0],reads[1][2]):
                            raise AssertionError(f"{payload}\n TS {reads[0][2]}\n RS {reads[1][2]}")
                        count += 1
                        continue
                    frame = encode(dict(_tag="Request", id=str(count), tag=tag, payload=payload, headers=[])).decode()
                    for ws in sockets:
                        ws.send(frame)
                    reference, rust = [ws.recv(timeout=10) for ws in sockets]
                    if reference != rust:
                        raise AssertionError(f"Phase {phase+1} {tag} {payload}\n TS {reference}\n RS {rust}")
                    count += 1
    print(f"{count}/{count} orchestration lifecycle comparisons matched across restart")


if __name__ == "__main__":
    main()
