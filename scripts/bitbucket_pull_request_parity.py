#!/usr/bin/env python3
"""Open single Bitbucket pull requests on the reference and on the Rust server (detail, preview,
checks, activity, reviewer candidates, the diff over HTTP, file contents, change breakdown) and act on them (merge, decline, edit, comment, review,
reply, resolve, react, request reviewers), against a loopback Bitbucket API
(`T3CODE_BITBUCKET_API_BASE_URL`), and compare every answer and every request each server
sends, bodies included.

The fake holds an open and a merged pull request with reviewers and approvals, paged diffstats
and comments (inline threads, replies, a resolved thread, deleted and pending comments), commits,
build statuses (reruns of one key, two pipelines with one job name), workspace members, diffs
(redirected as Bitbucket does, one over the 8 MiB cap, one redirected to another loopback origin),
and the
viewer's repository permission: write on one repository, read on another, where actions are
refused. All data is fake. Nothing may leave loopback: every server runs behind the harness's
loopback proxy, which forwards nothing (parity_harness.assert_nothing_escaped).
"""
import argparse
from contextlib import ExitStack
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import sqlite3
import subprocess
from tempfile import TemporaryDirectory
import threading
from urllib.parse import parse_qs, unquote, urlsplit

from websockets.sync.client import connect

import parity_harness
from parity_harness import encode, http, isolated_git_env, managed_server, recv_frame

TOKEN = "fixture-bitbucket-token"
WRITE, READ = "example-org/example-repo", "example-org/readonly"


def user(name, uuid):
    return {"uuid": uuid, "nickname": name, "display_name": name.replace("-", " ").title(),
            "links": {"avatar": {"href": f"https://example.invalid/avatars/{name}.png"}}}


AUTHOR, REVIEWER, OTHER, VIEWER = (user("pr-author", "{a}"), user("reviewer-one", "{r}"),
                                   user("other-member", "{o}"), user("fixture-viewer", "{v}"))


def pull_request(repository, number, state="OPEN"):
    merged = state != "OPEN"
    return {
        "id": number, "title": f"Change {number}", "description": f"Body of {number}", "state": state,
        "draft": False, "author": AUTHOR,
        "source": {"branch": {"name": f"feature/{number}"}, "commit": {"hash": f"{number:040x}"},
                   "repository": {"full_name": repository}},
        "destination": {"branch": {"name": "main"}, "commit": {"hash": "f" * 40}},
        "created_on": "2026-11-19T09:00:00.000000+00:00", "updated_on": "2026-11-20T10:00:00.123456+00:00",
        "closed_on": "2026-11-20T11:00:00.000000+00:00" if merged else None,
        "reviewers": [REVIEWER],
        "participants": [{"user": REVIEWER, "role": "REVIEWER", "approved": True, "state": "approved",
                          "participated_on": "2026-11-20T09:30:00.000000+00:00"},
                         {"user": OTHER, "role": "PARTICIPANT", "approved": False, "state": None,
                          "participated_on": "2026-11-20T09:40:00.000000+00:00"}],
        "links": {"html": {"href": f"https://example.invalid/{repository}/pull-requests/{number}"}},
    }


def comment(cid, body, at, **extra):
    return {"id": cid, "content": {"raw": body}, "user": REVIEWER, "created_on": at,
            "links": {"html": {"href": f"https://example.invalid/comments/{cid}"}}, **extra}


COMMENTS = [
    [comment(101, "A general remark", "2026-11-20T09:10:00+00:00"),
     comment(201, "Inline root", "2026-11-20T09:11:00+00:00", inline={"path": "src/a.rs", "from": None, "to": 3}),
     comment(203, "gone", "2026-11-20T09:12:00+00:00", deleted=True)],
    [comment(202, "A reply", "2026-11-20T09:13:00+00:00", parent={"id": 201}),
     comment(204, "draft", "2026-11-20T09:14:00+00:00", pending=True),
     comment(205, "Resolved on the old side", "2026-11-20T09:15:00+00:00",
             inline={"path": "src/b.rs", "from": 7, "to": None, "outdated": True}, resolution={"type": "resolved"}),
     comment(206, "   ", "2026-11-20T09:16:00+00:00")],
]
STATUSES = [
    {"key": "pipeline-a", "name": "build", "state": "SUCCESSFUL", "url": "https://example.invalid/a/1"},
    {"key": "pipeline-b", "name": "build", "state": "FAILED", "description": "Broke", "url": "https://example.invalid/b/1"},
    {"key": "lint", "name": "lint", "state": "INPROGRESS"},
    {"key": "pipeline-a", "name": "build", "state": "STOPPED", "url": "https://example.invalid/a/2"},
    {"key": "", "name": ""},
]
COMMITS = [{"hash": "c2" * 20, "message": "Second\n\nbody", "date": "2026-11-20T08:00:00+00:00",
            "author": {"raw": "Pr Author <a@example.invalid>", "user": AUTHOR}},
           {"hash": "c1" * 20, "message": "First", "date": "2026-11-19T08:00:00+00:00",
            "author": {"raw": "Someone Else <s@example.invalid>"}},
           {"hash": "c0" * 20, "message": "Undated"}]


PATCH = ("diff --git a/src/a.rs b/src/a.rs\nindex 1111111..2222222 100644\n--- a/src/a.rs\n+++ b/src/a.rs\n"
         "@@ -1,2 +1,3 @@ COMMIT\n fn main() {\n+    println!(\"fixture\");\n }\n")


# Another loopback origin a diff is redirected to; it answers the patch and records whether a
# credential came along.
ELSEWHERE = {}


def fake_elsewhere(seen):
    class Elsewhere(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            seen.append({"method": "GET", "origin": "elsewhere", "path": self.path,
                         "credential": self.headers.get("Authorization") is not None})
            data = PATCH.replace("COMMIT", "elsewhere").encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
    return Elsewhere


def fake_bitbucket(seen):
    class Bitbucket(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def reply(self, status, body=None):
            data = b"" if body is None else encode(body)
            self.send_response(status)
            if body is not None:
                self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def text(self, status, body):
            data = body.encode()
            self.send_response(status)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def redirect(self, location):
            self.send_response(302)
            self.send_header("Location", location)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def handle_any(self, method):
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b""
            parts = urlsplit(self.path)
            query = parse_qs(parts.query)
            entry = {"method": method, "path": unquote(parts.path), "query": {k: v for k, v in sorted(query.items())}}
            if raw:
                entry["body"] = json.loads(raw)
            seen.append(entry)
            if self.headers.get("Authorization") != f"Bearer {TOKEN}":
                return self.reply(401, {"type": "error"})
            origin = f"http://127.0.0.1:{self.server.server_port}"
            path = unquote(parts.path)
            if hasattr(self.server, "forge_rows"):
                base = f"/2.0/repositories/{WRITE}"
                if path == base:
                    return self.reply(200, {"full_name": WRITE, "name": "example-repo", "mainbranch": {"name": "main"}})
                if path == base + "/branching-model":
                    return self.reply(200, {"development": {"use_mainbranch": True}})
                if path == base + "/pullrequests":
                    if method == "GET":
                        return self.reply(200, {"values": self.server.forge_rows})
                    if method == "POST":
                        row = pull_request(WRITE, 7)
                        content = entry["body"]
                        row.update(title=content["title"], description=content["description"])
                        row["source"]["branch"]["name"] = content["source"]["branch"]["name"]
                        row["links"]["html"]["href"] = f"https://bitbucket.org/{WRITE}/pull-requests/7"
                        self.server.forge_rows.append(row)
                        return self.reply(201, row)
            if path == "/2.0/user":
                return self.reply(200, {"username": "fixture-viewer", "nickname": "fixture-viewer", "display_name": "Fixture Viewer"})
            if path == "/2.0/user/workspaces/example-org/permissions/repositories":
                permission = "read" if "readonly" in query.get("q", [""])[0] else "write"
                return self.reply(200, {"values": [{"permission": permission}]})
            if path == "/2.0/workspaces/example-org/members":
                return self.reply(200, {"values": [{"user": u} for u in (AUTHOR, REVIEWER, OTHER, VIEWER)]
                                        + [{"user": {"nickname": "no-uuid"}}], "next": None})
            if m := re.fullmatch(r"/2\.0/repositories/(example-org/[\w-]+)/diff/(.+)", path):
                return self.text(200, PATCH.replace("COMMIT", m.group(2)))
            m = re.fullmatch(r"/2\.0/repositories/(example-org/[\w-]+)/pullrequests/(\d+)(/.*)?", path)
            if not m:
                return self.reply(404, {"type": "error"})
            repository, number, rest = m.group(1), int(m.group(2)), m.group(3) or ""
            if number == 99:
                return self.reply(404, {"type": "error", "error": {"message": "Not found"}})
            state = "MERGED" if number == 8 else "OPEN"
            if method == "GET" and rest == "/diff":
                # Bitbucket answers a pull request's diff with a redirect to where it is rendered.
                if number == 15:
                    return self.text(200, PATCH * (9 * 1024 * 1024 // len(PATCH) + 1))
                if number == 16:
                    return self.redirect(f"{ELSEWHERE['url']}/rendered/{number}.diff")
                return self.redirect(f"/2.0/repositories/{repository}/diff/{repository}:{number:040x}..{'f' * 40}?from_pullrequest_id={number}")
            if method == "GET" and rest == "":
                return self.reply(200, pull_request(repository, number, state))
            if method == "GET" and rest == "/diffstat":
                if query.get("page") == ["2"]:
                    return self.reply(200, {"values": [{"lines_added": 4, "lines_removed": 0}, {"status": "x"}], "next": None})
                return self.reply(200, {"values": [{"lines_added": 10, "lines_removed": 2}, {"lines_added": None, "lines_removed": 5}],
                                        "next": f"{origin}{path}?pagelen=50&page=2"})
            if method == "GET" and rest == "/conflicts":
                return self.reply(200, {"values": [] if number == 7 else [{"path": "x"}]})
            if method == "GET" and rest == "/statuses":
                return self.reply(200, {"values": STATUSES, "next": None})
            if method == "GET" and rest == "/comments":
                page = int(query.get("page", ["1"])[0])
                more = page < len(COMMENTS)
                return self.reply(200, {"values": COMMENTS[page - 1],
                                        "next": f"{origin}{path}?pagelen=50&page={page + 1}" if more else None})
            if method == "GET" and rest == "/commits":
                return self.reply(200, {"values": COMMITS, "next": None})
            if method == "POST" and rest == "/merge":
                return self.reply(200, pull_request(repository, number, "MERGED"))
            if method == "POST" and rest == "/decline":
                return self.reply(200, pull_request(repository, number, "DECLINED"))
            if method in ("POST", "PUT", "DELETE"):
                return self.reply(200 if method != "DELETE" else 204, {} if method != "DELETE" else None)
            self.reply(404, {"type": "error"})

        def do_GET(self):
            self.handle_any("GET")

        def do_POST(self):
            self.handle_any("POST")

        def do_PUT(self):
            self.handle_any("PUT")

        def do_DELETE(self):
            self.handle_any("DELETE")
    return Bitbucket


def git(*args, env):
    # parity-env: git builds the fixture checkouts, not a T3 server
    subprocess.run(["git", *args], check=True, capture_output=True, env=env)


def call(url, cookie, tag, payload):
    with connect(url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2",
                 additional_headers={"Cookie": cookie}, proxy=None, compression=None) as ws:
        ws.send(encode({"_tag": "Request", "id": "r", "tag": tag, "payload": payload, "headers": []}).decode())
        while True:
            frame = json.loads(recv_frame(ws, timeout=90))
            if frame.get("_tag") == "Ping":
                ws.send(encode({"_tag": "Pong"}).decode())
            elif frame.get("requestId") == "r" and "exit" in frame:
                return frame["exit"]


def masked(value, port, home):
    text = parity_harness.mask_port(json.dumps(value), port).replace(str(home), "<home>")
    if ELSEWHERE.get("url"):
        text = text.replace(ELSEWHERE["url"], "<elsewhere>")
    return json.loads(re.sub(r'"observedAt": \d+', '"observedAt": "<ms>"', text))


def scenario(url, cookie, seen, port, home):
    record = {}

    def step(name, tag, payload):
        seen.clear()
        answer = call(url, cookie, tag, payload)
        # Reads of one pull request are made side by side, so compare them as a set.
        requests = sorted(json.dumps(r, sort_keys=True) for r in seen)
        record[name] = masked({"answer": answer, "requests": [json.loads(r) for r in requests]}, port, home)

    pr = {"projectId": "project:write", "repository": WRITE, "number": 7}
    ro = {"projectId": "project:read", "repository": READ, "number": 7}
    step("detail, open", "pullRequests.detail", pr)
    step("detail, merged", "pullRequests.detail", {**pr, "number": 8})
    step("detail, missing", "pullRequests.detail", {**pr, "number": 99})
    step("preview", "pullRequests.preview", {**pr, "number": 9})
    step("checks", "pullRequests.checks", {**pr, "number": 10})
    step("activity", "pullRequests.activity", {**pr, "number": 11})
    step("reviewer candidates", "pullRequests.reviewerCandidates", {**pr, "number": 12})

    def diff(name, payload):
        seen.clear()
        status, _, body = http(url, "POST", "/api/pull-requests/diff", encode(payload),
                               {"Cookie": cookie, "Content-Type": "application/json"})
        answer = json.loads(body) if body else None
        if isinstance(answer, dict) and isinstance(answer.get("patch"), str):
            patch = answer["patch"]
            answer = {**answer, "patch": {"length": len(patch), "sha256": hashlib.sha256(patch.encode()).hexdigest(),
                                          "start": patch[:120]}}
        requests = sorted(json.dumps(r, sort_keys=True) for r in seen)
        record[name] = masked({"answer": {"status": status, "body": answer},
                               "requests": [json.loads(r) for r in requests]}, port, home)

    diff("diff, through Bitbucket's redirect", pr)
    diff("diff of a listed commit by prefix", {**pr, "commit": "C2C2C2C"})
    diff("diff of an unlisted commit", {**pr, "commit": "abcdef1"})
    diff("diff larger than the cap", {**pr, "number": 15})
    diff("diff redirected to another host", {**pr, "number": 16})
    diff("diff of a missing pull request", {**pr, "number": 99})
    diff("diff with a commit that is not a sha", {**pr, "commit": "not-a-sha"})
    diff("diff of one file", {**pr, "path": "src/a.rs"})
    step("a file's contents at both sides", "pullRequests.diffFileContents",
         {**pr, "changeType": "change", "oldPath": "src/a.rs", "newPath": "src/a.rs"})
    step("change breakdown", "pullRequests.changeBreakdown", pr)
    # Viewed files, which this environment keeps for Bitbucket: a tick without a version takes the
    # head's from the diff (src/a.rs), one with an older version reads back dismissed (src/b.rs),
    # one the diff lacks is a deleted file (docs/gone.md), and an untick removes its mark.
    viewed = {**pr, "number": 17}
    step("files viewed, none yet", "pullRequests.filesViewed", viewed)
    step("tick files", "pullRequests.setFilesViewed", {**viewed, "files": [
        {"path": "src/a.rs", "viewed": True}, {"path": "src/b.rs", "viewed": True, "revision": "1111111"},
        {"path": "docs/gone.md", "viewed": True}]})
    step("files viewed after ticking", "pullRequests.filesViewed", viewed)
    step("untick a file", "pullRequests.setFilesViewed", {**viewed, "files": [{"path": "src/b.rs", "viewed": False}]})
    step("files viewed after unticking", "pullRequests.filesViewed", viewed)
    step("update title and body", "pullRequests.update", {**pr, "title": "New title", "body": "New body"})
    step("update nothing", "pullRequests.update", pr)
    step("comment", "pullRequests.comment", {**pr, "body": "Thanks!"})
    step("comment on a read-only repository", "pullRequests.comment", {**ro, "body": "Thanks!"})
    step("edit a comment", "pullRequests.updateComment", {**pr, "commentId": "101", "kind": "issue-comment", "body": "Edited"})
    step("approve with an inline comment and a summary", "pullRequests.submitReview", {
        **pr, "verdict": "approve", "body": "Looks good",
        "comments": [{"path": "src/a.rs", "position": {"kind": "added", "newLine": 4}, "body": "Nice"},
                     {"path": "src/b.rs", "position": {"kind": "context", "oldLine": 2, "newLine": 3, "side": "left"}, "body": "Old side"}]})
    step("request changes", "pullRequests.submitReview", {**pr, "verdict": "request-changes", "body": "Please fix", "comments": []})
    step("a comment-only review with nothing in it", "pullRequests.submitReview", {**pr, "verdict": "comment", "body": " ", "comments": []})
    step("approve on a read-only repository", "pullRequests.submitReview", {**ro, "verdict": "approve", "body": "", "comments": []})
    step("reply to a thread", "pullRequests.replyToThread", {**pr, "threadId": "201", "body": "Done"})
    step("resolve a thread", "pullRequests.setThreadResolution", {**pr, "threadId": "201", "resolved": True})
    step("reopen a thread", "pullRequests.setThreadResolution", {**pr, "threadId": "205", "resolved": False})
    step("react", "pullRequests.setReaction", {**pr, "content": "thumbs-up", "reacted": True})
    step("request a reviewer", "pullRequests.requestReviewers", {**pr, "reviewers": [{"id": "{o}", "kind": "user"}], "requested": True})
    step("withdraw a reviewer request", "pullRequests.requestReviewers", {**pr, "reviewers": [{"id": "{r}", "kind": "user"}], "requested": False})
    step("refuse a moved head", "pullRequests.runAction", {**pr, "action": "merge", "expectedHeadSha": "abc1234"})
    step("merge by squashing", "pullRequests.runAction", {**pr, "action": "merge", "mergeMethod": "squash", "expectedHeadSha": "0000000", "replyWithSummary": True})
    step("merge by rebasing", "pullRequests.runAction", {**pr, "number": 13, "action": "merge", "mergeMethod": "rebase"})
    step("decline", "pullRequests.runAction", {**pr, "number": 14, "action": "close", "replyWithSummary": True})
    step("an action Bitbucket lacks", "pullRequests.runAction", {**pr, "action": "ready"})
    step("merge on a read-only repository", "pullRequests.runAction", {**ro, "action": "merge"})
    return record


def viewed_rows(home):
    """The marks a server wrote to pull_request_files_viewed, its tick times masked."""
    [database] = list(home.rglob("statev2.sqlite"))
    with sqlite3.connect(database) as db:
        rows = db.execute("SELECT provider, host, repository, number, viewer, path, revision, viewed_at"
                          " FROM pull_request_files_viewed ORDER BY path").fetchall()
    return [[*row[:7], "<time>" if row[7] else None] for row in rows]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    answers = {}
    with TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        for side, command in (("original", args.original_command), ("rust", args.rust_command)):
            with ExitStack() as stack:
                seen = []
                elsewhere = stack.enter_context(ThreadingHTTPServer(("127.0.0.1", 0), fake_elsewhere(seen)))
                elsewhere.daemon_threads = True
                threading.Thread(target=elsewhere.serve_forever, daemon=True).start()
                stack.callback(elsewhere.shutdown)
                ELSEWHERE["url"] = f"http://127.0.0.1:{elsewhere.server_port}"
                api = stack.enter_context(ThreadingHTTPServer(("127.0.0.1", 0), fake_bitbucket(seen)))
                api.daemon_threads = True
                threading.Thread(target=api.serve_forever, daemon=True).start()
                stack.callback(api.shutdown)
                base = f"http://127.0.0.1:{api.server_port}/2.0"
                assert urlsplit(base).hostname == "127.0.0.1"
                home = tmp / side
                env = {**isolated_git_env(tmp / "git"), "T3CODE_BITBUCKET_API_BASE_URL": base,
                       "T3CODE_BITBUCKET_ACCESS_TOKEN": TOKEN}
                git_env = {**os.environ, **env}
                checkouts = {}
                for project_id, repository in (("project:write", WRITE), ("project:read", READ)):
                    checkout = home / project_id.split(":")[1]
                    checkout.mkdir(parents=True)
                    git("-C", str(checkout), "init", "-qb", "main", env=git_env)
                    git("-C", str(checkout), "remote", "add", "origin", f"git@bitbucket.org:{repository}.git", env=git_env)
                    checkouts[project_id] = checkout
                url, cookie = managed_server(stack, shlex.split(command), home, secrets.token_urlsafe(32),
                                             secrets.token_bytes(32), env=env)
                for project_id, checkout in checkouts.items():
                    status, _, body = http(url, "POST", "/api/projects/mutate", encode({
                        "type": "project.create", "commandId": f"create-{project_id}", "projectId": project_id,
                        "title": project_id, "workspaceRoot": str(checkout)}),
                        {"Cookie": cookie, "Content-Type": "application/json"})
                    assert status == 200, (status, body)
                answers[side] = scenario(url, cookie, seen, api.server_port, home)
                answers[side]["viewed files kept in the shared database"] = {
                    "answer": viewed_rows(home), "requests": []}
    parity_harness.assert_nothing_escaped()
    failures = 0
    # A detail's reads run side by side and the first failure stops the rest, so which of them
    # were sent before a missing pull request answered 404 depends on timing.
    answer_only = {"detail, missing"}
    for name in answers["original"]:
        for part in ("answer",) if name in answer_only else ("answer", "requests"):
            same = answers["original"][name][part] == answers["rust"].get(name, {}).get(part)
            failures += not same
            print(f"{'PASS' if same else 'FAIL'} {name}: {part}")
            if not same:
                print(f"  original {json.dumps(answers['original'][name][part])[:3000]}\n"
                      f"  rust     {json.dumps(answers['rust'].get(name, {}).get(part))[:3000]}")
    total = len(answers["original"]) * 2 - len(answer_only)
    print(f"{total - failures}/{total} single Bitbucket pull request checks matched; nothing left loopback")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
