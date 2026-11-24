#!/usr/bin/env python3
"""List pull requests for Bitbucket projects on the reference and on the Rust server, with the
Bitbucket API a loopback fake (`T3CODE_BITBUCKET_API_BASE_URL`), and compare what each lists
and every request each sends.

Viewer cases: `pullRequests.list` first asks for the viewer (`GET /user`), which the fake answers
with 401 (the credential is rejected; Bitbucket answers an anonymous `/user` so), 404, 500, a
dropped connection, nothing at all (a closed port), or an account with no name.

Listing, once the credential is accepted: the fake holds a dozen pull requests in every state,
two updated in the same millisecond, a draft, ones the viewer wrote or is asked to review, and
one that does not decode. It applies Bitbucket's `q` filters itself and pages four at a time with
next links on its own origin. Covered: each state, a search, author, draft and label filters,
following the list cursor to the end, and two more repositories whose reads fail (an error, and
a next link to another origin). All data is fake. Nothing may leave loopback: every server runs behind the harness's loopback proxy, which forwards nothing (parity_harness.assert_nothing_escaped).
"""
import argparse
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import socket
import subprocess
from tempfile import TemporaryDirectory
import threading
from urllib.parse import parse_qs, urlencode, urlsplit

from websockets.sync.client import connect

import parity_harness
from parity_harness import encode, http, isolated_git_env, managed_server, recv_frame

CASES = ("401", "404", "500", "dropped connection", "closed port", "an account with no name")
TOKEN = "fixture-bitbucket-token"
VIEWER = "fixture-viewer"
REPOSITORIES = {"project:main": "example-org/example-repo", "project:broken": "example-org/broken",
                "project:escape": "example-org/escape"}


def fake_failing(case, seen):
    class Bitbucket(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            seen.append({"path": self.path, "authorization": self.headers.get("Authorization")})
            if case == "dropped connection":
                self.connection.close()
                return
            status, body = (200, {"type": "user"}) if case == "an account with no name" else (
                int(case), {"type": "error", "error": {"message": "Fixture"}})
            data = encode(body)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
    return Bitbucket


def pull_requests():
    """The fixture repository's pull requests, newest last; #7 and #6 share a millisecond."""
    start = datetime(2026, 11, 20, 9, 0, tzinfo=timezone.utc)
    states = {3: "MERGED", 4: "DECLINED", 5: "SUPERSEDED"}
    titles = {8: "Fix the login flow", 9: "fix: typo", 12: "Refactor the fixture"}
    rows = []
    for number in range(1, 13):
        updated = start + timedelta(hours=number)
        if number == 7:
            updated = start + timedelta(hours=6, microseconds=400)
        if number == 6:
            updated = start + timedelta(hours=6, microseconds=100)
        author = {"nickname": VIEWER, "display_name": "Fixture Viewer"} if number == 11 else {
            "display_name": f"Author {number}", "links": {"avatar": {"href": f"https://example.invalid/a/{number}.png"}}}
        row = {"id": number, "title": titles.get(number, f"Change {number}"), "description": f"Body {number}",
               "state": states.get(number, "OPEN"), "draft": number == 10, "author": author,
               "source": {"branch": {"name": f"feature/{number}"}, "commit": {"hash": f"{number:040x}"},
                          "repository": {"full_name": "example-org/example-repo"}},
               "destination": {"branch": {"name": "main"}},
               "created_on": (start - timedelta(days=1)).isoformat(),
               "updated_on": updated.isoformat(),
               "reviewers": [{"nickname": VIEWER}] if number == 9 else [],
               "links": {"html": {"href": f"https://example.invalid/example-org/example-repo/pull-requests/{number}"}}}
        if number == 2:
            del row["links"]  # does not decode, so neither server lists it
        rows.append(row)
    return rows


def matches(row, predicate):
    """One of Bitbucket's `q` predicates, as both servers write them."""
    if m := re.fullmatch(r'\(title ~ "(.*)" OR description ~ "(.*)"\)', predicate):
        needle = m.group(1).replace('\\"', '"').replace("\\\\", "\\").lower()
        return needle in row["title"].lower() or needle in row.get("description", "").lower()
    if m := re.fullmatch(r'\(author\.nickname = "(.*)" OR author\.display_name = "(.*)"\)', predicate):
        who = m.group(1)
        return who in (row["author"].get("nickname"), row["author"].get("display_name"))
    if m := re.fullmatch(r"draft = (true|false)", predicate):
        return row["draft"] == (m.group(1) == "true")
    if m := re.fullmatch(r"updated_on (<=?) (\S+)", predicate):
        updated = datetime.fromisoformat(row["updated_on"])
        bound = datetime.fromisoformat(m.group(2).replace("Z", "+00:00"))
        return updated <= bound if m.group(1) == "<=" else updated < bound
    if m := re.fullmatch(r"id != (\d+)", predicate):
        return row["id"] != int(m.group(1))
    raise AssertionError(f"unexpected predicate {predicate!r}")


def fake_accepting(seen):
    rows = pull_requests()

    class Bitbucket(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def reply(self, status, body):
            data = encode(body)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            seen.append(self.path)
            if self.headers.get("Authorization") != f"Bearer {TOKEN}":
                return self.reply(401, {"type": "error"})
            parts = urlsplit(self.path)
            if parts.path == "/2.0/user":
                return self.reply(200, {"nickname": VIEWER, "display_name": "Fixture Viewer"})
            if parts.path == "/2.0/repositories/example-org/broken/pullrequests":
                return self.reply(500, {"type": "error"})
            query = parse_qs(parts.query)
            base = f"http://127.0.0.1:{self.server.server_port}"
            if parts.path == "/2.0/repositories/example-org/escape/pullrequests":
                return self.reply(200, {"values": [], "next": "http://localhost:1/2.0/elsewhere?page=2"})
            if parts.path != "/2.0/repositories/example-org/example-repo/pullrequests":
                return self.reply(404, {"type": "error"})
            states = query.get("state", [])
            predicates = query["q"][0].split(" AND ") if "q" in query else []
            listed = [r for r in rows if r["state"] in states and all(matches(r, p) for p in predicates)]
            listed.sort(key=lambda r: r["updated_on"], reverse=True)
            page = int(query.get("page", ["1"])[0])
            values = listed[(page - 1) * 4:page * 4]
            more = page * 4 < len(listed)
            following = {k: v for k, v in query.items() if k != "page"}
            next_url = f"{base}{parts.path}?{urlencode(following, doseq=True)}&page={page + 1}" if more else None
            self.reply(200, {"values": values, "size": len(listed), "next": next_url})

    return Bitbucket


def closed_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def git(*args, env):
    # parity-env: git builds the fixture checkout, not a T3 server
    subprocess.run(["git", *args], check=True, capture_output=True, env=env)


def list_pull_requests(url, cookie, payload):
    with connect(url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2",
                 additional_headers={"Cookie": cookie}, proxy=None, compression=None) as ws:
        ws.send(encode({"_tag": "Request", "id": "list", "tag": "pullRequests.list", "payload": payload,
                        "headers": []}).decode())
        while True:
            frame = json.loads(recv_frame(ws, timeout=90))
            if frame.get("_tag") == "Ping":
                ws.send(encode({"_tag": "Pong"}).decode())
            elif frame.get("requestId") == "list" and "exit" in frame:
                return frame["exit"]


def start_api(stack, handler):
    api = stack.enter_context(ThreadingHTTPServer(("127.0.0.1", 0), handler))
    api.daemon_threads = True
    threading.Thread(target=api.serve_forever, daemon=True).start()
    stack.callback(api.shutdown)
    return api.server_port


def start_server(stack, command, home, tmp, base, projects, extra_env=None):
    assert urlsplit(base).hostname == "127.0.0.1"
    env = {**isolated_git_env(tmp / "git"), "T3CODE_BITBUCKET_API_BASE_URL": base, **(extra_env or {})}
    git_env = {**os.environ, **env}
    checkouts = {}
    for project_id, repository in projects.items():
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
            "title": project_id, "workspaceRoot": str(checkout)}), {"Cookie": cookie, "Content-Type": "application/json"})
        assert status == 200, (status, body)
    return url, cookie


def mask(value, port):
    text = parity_harness.mask_port(json.dumps(value), port)
    return json.loads(re.sub(r'"observedAt": \d+', '"observedAt": "<ms>"', text))


def listing(url, cookie, port):
    """Every list call of the accepted-credential scenario: {name: answer}."""
    record = {}
    main = {"projectId": "project:main"}

    def call(name, payload):
        record[name] = mask(list_pull_requests(url, cookie, payload), port)
        return record[name]

    for state in ("open", "merged", "closed", "all"):
        call(f"state {state}", {**main, "state": state})
    call("a search", {**main, "state": "all", "query": "fix"})
    call("author @me", {**main, "state": "all", "filters": {"author": "@me"}})
    call("drafts only", {**main, "state": "all", "filters": {"draft": "only"}})
    call("a label filter (no Bitbucket row has labels)", {**main, "state": "all", "filters": {"labels": [["bug"]]}})
    # Six a page ends the first page on #7, which shares its millisecond with #6: the cursor
    # takes the whole millisecond again and leaves out the rows already sent.
    answer = call("page 1, ending inside a millisecond", {**main, "state": "all", "limit": 6})
    for page in (2, 3):
        cursors = answer.get("value", {}).get("nextCursors", {})
        if not cursors:
            break
        answer = call(f"page {page}", {**main, "state": "all", "limit": 6, "cursors": cursors})
    call("every project, two unreadable", {"state": "open"})
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    answers = {}
    with TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        for side, command in (("original", args.original_command), ("rust", args.rust_command)):
            record = answers.setdefault(side, {})
            for case in CASES:
                with ExitStack() as stack:
                    seen = []
                    port = closed_port() if case == "closed port" else start_api(stack, fake_failing(case, seen))
                    home = tmp / side / case.replace(" ", "-")
                    url, cookie = start_server(stack, command, home, tmp, f"http://127.0.0.1:{port}/2.0",
                                               {"project:main": REPOSITORIES["project:main"]})
                    record[f"viewer {case}: answer"] = mask(
                        list_pull_requests(url, cookie, {"state": "open", "projectId": "project:main"}), port)
                    record[f"viewer {case}: requests"] = seen
            with ExitStack() as stack:
                seen = []
                port = start_api(stack, fake_accepting(seen))
                url, cookie = start_server(stack, command, tmp / side / "accepted", tmp, f"http://127.0.0.1:{port}/2.0",
                                           REPOSITORIES, {"T3CODE_BITBUCKET_ACCESS_TOKEN": TOKEN})
                record.update({f"listing: {k}": v for k, v in listing(url, cookie, port).items()})
                # Repositories are read side by side, so compare the set of requests.
                record["listing: requests"] = sorted(set(json.loads(parity_harness.mask_port(json.dumps(seen), port))))
    # Every server ran behind a loopback proxy that forwards nothing; what it refused never left.
    parity_harness.assert_nothing_escaped()
    failures = 0
    for name in answers["original"]:
        same = answers["original"][name] == answers["rust"].get(name)
        failures += not same
        print(f"{'PASS' if same else 'FAIL'} {name}")
        if not same:
            print(f"  original {json.dumps(answers['original'][name])[:3000]}\n  rust     {json.dumps(answers['rust'].get(name))[:3000]}")
    total = len(answers["original"])
    print(f"{total - failures}/{total} Bitbucket pull request checks matched; nothing left loopback")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
