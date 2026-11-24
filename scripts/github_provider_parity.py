#!/usr/bin/env python3
"""Compare authenticated RPC and HTTP responses using a local GitHub API and real gh CLI."""
import argparse
from copy import deepcopy
from contextlib import ExitStack
from http.server import BaseHTTPRequestHandler
import json
from pathlib import Path
import secrets
import shlex
import re
from socketserver import ThreadingUnixStreamServer
import subprocess
import tempfile
from threading import Thread
from urllib.parse import parse_qs, urlsplit

from websockets.sync.client import connect
import parity_harness as harness


REPO = "example/parity"
PATCH = "diff --git a/lib.rs b/lib.rs\n--- a/lib.rs\n+++ b/lib.rs\n@@ -1 +1 @@\n-old\n+new\n"
AUTHOR = {"__typename":"User","id":"U1","login": "tester", "name": "Test User", "avatarUrl": "https://github.com/tester.png"}
LABEL = {"name": "bug", "color": "fff", "description": "Something needs fixing"}
ISSUE = {"id": "I_12", "__typename": "Issue", "number": 12, "title": "Regression", "body": "Read me",
         "author": AUTHOR, "url": "https://github.com/example/parity/issues/12", "state": "OPEN",
         "stateReason": None, "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:00:00Z",
         "closedAt": None, "comments": {"totalCount": 0,"pageInfo":{"hasPreviousPage":False,"startCursor":None},"nodes":[]},
         "assignees": {"pageInfo":{"hasNextPage":False,"endCursor":None},"nodes":[]},
         "labels": {"pageInfo":{"hasNextPage":False,"endCursor":None},"nodes": [{"name": "bug", "color": "fff"}]},
         "viewerCanUpdate": True, "viewerCanClose": True, "viewerCanReopen": True,
         "reactionGroups": [], "closedByPullRequestsReferences": {"pageInfo":{"hasNextPage":False,"endCursor":None},"nodes":[]},
         "timelineItems": {"nodes":[]}}
PR = {"viewerCanUpdate":True,"viewerDidAuthor":False,"headRepository":{"viewerPermission":"WRITE"},"id": "PR_3", "__typename": "PullRequest", "number": 3, "title": "Work", "url": "https://github.com/example/parity/pull/3",
      "state": "OPEN", "isDraft": False, "author": AUTHOR, "createdAt": "2026-01-02T00:00:00Z"}


PR["files"] = {"pageInfo":{"hasNextPage":False,"endCursor":None},"nodes":[
    {"path":path,"viewerViewedState":"UNVIEWED","additions":i+1,"deletions":2*i}
    for i,path in enumerate(["lib.rs","README.md","tests/test_api.py","__fixtures__/main.txt","Cargo.lock"])]}
PR.update({"body":"PR body","mergeable":"MERGEABLE","mergeStateStatus":"CLEAN","reviewDecision":"APPROVED",
           "additions":1,"deletions":1,"changedFiles":1,"updatedAt":"2026-02-01T00:00:00Z","mergedAt":None,"mergeCommit":None,"mergedBy":None,
           "closedAt":None,"headRefName":"feature","baseRefName":"main","headRefOid":"a"*40,
           "isCrossRepository":False,"maintainerCanModify":False,"headRepositoryOwner":{"__typename":"User","login":"tester"},
           "headRepository":{"viewerPermission":"WRITE","nameWithOwner":REPO,"id":"R1"},"headRef":{"id":"HD_3"},
           "autoMergeRequest":None,"mergeQueueRemovals":{"nodes":[]},"viewerCanUpdateBranch":True,"isMergeQueueEnabled":False,
           "baseRef":{"compare":{"behindBy":0}},"reviewRequests":{"nodes":[]},"labels":{"nodes":[LABEL]},
           "latestReviews":{"nodes":[]},"stack":None,"stackEntry":None,"commits":{"nodes":[{"commit":{
               "statusCheckRollup":{"state":"SUCCESS","contexts":{"nodes":[{"__typename":"CheckRun","name":"build",
                   "status":"COMPLETED","conclusion":"SUCCESS","startedAt":"2026-01-01T12:00:00Z",
                   "completedAt":"2026-01-01T13:00:00Z","detailsUrl":"https://example.test/build",
                   "isRequired":True,"checkSuite":{"workflowRun":{"event":"pull_request","runNumber":7,"workflow":{"id":"W1","name":"CI"}}}}],
                   "pageInfo":{"hasNextPage":False,"endCursor":None}}}}}]}})
REPOSITORY = {"__typename":"Repository", "viewerPermission":"WRITE","mergeCommitAllowed":True,"squashMergeAllowed":True,
              "rebaseMergeAllowed":True,"autoMergeAllowed":True,"pullRequest":PR}

STACK = {"number":6,"id":91,"url":"https://api.github.com/repos/example/parity/stacks/6",
         "html_url":"https://github.com/example/parity/stacks/6","base":{"ref":"main"},"pull_requests":[
             {"number":3,"head":{"ref":"feature","sha":"a"*40},"draft":False,"title":"First","state":"open"},
             {"number":4,"head":{"ref":"second"},"title":"Second","state":"closed","merged_at":"2026-11-10T00:00:00Z"}]}
THREAD_COMMENT = {"id":"C42","author":{"__typename":"User","login":"reader","avatarUrl":"https://example.test/avatar"},
                  "body":"A visible comment","createdAt":"2026-11-01T10:00:00Z","lastEditedAt":None,
                  "url":"https://example.test/comment","isMinimized":False,"pullRequestReview":{"url":"https://example.test/review"},
                  "reactionGroups":[{"content":"THUMBS_UP","viewerHasReacted":True,
                                     "reactors":{"totalCount":2,"nodes":[{"__typename":"User","login":"tester"},{"__typename":"User","login":"reader"}]}}]}

ACTION_METHODS = ["closePullRequest","reopenPullRequest","markPullRequestReadyForReview",
                  "convertPullRequestToDraft","mergePullRequest","revertPullRequest","disablePullRequestAutoMerge",
                  "enablePullRequestAutoMerge","updatePullRequestBranch"]

class Provider(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def reply(self, status, body, content_type="application/json"):
        data = harness.encode(body) if content_type == "application/json" else body.encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if hasattr(self.server, "requests"):
            self.server.requests.append(f"GET {self.path}")
        path = urlsplit(self.path).path
        if hasattr(self.server, "forge_calls"):
            self.server.forge_calls.append(("GET", self.path, None))
            if path == f"/repos/{REPO}":
                return self.reply(200, {"name": "parity", "full_name": REPO, "default_branch": "main",
                                        "owner": {"login": "example"}, "private": False, "fork": False})
        if path == f"/repos/{REPO}/pulls" and hasattr(self.server, "status_rows"):
            head = parse_qs(urlsplit(self.path).query).get("head", [""])[0]
            pulls = []
            for row in self.server.status_rows:
                owner = row["headRepositoryOwner"]["login"]
                if head != owner + ":" + row["headRefName"]:
                    continue
                repo = dict(name="parity", full_name=row["headRepository"]["nameWithOwner"],
                            owner=dict(login=owner, type="User"))
                pulls.append(dict(number=row["number"], title=row["title"], html_url=row["url"],
                                  state="open" if row["state"]=="OPEN" else "closed", draft=row["isDraft"],
                                  updated_at=row["updatedAt"], closed_at=row["closedAt"], merged_at=row["mergedAt"],
                                  head=dict(ref=row["headRefName"],sha=row["headRefOid"],repo=repo),
                                  base=dict(ref=row["baseRefName"],repo={"full_name":REPO})))
            self.reply(200,pulls)
        elif path == f"/repos/{REPO}/stacks":
            self.reply(200, [STACK] if self.path.endswith("pull_request=3") else [])
        elif path == f"/repos/{REPO}/stacks/6":
            self.reply(200, STACK)
        elif path == f"/repos/{REPO}/pulls/3/merge-async/merge-id":
            self.server.stack_merges.append(("GET", self.path, None))
            self.reply(200, {"status": "merged", "details": {}})
        elif path.endswith("/actions/runs"):
            self.reply(200, {"workflow_runs":[], "total_count":0})
        elif path == "/user":
            self.reply(200, {"login": "tester", "id": 1234})
        elif path == f"/repos/{REPO}/pulls/3" and "diff" in self.headers.get("Accept", ""):
            self.reply(200, PATCH, "application/vnd.github.diff")
        elif path == f"/repos/{REPO}/pulls/3/files":
            self.reply(200, [{"filename": "lib.rs", "status": "modified", "patch": "@@ -1 +1 @@\n-old\n+new"}])
        else:
            self.reply(404, {"message": "Unknown fixture path"})

    def do_PUT(self):
        if self.path == f"/repos/{REPO}/pulls/3/merge-async":
            body = self.read_json()
            self.server.stack_merges.append(("PUT", self.path, body))
            self.reply(200, {"status": "pending", "details": {"uuid": "merge-id"}})
        else:
            self.reply(404, {"message": "Not Found"})

    def read_json(self):
        if self.headers.get("transfer-encoding", "").lower() == "chunked":
            chunks = []
            while True:
                size = int(self.rfile.readline().split(b";")[0].strip(), 16)
                if size == 0:
                    self.rfile.readline()
                    break
                chunks.append(self.rfile.read(size))
                self.rfile.read(2)
            return json.loads(b"".join(chunks))
        return json.loads(self.rfile.read(int(self.headers["Content-Length"])))

    def do_POST(self):
        if hasattr(self.server, "requests"):
            self.server.requests.append(f"POST {self.path}")
        data = self.read_json()
        if hasattr(self.server, "forge_calls"):
            self.server.forge_calls.append(("POST", self.path, data))
        if self.path == f"/repos/{REPO}/pulls" and hasattr(self.server, "forge_calls"):
            pr = deepcopy(PR)
            pr.update(title=data["title"], body=data["body"], headRefName=data["head"].split(":")[-1],
                      isDraft=data.get("draft", False), baseRefName=data["base"])
            pr["headRepositoryOwner"] = {"__typename":"User", "login": "example"}
            pr["headRepository"] = {**pr["headRepository"], "name": "parity"}
            self.server.status_rows.append(pr)
            self.reply(201, {"number": 3, "html_url": pr["url"]})
        elif self.path == f"/repos/{REPO}/issues":
            self.server.creates.append(data)
            self.reply(201, {"number": 12, "html_url": ISSUE["url"]})
        elif self.path == f"/repos/{REPO}/issues/12/comments":
            self.server.comments.append(data)
            self.reply(201, {"id": 31})
        elif self.path == f"/repos/{REPO}/pulls/3/reviews":
            self.server.reviews.append(data)
            self.reply(201, {"id": 123})
        elif self.path == f"/repos/{REPO}/pulls/3/requested_reviewers":
            self.server.reviewers.append(data)
            self.reply(201, {})
        elif self.path == "/graphql":
            query = data.get("query", "")
            if query.lstrip().startswith("mutation"):
                if "markFileAsViewed(" in query:
                    result={}
                    args=data["variables"]
                    fields=re.finditer(r"(?:(\w+):\s*)?(unmarkFileAsViewed|markFileAsViewed)\s*\(\s*input\s*:\s*\{([^}]*)}\s*\)\s*(@(?:include|skip)\(if:\s*\$(\w+)\))?",query)
                    for f in fields:
                        alias, method, inputs, directive, flag=f.groups()
                        if directive and (bool(args[flag])!=directive.startswith("@include")):
                            continue
                        variables={k:args[v] for k,v in re.findall(r"(\w+)\s*:\s*\$(\w+)", inputs)}
                        if variables.get("pullRequestId")!="PR_3":
                            self.reply(422,{"errors":[{"message":"Wrong PR file mutation"}]})
                            return
                        path=variables["path"]
                        viewed=method=="markFileAsViewed"
                        for file in self.server.repository["pullRequest"]["files"]["nodes"]:
                            if file["path"]==path:
                                file["viewerViewedState"]="VIEWED" if viewed else "UNVIEWED"
                                break
                        self.server.file_writes.append((path,viewed))
                        result[alias or method]={"clientMutationId":None}
                    self.reply(200,{"data":result})
                    return
                if "updateRefs(" in query:
                    request = data["variables"]["input"]
                    expected = {"repositoryId":"R1","refUpdates":[{"name":"refs/heads/feature","beforeOid":"a"*40,"afterOid":"0"*40}]}
                    if request != expected or self.server.ref_oid != "a"*40:
                        self.reply(409, {"errors":[{"message":"Ref changed"}]})
                        return
                    self.server.ref_oid = None
                    self.server.deleted += 1
                    self.reply(200, {"data":{"updateRefs":{"clientMutationId":None}}})
                    return
                for key in ACTION_METHODS:
                    if key + "(" in query:
                        args = data["variables"]
                        target = args.get("input",args)
                        if target["pullRequestId"] != "PR_3":
                            self.reply(422, {"errors":[{"message":"Wrong PR"}]})
                            return
                        if key == "mergePullRequest":
                            self.server.merge_heads.append(target.get("expectedHeadOid"))
                        self.server.actions.append((key,target.get("mergeMethod"),target.get("updateMethod")))
                        pr = self.server.repository["pullRequest"]
                        if key == "closePullRequest":
                            pr.update(state="CLOSED", closedAt="2026-11-10T12:00:00Z")
                        elif key == "reopenPullRequest":
                            pr.update(state="OPEN",closedAt=None)
                        elif key in ("markPullRequestReadyForReview","convertPullRequestToDraft"):
                            pr.update(isDraft=(key=="convertPullRequestToDraft"))
                        result = {key:{"pullRequest":pr,"revertPullRequest":{"id":"revert"}}}
                        self.reply(200,{"data":result})
                        return
                for operation, key in [("updatePullRequest(", "updatePullRequest"), ("addComment(", "addComment")]:
                    if operation in query:
                        args = data.get("variables", {})
                        self.server.pr_writes.append((key, args.get("body", ""), args.get("title", "")))
                        result = {key: {"clientMutationId": None, "pullRequest":{"id":"PR_3"}, "commentEdge":{"node":{"id":"C9"}}}}
                        break
                else:
                    self.reply(422, {"errors":[{"message":"Unknown fixture mutation"}]})
                    return
            elif "search(" in query and "is:issue" in str(data.get("variables")):
                result = {"search": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": [ISSUE]}}
            elif "assignableUsers(" in query and "labels(" in query:
                result = {"repository": {"labels": {"pageInfo": {"hasNextPage": False}, "nodes": [LABEL]},
                                         "assignableUsers": {"pageInfo": {"hasNextPage": False}, "nodes": [AUTHOR]}}}
            elif "threadId" in data.get("variables", {}) and "comments(" in query:
                variables=data["variables"]
                pr="PR_3" if variables["threadId"]=="Thread1" else "Wrong"
                more=variables["after"]=="first"
                result={"viewer":{"login":"tester"},"repository":{"pullRequest":{"id":"PR_3"}},
                        "node":{"__typename":"PullRequestReviewThread","pullRequest":{"id":pr},
                                "comments":{"pageInfo":{"hasNextPage":more,"endCursor":"second" if more else None},
                                            "nodes":[THREAD_COMMENT,{**THREAD_COMMENT,"isMinimized":True}] if more else []}}}
            elif "issue(number:" in query:
                result = {"viewer": {"login": "tester"}, "repository": {"viewerPermission": "WRITE", "issue": ISSUE}}
            elif "pullRequests(" in query and hasattr(self.server, "status_rows"):
                alias = re.search(r"(\w+)\s*:\s*repository\(", query)
                head_variable = re.search(r"headRefName:\s*\$(\w+)",query)
                head = data["variables"][head_variable.group(1)]
                rows = [row for row in self.server.status_rows if row["headRefName"] == head]
                self.server.status_reads.append(head)
                result = {(alias.group(1) if alias else "repository"): {"pullRequests": {"nodes": rows}}}
            elif "pullRequest(number:" in query:
                alias = re.search(r"(\w+)\s*:\s*repository\(", query)
                result = {(alias.group(1) if alias else "repository"): self.server.repository}
            elif "defaultBranchRef" in query and "qualifiedName" in query:
                result = {"node":{"__typename":"Repository","defaultBranchRef":{"name":"main"},
                                  "ref":{"target":{"__typename":"Commit","oid":self.server.ref_oid}} if self.server.ref_oid else None}}
            elif "viewer" in query:
                result = {"viewer": {**AUTHOR, "databaseId": 1234}}
            else:
                self.reply(422, {"errors": [{"message": "Unknown fixture operation"}]})
                return
            self.reply(200, {"data": result})
        else:
            self.reply(404, {"message": "Unknown fixture write"})


def rpc(tag, payload):
    return {"name": tag, "frame": {"_tag": "Request", "tag": tag, "id": tag, "headers": [], "payload": payload}}


def github_fixture(stack, directory):
    config = directory
    config.mkdir(mode=0o700)
    endpoint = config / "api.sock"
    host = stack.enter_context(ThreadingUnixStreamServer(str(endpoint), Provider))
    host.creates, host.comments, host.pr_writes, host.reviews, host.reviewers = [], [], [], [], []
    host.requests = []
    host.repository = deepcopy(REPOSITORY)
    host.actions = []
    host.merge_heads = []
    host.stack_merges = []
    host.file_writes = []
    host.deleted = 0
    host.ref_oid = "a"*40
    thread = Thread(target=host.serve_forever, daemon=True)
    thread.start()
    stack.callback(thread.join)
    stack.callback(host.shutdown)
    (config / "config.yml").write_text(f"http_unix_socket: {endpoint}\n")
    return host, {"GH_CONFIG_DIR": str(config), "GH_TOKEN":"offline-test-fixture", "GH_NO_UPDATE_NOTIFIER":"1"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--faketime-lib", type=Path, help="libfaketime shared object, enables exact comparisons for reads containing observedAt")
    args = parser.parse_args()
    with ExitStack() as stack:
        root = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="t3-gh-parity-")))
        checkout = root / "repo"
        # parity-env: git, not a T3 server
        subprocess.run(["git", "init", "--quiet", checkout], check=True)
        # parity-env: git, not a T3 server
        subprocess.run(["git", "-C", checkout, "remote", "add", "origin", f"https://github.com/{REPO}.git"], check=True)
        projects = []
        sockets = []
        credentials = secrets.token_urlsafe(32)
        signing = secrets.token_bytes(32)
        providers = []
        for instance, cmd in [("original", args.original_command), ("rust", args.rust_command)]:
            # Each server can create and mutate its own independent provider state.
            host, env = github_fixture(stack, root / f"gh-{instance}")
            providers.append(host)
            prefix = ["env", *(f"{k}={v}" for k, v in env.items())]
            if args.faketime_lib:
                lib = args.faketime_lib.resolve(strict=True)
                prefix.extend([f"LD_PRELOAD={lib}", "FAKETIME=2026-11-10 12:00:00", "FAKETIME_DONT_FAKE_MONOTONIC=1", "TZ=UTC"])
            # Assert this gh version is actually using the fixture before starting either server.
            # parity-env: gh against the local fixture, not a T3 server
            probe = subprocess.run(prefix + ["gh", "api", "--hostname", "github.com", "user"],
                                   capture_output=True, check=True, timeout=8)
            if json.loads(probe.stdout)["id"] != 1234:
                raise RuntimeError("gh did not reach the local fixture")
            url, cookie = harness.managed_server(stack, prefix + shlex.split(cmd), root / instance, credentials, signing)
            headers = {"Cookie": cookie, "Content-Type": "application/json"}
            cmd = {"type": "project.create", "commandId": "create-fixture", "projectId": "test-project",
                   "title": "Parity", "workspaceRoot": str(checkout)}
            status, _, body = harness.http(url, "POST", "/api/projects/mutate", harness.encode(cmd), headers)
            if status != 200:
                raise RuntimeError(f"registering project on {instance} failed ({status}): {body!r}")
            projects.append((url, cookie))
            sockets.append(stack.enter_context(connect(url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2",
                                 additional_headers={"Cookie": cookie}, proxy=None, compression=None)))

        issue = {"projectId": "test-project", "tracker": "github", "scope": REPO}
        create = {"title": "A title", "body": "A body", "labels": ["bug"], "assignees": []}
        ref = {"projectId": "test-project", "repository": REPO, "number": 3}
        cases = [
            rpc("issues.create", {**issue, **create}),
            rpc("issues.comment", {**issue, "key": "12", "body": "Comment body"}),
            rpc("issues.create", {**issue, **create, "scope": "other/repo"}),
            rpc("issues.list", {"projectId": "test-project", "state": "open"}),
            rpc("issues.candidates", issue),
            rpc("pullRequests.preview", ref),
            rpc("pullRequests.stack", ref),
            rpc("pullRequests.stack", {**ref,"number":99}),
            rpc("pullRequests.threadComments",{**ref,"threadId":"Thread1","cursor":"first"}),
            rpc("pullRequests.threadComments",{**ref,"threadId":"Thread1","cursor":"second"}),
            rpc("pullRequests.threadComments",{**ref,"threadId":"Other","cursor":"first"}),
            rpc("pullRequests.filesViewed", ref),
            rpc("pullRequests.changeBreakdown", ref),
            rpc("pullRequests.setFilesViewed",{**ref,"files":[{"path":"lib.rs","viewed":True},{"path":"README.md","viewed":True},{"path":"lib.rs","viewed":False}]}),
            rpc("pullRequests.filesViewed",ref),
            rpc("pullRequests.update", {**ref,"title":"New title","body":"New body"}),
            rpc("pullRequests.comment", {**ref,"body":"PR comment"}),
            rpc("pullRequests.submitReview", {**ref,"body":"Reviewed","verdict":"comment","comments":[{"path":"lib.rs","position":{"kind":"added","newLine":1},"body":"Inline remark"}]}),
            rpc("pullRequests.requestReviewers", {**ref,"reviewers":[{"id":"reviewer","kind":"user"}],"requested":True}),
            {"name": "posted PR diff", "authenticated": True, "method": "POST", "path": "/api/pull-requests/diff", "body": ref},
            {"name": "paged PR diff", "authenticated": True, "method": "POST", "path": "/api/pull-requests/diff", "body": {**ref, "cursor": "1"}},
        ]
        if args.faketime_lib:
            cases.extend(rpc("pullRequests."+op, ref) for op in ("detail","checks","summary"))
        actions = ["draft","ready","close","delete-branch","reopen","disable-auto-merge","approve-workflows","update-branch","revert","merge"]
        for action in actions:
            payload = {**ref,"action":action}
            if args.faketime_lib and action == "close":
                payload["replyWithSummary"] = True
            if action == "merge":
                payload["mergeMethod"] = "squash"
                for label, sha in [("stale head", "abc1234"), ("abbreviated head", "AAAAAAA")]:
                    pinned = rpc("pullRequests.runAction", {**payload, "expectedHeadSha": sha})
                    pinned["name"] += " " + label
                    cases.append(pinned)
            case=rpc("pullRequests.runAction",payload)
            case["name"] += " " + action
            cases.append(case)
        cases.append(rpc("pullRequests.runAction",{**ref,"action":"close","stackNumber":2}))
        for label, head in [("stale stack", "b"*40), ("pinned stack", PR["headRefOid"])]:
            case = rpc("pullRequests.runAction", {**ref, "action": "merge", "stackNumber": STACK["number"],
                       "mergeMethod": "squash", "expectedHeadSha": PR["headRefOid"],
                       "expectedStackHeads": [{"number": 3, "headSha": head}]})
            case["name"] += " " + label
            cases.append(case)
        failures = 0
        for case in cases:
            if "frame" in case:
                for ws in sockets:
                    ws.send(harness.encode(case["frame"]).decode())
                packets = [harness.recv_frame(ws, timeout=15) for ws in sockets]
                payload = case["frame"]["payload"]
                expected_failure = payload.get("scope") == "other/repo" or payload.get("expectedHeadSha") == "abc1234" or ("stackNumber" in payload and not case["name"].endswith("pinned stack")) or payload.get("threadId")=="Other"
                exits = [json.loads(p)["exit"] for p in packets]
                ok = packets[0] == packets[1] and all(
                    e["_tag"] == ("Failure" if expected_failure else "Success") for e in exits)
                values = [repr(v)[:5000] for v in packets]
            else:
                ok, values = harness.check(case, projects, sockets)
                ok = ok and values[0].startswith("(200,")
            print(("PASS" if ok else "FAIL"), case["name"])
            if not ok:
                failures += 1
                print("\n".join(values))
        for provider in providers:
            expected_actions = [(m,None,None) for m in ("convertPullRequestToDraft","markPullRequestReadyForReview",
                "closePullRequest","reopenPullRequest","disablePullRequestAutoMerge","revertPullRequest")]
            expected_actions.extend([("mergePullRequest","SQUASH",None)] * 2)
            stack_endpoint = f"/repos/{REPO}/pulls/3/merge-async"
            if provider.stack_merges != [
                ("PUT", stack_endpoint, {"merge_method": "squash", "merge_action": "default", "sha": PR["headRefOid"]}),
                ("GET", stack_endpoint + "/merge-id", None),
            ]:
                failures += 1
                print("FAIL asynchronous stack merge requests", provider.stack_merges)
            if provider.merge_heads != [PR["headRefOid"], None]:
                failures += 1
                print("FAIL merge head pins", provider.merge_heads)
            if provider.file_writes != [("lib.rs",True),("README.md",True),("lib.rs",False)]:
                failures += 1
                print("FAIL file toggles",provider.file_writes)
            if provider.deleted != 1:
                failures += 1
                print("FAIL expected one atomic source branch deletion",provider.deleted)
            if provider.actions != expected_actions:
                failures += 1
                print("FAIL provider lifecycle actions",provider.actions)
            if provider.creates != [create] or provider.comments != [{"body": "Comment body"}]:
                failures += 1
                print("FAIL provider received unexpected issue writes", provider.creates, provider.comments)
            if provider.pr_writes != [("updatePullRequest","New body","New title"),("addComment","PR comment","")] or provider.reviewers != [{"reviewers":["reviewer"],"team_reviewers":[]}]:
                failures += 1
                print("FAIL unexpected PR write",provider.pr_writes, provider.reviewers)
            if provider.reviews != [{"event":"COMMENT","body":"Reviewed","comments":[{"path":"lib.rs","side":"RIGHT","line":1,"body":"Inline remark"}]}]:
                failures += 1
                print("FAIL unexpected submitted review",provider.reviews)
        harness.assert_nothing_escaped()
        print(f"{len(cases)} client comparisons and two provider write logs checked")
        return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
