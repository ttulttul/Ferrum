#!/usr/bin/env python3
"""Compare provider-specific git.resolvePullRequest and git.preparePullRequestThread frames."""
import argparse
from contextlib import ExitStack
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import secrets
import shlex
import shutil
import subprocess
from tempfile import TemporaryDirectory
from threading import Thread
from urllib.parse import parse_qs, urlsplit

from websockets.sync.client import connect
from parity_harness import encode, managed_server, recv_frame
from git_workflow_parity import git

CLI = """#!/usr/bin/env python3
import json, os, pathlib, subprocess, sys
cmd=pathlib.Path(sys.argv[0]).name
args=sys.argv[1:]
if cmd=="gt" and args==["auth","status","--json","host,user,active"]:
    print(json.dumps([dict(host="https://codeberg.org",user="fixture",active=True)]))
    sys.exit(0)
data=json.loads(pathlib.Path(".git/provider-fixture").read_text())
if cmd=="glab" and args==["mr","checkout","--","3"] or cmd=="gt" and args==["pr","checkout","3","--host","https://codeberg.org","--user","fixture","--repo","org/repo"]:
    subprocess.run(["git","switch","--quiet","--","topic"],check=True)
elif cmd=="glab" and args==["mr","view","--output","json","--","3"] or cmd=="az" and args==["repos","pr","show","--detect","false","--id","3","--only-show-errors","--output","json","--organization","https://dev.azure.com/org"]:
    print(json.dumps(data))
elif cmd=="glab" and args==["mr","list","--source-branch","topic","--all","--per-page","100","--output","json"] or cmd=="az" and args==["repos","pr","list","--detect","false","--source-branch","topic","--status","all","--top","100","--only-show-errors","--output","json","--organization","https://dev.azure.com/org","--project=project","--repository=repo"]:
    print(json.dumps([data]))
elif cmd=="gt" and args[:7]==["api","--include","--host","https://codeberg.org","--user","fixture","--method"] and "/pulls?state=all&head=topic" in args[-1]:
    print("HTTP/1.1 200 OK",file=sys.stderr)
    print(json.dumps([data] if args[-1].endswith("page=1") else []))
elif cmd=="gt" and args==["api","--include","--host","https://codeberg.org","--user","fixture","--method","GET","https://codeberg.org/api/v1/repos/org/repo/pulls/3"]:
    print("HTTP/1.1 200 OK",file=sys.stderr)
    print(json.dumps(data))
else:
    sys.exit(1)
"""

class Bitbucket(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlsplit(self.path)
        query = parse_qs(url.query)
        if self.headers.get("Authorization") != "Bearer local-fixture":
            self.send_error(403)
            return
        if url.path == "/2.0/repositories/org/repo/pullrequests/3":
            result = self.server.pull
        elif url.path == "/2.0/repositories/org/repo/pullrequests" and 'source.branch.name = "topic"' in query.get("q", [""])[0]:
            result = {"values":[self.server.pull]}
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type","application/json")
        self.end_headers()
        self.wfile.write(encode(result))

    def log_message(self, *args):
        pass


def observe(server, root, api):
    url, cookie = server
    results = {}
    with connect(url.replace("http:", "ws:") + "/ws?orchestrationProtocol=2",
                 additional_headers={"Cookie":cookie}, compression=None, proxy=None) as socket:
        def call(host, label, op, payload, success):
            request_id = str(len(results))
            socket.send(encode(dict(_tag="Request", id=request_id, tag=op if "." in op else "git."+op, payload=payload, headers=[])).decode())
            frame = recv_frame(socket, timeout=20)
            exit = json.loads(frame)["exit"]
            if op=="vcs.refreshStatus":
                status=exit.get("value", {})
                pr=status.get("pr") or {}
                assert pr.get("number")==3 and pr.get("isDraft") and pr["updatedAt"]=="2026-01-02T03:04:05.000Z", (host,status)
                frame=status
            results[host + " " + label] = (frame, (exit["_tag"] == "Success") == success)

        for host, address in (("GitLab","git@gitlab.com:org/repo"), ("Azure","git@ssh.dev.azure.com:v3/org/project/repo"),
                              ("Bitbucket","git@bitbucket.org:org/repo"), ("Forgejo","git@codeberg.org:org/repo"),
                              ("GitCafe","git@git.cafe:org/repo"), ("Cursor Origin","git@origin.cursor.com:org/repo")):
            repo = root / host.replace(" ","")
            repo.mkdir()
            git(repo, "init", "-q", "-b", "main")
            git(repo, "config", "user.name", "Fixture")
            git(repo, "config", "user.email", "fixture@example.test")
            (repo / "tracked").write_text("Base")
            git(repo, "add", "tracked")
            git(repo, "commit", "-qm", "Base")
            git(repo, "remote", "add", "origin", address)
            if host in ("GitCafe","Cursor Origin"):
                call(host,"invalid reference","resolvePullRequest",dict(cwd=str(repo),reference="bad-reference"),False)
                call(host,"missing credential","resolvePullRequest",dict(cwd=str(repo),reference="3"),False)
                shutil.rmtree(repo)
                continue
            git(repo, "switch", "-qc", "topic")
            git(repo, "commit", "--allow-empty", "-qm", "Change")
            sha = git(repo,"rev-parse","HEAD").decode().strip()
            git(repo, "switch", "-q", "main")
            bare = root / "remote.git"
            # parity-env: git, not a T3 server
            subprocess.run(["git","clone","-q","--bare",repo,bare],check=True)
            branch = lambda name: dict(repository={"full_name":"org/repo"},branch={"name":name},commit={"hash":sha})
            pull = dict(number=3, title="Example", iid=3, web_url="https://gitlab.com/org/repo/-/merge_requests/3",
                        source_branch="topic",target_branch="main",sha=sha,state="opened",
                        pullRequestId=3,sourceRefName="refs/heads/topic",targetRefName="refs/heads/main",status="active",
                        lastMergeSourceCommit={"commitId":sha},_links={"web":{"href":"https://dev.azure.com/org/project/_git/repo/pullrequest/3"}})
            if host=="Forgejo":
                ref = lambda name: dict(ref=name,sha=sha,repo={"full_name":"org/repo","owner":{"login":"org"}})
                pull=dict(number=3,title="Example",html_url="https://codeberg.org/org/repo/pulls/3",state="open",
                          merged=False,base=ref("main"),head=ref("topic"))
            api.pull = dict(id=3,title="Example",state="OPEN",links={"html":{"href":"https://bitbucket.org/org/repo/pull-requests/3"}},
                            source=branch("topic"),destination=branch("main"))
            stamp="2026-01-02T03:04:05Z"
            pull.update(draft=True, isDraft=True, updated_at=stamp, creationDate=stamp)
            api.pull.update(draft=True, updated_on=stamp)
            (repo / ".git/provider-fixture").write_text(json.dumps(pull))
            args=dict(cwd=str(repo),reference="#3")
            call(host,"resolve","resolvePullRequest",args,True)
            (repo / "tracked").write_text("Dirty")
            call(host,"dirty checkout","preparePullRequestThread",dict(**args,mode="local"),False)
            git(repo,"restore","tracked")
            call(host,"local checkout","preparePullRequestThread",dict(**args,mode="local"),True)
            call(host,"occupied worktree","preparePullRequestThread",dict(**args,mode="worktree"),False)
            call(host,"hosted status","vcs.refreshStatus",dict(cwd=str(repo)),True)
            results[host+" branch"] = (git(repo,"branch","--show-current"),git(repo,"rev-parse","HEAD").decode().strip()==sha)
            shutil.rmtree(bare)
            shutil.rmtree(repo)
    return results


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command",required=True)
    parser.add_argument("--rust-command",required=True)
    args=parser.parse_args()
    with TemporaryDirectory() as temp, ExitStack() as stack:
        root=Path(temp)
        api=stack.enter_context(ThreadingHTTPServer(("127.0.0.1",0),Bitbucket))
        Thread(target=api.serve_forever,daemon=True).start()
        stack.callback(api.shutdown)
        bin=root/"bin"
        bin.mkdir()
        for name in ("git","git-upload-pack","sh","env","python3","node","hostname","which"):
            executable=shutil.which(name)
            if executable:
                (bin/name).symlink_to(executable)
        for name in ("glab","az","gt"):
            tool=bin/name
            tool.write_text(CLI)
            tool.chmod(0o700)
        ssh=bin/"fixture-ssh"
        ssh.write_text("#!/bin/sh\nexec git-upload-pack "+shlex.quote(str(root/"remote.git"))+"\n")
        ssh.chmod(0o700)
        seed=secrets.token_urlsafe(32)
        key=secrets.token_bytes(32)
        prefix=["env",f"PATH={bin}",f"GIT_SSH_COMMAND={ssh}","GIT_SSH_VARIANT=ssh","GIT_ALLOW_PROTOCOL=file:ssh",
                f"T3CODE_BITBUCKET_API_BASE_URL=http://127.0.0.1:{api.server_port}/2.0","T3CODE_BITBUCKET_ACCESS_TOKEN=local-fixture"]
        servers=[managed_server(stack,prefix+shlex.split(cmd),root/name,seed,key) for name,cmd in
                 (("original",args.original_command),("rust",args.rust_command))]
        original,rust=[observe(server,root,api) for server in servers]
        failures=0
        for name,(expected,valid) in original.items():
            actual,outcome=rust[name]
            if valid and outcome and expected==actual:
                print("PASS",name)
            else:
                print(f"FAIL {name}\n  original {expected}\n  rust     {actual}")
                failures+=1
        return bool(failures)


if __name__=="__main__":
    raise SystemExit(main())
