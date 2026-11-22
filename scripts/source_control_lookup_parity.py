#!/usr/bin/env python3
"""Look repositories up by name on every source-control host, and clone one,
on the reference and on the Rust server, and compare every answer.

`sourceControl.lookupRepository` asks the host that `provider` names for a
repository's clone URLs, and `sourceControl.cloneRepository` with `provider`
and `repository` clones what it reports. Every host is a local fake: GitHub
through the real `gh` CLI with its `http_unix_socket` pointed at a fake API,
Bitbucket through `T3CODE_BITBUCKET_API_BASE_URL` on loopback, and GitLab,
Azure DevOps, Forgejo/Gitea, GitCafe and Cursor Origin through fake CLIs that
answer scripted commands and refuse any other. GitCafe and Cursor Origin call
fixed real APIs once signed in, so they are checked only signed out, where
neither server sends a request. The repositories they report
are local bare repositories, so a clone never leaves the machine. All data is
fake.
"""
import argparse
from contextlib import ExitStack
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import socketserver
import subprocess
from tempfile import TemporaryDirectory
import threading

from websockets.sync.client import connect

from parity_harness import encode, isolated_git_env, managed_server

FAKE_CLI = """#!/usr/bin/env python3
import json, os, pathlib, sys
command = " ".join([pathlib.Path(sys.argv[0]).name, *sys.argv[1:]])
with open(os.environ["FAKE_HOST_LOG"], "a") as out:
    out.write(command + "\\n")
answers = json.loads(pathlib.Path(os.environ["FAKE_HOST_SCRIPT"]).read_text())
answer = answers.get(command)
if answer is None:
    print("fixture: not scripted", file=sys.stderr)
    sys.exit(1)
sys.stdout.write(answer.get("stdout", ""))
sys.stderr.write(answer.get("stderr", ""))
sys.exit(answer.get("code", 0))
"""


def git(*args, cwd=None, env=None):
    # parity-env: git builds the fixture repositories, not a T3 server
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True, env=env).stdout.strip()


def bare_repository(root, env):
    work = root / "work"
    work.mkdir(parents=True)
    git("init", "-q", "-b", "main", cwd=work, env=env)
    (work / "README.md").write_text("Fixture repository\n")
    git("add", "README.md", cwd=work, env=env)
    git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-q", "-m", "Initial", cwd=work, env=env)
    bare = root / "remote.git"
    git("clone", "-q", "--bare", str(work), str(bare), env=env)
    return bare, git("rev-parse", "HEAD", cwd=work, env=env)


def json_handler(routes, token):
    """An HTTP handler answering GET `routes` ({path: body}); anything else is a 404."""
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            self.server.seen.append(self.path)
            if token and self.headers.get("Authorization") not in (f"Bearer {token}", f"token {token}"):
                return self.reply(401, {"message": "Bad credentials"})
            body = routes.get(self.path.split("?")[0])
            self.reply(404, {"message": "Not Found"}) if body is None else self.reply(200, body)

        def reply(self, status, body):
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
    return Handler


class UnixHttp(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True

    def get_request(self):
        request, _ = super().get_request()
        return request, ("local", 0)


class Server:
    def __init__(self, stack, command, home, secret, key, env):
        self.url, cookie = managed_server(stack, shlex.split(command), home, secret, key, env=env)
        self.socket = stack.enter_context(connect(
            self.url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2",
            additional_headers={"Cookie": cookie}, compression=None, proxy=None))
        self.sequence = 0

    def rpc(self, tag, payload):
        self.sequence += 1
        request = f"lookup-{self.sequence}"
        self.socket.send(encode(dict(_tag="Request", id=request, tag=tag, payload=payload, headers=[])).decode())
        while True:
            frame = json.loads(self.socket.recv(timeout=120))
            if frame.get("_tag") == "Ping":
                self.socket.send(encode({"_tag": "Pong"}).decode())
                continue
            if frame.get("requestId") == request and "exit" in frame:
                return frame["exit"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--show", action="store_true", help="print each answer as the reference gave it")
    parser.add_argument("--log", action="store_true", help="print the host commands each server ran")
    args = parser.parse_args()
    answers = {}
    with TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        git_env = {**os.environ, **isolated_git_env(tmp / "git")}
        bare, commit = bare_repository(tmp / "fixture", git_env)
        remote = f"file://{bare}"
        # The hosts' answers; every clone URL is the local bare repository.
        github = {
            "/repos/org/repo": {"full_name": "org/repo", "html_url": remote, "ssh_url": remote, "clone_url": remote},
            "/user": {"login": "fixture"},
            "/repos/fixture/solo": {"full_name": "fixture/solo", "html_url": remote, "ssh_url": remote, "clone_url": remote},
        }
        bitbucket = {"/2.0/repositories/org/repo": {"full_name": "org/repo", "links": {
            "html": {"href": remote}, "clone": [{"name": "https", "href": remote}, {"name": "ssh", "href": remote}]}}}
        script = {
            "glab api projects/org%2Frepo": {"stdout": json.dumps({
                "path_with_namespace": "org/repo", "web_url": remote, "http_url_to_repo": remote, "ssh_url_to_repo": remote})},
            "glab api projects/org%2Fmissing": {"stderr": "glab: 404 Project Not Found (HTTP 404)\n", "code": 1},
            "az repos show --detect true --repository repo --project project --only-show-errors --output json": {"stdout": json.dumps({
                "name": "repo", "remoteUrl": remote, "sshUrl": remote, "webUrl": remote, "project": {"name": "project"}})},
            "tea login list --output json": {"stdout": json.dumps([
                {"name": "forge", "url": "https://codeberg.org", "user": "fixture", "default": "true"}])},
            # Signed out: Cursor Origin's API is a fixed real host, so no case may reach it.
            "origin auth token": {"stdout": ""},
            "tea api --include --login forge --repo org/repo --method GET https://codeberg.org/api/v1/repos/org/repo": {
                "stderr": "HTTP/1.1 200 OK\n", "stdout": json.dumps({
                    "full_name": "org/repo", "html_url": remote, "clone_url": remote, "ssh_url": remote})},
            "tea api --include --login forge --repo org/missing --method GET https://codeberg.org/api/v1/repos/org/missing": {
                "stderr": "HTTP/1.1 404 Not Found\n", "stdout": json.dumps({"message": "The target couldn't be found."})},
        }
        for side, command in (("original", args.original_command), ("rust", args.rust_command)):
            with ExitStack() as stack:
                home = tmp / side
                fake_bin = tmp / f"{side}-bin"
                fake_bin.mkdir()
                for name in ("glab", "az", "tea", "fj", "cafe", "origin"):
                    (fake_bin / name).write_text(FAKE_CLI)
                    (fake_bin / name).chmod(0o755)
                log = tmp / f"{side}-hosts.log"
                script_file = tmp / f"{side}-script.json"
                script_file.write_text(json.dumps(script))
                gh_socket = tmp / f"{side}-gh.sock"
                api = stack.enter_context(UnixHttp(str(gh_socket), json_handler(github, "fixture-gh-token")))
                api.seen = []
                threading.Thread(target=api.serve_forever, daemon=True).start()
                stack.callback(api.shutdown)
                gh_config = tmp / f"{side}-gh"
                gh_config.mkdir()
                (gh_config / "config.yml").write_text(f"http_unix_socket: {gh_socket}\n")
                bb = stack.enter_context(ThreadingHTTPServer(("127.0.0.1", 0), json_handler(bitbucket, "fixture-bb-token")))
                bb.seen = []
                threading.Thread(target=bb.serve_forever, daemon=True).start()
                stack.callback(bb.shutdown)
                env = {
                    **isolated_git_env(tmp / "git"),
                    "PATH": os.pathsep.join([str(fake_bin), os.environ["PATH"]]),
                    "FAKE_HOST_LOG": str(log), "FAKE_HOST_SCRIPT": str(script_file),
                    "GH_CONFIG_DIR": str(gh_config), "GH_TOKEN": "fixture-gh-token", "GH_NO_UPDATE_NOTIFIER": "1",
                    # Never the real Bitbucket: the API is the local fake.
                    "T3CODE_BITBUCKET_API_BASE_URL": f"http://127.0.0.1:{bb.server_port}/2.0",
                    "T3CODE_BITBUCKET_ACCESS_TOKEN": "fixture-bb-token",
                }
                assert env["T3CODE_BITBUCKET_API_BASE_URL"].startswith("http://127.0.0.1:")
                server = Server(stack, command, home, secrets.token_urlsafe(32), secrets.token_bytes(32), env)
                record = {}

                def lookup(name, provider, repository):
                    record[name] = server.rpc("sourceControl.lookupRepository",
                                              {"provider": provider, "repository": repository})

                lookup("github owner/name", "github", "org/repo")
                lookup("github URL", "github", "https://github.com/org/repo.git")
                lookup("github bare name is the viewer's", "github", "solo")
                lookup("github missing", "github", "org/missing")
                lookup("github not owner/name", "github", "a/b/c/d")
                lookup("gitlab", "gitlab", "org/repo")
                lookup("gitlab missing", "gitlab", "org/missing")
                lookup("bitbucket", "bitbucket", "org/repo")
                lookup("bitbucket missing", "bitbucket", "org/missing")
                lookup("azure devops", "azure-devops", "org/project/repo")
                lookup("forgejo", "forgejo", "org/repo")
                lookup("forgejo missing", "forgejo", "org/missing")
                lookup("gitcafe not signed in", "gitcafe", "org/repo")
                lookup("cursor origin signed out", "cursor-origin", "org/repo")
                lookup("no provider", "unknown", "org/repo")
                record["clone by name"] = server.rpc("sourceControl.cloneRepository", {
                    "provider": "github", "repository": "org/repo", "destinationPath": str(home / "clones" / "named")})
                named = home / "clones" / "named"
                record["clone by name checked out"] = (named / ".git").exists() and git(
                    "rev-parse", "HEAD", cwd=named, env=git_env) == commit
                record["clone of a missing repository"] = server.rpc("sourceControl.cloneRepository", {
                    "provider": "gitlab", "repository": "org/missing", "destinationPath": str(home / "clones" / "none")})
                if args.log:
                    print(f"--- {side} commands")
                    print(log.read_text() if log.exists() else "")
                    print(f"--- {side} github API {api.seen}, bitbucket API {bb.seen}")
                text = json.dumps(record)
                for path, mark in ((str(home), "<home>"), (str(bare), "<bare>"), (str(tmp), "<tmp>")):
                    text = text.replace(path, mark)
                answers[side] = json.loads(re.sub(r"127\.0\.0\.1:\d+", "127.0.0.1:<port>", text))
    failures = 0
    for name in answers["original"]:
        same = answers["original"][name] == answers["rust"].get(name)
        failures += not same
        if args.show:
            print(f"{name}: {json.dumps(answers['original'][name])[:1800]}")
        print(f"{'PASS' if same else 'FAIL'} {name}")
        if not same:
            print(f"  original {json.dumps(answers['original'][name])[:2500]}\n  rust     {json.dumps(answers['rust'].get(name))[:2500]}")
    total = len(answers["original"])
    print(f"{total - failures}/{total} lookup checks matched across the reference and the Rust server")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
