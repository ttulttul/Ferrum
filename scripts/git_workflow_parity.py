#!/usr/bin/env python3
"""Compare complete RPC streams and repository effects against reproducible local remotes."""
import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import secrets
import shlex
import shutil
import subprocess
from tempfile import TemporaryDirectory
from socketserver import ThreadingUnixStreamServer
from threading import Thread

from websockets.sync.client import connect

from parity_harness import encode, managed_server
from github_provider_parity import Provider, PR, REPO, REPOSITORY

DATE = "2026-01-01T00:00:00+00:00"


def git(repo, *args):
    env = dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1",
               GIT_COMMITTER_DATE=DATE, GIT_AUTHOR_DATE=DATE)
    return subprocess.check_output(["git", "-C", repo, *args], env=env)


def observe(server, root):
    repo = root / "work"
    remote = root / "remote.git"
    repo.mkdir()
    remote.mkdir()
    git(remote, "init", "-q", "--bare", "-b", "main")
    git(repo, "init", "-q", "-b", "main")
    git(repo, "config", "user.name", "Fixture")
    git(repo, "config", "user.email", "fixture@example.test")
    git(repo, "commit", "-qm", "Base", "--allow-empty")
    url, cookie = server
    received = {}
    with connect(url.replace("http:", "ws:") + "/ws?orchestrationProtocol=2",
                 additional_headers={"Cookie": cookie}, proxy=None, compression=None) as socket:
        def call(label, method, params):
            request_id = str(len(received))
            socket.send(encode(dict(_tag="Request", id=request_id, tag=method,
                                    payload=dict(cwd=str(repo), **params), headers=[])).decode())
            frames = []
            while True:
                raw = socket.recv(timeout=20)
                frames.append(raw)
                if json.loads(raw)["_tag"] == "Exit":
                    break
                socket.send(encode(dict(_tag="Ack", requestId=request_id)).decode())
            expected_errors = {"resolvePullRequest", "preparePullRequestThread", "no-remote",
                               "Github dirty checkout", "Github occupied worktree"}
            expected = "Failure" if label in expected_errors else "Success"
            if json.loads(frames[-1])["exit"]["_tag"] != expected:
                raise RuntimeError(f"Unexpected {label} outcome, expected {expected}: {frames[-1]}")
            received[label] = frames

        def action(label, name, **params):
            call(label, "git.runStackedAction", dict(action=name, actionId=label, **params))

        for method, args in (("resolvePullRequest", {}), ("preparePullRequestThread", {"mode": "local"})):
            call(method, "git." + method, dict(reference="#123", **args))
        action("clean", "commit", commitMessage="Skip clean tree")
        action("no-remote", "push")
        git(repo, "remote", "add", "origin", str(remote))
        (repo / "file").write_text("Hello\n")
        action("feature", "commit", featureBranch=True, commitMessage="Hello world")
        action("publish", "push")
        action("current", "push")
        (repo / "file").write_text("Updated\n")
        (repo / "unselected").write_text("Keep working\n")
        git(repo, "add", "unselected")
        action("select", "commit", filePaths=["file"], commitMessage="Update file\n\nDetails")
        # The published provider URL resolves through a local SSH fixture for Git fetches.
        # The GitHub CLI uses a local Unix socket for all GraphQL reads.
        PR["headRefName"] = "pull-three"
        git(repo, "branch", "pull-three", "HEAD")
        git(repo, "push", "--quiet", "origin", "pull-three:refs/heads/pull-three", "pull-three:refs/pull/3/head")
        git(repo, "branch", "-D", "pull-three")
        old_repo = repo
        repo = root / "github-repo"
        shutil.copytree(old_repo, repo)
        git(repo, "remote", "set-url", "origin", f"git@github.com:{REPO}.git")
        PR["headRefOid"] = git(repo, "rev-parse", "HEAD").decode().strip()
        PR["headRepository"]["name"] = "parity"
        call("Github resolve", "git.resolvePullRequest", dict(reference="#3"))
        (repo / "file").write_text("Dirty\n")
        call("Github dirty checkout", "git.preparePullRequestThread", dict(reference="3", mode="local"))
        git(repo, "restore", "file")
        call("Github local checkout", "git.preparePullRequestThread", dict(reference="3", mode="local"))
        call("Github occupied worktree", "git.preparePullRequestThread", dict(reference="3", mode="worktree"))
        for label, args in (("commit history", ("log", "--format=raw")),
                            ("working tree", ("status", "--porcelain=v1")),
                            ("tracking refs", ("show-ref",))):
            received[label] = git(repo, *args)
        received["remote refs"] = git(remote, "show-ref")
    shutil.rmtree(old_repo)
    shutil.rmtree(repo)
    shutil.rmtree(remote)
    return received


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    with TemporaryDirectory() as directory, ExitStack() as stack:
        root = Path(directory)
        token = secrets.token_urlsafe(32)
        key = secrets.token_bytes(32)
        ssh = root / "fixture-ssh"
        ssh.write_text("#!/bin/sh\nexec git-upload-pack " + shlex.quote(str(root / "remote.git")) + "\n")
        ssh.chmod(0o700)
        servers = []
        for name, prefix in (("original", args.original_command), ("rust", args.rust_command)):
            config = root / f"gh-{name}"
            config.mkdir()
            endpoint = config / "api.sock"
            provider = stack.enter_context(ThreadingUnixStreamServer(str(endpoint), Provider))
            provider.repository = REPOSITORY
            Thread(target=provider.serve_forever, daemon=True).start()
            stack.callback(provider.shutdown)
            (config / "config.yml").write_text(f"http_unix_socket: {endpoint}\n")
            cmd = ["env", f"GIT_AUTHOR_DATE={DATE}", f"GIT_COMMITTER_DATE={DATE}",
                   f"GIT_SSH_COMMAND={ssh}", "GIT_SSH_VARIANT=ssh", "GIT_ALLOW_PROTOCOL=file:ssh",
                   f"GH_CONFIG_DIR={config}", "GH_TOKEN=offline-test-fixture", "GH_NO_UPDATE_NOTIFIER=1"]
            cmd.extend(shlex.split(prefix))
            servers.append(managed_server(stack, cmd, root / name, token, key))
        reference, rust = [observe(server, root) for server in servers]
        failures = 0
        for key, expected in reference.items():
            if expected == rust[key]:
                print("PASS", key)
                continue
            failures += 1
            print(f"FAIL {key}\n  original {expected}\n  rust     {rust[key]}")
        return bool(failures)


if __name__ == "__main__":
    raise SystemExit(main())
