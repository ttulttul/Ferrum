#!/usr/bin/env python3
"""Compare how both servers find tools installed after they started.

The reference re-reads the user's login shell before source-control discovery,
at most once per ten seconds, and merges its PATH into the environment every
later child process starts with. Each server here gets a login profile that
adds ~/.local/bin only once that directory exists, as Ubuntu's default profile
does. After both servers start, `jj` and `glab` are installed there. Then:

- discovery right after the install reuses the refresh it ran just before, so
  `jj` is still missing;
- a GitLab pull request lookup, which runs `glab` outside discovery, cannot
  find it yet either;
- once the ten seconds pass, discovery reads the shell again and finds `jj`;
- and the same lookup now finds `glab`, because the refreshed PATH reaches
  every later child process, not only discovery's.
"""
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
import time

from websockets.sync.client import connect
from parity_harness import encode, managed_server
from git_workflow_parity import git
from source_control_hosts_parity import CLI as GLAB

PROFILE = 'if [ -d "$HOME/.local/bin" ]; then\n    PATH="$HOME/.local/bin:$PATH"\nfi\nexport PATH\n'
JJ = "#!/bin/sh\necho 'jj 0.99.0-fixture'\n"
# Longer than the reference's ten-second refresh window.
REFRESH_WINDOW = 11


class Session:
    def __init__(self, stack, server, root):
        url, cookie = server
        self.root = root
        self.socket = stack.enter_context(connect(
            url.replace("http:", "ws:") + "/ws?orchestrationProtocol=2",
            additional_headers={"Cookie": cookie}, compression=None, proxy=None))
        self.calls = 0

    def call(self, tag, payload):
        self.calls += 1
        self.socket.send(encode(dict(_tag="Request", id=str(self.calls), tag=tag, payload=payload,
                                     headers=[])).decode())
        return json.loads(self.socket.recv(timeout=30))["exit"]

    def jj(self):
        """Discovery's Jujutsu row, with this server's home masked."""
        exit = self.call("server.discoverSourceControl", {})
        if exit["_tag"] != "Success":
            return exit
        row = next(row for row in exit["value"]["versionControlSystems"] if row["kind"] == "jj")
        return json.loads(json.dumps(row).replace(str(self.root), "<home>"))

    def pull_request(self, repo):
        return self.call("git.resolvePullRequest", dict(cwd=str(repo), reference="#3"))


def gitlab_repo(root):
    repo = root / "gitlab"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main")
    git(repo, "config", "user.name", "Fixture")
    git(repo, "config", "user.email", "fixture@example.test")
    (repo / "tracked").write_text("Base")
    git(repo, "add", "tracked")
    git(repo, "commit", "-qm", "Base")
    git(repo, "remote", "add", "origin", "git@gitlab.com:org/repo")
    sha = git(repo, "rev-parse", "HEAD").decode().strip()
    (repo / ".git/provider-fixture").write_text(json.dumps(dict(
        iid=3, title="Example", web_url="https://gitlab.com/org/repo/-/merge_requests/3",
        source_branch="topic", target_branch="main", sha=sha, state="opened")))
    return repo


def install(root):
    tools = root / ".local/bin"
    tools.mkdir(parents=True)
    for name, body in (("jj", JJ), ("glab", GLAB)):
        (tools / name).write_text(body)
        (tools / name).chmod(0o700)


def masked(exit, root):
    return json.loads(json.dumps(exit).replace(str(root), "<home>"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    with TemporaryDirectory() as temp, ExitStack() as stack:
        temp = Path(temp)
        # The servers' own PATH holds only what they need to run; jj and glab are nowhere on it.
        bin = temp / "bin"
        bin.mkdir()
        for name in ("git", "git-upload-pack", "sh", "env", "python3", "node", "hostname", "which"):
            executable = shutil.which(name)
            if executable:
                (bin / name).symlink_to(executable)
        assert shutil.which("bash"), "the login shell is bash"
        seed = secrets.token_urlsafe(32)
        key = secrets.token_bytes(32)
        sessions = {}
        repos = {}
        for name, command in (("original", args.original_command), ("rust", args.rust_command)):
            root = temp / name
            root.mkdir()
            (root / ".profile").write_text(PROFILE)
            repos[name] = gitlab_repo(root)
            server = managed_server(stack, shlex.split(command), root, seed, key,
                                    env={"PATH": str(bin)})
            sessions[name] = Session(stack, server, root)

        observed = {name: {} for name in sessions}

        def step(label, observe):
            for name, session in sessions.items():
                observed[name][label] = observe(name, session)

        step("before install: jj", lambda name, s: s.jj())
        for session in sessions.values():
            install(session.root)
        step("within the refresh window: jj", lambda name, s: s.jj())
        step("within the refresh window: glab lookup",
             lambda name, s: s.pull_request(repos[name])["_tag"])
        time.sleep(REFRESH_WINDOW)
        step("after the refresh window: jj", lambda name, s: s.jj())
        step("after the refresh window: glab lookup",
             lambda name, s: masked(s.pull_request(repos[name]), s.root))

        expected = {
            "before install: jj": lambda row: row.get("status") == "missing",
            "within the refresh window: jj": lambda row: row.get("status") == "missing",
            "within the refresh window: glab lookup": lambda tag: tag == "Failure",
            "after the refresh window: jj": lambda row: row.get("status") == "available",
            "after the refresh window: glab lookup": lambda exit: exit.get("_tag") == "Success",
        }
        failures = 0
        for label, holds in expected.items():
            original, rust = observed["original"][label], observed["rust"][label]
            if original == rust and holds(original):
                print("PASS", label)
            else:
                print(f"FAIL {label}\n  original {json.dumps(original, sort_keys=True)}\n"
                      f"  rust     {json.dumps(rust, sort_keys=True)}")
                failures += 1
        return bool(failures)


if __name__ == "__main__":
    raise SystemExit(main())
