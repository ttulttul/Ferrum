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
    # parity-env: git, not a T3 server
    return subprocess.check_output(["git", "-C", repo, *args], env=env)


def observe(server, root, provider):
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
                               "Github dirty checkout", "Github occupied worktree", "missing sparse patterns",
                               "refuse dirty annex worktree","failed sparse checkout"}
            expected = "Failure" if label in expected_errors else "Success"
            if json.loads(frames[-1])["exit"]["_tag"] != expected:
                raise RuntimeError(f"Unexpected {label} outcome, expected {expected}: {frames[-1]}")
            received[label] = frames
            return json.loads(frames[-1])["exit"].get("value")

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
        # Publishing a child branch must preserve its original comparison base.
        feature = git(repo, "branch", "--show-current").decode().strip()
        git(repo, "config", "branch.autoSetupMerge", "false")
        base = f"origin/{feature}"
        created = call("stacked worktree", "vcs.createWorktree", dict(
            refName=base, newRefName="stacked-child", baseRefName=base, path=None))
        worktree = Path(created["worktree"]["path"])
        received["stacked worktree"] = (worktree.name, created["worktree"]["refName"])
        main_checkout, repo = repo, worktree
        received["stacked tracking before push"] = git(repo, "rev-parse", "--symbolic-full-name", "@{u}")
        received["worktree base branch"] = git(repo, "config", "--get", "branch.stacked-child.t3-merge-base")
        received["worktree base ref"] = git(repo, "config", "--get", "branch.stacked-child.t3-merge-base-ref")
        # Simulate a child created by plain Git: publishing must remember its old upstream too.
        git(repo, "config", "--unset", "branch.stacked-child.t3-merge-base")
        git(repo, "config", "--unset", "branch.stacked-child.t3-merge-base-ref")
        git(repo, "commit", "-qm", "Child", "--allow-empty")
        action("publish stacked branch", "push")
        received["publish stacked branch"] = [frame.replace(str(repo), "<worktree>") for frame in
                                             received["publish stacked branch"]]
        received["stacked tracking after push"] = git(repo, "rev-parse", "--symbolic-full-name", "@{u}")
        received["recorded base"] = git(repo, "config", "--get", "branch.stacked-child.t3-merge-base-ref")
        repo = main_checkout
        call("remove stacked worktree", "vcs.removeWorktree", dict(path=str(worktree)))
        if worktree.exists():
            raise RuntimeError("worktree still present after removal")
        # Select the sparse configuration from the previous checkout, even though
        # neither the current branch nor the worktree includes the project file.
        git(repo, "switch", "-qc", "sparse-base")
        (repo / "t3.json").write_text('{/* shared */ "worktreeSparsePatternsPath": "./patterns",}')
        (repo / "patterns").write_text("/file\n")
        git(repo, "add", "t3.json", "patterns")
        git(repo, "commit", "-qm", "Configure sparse worktree")
        git(repo, "switch", "-q", feature)
        def sparse(name, ref, label):
            return call(label, "vcs.createWorktree", dict(refName=ref, newRefName=name, path=None))
        created = sparse("sparse-child", "-", "sparse worktree")
        path = Path(created["worktree"]["path"])
        received["sparse worktree"] = (path.name, git(path, "sparse-checkout", "list"),
                                       sorted(p.name for p in path.iterdir()))
        assert received["sparse worktree"][2] == [".git", "file"], received["sparse worktree"]
        if os.name != "nt":
            # git-annex uses a directory link in place of the .git file.
            admin = git(path, "rev-parse", "--absolute-git-dir").decode().strip()
            dotgit = path / ".git"
            dotgit.unlink()
            dotgit.symlink_to(admin, target_is_directory=True)
            (path / "dirty").write_text("keep")
            call("refuse dirty annex worktree", "vcs.removeWorktree", dict(path=str(path)))
            assert dotgit.is_symlink() and (path / "dirty").read_text() == "keep"
            received["refuse dirty annex worktree"] = git(path, "branch", "--show-current")
            (path / "dirty").unlink()
        call("remove sparse worktree", "vcs.removeWorktree", dict(path=str(path)))
        assert not path.exists()
        hook = repo / ".git/hooks/post-checkout"
        hook.write_text("#!/bin/sh\nexit 1\n")
        hook.chmod(0o700)
        sparse("failed-sparse", "sparse-base", "failed sparse checkout")
        hook.unlink()
        assert not git(repo, "branch", "--list", "failed-sparse")
        received["failed sparse checkout"] = len(git(repo, "worktree", "list", "--porcelain").split(b"worktree ")) - 1
        assert received["failed sparse checkout"] == 1
        git(repo, "switch", "-q", "sparse-base")
        module = root / "module"
        module.mkdir()
        git(module, "init", "-qb", "main")
        git(module, "config", "user.name", "Fixture")
        git(module, "config", "user.email", "fixture@example.test")
        (module / "content").write_text("module contents")
        git(module, "add", "content")
        git(module, "commit", "-qm", "Module")
        git(repo, "-c", "protocol.file.allow=always", "submodule", "add", "-q", str(module), "shared")
        (repo / "patterns").write_text("/shared/\n")
        git(repo, "add", "patterns")
        git(repo, "commit", "-qm", "Sparse submodule")
        for mode, populated in (("recursive", True), ("none", False)):
            (repo / "t3.json").write_text(json.dumps({"worktreeSparsePatternsPath": "patterns",
                                                       "worktreeSubmodules": mode}))
            git(repo, "commit", "-qam", "Use " + mode)
            label = "sparse submodules " + mode
            created = sparse("module-" + mode, "sparse-base", label)
            path = Path(created["worktree"]["path"])
            assert not (path / "t3.json").exists() and not (path / ".gitmodules").exists()
            assert (path / "shared/content").is_file() == populated
            received[label] = (path.name, populated)
            call("remove " + label, "vcs.removeWorktree", dict(path=str(path), force=True))
        git(repo, "submodule", "deinit", "-f", "--", "shared")
        shutil.rmtree(module)
        git(repo, "rm", "-q", "patterns")
        git(repo, "commit", "-qm", "Remove patterns")
        sparse("never-created", "sparse-base", "missing sparse patterns")
        assert not git(repo, "branch", "--list", "never-created")
        git(repo, "switch", "-q", feature)
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
        row = dict(PR, headRepositoryOwner={"__typename":"User","login":"example"}, isDraft=True)
        provider.status_rows = [dict(row, number=99, isCrossRepository=True, headRepository={"name":"parity","nameWithOwner":"other/parity"}),
                                dict(row, number=12, state="CLOSED"), row]
        call("Github resolve", "git.resolvePullRequest", dict(reference="#3"))
        (repo / "file").write_text("Dirty\n")
        call("Github dirty checkout", "git.preparePullRequestThread", dict(reference="3", mode="local"))
        git(repo, "restore", "file")
        call("Github local checkout", "git.preparePullRequestThread", dict(reference="3", mode="local"))
        def status(label, expected, **options):
            value = call(label, "vcs.refreshStatus", options)
            assert value["pr"] == expected, (label, value["pr"])
            received[label] = value
            return value
        expected_pr = dict(number=3,title="Work",url=PR["url"],headRef="pull-three",baseRef="main",
                           state="open",isDraft=True,updatedAt="2026-02-01T00:00:00.000Z")
        status("draft PR status", expected_pr)
        reads = provider.status_reads.count("pull-three")
        assert reads > 0
        status("cached local PR status", expected_pr, localOnly=True)
        assert provider.status_reads.count("pull-three") == reads
        status("repeat hosted status", expected_pr)
        assert provider.status_reads.count("pull-three") > reads
        # A subscriber joining after refresh sees the cached remote in its first frame.
        with connect(url.replace("http:", "ws:") + "/ws?orchestrationProtocol=2",
                     additional_headers={"Cookie":cookie}, proxy=None, compression=None) as stream:
            stream.send(encode(dict(_tag="Request",id="1",tag="subscribeVcsStatus",
                                    payload=dict(cwd=str(repo),includeRemote=False),headers=[])).decode())
            snapshot=json.loads(stream.recv(timeout=20))["values"][0]
            assert snapshot["remote"]["pr"]==expected_pr
            received["cached subscription"] = snapshot["remote"]
        git(repo, "switch", "-qc", "renamed")
        git(repo, "branch", "--set-upstream-to=origin/pull-three")
        status("renamed tracking branch status", expected_pr)
        git(repo, "switch", "-qc", "unpublished")
        status("unpublished branch status", None)
        git(repo, "update-ref", "refs/remotes/origin/main", "main")
        git(repo, "symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/main")
        git(repo, "switch", "-qc", "shipped")
        git(repo, "update-ref", "refs/remotes/origin/shipped", "HEAD")
        provider.status_rows = [dict(row, headRefName="shipped", state="MERGED", isDraft=False,
                                     mergedAt=row["updatedAt"])]
        merged = dict(expected_pr,headRef="shipped",state="merged")
        del merged["isDraft"]
        counts = status("squash merged PR status", merged)
        assert counts["aheadOfDefaultCount"]==0 and counts["aheadCount"]==0
        # A new commit is ahead again while the merged PR remains cached.
        git(repo, "commit", "-qm", "After squash", "--allow-empty")
        assert status("commits after squash", merged)["aheadOfDefaultCount"]>0
        git(repo, "switch", "-q", "main")
        git(repo, "branch", "--set-upstream-to=origin/main")
        provider.status_rows = [dict(provider.status_rows[0],headRefName="main")]
        status("default excludes merged PR", None)
        git(repo, "remote", "add", "contributor", "git@github.com:person/parity.git")
        git(repo, "update-ref", "refs/remotes/contributor/main", "HEAD")
        git(repo, "switch", "-qc", "fork-checkout", "--track", "contributor/main")
        provider.status_rows = [dict(row,headRefName="main",isCrossRepository=True,
                                     headRepositoryOwner={"__typename":"User","login":"person"},
                                     headRepository={"name":"parity","nameWithOwner":"person/parity"})]
        status("fork default as PR head", dict(expected_pr,headRef="main"))
        git(repo, "switch", "-qc", "tracks-base", "--track", "origin/main")
        status("tracking the base is not a PR head", None)
        git(repo, "switch", "-q", "pull-three")
        call("Github occupied worktree", "git.preparePullRequestThread", dict(reference="3", mode="worktree"))
        git(repo, "switch", "-q", "main")
        for label in ("Github PR worktree", "Github reuse PR worktree"):
            prepared = call(label, "git.preparePullRequestThread", dict(reference="3", mode="worktree"))
            path = Path(prepared["worktreePath"])
            if git(path, "rev-parse", "HEAD").decode().strip() != PR["headRefOid"]:
                raise RuntimeError("PR worktree did not check out the requested head")
            received[label] = {**prepared, "worktreePath": path.name}
        call("remove PR worktree", "vcs.removeWorktree", dict(path=str(path)))
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
        providers = []
        for name, prefix in (("original", args.original_command), ("rust", args.rust_command)):
            config = root / f"gh-{name}"
            config.mkdir()
            endpoint = config / "api.sock"
            provider = stack.enter_context(ThreadingUnixStreamServer(str(endpoint), Provider))
            provider.repository = REPOSITORY
            provider.status_rows = []
            provider.status_reads = []
            providers.append(provider)
            Thread(target=provider.serve_forever, daemon=True).start()
            stack.callback(provider.shutdown)
            (config / "config.yml").write_text(f"http_unix_socket: {endpoint}\n")
            cmd = ["env", f"GIT_AUTHOR_DATE={DATE}", f"GIT_COMMITTER_DATE={DATE}",
                   f"GIT_SSH_COMMAND={ssh}", "GIT_SSH_VARIANT=ssh", "GIT_ALLOW_PROTOCOL=file:ssh",
                   f"GH_CONFIG_DIR={config}", "GH_TOKEN=offline-test-fixture", "GH_NO_UPDATE_NOTIFIER=1"]
            cmd.extend(shlex.split(prefix))
            servers.append(managed_server(stack, cmd, root / name, token, key))
        reference, rust = [observe(server, root, provider) for server, provider in zip(servers, providers)]
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
