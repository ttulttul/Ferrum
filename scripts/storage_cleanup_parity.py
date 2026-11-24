#!/usr/bin/env python3
"""Compare storage cleanup reports and their effects in disposable T3 homes and Git repositories."""
import argparse
import base64
from contextlib import ExitStack, closing
import hashlib
import json
import os
from pathlib import Path
import secrets
import shlex
import sqlite3
import subprocess
import time
from tempfile import TemporaryDirectory

from websockets.sync.client import connect

from parity_harness import encode, isolated_git_env, managed_server, server_env

TIME = "2026-01-01T00:00:00.000Z"


def b64(text):
    return base64.urlsafe_b64encode(text.encode()).rstrip(b"=").decode()


def seed_checkpoints(state, git, repo):
    # The worktree is already gone. The project checkout still shares its refs.
    def legacy(owner):
        return f"refs/t3/checkpoints/{b64(owner)}/turn/"

    scope = "scope:gone"
    scope_key = hashlib.sha256(scope.encode()).hexdigest()[:32]
    scoped = f"refs/t3/orchestration-v2/checkpoints/{b64(scope_key)}/ordinal/"
    with closing(sqlite3.connect(state / "statev2.sqlite")) as db, db:
        db.execute("""INSERT OR IGNORE INTO projection_projects(project_id,title,workspace_root,scripts_json,
                      created_at,updated_at) VALUES('cleanup','Cleanup',?,'[]',?,?)""", (str(repo), TIME, TIME))
        for owner, archived, deleted, source in [
            ("gone", None, TIME, None), ("ancestor", None, TIME, None),
            ("middle", None, TIME, "ancestor"), ("archived", TIME, None, "middle"),
            ("pending", None, TIME, None), ("running", None, TIME, None),
        ]:
            payload = {"worktreePath": str(repo.parent / "removed-checkout"),
                       "forkedFrom": {"type": "run", "threadId": source} if source else None}
            db.execute("""INSERT INTO orchestration_v2_projection_threads(thread_id,project_id,title,default_provider,
                          runtime_mode,interaction_mode,created_at,updated_at,archived_at,deleted_at,payload_json)
                          VALUES (?,'cleanup',?,'codex','full-access','default',?,?,?,?,?)""",
                       (owner, owner, TIME, TIME, archived, deleted, json.dumps(payload)))
        db.execute("""INSERT INTO orchestration_v2_projection_checkpoint_scopes(scope_id,thread_id,node_id,kind,
                      ordinal_within_parent,advances_app_run_count,created_at,payload_json)
                      VALUES (?,'gone','node','root_run',0,1,?,'{}')""", (scope, TIME))
        for status in ("pending", "running"):
            db.execute("""INSERT INTO orchestration_v2_effect_outbox(effect_id,command_id,thread_id,effect_type,
                          payload_json,status,available_at,created_at,updated_at)
                          VALUES (?,'cmd',?,'checkpoint.capture','{}',?,?,?,?)""", (status, status, status, TIME, TIME, TIME))
    kept = {legacy(owner) + "0" for owner in ("ancestor", "middle", "archived", "unknown", "pending", "running")}
    kept.update(scoped + tail for tail in ("01", "1-extra", "3/nested"))
    for ref in sorted(kept | {legacy("gone") + "12", scoped + "0"}):
        git("update-ref", ref, "HEAD")
    git("pack-refs", "--all")
    git("symbolic-ref", scoped + "2", "refs/heads/main")
    return kept


def exercise(command, home):
    state, repo = home / "userdata", home / "project"
    state.mkdir(parents=True)
    repo.mkdir()
    env = isolated_git_env(home / "git")
    git_env = server_env(home, env)

    def git(*args):
        # parity-env: the disposable fixture repository, using the harness-isolated host
        return subprocess.run(["git", *args], cwd=repo, env=git_env, check=True,
                              text=True, capture_output=True).stdout.strip()

    git("init", "-q", "-b", "main")
    git("-c", "user.name=Cleanup", "-c", "user.email=cleanup@example.test", "commit", "-q",
        "--allow-empty", "-m", "fixture")
    (state / "settings.json").write_text(json.dumps({"storageCleanup": {"browserArtifactsAfterDays": 1, "logsAfterDays": 1}}))
    with ExitStack() as stack:
        url, cookie = managed_server(stack, shlex.split(command), home, secrets.token_urlsafe(32), secrets.token_bytes(32), env)
        ws = stack.enter_context(connect(url.replace("http:", "ws:", 1) + "/ws?orchestrationProtocol=2",
                                         additional_headers={"Cookie": cookie}, proxy=None))

        def request(i):
            ws.send(encode({"_tag": "Request", "id": i, "tag": "server.runStorageCleanup", "payload": {}, "headers": []}).decode())

        def response():
            reply = json.loads(ws.recv(timeout=30))["exit"]
            assert reply["_tag"] == "Success", "storage cleanup RPC failed"
            report = reply["value"]
            assert report.pop("startedAt") <= report.pop("finishedAt")
            assert report["trigger"] == "manual"
            return report

        counter = 10
        def call(tag, payload):
            nonlocal counter
            counter += 1
            ws.send(encode({"_tag": "Request", "id": counter, "tag": tag, "payload": payload, "headers": []}).decode())
            frame = json.loads(ws.recv(timeout=30))
            exit = frame["exit"]
            assert exit["_tag"] == "Success", f"{tag} failed: {exit}"
            return exit["value"]

        request(1)
        response()  # Drain startup before creating eligible files.
        # A deleted thread with worktreeOnDelete removes its managed linked worktree.
        worktree = home / "worktrees" / "cleanup-branch"
        worktree.parent.mkdir(parents=True, exist_ok=True)
        git("branch", "cleanup-branch")
        subprocess.run(["git", "worktree", "add", "-q", str(worktree), "cleanup-branch"], cwd=repo, env=git_env, check=True)
        (worktree / "payload.txt").write_text("retained until cleanup\n")
        subprocess.run(["git", "add", "payload.txt"], cwd=worktree, env=git_env, check=True)
        subprocess.run(["git", "-c", "user.name=Cleanup", "-c", "user.email=cleanup@example.test", "commit", "-q", "-m", "payload"], cwd=worktree, env=git_env, check=True)
        call("projects.mutate", {"type":"project.create", "commandId":"cleanup-project", "projectId":"cleanup", "title":"Cleanup", "workspaceRoot":str(repo)})
        call("server.updateSettings", {"patch":{"storageCleanup":{"worktreeOnDelete":True}}})
        call("orchestration.dispatchCommand", {"type":"thread.create", "commandId":"cleanup-thread-create", "threadId":"thread:cleanup", "projectId":"cleanup", "title":"Cleanup thread", "modelSelection":{"instanceId":"codex","model":"fixture"}, "runtimeMode":"full-access", "interactionMode":"default", "branch":"cleanup-branch", "worktreePath":str(worktree), "createdBy":"user", "creationSource":"web"})
        call("server.runStorageCleanup", {})
        call("orchestration.dispatchCommand", {"type":"thread.delete", "commandId":"cleanup-thread-delete", "threadId":"thread:cleanup"})
        last_worktree_report = None
        for _ in range(20):
            last_worktree_report = call("server.runStorageCleanup", {})
            if not worktree.exists():
                break
            time.sleep(0.1)
        assert not worktree.exists(), "deleted managed worktree was not removed"
        call("server.updateSettings", {"patch":{"storageCleanup":{"worktreeOnDelete":False}}})
        # Settings changes queue an automatic sweep. Drain it before creating the
        # files used by the concurrent manual-run assertion, or that sweep can
        # legitimately remove the fixtures before either manual request starts.
        call("server.runStorageCleanup", {})
        kept = seed_checkpoints(state, git, repo)
        logs = state / "logs"
        artifacts = state / "browser-artifacts"
        old = [artifacts / "one.png", artifacts / "two.webm", logs / "worker" / "trace.ndjson.27", logs / "server.log.1"]
        retained = [artifacts / "nested" / "nested.png", logs / "worker" / "current.ndjson",
                    logs / "server.log", logs / "report.log.bak", home / "outside" / "private.log.1"]
        for path in old + retained:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"fixture")
            os.utime(path, (1, 1))
        (logs / "fresh.log.12").write_bytes(b"fresh")
        (logs / "linked-directory").symlink_to(home / "outside", target_is_directory=True)
        (artifacts / "link").symlink_to(retained[-1])
        request(2)
        request(3)
        reports = sorted([response(), response()], key=lambda r: -r["bytesFreed"])
        bytes_freed = [r["bytesFreed"] for r in reports]
        assert bytes_freed == [28, 0], f"eligible files must be removed exactly once, got {bytes_freed}"
        assert reports[0]["counts"] == {"removed": 2, "kept": 0, "failed": 0}
        assert [e["kind"] for e in reports[0]["entries"]] == ["browser-artifacts", "logs"]
        assert all(not p.exists() for p in old)
        assert all(p.is_file() for p in retained) and (logs / "fresh.log.12").is_file()
        assert (artifacts / "link").is_symlink()
        assert set(git("for-each-ref", "--format=%(refname)", "refs/t3/").splitlines()) == kept
        git("show-ref", "--verify", "refs/heads/main")
        # A dangling link is skipped (upstream #40528): the file before it is
        # removed and the other category still completes.
        old_file = artifacts / "a-first.png"
        old_file.write_bytes(b"partial")
        os.utime(old_file, (1, 1))
        (artifacts / "b-broken").symlink_to(home / "missing")
        with closing(sqlite3.connect(state / "statev2.sqlite")) as db, db:
            db.execute("UPDATE orchestration_v2_projection_threads SET deleted_at=? WHERE thread_id='archived'", (TIME,))
            db.execute("UPDATE orchestration_v2_effect_outbox SET status='succeeded' WHERE effect_id IN ('pending','running')")
        request(4)
        failed = response()
        assert failed["counts"] == {"removed": 1, "kept": 1, "failed": 0}
        assert failed["bytesFreed"] == 7 and failed["entries"][0]["files"] == 1
        failed["entries"][0]["reason"] = failed["entries"][0]["reason"].replace(str(home), "$HOME")
        assert not old_file.exists()
        retained_refs = {ref for ref in kept if ref.startswith("refs/t3/orchestration-v2/")}
        retained_refs.add("refs/t3/checkpoints/" + b64("unknown") + "/turn/0")
        assert set(git("for-each-ref", "--format=%(refname)", "refs/t3/").splitlines()) == retained_refs
        # A linked category root is excluded even when the outside target
        # contains expired captures. A symlinked ancestor is allowed.
        artifacts.rename(home / "original-artifacts")
        artifacts.symlink_to(home / "outside", target_is_directory=True)
        request(5)
        linked = response()
        assert linked["counts"] == {"removed": 0, "kept": 2, "failed": 0}
        assert retained[-1].read_bytes() == b"fixture"
        return [*reports, failed, linked]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    with TemporaryDirectory(prefix="t3-storage-cleanup-") as tmp:
        reference = exercise(args.original_command, Path(tmp) / "original")
        rust = exercise(args.rust_command, Path(tmp) / "rust")
        assert rust == reference, "cleanup reports differ"
    print("PASS storage cleanup reports, file retention, checkpoint ownership, and concurrent sweeps")


if __name__ == "__main__":
    main()
