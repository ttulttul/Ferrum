#!/usr/bin/env python3
"""Compare settings mutation RPC frames and migration from a populated upstream SQLite database."""

import argparse
from contextlib import ExitStack
import json
from pathlib import Path
import secrets
import shlex
import sqlite3
from tempfile import TemporaryDirectory

from websockets.sync.client import connect
from parity_harness import encode, managed_server


def run_mutations(compare):
    """compare(label, method, payload) must compare raw frames and require success."""

    def patch(label, **changes):
        compare(label, "server.updateSettings", {"patch": changes})

    patch("old idle compaction", compactIdleThreads=True)
    patch("disable idle compaction", idleCompaction="off")
    patch("old client preserves off", compactIdleThreads=False)
    patch("legacy background preset", backgroundActivityProfile="battery-saver")
    patch("legacy interval on preset", automaticGitFetchInterval=1000)
    patch(
        "replace modern overrides",
        backgroundActivity={
            "profile": "custom",
            "baseProfile": "balanced",
            "overrides": {"pauseWhenOnBattery": True},
        },
    )
    patch(
        "mixed legacy background patch",
        backgroundActivityProfile="performance",
        providerHealthRefreshInterval=18000,
    )
    patch("reset background preset", backgroundActivityProfile="balanced")
    for i, directory in enumerate(("/fixture/a", "/fixture/b", "/fixture/a", "")):
        patch(f"worktree directory history {i}", worktreesDirectory=directory)
    patch("model favorites", favorites=[{"provider": "codex", "model": "abc"}])
    patch(
        "change model favorites",
        setModelFavorites=[
            {"provider": "codex", "model": "abc", "favorite": False},
            {"provider": "codex", "model": "other", "favorite": True},
        ],
    )
    patch(
        "hide models",
        setModelsHidden=[{"provider": "codex", "model": "abc", "hidden": True}],
    )
    patch(
        "order models",
        setProviderModelOrder={"provider": "codex", "modelOrder": ["other", "abc"]},
    )
    patch(
        "new selection with options",
        textGenerationModelSelection={
            "instanceId": "codex",
            "model": "model-a",
            "options": [{"id": "opt", "value": "a"}],
        },
    )
    patch(
        "merge selection options",
        textGenerationModelSelection={
            "options": [{"id": "opt", "value": True}, {"id": "b", "value": "ok"}]
        },
    )
    patch(
        "clear options through replacement",
        textGenerationModelSelection={"model": "model-b"},
    )
    patch(
        "canonical project overrides",
        projectSettingsOverrides={
            "p": {
                "defaultAutoPull": True,
                "providerInstances": {"codex": {"enabled": False}},
            }
        },
    )
    patch(
        "legacy project overrides",
        projectAgentBrowserAccessOverrides={"p": True, "q": False},
    )
    patch(
        "retain provider overrides",
        projectSettingsOverrides={
            "p": {"continueThreadsAfterServerUpdate": True},
            "q": None,
        },
    )
    patch("empty project scripts", projectScriptOverrides={"p": []})
    patch(
        "canonical entries outrank legacy",
        projectAutoPullOverrides={"p": False},
        projectSettingsOverrides={"p": {"enableAgentBrowserAccess": True}},
    )
    patch("delete project overrides", projectSettingsOverrides={"p": None})
    patch(
        "custom cleanup",
        worktreeCleanup={"mode": "custom", "rules": {"worktreeAfterDays": 11}},
    )
    patch(
        "partial cleanup rules",
        worktreeCleanup={"mode": "custom", "rules": {"worktreeOnDelete": False}},
    )
    for op, instance in (
        ("create", {"driver": "codex", "config": {"enabled": True}}),
        ("upsert", {"driver": "codex", "enabled": True, "config": {"enabled": False}}),
        ("remove", None),
    ):
        mutation = {"operation": op, "instanceId": "parity-instance"}
        if instance is not None:
            mutation["instance"] = instance
        compare(
            f"{op} provider instance",
            "server.updateSettings",
            {"patch": {}, "providerInstanceMutation": mutation},
        )
    compare("read back mutations", "server.getSettings", {})


def paired(stack, root, executables, seed=None):
    sockets = []
    homes = []
    secret, key = secrets.token_urlsafe(32), secrets.token_bytes(32)
    for name, cmd in zip(("original", "rust"), executables):
        home = root / name
        if seed:
            seed(home / "userdata")
        base, cookie = managed_server(stack, shlex.split(cmd), home, secret, key)
        sockets.append(
            stack.enter_context(
                connect(
                    base.replace("http:", "ws:") + "/ws?orchestrationProtocol=2",
                    additional_headers={"Cookie": cookie},
                    compression=None,
                    proxy=None,
                )
            )
        )
        homes.append(home)
    return sockets, homes


def comparator(sockets):
    seq = 0

    def compare(label, method, payload):
        nonlocal seq
        frame = encode(
            dict(_tag="Request", id=str(seq), tag=method, payload=payload, headers=[])
        ).decode()
        seq += 1
        for socket in sockets:
            socket.send(frame)
        packets = [socket.recv(timeout=15) for socket in sockets]
        if packets[0] != packets[1]:
            left, right = packets
            pos = next(
                (i for i, (a, b) in enumerate(zip(left, right)) if a != b),
                min(len(left), len(right)),
            )
            raise AssertionError(
                f"{label}: frame mismatch at {pos}\noriginal: {left[pos : pos + 240]}\nrust: {right[pos : pos + 240]}"
            )
        exit = json.loads(packets[0])["exit"]
        if exit["_tag"] != "Success":
            raise AssertionError(f"{label}: expected success, got {exit}")
        print("PASS", label)
        return exit["value"]

    return compare


def run_migration(original_command, rust_command):
    with TemporaryDirectory() as directory:
        root = Path(directory)
        # The original server creates the complete version-matched schema.
        with ExitStack() as init:
            managed_server(
                init,
                shlex.split(original_command),
                root / "schema",
                secrets.token_urlsafe(32),
                secrets.token_bytes(32),
            )
        db_path = root / "schema/userdata/statev2.sqlite"
        if not db_path.exists():
            raise AssertionError("upstream did not initialize its SQLite database")
        timestamp = "2026-01-01T00:00:00.000Z"
        script = {
            "id": "build",
            "name": "Build",
            "command": "printf test",
            "icon": "build",
            "runOnWorktreeCreate": True,
        }
        with sqlite3.connect(db_path) as db:
            for project, deleted in (
                ("plain", None),
                ("reset", None),
                ("deleted", timestamp),
            ):
                db.execute(
                    """INSERT INTO projection_projects
                    (project_id,title,workspace_root,default_model_selection_json,default_thread_env_mode,
                     auto_pull,scripts_json,created_at,updated_at,deleted_at) VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (
                        project,
                        project,
                        f"/fixture/{project}",
                        json.dumps({"provider": "codex", "model": "test-model"}),
                        "worktree",
                        1,
                        json.dumps([script]),
                        timestamp,
                        timestamp,
                        deleted,
                    ),
                )
            db.execute(
                """INSERT INTO projection_thread_sessions (thread_id,status,provider_name,provider_instance_id,updated_at)
                       VALUES ('session-1','closed','cursor','custom',?)""",
                (timestamp,),
            )
            db.execute(
                """INSERT INTO provider_session_runtime (thread_id,provider_name,provider_instance_id,adapter_key,status,last_seen_at)
                       VALUES ('runtime-1','grok',NULL,'grok','stopped',?)""",
                (timestamp,),
            )
        initial = {
            "providers": {
                "codex": {"enabled": False, "legacy": "value"},
                "cursor": {},
                "opencode": {},
            },
            "providerInstances": {"custom": {"driver": "cursor", "config": {}}},
            "projectScriptOverrides": {"reset": None},
            "projectAutoPullOverrides": {"reset": False},
            "projectSettingsOverrides": {"plain": {"enableAgentBrowserAccess": True}},
            "bitbucket": {"email": "fixture@example.test"},
            "github": {"authMode": "cli"},
        }

        def seed(state):
            state.mkdir(parents=True)
            with (
                sqlite3.connect(db_path) as source,
                sqlite3.connect(state / "statev2.sqlite") as dest,
            ):
                source.backup(dest)
            (state / "settings.json").write_bytes(encode(initial))

        with ExitStack() as stack:
            sockets, homes = paired(stack, root, (original_command, rust_command), seed)
            cfg = comparator(sockets)(
                "SQLite and legacy file migration", "server.getSettings", {}
            )
            if (
                cfg["projectSettingsOverrides"]["reset"].get("defaultProjectScripts")
                is not None
                or "deleted" in cfg["projectSettingsOverrides"]
            ):
                raise AssertionError(
                    "deleted or explicitly reset project settings resurfaced"
                )
            if (
                not cfg["providerInstances"]["custom"]["enabled"]
                or not cfg["providerInstances"]["grok"]["enabled"]
            ):
                raise AssertionError(
                    "historical provider usage failed to enable its instances"
                )
            if (
                cfg["projectSettingsOverrides"]["plain"]["defaultModelSelection"][
                    "model"
                ]
                != "test-model"
            ):
                raise AssertionError("missing SQLite project model")
            saved = [(home / "userdata/settings.json").read_bytes() for home in homes]
            if saved[0] != saved[1]:
                raise AssertionError(
                    f"migration settings.json bytes differ\noriginal: {saved[0]!r}\nrust: {saved[1]!r}"
                )
            print("PASS migrated settings.json bytes")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    with TemporaryDirectory() as temp, ExitStack() as stack:
        sockets, _ = paired(
            stack, Path(temp), (args.original_command, args.rust_command)
        )
        run_mutations(comparator(sockets))
    run_migration(args.original_command, args.rust_command)


if __name__ == "__main__":
    main()
