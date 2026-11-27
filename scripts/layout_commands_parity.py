#!/usr/bin/env python3
"""Compare worktree layout inspection, approval, revocation and the shared SQLite rows."""
import argparse
from contextlib import ExitStack
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory

from parity_harness import assert_nothing_escaped
from project_favicon_parity import Server


def observe(command, home):
    project = home / "workspace"
    project.mkdir(parents=True)
    file = project / "t3.json"
    with ExitStack() as stack:
        server = Server(stack, command, home, {})
        server.mutate({"type": "project.create", "commandId": "create-layout", "projectId": "layout",
                       "title": "Layout fixture", "workspaceRoot": str(project)})
        def read(project_id="layout"):
            return server.rpc("workspace.layoutCommands", {"projectId": project_id})
        def approve(*values):
            return server.rpc("workspace.approveLayoutCommands", {
                "projectId": "layout", "approvals": [{"kind": kind, "command": text} for kind, text in values]})
        result = {"missing project": read("missing"), "no layout": read()}
        file.write_text('{// project commands\n"worktreeLayout":{"create":"  echo create  ","remove":"echo remove",},}')
        result["declared"] = read()
        result["approved"] = approve(("create", "echo create"), ("remove", "echo remove"))
        assert all(row["approved"] for row in result["approved"]["value"]["commands"])
        with sqlite3.connect(home / "userdata" / "statev2.sqlite") as conn:
            result["ledger"] = conn.execute(
                "SELECT kind,command_hash FROM layout_command_approvals WHERE project_id='layout' ORDER BY kind"
            ).fetchall()
        file.write_text(json.dumps({"worktreeLayout": {"create": "echo changed", "remove": "echo remove"}}))
        result["changed since approval"] = read()
        result["stale dialog"] = approve(("create", "echo create"))
        result["revoked"] = approve(("remove", None))
        file.write_text('{"worktreeLayout":{"remove":"echo remove"}}')
        result["layout missing create"] = read()
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    with TemporaryDirectory() as directory:
        root = Path(directory)
        reference = observe(args.original_command, root / "reference")
        rust = observe(args.rust_command, root / "rust")
    assert_nothing_escaped()
    for key in reference:
        if reference[key] != rust[key]:
            print(f"FAIL {key}: reference {reference[key]!r}, rust {rust[key]!r}")
            return 1
    print(f"{len(reference)} layout command checks matched")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
