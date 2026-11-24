#!/usr/bin/env python3
"""A client follows and answers workflow runs through the workflows.* RPCs, on the reference and
on the Rust server, and both must answer the same.

As in workflow_parity.py, the reference alone first builds history from a fake workflow (no
network and no agents), and every server starts from a copy of that database, with workflows
off, on with no run kept, and on with the copied runs. Covered: the first snapshot of
workflows.subscribe and of workflows.counts, workflows.get (a run, one that is not there),
workflows.answer (a question, the same question again, a wait that asks nothing, a run that is
not there, a choice that does not fit), and what both streams send after a cancel and after
workflows are turned off and on again. Times and ids are masked. All data is fake.
"""
import argparse
from contextlib import ExitStack
import json
from pathlib import Path
import threading
import time
from tempfile import TemporaryDirectory

from websockets.sync.client import connect

from parity_harness import encode
from peer_forwarding_parity import McpClient, mcp_token
from workflow_parity import build_history, ids, mask, start_server

SETTLE = 1.5


class Watcher:
    """One stream on its own connection: every value it sends."""

    def __init__(self, stack, server, tag):
        self.values = []
        self.tag = tag
        socket = stack.enter_context(connect(
            server.url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2",
            additional_headers={"Cookie": server.cookie}, compression=None, proxy=None))
        socket.send(encode(dict(_tag="Request", id="watch", tag=tag, payload={}, headers=[])).decode())
        threading.Thread(target=self.follow, args=(socket,), daemon=True).start()

    def follow(self, socket):
        try:
            while True:
                frame = json.loads(socket.recv())
                if frame.get("_tag") == "Ping":
                    socket.send(encode({"_tag": "Pong"}).decode())
                elif frame.get("_tag") == "Chunk":
                    socket.send(encode({"_tag": "Ack", "requestId": "watch"}).decode())
                    self.values.extend(frame["values"])
                elif frame.get("_tag") == "Exit":
                    self.values.append({"exit": frame["exit"]})
        except Exception:
            pass

    def taken(self):
        values, self.values = self.values, []
        return values


def settings(server, enabled):
    return server.rpc("server.updateSettings", {"patch": {"workflowsEnabled": enabled}})["_tag"]


def observe(server, mode):
    record = {}
    with ExitStack() as stack:
        snapshots = Watcher(stack, server, "workflows.subscribe")
        counts = Watcher(stack, server, "workflows.counts")
        time.sleep(SETTLE)
        record["first snapshot"] = snapshots.taken()
        record["first counts"] = counts.taken()
        agent = McpClient(server.url, mcp_token(server, "full-access"))
        run = ids(agent) if mode == "history" else {"ask": "run-1", "wait": "run-2", "cancel-me": "run-3"}
        record["get"] = server.rpc("workflows.get", {"runId": run["ask"]})
        record["get a run that is not there"] = server.rpc("workflows.get", {"runId": "run-missing"})
        answer = lambda **payload: server.rpc("workflows.answer", payload)
        record["answer that does not fit"] = answer(runId=run["ask"], name="approve", choice="maybe")
        record["answer"] = answer(runId=run["ask"], name="approve", choice="yes", reason="looks right")
        record["answer again"] = answer(runId=run["ask"], name="approve", choice="no")
        record["answer a wait that asks nothing"] = answer(runId=run["wait"], name="go", choice="yes")
        record["answer a run that is not there"] = answer(runId="run-missing", name="approve", choice="yes")
        record["get after answering"] = server.rpc("workflows.get", {"runId": run["ask"]})
        time.sleep(SETTLE)
        record["after answering: snapshots"] = snapshots.taken()
        record["after answering: counts"] = counts.taken()
        agent.call("t3_workflow", {"action": "cancel", "runId": run["cancel-me"], "reason": "done here"})
        time.sleep(SETTLE)
        record["after a cancel: snapshots"] = snapshots.taken()
        record["after a cancel: counts"] = counts.taken()
        record["turned off"] = settings(server, False)
        time.sleep(SETTLE)
        record["off: snapshots"] = snapshots.taken()
        record["off: counts"] = counts.taken()
        record["turned on"] = settings(server, True)
        time.sleep(SETTLE)
        record["on again: snapshots"] = snapshots.taken()
        record["on again: counts"] = counts.taken()
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--dump", type=Path, help="write both servers' answers here as JSON")
    args = parser.parse_args()
    results = {}
    with TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        database = build_history(args.original_command, tmp)
        for side, command in (("original", args.original_command), ("rust", args.rust_command)):
            record = {}
            for mode in ("off", "disabled", "history"):
                with ExitStack() as stack:
                    home = tmp / side / mode
                    empty = home / "no-workflows"
                    empty.mkdir(parents=True)
                    server = start_server(stack, command, home, workflows=empty, enabled=mode != "off",
                                          database=None if mode == "disabled" else database)
                    record.update({f"{mode}: {name}": value for name, value in observe(server, mode).items()})
            results[side] = mask(record)
    if args.dump:
        args.dump.write_text(json.dumps(results, indent=1, sort_keys=True))
    failures = 0
    original, rust = results["original"], results["rust"]
    for name in original:
        same = original[name] == rust.get(name)
        failures += not same
        print(f"{'PASS' if same else 'FAIL'} {name}")
        if not same:
            print(f"  original {json.dumps(original[name])[:2500]}\n  rust     {json.dumps(rust.get(name))[:2500]}")
    print(f"{len(original) - failures}/{len(original)} workflow RPC checks matched across the reference and the Rust server")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
