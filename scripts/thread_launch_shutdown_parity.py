#!/usr/bin/env python3
"""A thread launched with t3_thread_launch is still in its first turn when its server stops
gracefully, and the other server then starts on the same home, as when one server is swapped for
the other: what the stop left, and what the next server's startup recovery makes of it, must read
as when the reference stops and starts again. Each case runs reference -> reference (the
expectation), rust -> reference (Rust's shutdown) and reference -> rust (Rust's startup
recovery), each on its own home.

- running: the launched thread's turn is running a command when the server stops. The stop
  cuts the turn off, and the server that starts next continues it (a restart continuation).
- approval: the launched thread (approval-required) waits on a command approval when the
  server stops. The stop cancels the run and its approval, with the reference's notice.

Compared, from the server that started second, with the reference's own restart: the launched thread's runs (status, whether
the run continues another), its runtime requests' statuses, and its turn items (type, status,
notice title) in order. All data is fake: temporary homes, loopback addresses, generated keys and
fake CLIs.
"""
import argparse
import contextlib
import json
from pathlib import Path
import secrets
import shlex
import sqlite3
from tempfile import TemporaryDirectory
import time

import client_thread_parity as turns
from parity_harness import managed_server, run_concurrently

CASES = {
    "running": {"fixture": "claude_launch_shutdown_running.json",
                "stop_when": lambda p: any(i.get("type") == "command_execution" for i in p.get("turnItems", []))},
    "approval": {"fixture": "claude_launch_shutdown_approval.json",
                 "stop_when": lambda p: any(r.get("status") == "pending" for r in p.get("runtimeRequests", []))
                 and any(i.get("type") == "approval_request" for i in p.get("turnItems", []))},
}
ORDERS = {"reference -> reference": ("reference", "reference"), "rust -> reference": ("rust", "reference"),
          "reference -> rust": ("reference", "rust")}
LIVE = ("preparing", "starting", "running", "waiting", "queued")


def launched_thread(log, seconds=60):
    path = Path(f"{log}.mcp")
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        for line in path.read_text().splitlines() if path.exists() else []:
            record = json.loads(line)
            if (record.get("request") or {}).get("tool") == "t3_thread_launch":
                result = (record.get("answer") or {}).get("result") or {}
                text = "".join(part.get("text", "") for part in result.get("content", []))
                with contextlib.suppress(ValueError, KeyError, TypeError):
                    return json.loads(text)["threadId"]
                return None
        time.sleep(0.25)
    return None


def effects_pending(root, thread):
    """Effects for `thread` still pending or running in the shared database's outbox."""
    try:
        with contextlib.closing(sqlite3.connect(f"file:{root}/userdata/statev2.sqlite?mode=ro", uri=True,
                                                timeout=5)) as db:
            return db.execute("SELECT count(*) FROM orchestration_v2_effect_outbox WHERE thread_id = ?"
                              " AND status IN ('pending', 'running')", (thread,)).fetchone()[0] > 0
    except sqlite3.OperationalError:
        return True


def outcome(view):
    runs = sorted(view.get("runs", []), key=lambda run: run.get("ordinal", 0))
    items = sorted(view.get("turnItems", []), key=lambda item: (item.get("ordinal", 0), item.get("id")))
    return {
        "runs": [{"status": run.get("status"), "continues": run.get("restartContinuationOfRunId") is not None}
                 for run in runs],
        "runtime requests": [request.get("status") for request in view.get("runtimeRequests", [])],
        "items": [f'{item.get("type")} {item.get("status")}' + (f' {item.get("title")}' if item.get("type") == "system_notice" else "")
                  for item in items],
    }


def one(args, credentials, case, first, second):
    with TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        root = tmp / "home"
        scenario = {"name": f"claude launch shutdown {case}", "instance": "claudeAgent",
                    "model": "claude-opus-5-5", "scenario": CASES[case]["fixture"]}
        env, _, log = turns.scenario_env(tmp, "cross", root, scenario, turns.scenario_drivers(scenario))
        with contextlib.ExitStack() as stack:
            url, cookie = managed_server(stack, shlex.split(first), root, *credentials, env=env)
            seeded = turns.seed_turn(url, cookie, tmp / "checkouts", scenario)
            turns.rpc(url, cookie, "orchestration.dispatchCommand", seeded["dispatch"])
            launched = launched_thread(log)
            if launched is None:
                return {"error": "no thread was launched"}
            turns.wait_for(url, cookie, CASES[case]["stop_when"], 60, thread=launched)
            # What the server is still recording lands before the stop.
            previous = {}
            turns.wait_for(url, cookie, lambda p: (p == previous.get("p")) or previous.update(p=p) or False,
                           15, thread=launched)
        # Stopped as a service manager stops it; the other server starts on the same home.
        with contextlib.ExitStack() as stack:
            url, cookie = managed_server(stack, shlex.split(second), root, *credentials, env=env)
            # Settled once the server's startup recovery has queued its continuations, nothing
            # runs there, and nothing for the thread waits in the effect outbox.
            settled = turns.wait_for(url, cookie, lambda p: turns.startup_recorded(root, url) and p.get("runs")
                                     and not any(run.get("status") in LIVE for run in p.get("runs", []))
                                     and not effects_pending(root, launched), 60, thread=launched)
            return outcome(settled)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--reference-root", type=Path)
    parser.add_argument("--faketime-lib")
    parser.add_argument("--real-time", action="store_true")
    parser.add_argument("--dump", type=Path, help="write each order's outcome here as JSON")
    args = parser.parse_args()
    git_home = TemporaryDirectory()
    turns.GIT_ISOLATION.update(turns.isolated_git_env(Path(git_home.name) / "git"))
    credentials = secrets.token_urlsafe(32), secrets.token_bytes(32)
    commands = {"reference": args.original_command, "rust": args.rust_command}
    results, failures = {}, 0
    for case in CASES:
        outcomes = dict(zip(ORDERS, run_concurrently([
            (lambda first=first, second=second: one(args, credentials, case, commands[first], commands[second]))
            for first, second in ORDERS.values()], len(ORDERS))))
        results[case] = outcomes
        expected = outcomes["reference -> reference"]
        for order in ("rust -> reference", "reference -> rust"):
            actual = outcomes[order]
            for check in expected:
                same = expected.get(check) == actual.get(check)
                failures += not same
                print(f"{'PASS' if same else 'FAIL'} {case}, {order}: {check}")
                if not same:
                    print(f"  reference -> reference {json.dumps(expected.get(check))[:1500]}")
                    print(f"  {order:<22} {json.dumps(actual.get(check))[:1500]}")
    if args.dump:
        args.dump.write_text(json.dumps(results, indent=1))
    print(f"{2 * sum(len(r['reference -> reference']) for r in results.values()) - failures} launched-thread "
          "shutdown checks matched across a swap between the servers")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
