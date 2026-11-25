#!/usr/bin/env python3
"""Measure each provider scenario of client_thread_parity.py from CI logs, for balancing shards.

  scripts/thread_shard_durations.py RUN_ID [RUN_ID ...]          # update the durations file
  scripts/thread_shard_durations.py --check RUN_ID [RUN_ID ...]  # print, change nothing

Each RUN_ID is a parity workflow run on main (PARITY_REAL_TIME=1). Scenarios run one after
another, and each prints its result when it ends, so a scenario's time is from the previous
result (or the script's start) to its own last result line. The slowest of the runs is kept;
scenarios the runs did not reach keep their old value. Needs the GitHub CLI (`gh`).
"""
import argparse
import collections
import datetime
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DURATIONS = ROOT / "scripts/client_thread_parity_durations.json"
sys.path.insert(0, str(ROOT / "scripts"))


def timestamp(text):
    return datetime.datetime.fromisoformat(text[:26])


def scenario_times(log, names):
    """{scenario: seconds} from one thread shard job's log."""
    began, rows = None, []
    for line in log.splitlines():
        parts = line.split("\t", 2)
        if len(parts) < 3:
            continue
        took = re.search(r"(\S+Z) ##\[notice\]client_thread_parity.py took (\d+) s", parts[2])
        if took:
            began = timestamp(took[1]) - datetime.timedelta(seconds=int(took[2]))
        result = re.match(r"﻿?(\S+Z) (PASS|FAIL|XFAIL) (.*)", parts[2])
        if result:
            text = result[3]
            name = next((n for n in names if text == n or text.startswith((n + ":", n + " "))), None)
            rows.append((timestamp(result[1]), name))
    times, start, current, previous = {}, None, None, began
    for at, name in rows:
        if name is not None and name != current:
            start, current = previous, name
        if name is not None and start is not None:
            times[name] = (at - start).total_seconds()
        previous = at
    return times


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("runs", nargs="+", help="parity workflow run ids on main")
    parser.add_argument("--repo", default="ttulttul/Ferrum")
    parser.add_argument("--check", action="store_true", help="print the measurements; write nothing")
    args = parser.parse_args()
    import client_thread_parity
    # Longest first, so a name that starts with another's is matched whole.
    names = sorted((s["name"] for s in client_thread_parity.PROVIDER_SCENARIOS), key=len, reverse=True)
    measured = collections.defaultdict(float)
    for run in args.runs:
        jobs = json.loads(subprocess.run(
            ["gh", "run", "view", run, "--repo", args.repo, "--json", "jobs"],
            check=True, capture_output=True, text=True).stdout)["jobs"]
        for job in jobs:
            if not re.search(r"\(thread-\d+-\d+", job["name"]):
                continue
            log = subprocess.run(["gh", "run", "view", "--repo", args.repo, "--job", str(job["databaseId"]), "--log"],
                                 check=True, capture_output=True, text=True).stdout
            for name, seconds in scenario_times(log, names).items():
                measured[name] = max(measured[name], seconds)
    current = json.loads(DURATIONS.read_text())
    seconds = {**current["seconds"], **{name: round(value, 1) for name, value in measured.items()}}
    known = set(names)
    seconds = {name: value for name, value in sorted(seconds.items()) if name in known}
    unmeasured = sorted(known - set(seconds))
    print(f"{len(measured)} scenarios measured, {len(seconds)} in the file, "
          f"{round(sum(seconds.values()))} s in all; not measured: {unmeasured or 'none'}")
    if not args.check:
        current["seconds"] = seconds
        DURATIONS.write_text(json.dumps(current, indent=1, ensure_ascii=False) + "\n")
        print(f"wrote {DURATIONS.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
