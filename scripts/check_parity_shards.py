#!/usr/bin/env python3
"""Check that every parity suite runs in CI exactly once.

Each scripts/*_parity.py (and parity_harness.py) must appear in exactly one shard of
scripts/ci_parity_shard.sh, unless it is listed in NOT_SHARDED with the reason. The
shards in .github/workflows/parity.yml's matrix must match the shard script's, and the
thread shards must cover 1..N of one N.
"""

from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parent.parent
SHARD_SCRIPT = ROOT / "scripts/ci_parity_shard.sh"
WORKFLOW = ROOT / ".github/workflows/parity.yml"

# Suites that are not run by a parity shard, and why.
NOT_SHARDED = {
    "managed_policy_parity.py": "a module of config_parity.py",
    "settings_mutation_parity.py": "a module of config_parity.py",
    "settings_secrets_parity.py": "a module of config_parity.py",
    "settings_syntax_parity.py": "a module of config_parity.py",
    "workers_initial_sync_parity.py": "runs in .github/workflows/workers.yml",
    "workers_peer_link_parity.py": "runs in .github/workflows/workers.yml",
}


def shard_commands(text):
    """{shard pattern: [script names]} from the shard script's case statement."""
    shards, current = {}, None
    for line in text.splitlines():
        label = re.match(r"^\s{2}([\w*-]+)\)\s*$", line)
        if label:
            current = label.group(1)
            shards[current] = []
            continue
        if re.match(r"^\s*;;\s*$", line):
            current = None
            continue
        if current is not None:
            shards[current] += re.findall(r"scripts/(\w+\.(?:py|mjs))", line)
    return shards


def main():
    problems = []
    shards = shard_commands(SHARD_SCRIPT.read_text())
    shards.pop("*", None)
    placed = {}
    for shard, scripts in shards.items():
        for script in scripts:
            placed.setdefault(script, []).append(shard)
    suites = sorted(path.name for path in (ROOT / "scripts").glob("*_parity.py")) + ["parity_harness.py"]
    for suite in suites:
        where = placed.get(suite, [])
        if suite in NOT_SHARDED:
            if where:
                problems.append(f"{suite} is in shard {', '.join(where)} but listed in NOT_SHARDED")
        elif not where:
            problems.append(f"{suite} is in no shard of scripts/ci_parity_shard.sh "
                            "(add it to one, or to NOT_SHARDED in scripts/check_parity_shards.py with the reason)")
        elif len(where) > 1:
            problems.append(f"{suite} is in more than one shard: {', '.join(where)}")
    for name in NOT_SHARDED:
        if not (ROOT / "scripts" / name).exists():
            problems.append(f"NOT_SHARDED lists {name}, which no longer exists")

    listed = re.search(r"^\s*shard:\s*\[([^\]]*)\]\s*$", WORKFLOW.read_text(), re.MULTILINE)
    matrix = [shard.strip() for shard in listed.group(1).split(",") if shard.strip()] if listed else []
    if not matrix:
        problems.append(f"no matrix shards found in {WORKFLOW.relative_to(ROOT)}")
    threads = [shard for shard in matrix if shard.startswith("thread-")]
    named = sorted(shard for shard in matrix if not shard.startswith("thread-"))
    if named != sorted(shard for shard in shards if shard != "thread-*"):
        problems.append(f"workflow matrix shards {named} differ from the shard script's "
                        f"{sorted(s for s in shards if s != 'thread-*')}")
    counts = {shard.rsplit("-", 1)[-1] for shard in threads}
    if len(counts) != 1:
        problems.append(f"thread shards {threads} must all be I-N of one N")
    else:
        count = int(counts.pop())
        if sorted(threads) != sorted(f"thread-{index}-{count}" for index in range(1, count + 1)):
            problems.append(f"thread shards {threads} must be thread-1-{count} .. thread-{count}-{count}")

    for problem in problems:
        print(f"FAIL {problem}")
    if not problems:
        print(f"{len(suites) - len(NOT_SHARDED)} parity suites each run in one of "
              f"{len(matrix)} shards ({len(NOT_SHARDED)} run elsewhere)")
    return int(bool(problems))


if __name__ == "__main__":
    sys.exit(main())
