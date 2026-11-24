#!/usr/bin/env python3
"""Check that every parity suite runs in CI exactly once, and never with a real provider CLI.

Each scripts/*_parity.py (and parity_harness.py) must appear in exactly one shard of
scripts/ci_parity_shard.sh, unless it is listed in NOT_SHARDED with the reason. The
shards in .github/workflows/parity.yml's matrix must match the shard script's, and a split
suite's shards (thread-I-N, status-I-N) must cover 1..N of one N.

Every process a suite starts must take its environment from parity_harness.server_env
(directly, or through managed_server), which hides the host's provider CLIs (codex,
claude, ...) and its login profile from both servers. CI runners have no such CLIs, so
only this check notices a suite that would start a developer's real ones. A process
that is not a T3 server (git, a client driver, wrangler) says so with a
`# parity-env: <reason>` comment on its call.
"""

import ast
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


PROCESS_CALLS = {"Popen", "run", "check_output", "check_call", "call", "launch"}
ENV_MARKER = "# parity-env:"


def call_name(node):
    func = node.func
    if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name) and \
            func.value.id in ("subprocess", "harness", "parity_harness"):
        return func.attr
    if isinstance(func, ast.Name) and func.id in ("Popen", "launch"):
        return func.id
    return None


def unguarded_processes(path):
    """Calls in `path` that start a process whose environment server_env did not build."""
    text = path.read_text()
    lines = text.splitlines()
    tree = ast.parse(text)
    parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}

    def from_server_env(expression, node):
        if "server_env(" in ast.get_source_segment(text, expression):
            return True
        if not isinstance(expression, ast.Name):
            return False
        scope = node
        while scope in parents and not isinstance(scope, (ast.FunctionDef, ast.Module)):
            scope = parents[scope]
        return any(isinstance(assign, ast.Assign) and "server_env(" in ast.get_source_segment(text, assign.value)
                   and any(isinstance(target, ast.Name) and target.id == expression.id for target in assign.targets)
                   for assign in ast.walk(scope))

    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or call_name(node) not in PROCESS_CALLS:
            continue
        span = lines[max(node.lineno - 2, 0):node.end_lineno]
        if any(ENV_MARKER in line for line in span):
            continue
        env = next((keyword.value for keyword in node.keywords if keyword.arg == "env"), None)
        if env is not None and from_server_env(env, node):
            continue
        found.append(f"{path.relative_to(ROOT)}:{node.lineno}")
    return found


def main():
    problems = []
    suites_and_harness = sorted((ROOT / "scripts").glob("*_parity.py")) + [ROOT / "scripts/parity_harness.py"]
    for path in suites_and_harness:
        for where in unguarded_processes(path):
            problems.append(f"{where} starts a process without parity_harness.server_env, so a server could "
                            f"run the host's real provider CLIs (use server_env or managed_server, or mark a "
                            f"process that is not a T3 server with `{ENV_MARKER} <reason>`)")
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
    # A `<prefix>-*` shard runs part I of N of one suite as `<prefix>-I-N`.
    split = sorted(shard[:-2] for shard in shards if shard.endswith("-*"))
    named = sorted(shard for shard in matrix if not any(shard.startswith(f"{p}-") for p in split))
    if named != sorted(shard for shard in shards if not shard.endswith("-*")):
        problems.append(f"workflow matrix shards {named} differ from the shard script's "
                        f"{sorted(s for s in shards if not s.endswith('-*'))}")
    for prefix in split:
        parts = [shard for shard in matrix if re.fullmatch(rf"{prefix}-\d+-\d+", shard)]
        counts = {shard.rsplit("-", 1)[-1] for shard in parts}
        if len(counts) != 1:
            problems.append(f"{prefix} shards {parts} must all be I-N of one N")
            continue
        count = int(counts.pop())
        if sorted(parts) != sorted(f"{prefix}-{index}-{count}" for index in range(1, count + 1)):
            problems.append(f"{prefix} shards {parts} must be {prefix}-1-{count} .. {prefix}-{count}-{count}")

    for problem in problems:
        print(f"FAIL {problem}")
    if not problems:
        print(f"{len(suites) - len(NOT_SHARDED)} parity suites each run in one of "
              f"{len(matrix)} shards ({len(NOT_SHARDED)} run elsewhere); every server they start "
              f"gets its environment from server_env")
    return int(bool(problems))


if __name__ == "__main__":
    sys.exit(main())
