#!/usr/bin/env python3
"""Measure how much smaller and faster the Rust server is than the reference, on one machine.

Both servers run the same workload, one at a time, on free loopback ports, each in its own
temporary home with the environment parity_harness.server_env builds: no real provider CLI, no
login profile, and all HTTP(S) sent to the harness's refusing proxy. Provider turns run against
the fake `codex` and `claude` CLIs of tests/fixtures/providers, replaying the recorded
`codex_turn.json` and `claude_turn.json` sessions, so no model is called and nothing leaves the
machine.

What is measured (see docs/performance.md):

- size: the Rust release binary against what the reference needs to run `t3 serve`: the Node
  binary, the production bundle (apps/server/dist, without source maps), and the packages the
  bundle leaves external (with their dependencies);
- startup: launch to the first protocol answer (`GET /ws` 426) and to the first successful RPC
  (browser sign-in, WebSocket, `server.getConfig`), then resident memory and CPU while idle;
- latency: p50 and p95 of client RPCs on one WebSocket, against a seeded thread;
- turns: N threads at once, each running short turns one after another on its own provider
  process (a Claude CLI per thread; an app-server per Codex instance), with turns per second,
  the server's CPU time, and its peak resident memory.

Each measurement is repeated (the servers alternate which goes first) and summarised as a median
with its range. The headline is the ratio of the two medians, measured in the same run, so a
slower or faster machine moves both sides alike.

Usage:
  scripts/perf_compare.py --original-command "node REF/apps/server/dist/bin.mjs" \\
      --rust-command target/release/t3code-server --reference-root REF \\
      --json perf.json --markdown perf.md
  scripts/perf_compare.py --history DIR --markdown history.md   # a trend table of results in DIR
"""
import argparse
import contextlib
import datetime
import json
import os
from pathlib import Path
import platform
import re
import secrets
import shlex
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import uuid

import parity_harness
from parity_harness import (PROVIDER_CLIS, RELAY_TOOLS, encode, free_port, http, isolated_git_env, path_without,
                            recv_frame, server_env, session_cookie)

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/fixtures/providers"
SCHEMA = 1
TICK = os.sysconf("SC_CLK_TCK")
SIDES = ("original", "rust")
FAKE_MARKERS = ("fake_cli.py", "status_cli.py", "fake_opencode.py")
# The fake CLIs (as in client_thread_parity.py): version probes answer as a supported release
# would, and protocol sessions replay the scenario.
FAKE_CODEX = """#!/bin/sh
case "$1 $2" in
  "--version "*) echo "codex-cli 0.170.0"; exit 0;;
  "app-server --help") echo "Usage: codex app-server"; exit 0;;
  "app-server "*) exec python3 "$FAKE_PROVIDER_CLI" "$@";;
  "exec "*) exec python3 "$FAKE_PROVIDER_CLI" "$@";;
esac
case "$*" in
  *"mcp list --json") exec python3 "$FAKE_PROVIDER_CLI" "$@";;
esac
exit 0
"""
FAKE_CLAUDE = """#!/bin/sh
case "$*" in
  --version*) echo "2.1.200 (Claude Code)"; exit 0;;
  "-p "*) exec python3 "$FAKE_PROVIDER_CLI" "$@";;
  *--output-format*stream-json*) exec python3 "$FAKE_PROVIDER_CLI" "$@";;
esac
exit 0
"""
PROVIDERS = {"codex": {"driver": "codex", "model": "fixture"},
             "claude": {"driver": "claudeAgent", "model": "claude-opus-5-5"}}
THREAD_INPUT = {"requestCompletionMarker": True, "acceptBoundedSnapshot": True, "acceptDeferredWork": True,
                "acceptCompactTurnItems": True, "acceptCompactIds": True, "acceptDedupedBodies": True}
TERMINAL_RUN = {"completed", "failed", "interrupted", "cancelled"}
UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


# --- Fake provider sessions -------------------------------------------------------------------

def _renumbered(steps, turn, keep):
    """`steps` with every UUID except those in `keep` replaced by one derived from it and `turn`,
    so each replayed turn has its own turn, item and message ids, consistently within the turn."""
    if turn == 0:
        return steps
    text = UUID.sub(lambda m: m.group(0) if m.group(0) in keep else
                    str(uuid.uuid5(uuid.NAMESPACE_OID, f"{m.group(0)}/{turn}")), json.dumps(steps))
    return json.loads(text)


def perf_scenario(turns):
    """The fake CLIs' scenario for `turns` turns per provider process, without the pause the
    parity fixtures leave before a tool call's end (toolGap): a Codex app-server initializes and
    starts its one thread, then plays codex_turn.json's turn `turns` times; a Claude CLI plays
    claude_turn.json's turn `turns` times. Every replayed turn gets fresh ids."""
    codex = json.loads((FIXTURES / "codex_turn.json").read_text())["steps"]
    start = next(i for i, step in enumerate(codex) if step.get("expect") == "turn/start")
    head, turn = codex[:start], codex[start:]
    keep = set(UUID.findall(json.dumps(head)))
    codex_steps = head + [step for n in range(turns) for step in _renumbered(turn, n, keep)]
    claude = json.loads((FIXTURES / "claude_turn.json").read_text())["steps"]
    sessions = {step["send"]["session_id"] for step in claude
                if "send" in step and isinstance(step["send"].get("session_id"), str)}
    claude_steps = []
    for n in range(turns):
        steps = _renumbered(claude, n, sessions)
        if n:
            # A later turn's messages carry the session the CLI runs (as claude_two_turns.json).
            text = json.dumps(steps)
            for session in sessions:
                text = text.replace(f'"session_id": "{session}"', '"session_id": "$session"')
            steps = json.loads(text)
        claude_steps += steps
    return {"toolGap": 0, "variants": [{"argsContain": "app-server", "steps": codex_steps},
                                       {"steps": claude_steps}]}


def write_fakes(directory, turns):
    directory.mkdir(parents=True, exist_ok=True)
    for name, script in (("codex", FAKE_CODEX), ("claude", FAKE_CLAUDE)):
        (directory / name).write_text(script)
        (directory / name).chmod(0o755)
    scenario = directory / "perf_scenario.json"
    scenario.write_text(json.dumps(perf_scenario(turns)))
    return scenario


# --- Processes ----------------------------------------------------------------------------------

def proc_times(pid):
    """(own CPU, reaped children's CPU) of `pid` in seconds."""
    with open(f"/proc/{pid}/stat") as source:
        fields = source.read().rsplit(")", 1)[1].split()
    utime, stime, cutime, cstime = (int(fields[i]) for i in (11, 12, 13, 14))
    return (utime + stime) / TICK, (cutime + cstime) / TICK


def proc_memory(pid):
    """{"rss": kB, "hwm": kB} of `pid`."""
    found = {}
    with open(f"/proc/{pid}/status") as source:
        for line in source:
            if line.startswith(("VmRSS:", "VmHWM:")):
                found[line[2:5].lower()] = int(line.split()[1])
    return found


def descendants(pid):
    """Every process below `pid`."""
    children = {}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat") as source:
                ppid = int(source.read().rsplit(")", 1)[1].split()[1])
        except (OSError, ValueError, IndexError):
            continue
        children.setdefault(ppid, []).append(int(entry))
    found, queue = [], [pid]
    while queue:
        for child in children.get(queue.pop(), []):
            found.append(child)
            queue.append(child)
    return found


def is_fake(pid):
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as source:
            command = source.read().decode("utf-8", "replace")
    except OSError:
        return True
    return any(marker in command for marker in FAKE_MARKERS)


class TreeSampler:
    """Samples the server's process tree, leaving out the fake CLIs: the peak resident memory of
    the whole tree, and the CPU used by helper processes the server keeps running (git, Node
    workers), which are not in the main process's own or reaped-children times until they exit."""

    def __init__(self, pid, interval=0.05):
        self.pid, self.interval = pid, interval
        self.peak_rss = 0
        self.helper_cpu = {}
        self.helpers = set()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self):
        self._sample()
        self.start_helper_cpu = dict(self.helper_cpu)
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join()

    def _run(self):
        while not self._stop.wait(self.interval):
            self._sample()

    def _sample(self):
        with contextlib.suppress(OSError):
            total = proc_memory(self.pid).get("rss", 0)
            for child in descendants(self.pid):
                if is_fake(child):
                    continue
                with contextlib.suppress(OSError, ValueError):
                    total += proc_memory(child).get("rss", 0)
                    self.helper_cpu[child] = proc_times(child)[0]
                    self.helpers.add(child)
            self.peak_rss = max(self.peak_rss, total)

    def helper_seconds(self):
        """CPU the server's live helpers used while sampled."""
        return sum(cpu - self.start_helper_cpu.get(pid, 0.0) for pid, cpu in self.helper_cpu.items())


class Server:
    """One server in a fresh home, launched as managed_server does (desktop bootstrap on a file
    descriptor), timing its start."""

    def __init__(self, command, root, env):
        self.command, self.root, self.env = command, root, env
        self.secret = secrets.token_urlsafe(32)
        self.process = None

    def start(self):
        secrets_dir = self.root / "userdata" / "secrets"
        secrets_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
        key = secrets_dir / "mcp-oauth-signing-key.bin"
        key.write_bytes(secrets.token_bytes(32))
        key.chmod(0o600)
        prefix, variables = parity_harness.split_env_prefix(self.command)
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        envelope = dict(mode="desktop", noBrowser=True, host="127.0.0.1", port=self.port, t3Home=str(self.root),
                        desktopBootstrapToken=self.secret, tailscaleServeEnabled=False, tailscaleServePort=443)
        bootstrap = tempfile.TemporaryFile()
        bootstrap.write(encode(envelope) + b"\n")
        bootstrap.seek(0)
        env = server_env(self.root, {**variables, **self.env, "HOME": str(self.root), "T3CODE_HOME": str(self.root)})
        self.stderr = (self.root.parent / f"{self.root.name}.stderr").open("w")
        started = time.monotonic()
        self.process = subprocess.Popen(prefix + ["--bootstrap-fd", str(bootstrap.fileno())], env=env,
                                        cwd=self.root, pass_fds=(bootstrap.fileno(),), start_new_session=True,
                                        stdout=subprocess.DEVNULL, stderr=self.stderr)
        bootstrap.close()
        deadline = started + 60
        while True:
            if self.process.poll() is not None:
                raise RuntimeError(f"{self.command[0]} exited before it listened ({self.process.returncode}); "
                                   f"see {self.stderr.name}")
            if time.monotonic() > deadline:
                raise RuntimeError(f"{self.url} did not answer within 60 s")
            with contextlib.suppress(OSError, parity_harness.http_client.HTTPException):
                if http(self.url, "GET", "/ws", timeout=0.5)[0] == 426:
                    break
            time.sleep(0.005)
        self.listen_s = time.monotonic() - started
        self.cookie = session_cookie(self.url, self.secret)
        with self.connect() as client:
            client.call("server.getConfig", {})
        self.rpc_s = time.monotonic() - started
        return self

    def connect(self):
        return Client(self.url, self.cookie)

    def stop(self):
        if self.process is None:
            return
        with contextlib.suppress(ProcessLookupError):
            os.killpg(self.process.pid, signal.SIGTERM)
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
        with contextlib.suppress(ProcessLookupError):
            os.killpg(self.process.pid, signal.SIGKILL)
        self.process.wait()
        self.stderr.close()

    def cpu(self):
        return proc_times(self.process.pid)


class Client:
    """A WebSocket RPC client speaking the servers' Effect RPC frames."""

    def __init__(self, url, cookie):
        from websockets.sync.client import connect
        self.ws = connect(url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2",
                          additional_headers={"Cookie": cookie}, proxy=None, compression=None, max_size=None)
        self.next_id = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.ws.close()

    def send(self, tag, payload):
        self.next_id += 1
        request = str(self.next_id)
        self.ws.send(encode({"_tag": "Request", "id": request, "tag": tag, "payload": payload,
                             "headers": []}).decode())
        return request

    def frame(self, timeout):
        return json.loads(recv_frame(self.ws, timeout=timeout))

    def call(self, tag, payload, timeout=30):
        request = self.send(tag, payload)
        while True:
            frame = self.frame(timeout)
            if frame.get("_tag") == "Exit" and frame.get("requestId") == request:
                if frame["exit"]["_tag"] != "Success":
                    raise RuntimeError(f"{tag} failed: {json.dumps(frame)[:800]}")
                return frame["exit"]["value"]

    def stream_until(self, tag, payload, done, timeout=30):
        """Subscribe, and end the stream once `done(value)` holds for a streamed value."""
        request = self.send(tag, payload)
        while True:
            frame = self.frame(timeout)
            if frame.get("requestId") != request:
                continue
            if frame.get("_tag") == "Exit":
                raise RuntimeError(f"{tag} ended early: {json.dumps(frame)[:800]}")
            if frame.get("_tag") == "Chunk":
                self.ws.send(encode({"_tag": "Ack", "requestId": request}).decode())
                if any(done(value) for value in frame.get("values", [])):
                    break
        self.ws.send(encode({"_tag": "Interrupt", "requestId": request}).decode())
        while True:
            frame = self.frame(timeout)
            if frame.get("_tag") == "Exit" and frame.get("requestId") == request:
                return


# --- Seeding ------------------------------------------------------------------------------------

def git(*args):
    # parity-env: git, not a T3 server
    subprocess.run(["git", *args], check=True, capture_output=True,
                   env={**os.environ, **GIT_ISOLATION})


GIT_ISOLATION = {}


def make_checkout(path):
    git("init", "-qb", "main", str(path))
    git("-C", str(path), "remote", "add", "origin", "https://github.com/Example-Org/Example.Repo.git")
    for n in range(40):
        (path / "src" / f"module_{n // 10}").mkdir(parents=True, exist_ok=True)
        (path / "src" / f"module_{n // 10}" / f"file_{n}.txt").write_text(f"line {n}\n" * 50)
    (path / "README.md").write_text("fixture\n")
    git("-C", str(path), "add", ".")
    git("-C", str(path), "-c", "user.email=test@example.test", "-c", "user.name=Perf", "commit", "-qm", "Fixture")


def create_project(server, checkout, project_id="project:perf"):
    status, _, body = http(server.url, "POST", "/api/projects/mutate", encode({
        "type": "project.create", "commandId": f"create-{project_id}", "projectId": project_id,
        "title": "Performance", "workspaceRoot": str(checkout)}),
        {"Cookie": server.cookie, "Content-Type": "application/json"})
    if status != 200:
        raise RuntimeError(f"project.create failed on {server.url}: HTTP {status} {body[:400]!r}")


def create_thread(client, thread_id, instance, model, project_id="project:perf"):
    client.call("orchestration.dispatchCommand", {
        "type": "thread.create", "commandId": f"create-{thread_id}", "threadId": thread_id,
        "projectId": project_id, "title": "Thread", "modelSelection": {"instanceId": instance, "model": model},
        "runtimeMode": "full-access", "interactionMode": "default", "branch": None, "worktreePath": None,
        "createdBy": "user", "creationSource": "web"})


def terminal_runs(value):
    """{run id: status} of the settled runs a thread stream item shows."""
    if value.get("kind") == "snapshot":
        runs = (value.get("projection") or {}).get("runs") or []
    elif value.get("kind") == "event" and (value.get("event") or {}).get("type") == "run.updated":
        runs = [value["event"].get("payload") or {}]
    else:
        runs = []
    return {run.get("id"): run.get("status") for run in runs if run.get("status") in TERMINAL_RUN}


def run_turns(server, thread_id, turns, timeout=120):
    """Send `turns` messages to the thread one after another, each once the previous run settled;
    the seconds each took from dispatch to its run settling."""
    took = []
    with server.connect() as client:
        request = client.send("orchestration.subscribeThread", {"threadId": thread_id, **THREAD_INPUT})
        settled = {}

        def read(until):
            while True:
                frame = client.frame(timeout)
                if frame.get("requestId") != request:
                    continue
                if frame.get("_tag") == "Exit":
                    raise RuntimeError(f"thread stream ended: {json.dumps(frame)[:800]}")
                if frame.get("_tag") != "Chunk":
                    continue
                client.ws.send(encode({"_tag": "Ack", "requestId": request}).decode())
                for value in frame.get("values", []):
                    settled.update(terminal_runs(value))
                    if until(value):
                        return

        read(lambda value: value.get("kind") == "synchronized")
        for n in range(turns):
            before = set(settled)
            started = time.monotonic()
            client.send("orchestration.dispatchCommand", {
                "type": "message.dispatch", "commandId": f"{thread_id}-message-{n}-{uuid.uuid4()}",
                "threadId": thread_id, "messageId": f"message:{uuid.uuid4()}",
                "text": "Run echo hi and write notes.txt", "attachments": [], "createdBy": "user",
                "creationSource": "web", "dispatchMode": {"type": "start_immediately"}})
            read(lambda value: bool(set(settled) - before))
            took.append(time.monotonic() - started)
            new = {settled[run] for run in set(settled) - before}
            if new != {"completed"}:
                raise RuntimeError(f"turn {n} on {thread_id} ended {sorted(new)}; see {server.stderr.name}")
        client.ws.send(encode({"_tag": "Interrupt", "requestId": request}).decode())
    return took


# --- Measurements -------------------------------------------------------------------------------

class Bench:
    def __init__(self, args, temporary):
        self.args, self.temporary = args, temporary
        self.commands = {"original": shlex.split(args.original_command), "rust": shlex.split(args.rust_command)}
        self.fake_bin = temporary / "fake-bin"
        self.scenario = write_fakes(self.fake_bin, args.turns)
        self.hidden = path_without(PROVIDER_CLIS | RELAY_TOOLS, temporary / "path")
        self.count = 0

    @contextlib.contextmanager
    def server(self, side):
        self.count += 1
        root = self.temporary / f"{side}-{self.count}"
        root.mkdir()
        log = self.temporary / f"{side}-{self.count}-provider.log"
        env = {**GIT_ISOLATION, "PATH": os.pathsep.join([str(self.fake_bin), self.hidden]),
               "FAKE_PROVIDER_CLI": str(FIXTURES / "fake_cli.py"), "FAKE_PROVIDER_LOG": str(log),
               "FAKE_PROVIDER_SCENARIO": str(self.scenario)}
        server = Server(self.commands[side], root, env)
        try:
            yield server.start()
        finally:
            server.stop()
            if not self.args.keep:
                shutil.rmtree(root, ignore_errors=True)

    def orders(self, repeat):
        """Each repetition's order: the servers alternate going first."""
        return [SIDES if n % 2 == 0 else SIDES[::-1] for n in range(repeat)]

    def startup(self):
        result = {side: {"listen_ms": [], "first_rpc_ms": [], "idle_rss_kb": [], "idle_cpu_ms": []}
                  for side in SIDES}
        for run, order in enumerate(self.orders(self.args.startup_runs + 1)):
            for side in order:
                with self.server(side) as server:
                    # Settle: start-up work (provider checks, migrations) ends, then idle.
                    time.sleep(self.args.settle)
                    before = sum(server.cpu())
                    time.sleep(self.args.idle)
                    idle_cpu = sum(server.cpu()) - before
                    rss = proc_memory(server.process.pid)["rss"] + sum(
                        proc_memory(child).get("rss", 0) for child in descendants(server.process.pid)
                        if not is_fake(child))
                    if run == 0:
                        continue  # the first run warms the page cache and is not recorded
                    result[side]["listen_ms"].append(server.listen_s * 1000)
                    result[side]["first_rpc_ms"].append(server.rpc_s * 1000)
                    result[side]["idle_rss_kb"].append(rss)
                    result[side]["idle_cpu_ms"].append(idle_cpu * 1000)
                    print(f"startup {side}: listen {server.listen_s * 1000:.0f} ms, first RPC "
                          f"{server.rpc_s * 1000:.0f} ms, idle RSS {rss / 1024:.1f} MiB", flush=True)
        return result

    def latency(self):
        names = ["server.getConfig", "server.getSettings", "orchestration.subscribeShell",
                 "orchestration.subscribeThread", "orchestration.getThreadProjection", "orchestration.getTurnDiff"]
        result = {side: {name: {"p50_ms": [], "p95_ms": []} for name in names} for side in SIDES}
        for order in self.orders(self.args.latency_runs):
            for side in order:
                with self.server(side) as server:
                    checkout = self.temporary / f"{side}-{self.count}-checkout"
                    make_checkout(checkout)
                    create_project(server, checkout)
                    with server.connect() as client:
                        create_thread(client, "thread:latency", "codex", "fixture")
                    run_turns(server, "thread:latency", min(3, self.args.turns))
                    time.sleep(self.args.settle)
                    calls = {
                        "server.getConfig": lambda c: c.call("server.getConfig", {}),
                        "server.getSettings": lambda c: c.call("server.getSettings", {}),
                        "orchestration.subscribeShell": lambda c: c.stream_until(
                            "orchestration.subscribeShell", {"requestCompletionMarker": True},
                            lambda value: value.get("kind") == "snapshot" or "snapshot" in value),
                        "orchestration.subscribeThread": lambda c: c.stream_until(
                            "orchestration.subscribeThread", {"threadId": "thread:latency", **THREAD_INPUT},
                            lambda value: value.get("kind") == "synchronized"),
                        "orchestration.getThreadProjection": lambda c: c.call(
                            "orchestration.getThreadProjection", {"threadId": "thread:latency"}),
                        "orchestration.getTurnDiff": lambda c: c.call(
                            "orchestration.getTurnDiff", {"threadId": "thread:latency", "fromTurnCount": 0,
                                                          "toTurnCount": 1}),
                    }
                    with server.connect() as client:
                        for name in names:
                            for _ in range(self.args.warmup):
                                calls[name](client)
                            samples = []
                            for _ in range(self.args.samples):
                                started = time.perf_counter()
                                calls[name](client)
                                samples.append((time.perf_counter() - started) * 1000)
                            p50, p95 = percentile(samples, 50), percentile(samples, 95)
                            result[side][name]["p50_ms"].append(p50)
                            result[side][name]["p95_ms"].append(p95)
                            print(f"latency {side} {name}: p50 {p50:.2f} ms, p95 {p95:.2f} ms", flush=True)
        return result

    def turns(self, provider):
        spec = PROVIDERS[provider]
        result = {side: {"turns_per_s": [], "turn_p50_ms": [], "cpu_s": [], "cpu_per_turn_ms": [],
                         "peak_rss_kb": [], "peak_tree_rss_kb": []} for side in SIDES}
        lanes, turns = self.args.lanes, self.args.turns
        for order in self.orders(self.args.turn_runs):
            for side in order:
                with self.server(side) as server:
                    checkout = self.temporary / f"{side}-{self.count}-checkout"
                    make_checkout(checkout)
                    create_project(server, checkout)
                    instances = []
                    with server.connect() as client:
                        if provider == "codex":
                            # A Codex instance shares one app-server among its threads; one instance
                            # per thread gives each thread its own, as each Claude thread has its CLI.
                            instances = [f"codex-perf-{n}" for n in range(lanes)]
                            client.call("server.updateSettings", {"patch": {"providerInstances": {
                                instance: {"driver": "codex", "enabled": True} for instance in instances}}})
                        else:
                            instances = [spec["driver"]] * lanes
                        threads = [f"thread:perf-{n}" for n in range(lanes)]
                        for thread, instance in zip(threads, instances):
                            create_thread(client, thread, instance, spec["model"])
                    time.sleep(self.args.settle)
                    own_before, children_before = server.cpu()
                    outcomes = [None] * lanes
                    with TreeSampler(server.process.pid) as sampler:
                        started = time.monotonic()
                        def lane(n):
                            try:
                                outcomes[n] = run_turns(server, threads[n], turns)
                            except Exception as error:  # reported below
                                outcomes[n] = error
                        workers = [threading.Thread(target=lane, args=(n,)) for n in range(lanes)]
                        for worker in workers:
                            worker.start()
                        for worker in workers:
                            worker.join()
                        wall = time.monotonic() - started
                    errors = [outcome for outcome in outcomes if isinstance(outcome, Exception)]
                    if errors:
                        raise errors[0]
                    own_after, children_after = server.cpu()
                    cpu = (own_after - own_before) + (children_after - children_before) + sampler.helper_seconds()
                    took = [t for outcome in outcomes for t in outcome]
                    memory = proc_memory(server.process.pid)
                    total = lanes * turns
                    result[side]["turns_per_s"].append(total / wall)
                    result[side]["turn_p50_ms"].append(percentile(took, 50) * 1000)
                    result[side]["cpu_s"].append(cpu)
                    result[side]["cpu_per_turn_ms"].append(cpu / total * 1000)
                    result[side]["peak_rss_kb"].append(memory["hwm"])
                    result[side]["peak_tree_rss_kb"].append(max(sampler.peak_rss, memory["hwm"]))
                    print(f"turns {provider} {side}: {total / wall:.2f} turns/s, CPU {cpu:.2f} s, "
                          f"peak RSS {memory['hwm'] / 1024:.1f} MiB", flush=True)
        return result


def percentile(values, pct):
    values = sorted(values)
    if not values:
        return None
    rank = (len(values) - 1) * pct / 100
    low = int(rank)
    high = min(low + 1, len(values) - 1)
    return values[low] + (values[high] - values[low]) * (rank - low)


# --- Size ---------------------------------------------------------------------------------------

def tree_bytes(path, skip=()):
    """Bytes of the files under `path`, leaving out directories named in `skip` and the native
    prebuilds for other platforms (`prebuilds/<platform>-<arch>`), which a platform's release
    archive need not carry."""
    host = f"{sys.platform}-{ {'x86_64': 'x64', 'aarch64': 'arm64'}.get(platform.machine(), platform.machine())}"
    total = 0
    for directory, folders, files in os.walk(path):
        folders[:] = [folder for folder in folders if folder not in skip and not (
            os.path.basename(directory) == "prebuilds" and not folder.startswith(host))]
        for name in files:
            with contextlib.suppress(OSError):
                total += os.lstat(os.path.join(directory, name)).st_size
    return total


# External packages for features the Rust server does not have, left out of the comparable size:
# local speech-to-text (transcribe-cpp) and Cursor agent turns (the Cursor SDK).
WITHOUT_RUST_COUNTERPART = ("transcribe-cpp", "@cursor/sdk")


def external_packages(reference_root):
    """The package names (or name prefixes) the reference's CLI bundle leaves external, read from
    the reference's own list (scripts/lib/cli-external-packages.ts)."""
    text = (reference_root / "scripts/lib/cli-external-packages.ts").read_text()
    names = []
    for array in ("CLI_RUNTIME_EXTERNAL_PREFIXES", "CURSOR_RUNTIME_DEPENDENCIES"):
        body = re.search(rf"const {array} = \[(.*?)\] as const", text, re.S).group(1)
        names += re.findall(r'"([^"]+)"', re.sub(r"//[^\n]*", "", body))
    return names


def resolve_package(name, start):
    """The directory of package `name` as Node resolves it from `start`."""
    directory = Path(start)
    while True:
        candidate = directory / "node_modules" / name
        if (candidate / "package.json").exists():
            return Path(os.path.realpath(candidate))
        if directory.parent == directory:
            return None
        directory = directory.parent


def reference_footprint(reference_root, node):
    """What `t3 serve` from the reference needs on disk: the Node binary, the bundle (without source
    maps or the web client, which the Rust server does not ship), and the external packages with
    every dependency they load."""
    server = reference_root / "apps/server"
    dist = server / "dist"
    bundle = sum(path.stat().st_size for path in dist.rglob("*")
                 if path.is_file() and not path.name.endswith(".map") and "client" not in path.relative_to(dist).parts)
    manifest = json.loads((server / "package.json").read_text())
    externals = external_packages(reference_root)
    roots = [name for name in manifest.get("dependencies", {})
             if any(name == prefix or name.startswith(prefix) for prefix in externals)]

    def closure(names):
        seen, queue = {}, [(name, server) for name in names]
        while queue:
            name, start = queue.pop()
            found = resolve_package(name, start)
            if found is None or found in seen:
                continue
            seen[found] = tree_bytes(found, skip=("node_modules",))
            package = json.loads((found / "package.json").read_text())
            for dependency in {**package.get("dependencies", {}), **package.get("optionalDependencies", {})}:
                queue.append((dependency, found))
        return seen

    every = closure(roots)
    comparable = closure([name for name in roots if name not in WITHOUT_RUST_COUNTERPART])
    node_bytes = os.path.getsize(os.path.realpath(node))
    return {"node": node_bytes, "bundle": bundle, "external_packages": sum(every.values()),
            "external_package_count": len(every), "total": node_bytes + bundle + sum(every.values()),
            "without_rust_counterpart": sum(every.values()) - sum(comparable.values()),
            "comparable": node_bytes + bundle + sum(comparable.values())}


def node_binary(command):
    program = shlex.split(command)[0]
    return shutil.which(program) if os.sep not in program else program


# --- Report -------------------------------------------------------------------------------------

# (section, metric, label, unit, lower is better)
METRICS = [
    ("size", "comparable", "Installed size", "MiB", True),
    ("startup", "first_rpc_ms", "Start to first RPC", "ms", True),
    ("startup", "idle_rss_kb", "Memory at idle (RSS)", "MiB", True),
    ("startup", "idle_cpu_ms", "CPU while idle (per 10 s)", "ms", True),
    ("turns:codex", "peak_rss_kb", "Peak memory, Codex turns", "MiB", True),
    ("turns:claude", "peak_rss_kb", "Peak memory, Claude turns", "MiB", True),
    ("turns:codex", "turns_per_s", "Codex turns per second", "/s", False),
    ("turns:claude", "turns_per_s", "Claude turns per second", "/s", False),
    ("turns:codex", "cpu_per_turn_ms", "CPU per Codex turn", "ms", True),
    ("turns:claude", "cpu_per_turn_ms", "CPU per Claude turn", "ms", True),
]


def scaled(value, unit):
    if value is None:
        return None
    if unit == "MiB":
        return value / 1024
    return value


def side_values(results, section, metric, side):
    if section == "size":
        value = results["size"][side][metric]
        return [value / 1024]  # bytes to kB, scaled to MiB below
    if section.startswith("turns:"):
        provider = results["turns"].get(section.split(":", 1)[1])
        return provider[side][metric] if provider else []
    if section == "latency":
        return results["latency"][side][metric[0]][metric[1]]
    return results[section][side][metric]


def summarise(values):
    values = [v for v in values if v is not None]
    if not values:
        return None
    return {"median": statistics.median(values), "min": min(values), "max": max(values), "n": len(values)}


def summary_rows(results):
    rows = []
    metrics = list(METRICS)
    for name in results.get("latency", {}).get("rust", {}):
        metrics.append(("latency", (name, "p50_ms"), f"{name} p50", "ms", True))
        metrics.append(("latency", (name, "p95_ms"), f"{name} p95", "ms", True))
    for section, metric, label, unit, lower in metrics:
        if section.split(":")[0] not in results:
            continue
        stats = {}
        for side in SIDES:
            summary = summarise([scaled(v, unit) for v in side_values(results, section, metric, side)])
            stats[side] = summary
        if not stats["rust"] or not stats["original"]:
            continue
        original, rust = stats["original"]["median"], stats["rust"]["median"]
        ratio = (original / rust if lower else rust / original) if original and rust else None
        rows.append({"section": section, "metric": metric if isinstance(metric, str) else "/".join(metric),
                     "label": label, "unit": unit, "lower_is_better": lower,
                     "original": stats["original"], "rust": stats["rust"], "ratio": ratio})
    return rows


def number(value, unit):
    if value is None:
        return "–"
    if unit == "/s" or abs(value) < 10:
        return f"{value:.2f}"
    return f"{value:.0f}" if abs(value) >= 100 else f"{value:.1f}"


def cell(stats, unit):
    if stats["n"] == 1:
        return f"{number(stats['median'], unit)} {unit}"
    return (f"{number(stats['median'], unit)} {unit} "
            f"<sub>({number(stats['min'], unit)}–{number(stats['max'], unit)})</sub>")


def markdown(results):
    rows = summary_rows(results)
    meta = results["meta"]
    lines = [f"Commit `{meta['commit'][:12]}`, reference `{meta['reference'][:8]}`, "
             f"{meta['date']}, {meta['host']['cpu']} ({meta['host']['cpus']} CPUs), {meta['host']['os']}.", "",
             "Ratio: how many times smaller, faster or lighter the Rust server is (the reference's median over "
             "Rust's, or Rust's over the reference's for turns per second). Medians, with the range in brackets.",
             "", "| Measure | Rust | Reference | Ratio |", "| --- | ---: | ---: | ---: |"]
    for row in rows:
        ratio = f"**{row['ratio']:.1f}×**" if row["ratio"] else "–"
        lines.append(f"| {row['label']} | {cell(row['rust'], row['unit'])} | "
                     f"{cell(row['original'], row['unit'])} | {ratio} |")
    size = results.get("size")
    if size:
        original = size["original"]
        lines += ["", f"Installed size: Rust is one binary ({size['rust']['total'] / 2**20:.1f} MiB). The reference "
                      f"is Node ({original['node'] / 2**20:.1f} MiB), its bundle ({original['bundle'] / 2**20:.1f} MiB) "
                      f"and {original['external_package_count']} external packages "
                      f"({original['external_packages'] / 2**20:.1f} MiB), {original['total'] / 2**20:.1f} MiB in all; "
                      f"the table leaves out the {original['without_rust_counterpart'] / 2**20:.1f} MiB of packages for "
                      f"features Rust lacks (local speech-to-text, Cursor turns)."]
    config = meta["config"]
    lines += ["", f"Startup: {config['startup_runs']} runs per server after one warm-up; idle measured over "
                  f"{config['idle']:.0f} s after {config['settle']:.0f} s. Latency: {config['latency_runs']} servers each, "
                  f"{config['samples']} calls per RPC after {config['warmup']} warm-up calls. Turns: "
                  f"{config['lanes']} threads at once, {config['turns']} turns each, {config['turn_runs']} runs."]
    return "\n".join(lines) + "\n"


def history_markdown(directory, limit=30):
    """A trend table of the headline ratios in every result file under `directory`, newest first."""
    runs = []
    for path in Path(directory).glob("**/*.json"):
        with contextlib.suppress(ValueError, KeyError, OSError):
            data = json.loads(path.read_text())
            if data.get("schema") == SCHEMA:
                runs.append(data)
    runs.sort(key=lambda data: data["meta"]["date"], reverse=True)
    if not runs:
        return "No results yet.\n"
    labels = [label for _, _, label, _, _ in METRICS]
    lines = ["# Performance results", "",
             "The Rust server against the pinned reference, measured by scripts/perf_compare.py on each push "
             "to main (.github/workflows/perf.yml); docs/performance.md on main explains the method. One "
             "file per run is under results/. This file is generated: do not edit it.", "",
             "## Latest", "", markdown(runs[0]), "## History", "",
             "Ratios (Rust against the reference, higher is better for Rust) by commit, newest first.", "",
             "| Date | Commit | " + " | ".join(labels) + " |",
             "| --- | --- | " + " | ".join("---:" for _ in labels) + " |"]
    for data in runs[:limit]:
        by_label = {row["label"]: row["ratio"] for row in summary_rows(data)}
        commit = data["meta"]["commit"]
        lines.append(f"| {data['meta']['date'][:10]} | `{commit[:8]}` | " + " | ".join(
            f"{by_label[label]:.1f}×" if by_label.get(label) else "–" for label in labels) + " |")
    return "\n".join(lines) + "\n"


def host():
    cpu = platform.processor() or platform.machine()
    with contextlib.suppress(OSError):
        for line in open("/proc/cpuinfo"):
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    return {"cpu": cpu, "cpus": os.cpu_count(), "os": f"{platform.system()} {platform.release()}",
            "python": platform.python_version()}


def commit_of(directory):
    # parity-env: git, not a T3 server
    result = subprocess.run(["git", "-C", str(directory), "rev-parse", "HEAD"], capture_output=True, text=True)
    return result.stdout.strip() or "unknown"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--original-command", help="the reference's production server, e.g. node REF/apps/server/dist/bin.mjs")
    parser.add_argument("--rust-command", help="the Rust release binary")
    parser.add_argument("--reference-root", type=Path, help="the reference checkout (for its installed size)")
    parser.add_argument("--json", type=Path, help="write the results here")
    parser.add_argument("--markdown", type=Path, help="write the summary (or, with --history, the trend) here")
    parser.add_argument("--history", type=Path, help="summarise the result files in this directory instead")
    parser.add_argument("--only", nargs="*", choices=["size", "startup", "latency", "codex", "claude"],
                        help="run only these measurements")
    parser.add_argument("--startup-runs", type=int, default=5)
    parser.add_argument("--latency-runs", type=int, default=3)
    parser.add_argument("--turn-runs", type=int, default=3)
    parser.add_argument("--samples", type=int, default=200, help="calls per RPC per server")
    parser.add_argument("--warmup", type=int, default=20, help="unrecorded calls per RPC first")
    parser.add_argument("--lanes", type=int, default=8, help="threads running turns at once")
    parser.add_argument("--turns", type=int, default=5, help="turns each thread runs")
    parser.add_argument("--settle", type=float, default=3.0, help="seconds to let a server settle before measuring")
    parser.add_argument("--idle", type=float, default=10.0, help="seconds over which idle CPU is measured")
    parser.add_argument("--keep", action="store_true", help="keep each server's home (logs) for debugging")
    args = parser.parse_args()

    if args.history:
        text = history_markdown(args.history)
        if args.markdown:
            args.markdown.write_text(text)
        else:
            sys.stdout.write(text)
        return 0
    if not (args.original_command and args.rust_command and args.reference_root):
        parser.error("--original-command, --rust-command and --reference-root are required")
    only = set(args.only or ["size", "startup", "latency", "codex", "claude"])
    meta = {"commit": os.environ.get("GITHUB_SHA") or commit_of(ROOT), "reference": commit_of(args.reference_root),
            "date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "host": host(),
            "config": {name: getattr(args, name) for name in (
                "startup_runs", "latency_runs", "turn_runs", "samples", "warmup", "lanes", "turns", "settle", "idle")}}
    results = {"schema": SCHEMA, "meta": meta}
    if "size" in only:
        rust = shlex.split(args.rust_command)[0]
        results["size"] = {"rust": {"binary": os.path.getsize(rust), "total": os.path.getsize(rust),
                                    "comparable": os.path.getsize(rust)},
                           "original": reference_footprint(args.reference_root, node_binary(args.original_command))}
        meta["node"] = subprocess.run([node_binary(args.original_command), "--version"],  # parity-env: version only
                                      capture_output=True, text=True).stdout.strip()
    with tempfile.TemporaryDirectory(prefix="t3-perf-", delete=not args.keep) as temporary:
        temporary = Path(temporary)
        GIT_ISOLATION.update(isolated_git_env(temporary))
        bench = Bench(args, temporary)
        if args.keep:
            print(f"keeping server homes in {temporary}", flush=True)
        if "startup" in only:
            results["startup"] = bench.startup()
        if "latency" in only:
            results["latency"] = bench.latency()
        for provider in ("codex", "claude"):
            if provider in only:
                results.setdefault("turns", {})[provider] = bench.turns(provider)
    parity_harness.assert_nothing_escaped()
    results["summary"] = summary_rows(results)
    if args.json:
        args.json.write_text(json.dumps(results, indent=1) + "\n")
    text = markdown(results)
    if args.markdown:
        args.markdown.write_text(text)
    sys.stdout.write("\n" + text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
