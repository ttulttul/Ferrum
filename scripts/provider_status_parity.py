#!/usr/bin/env python3
"""Compare the provider status checks of both servers with fake provider CLIs.

Each scenario starts each server on a fresh home with fake `claude` and
`codex` executables (or none), settings seeded before the start, and waits
until the server has checked the scenario's provider instance. It then
compares that instance's row in `server.getConfig`: installed, version,
status, auth, message, models, slash commands, skills, usage limits and the
version and compatibility advisories, with check times left out and the
temporary home masked. A scenario with "refresh" then changes what the fake
CLI answers, calls `server.refreshProviders` with a config subscription open,
and compares the refresh's row and the `providerStatuses` event the
subscription receives.

Every scenario turns `enableProviderUpdateChecks` off, so neither server
looks up the latest CLI version or the model manifest on the network, and
the fakes never call a model. The fakes replay tests/fixtures/providers/
status_*.json through fake_cli.py; `--version` is answered by the wrapper.

Known differences are listed in provider_status_parity_xfail.json; an entry
that starts matching fails the run until it is removed.
"""
import argparse
import contextlib
import json
import os
from pathlib import Path
import secrets
import shlex
import shutil
import sys
import tempfile
import time

from websockets.sync.client import connect

from client_initial_sync_parity import rpc
from parity_harness import PROVIDER_CLIS, RELAY_TOOLS, encode, managed_server, path_without

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/fixtures/providers"
XFAIL = Path(__file__).with_name("provider_status_parity_xfail.json")
TIMES = {"checkedAt", "updatedAt", "lastCheckedAt"}

# How each fake answers `--version`.
VERSION = {
    "claude": {"ok": 'echo "2.1.300 (Claude Code)"; exit 0', "old": 'echo "2.1.200 (Claude Code)"; exit 0',
               "fails": 'echo "claude: cannot load config" >&2; exit 2', "hangs": "exec sleep 60"},
    "codex": {"ok": 'echo "codex-cli 0.170.0"; exit 0', "fails": 'echo "codex: broken" >&2; exit 2',
              "hangs": "exec sleep 60"},
}
# How the fake Codex answers `app-server`.
APP_SERVER = {
    "ok": 'exec python3 "$FAKE_PROVIDER_CLI" "$@"',
    "exits": 'echo "codex: unsupported config key" >&2; exit 3',
}


def claude_script(version):
    return f"""#!/bin/sh
case "$*" in
  --version*) {VERSION["claude"][version]};;
  *--output-format*stream-json*) exec python3 "$FAKE_PROVIDER_CLI" "$@";;
esac
exit 0
"""


def codex_script(version, app_server):
    return f"""#!/bin/sh
case "$1" in
  --version) {VERSION["codex"][version]};;
  app-server) {APP_SERVER[app_server]};;
esac
exit 0
"""


# name, instance to compare, fakes to install ({cli: {...}}), settings, and how long the
# reference may take (a timeout scenario waits out its deadline).
SCENARIOS = [
    {"name": "claude signed in", "instance": "claudeAgent",
     "fakes": {"claude": {"version": "ok", "fixture": "status_claude_signed_in.json"}}},
    {"name": "claude signed out", "instance": "claudeAgent",
     "fakes": {"claude": {"version": "ok", "fixture": "status_claude_signed_out.json"}}},
    {"name": "claude signed out with a config directory", "instance": "claudeAgent",
     "fakes": {"claude": {"version": "ok", "fixture": "status_claude_signed_out.json"}},
     "settings": {"providerInstances": {"claudeAgent": {"driver": "claudeAgent",
                                                        "config": {"homePath": "~/claude-work"}}}}},
    {"name": "claude too old for the newest models", "instance": "claudeAgent",
     "fakes": {"claude": {"version": "old", "fixture": "status_claude_signed_in.json"}}},
    {"name": "claude missing", "instance": "claudeAgent", "fakes": {}},
    {"name": "claude version fails", "instance": "claudeAgent",
     "fakes": {"claude": {"version": "fails", "fixture": "status_claude_signed_in.json"}}},
    {"name": "claude version times out", "instance": "claudeAgent", "wait": 45,
     "fakes": {"claude": {"version": "hangs", "fixture": "status_claude_signed_in.json"}}},
    {"name": "claude account check times out", "instance": "claudeAgent", "wait": 45,
     "fakes": {"claude": {"version": "ok", "fixture": "status_silent.json"}}},
    {"name": "claude disabled", "instance": "claudeAgent", "settled": True,
     "fakes": {"claude": {"version": "ok", "fixture": "status_claude_signed_in.json"}},
     "settings": {"providerInstances": {"claudeAgent": {"driver": "claudeAgent", "enabled": False}}}},
    {"name": "claude custom binaryPath", "instance": "claudeAgent",
     "fakes": {"claude": {"version": "ok", "fixture": "status_claude_signed_in.json", "at": "tools/claude-custom"}},
     "settings": {"providerInstances": {"claudeAgent": {"driver": "claudeAgent",
                                                        "config": {"binaryPath": "~/tools/claude-custom"}}}}},
    {"name": "claude custom binaryPath missing", "instance": "claudeAgent",
     "fakes": {"claude": {"version": "ok", "fixture": "status_claude_signed_in.json"}},
     "settings": {"providerInstances": {"claudeAgent": {"driver": "claudeAgent",
                                                        "config": {"binaryPath": "/nonexistent/claude"}}}}},
    {"name": "claude sign-in picked up by a refresh", "instance": "claudeAgent",
     "fakes": {"claude": {"version": "ok", "fixture": "status_claude_signed_out.json"}},
     "refresh": "status_claude_signed_in.json"},
    {"name": "codex signed in", "instance": "codex",
     "fakes": {"codex": {"version": "ok", "app": "ok", "fixture": "status_codex_signed_in.json"}}},
    {"name": "codex signed out", "instance": "codex",
     "fakes": {"codex": {"version": "ok", "app": "ok", "fixture": "status_codex_signed_out.json"}}},
    {"name": "codex signed out with a home", "instance": "codex",
     "fakes": {"codex": {"version": "ok", "app": "ok", "fixture": "status_codex_signed_out.json"}},
     "settings": {"providerInstances": {"codex": {"driver": "codex", "config": {"homePath": "~/codex-work"}}}}},
    {"name": "codex missing", "instance": "codex", "fakes": {}},
    {"name": "codex app-server exits", "instance": "codex",
     "fakes": {"codex": {"version": "ok", "app": "exits", "fixture": "status_codex_signed_in.json"}}},
    {"name": "codex app-server exits and version fails", "instance": "codex",
     "fakes": {"codex": {"version": "fails", "app": "exits", "fixture": "status_codex_signed_in.json"}}},
    {"name": "codex times out", "instance": "codex", "wait": 45,
     "fakes": {"codex": {"version": "ok", "app": "ok", "fixture": "status_silent.json"}}},
    {"name": "codex disabled", "instance": "codex", "settled": True,
     "fakes": {"codex": {"version": "ok", "app": "ok", "fixture": "status_codex_signed_in.json"}},
     "settings": {"providerInstances": {"codex": {"driver": "codex", "enabled": False}}}},
    {"name": "codex custom binaryPath", "instance": "codex",
     "fakes": {"codex": {"version": "ok", "app": "ok", "fixture": "status_codex_signed_in.json",
                         "at": "tools/codex-custom"}},
     "settings": {"providerInstances": {"codex": {"driver": "codex",
                                                  "config": {"binaryPath": "~/tools/codex-custom"}}}}},
    {"name": "codex custom binaryPath missing", "instance": "codex",
     "fakes": {"codex": {"version": "ok", "app": "ok", "fixture": "status_codex_signed_in.json"}},
     "settings": {"providerInstances": {"codex": {"driver": "codex",
                                                  "config": {"binaryPath": "/nonexistent/codex"}}}}},
    {"name": "codex sign-in picked up by a refresh", "instance": "codex",
     "fakes": {"codex": {"version": "ok", "app": "ok", "fixture": "status_codex_signed_out.json"}},
     "refresh": "status_codex_signed_in.json"},
]


def strip(value, root):
    """`value` without check times and with the temporary home masked."""
    if isinstance(value, dict):
        return {key: strip(item, root) for key, item in value.items() if key not in TIMES}
    if isinstance(value, list):
        return [strip(item, root) for item in value]
    if isinstance(value, str):
        for server in ("original", "rust"):
            value = value.replace(f"{root}/{server}", "<home>")
        return value.replace(str(root), "<tmp>")
    return value


def row_of(providers, instance):
    return next((p for p in providers if p.get("instanceId") == instance), None)


def wait_checked(url, cookie, instance, seconds, settled, root):
    """The instance's row once its first check has reported and the row has stopped changing
    (the reference publishes the probe, then the row with its version advisory), or after a few
    seconds for a scenario whose row is final from the start."""
    if settled:
        time.sleep(4)
        return row_of(rpc(url, cookie, "server.getConfig", {})["providers"], instance)
    deadline = time.monotonic() + seconds
    last, stable_since = None, None
    while True:
        row = row_of(rpc(url, cookie, "server.getConfig", {})["providers"], instance)
        checked = row is not None and "has not been checked" not in str(row.get("message"))
        if checked:
            if strip(row, root) == last:
                if time.monotonic() - stable_since >= 1.5:
                    return row
            else:
                last, stable_since = strip(row, root), time.monotonic()
        if time.monotonic() > deadline:
            return row
        time.sleep(0.25)


def config_socket(url, cookie):
    ws = connect(url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2",
                 additional_headers={"Cookie": cookie}, proxy=None, compression=None, max_size=None)
    ws.send(encode({"_tag": "Request", "id": "config", "tag": "subscribeServerConfig", "payload": {},
                    "headers": []}).decode())
    return ws


def next_provider_event(ws, instance, seconds=15):
    """The instance's row in the next `providerStatuses` event whose row is checked."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            frame = json.loads(ws.recv(timeout=max(0.1, deadline - time.monotonic())))
        except TimeoutError:
            break
        if frame.get("_tag") == "Ping":
            ws.send(encode({"_tag": "Pong"}).decode())
            continue
        if frame.get("_tag") != "Chunk":
            continue
        ws.send(encode({"_tag": "Ack", "requestId": "config"}).decode())
        for event in frame.get("values", []):
            if event.get("type") == "providerStatuses":
                row = row_of(event["payload"]["providers"], instance)
                if row is not None and row.get("auth", {}).get("status") == "authenticated":
                    return row
    return None


def run_server(args, command, root, scenario, temporary):
    """One server's outcome for `scenario`."""
    root.mkdir(parents=True)
    bin_dir = root / "fake-bin"
    bin_dir.mkdir()
    fixture = root / "fixture.json"
    log = root / "provider.log"
    for cli, fake in scenario["fakes"].items():
        target = root / fake["at"] if "at" in fake else bin_dir / cli
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(claude_script(fake["version"]) if cli == "claude"
                          else codex_script(fake["version"], fake["app"]))
        target.chmod(0o755)
        shutil.copyfile(FIXTURES / fake["fixture"], fixture)
    settings = {"enableProviderUpdateChecks": False, **scenario.get("settings", {})}
    (root / "userdata").mkdir(parents=True, exist_ok=True)
    (root / "userdata/settings.json").write_text(json.dumps(settings))
    env = {"PATH": os.pathsep.join([str(bin_dir), path_without(PROVIDER_CLIS | RELAY_TOOLS, temporary / "path")]),
           "FAKE_PROVIDER_CLI": str(FIXTURES / "fake_cli.py"), "FAKE_PROVIDER_LOG": str(log),
           "FAKE_PROVIDER_SCENARIO": str(fixture)}
    outcome = {}
    with contextlib.ExitStack() as stack:
        url, cookie = managed_server(stack, command, root, secrets.token_hex(16), secrets.token_bytes(32), env=env)
        outcome["row"] = strip(wait_checked(url, cookie, scenario["instance"], scenario.get("wait", 20),
                                            scenario.get("settled", False), temporary), temporary)
        if "refresh" in scenario:
            shutil.copyfile(FIXTURES / scenario["refresh"], fixture)
            ws = stack.enter_context(contextlib.closing(config_socket(url, cookie)))
            refreshed = rpc(url, cookie, "server.refreshProviders", {})
            outcome["refresh"] = strip(row_of(refreshed["providers"], scenario["instance"]), temporary)
            outcome["event"] = strip(next_provider_event(ws, scenario["instance"]), temporary)
    launches = Path(str(log) + ".args")
    # `codex app-server --help` is the reference's managed Codex runtime deciding whether a
    # local codex can stand in for the one it downloads, not a status check; this server does
    # not manage a Codex runtime.
    outcome["launches"] = sorted({line for line in launches.read_text().splitlines()
                                  if line != "app-server --help"}) if launches.exists() else []
    return outcome


def compare(outcomes):
    problems = {}
    original, rust = outcomes["original"], outcomes["rust"]
    for check in ("row", "refresh", "event"):
        if original.get(check) != rust.get(check):
            problems[check] = [f"original {json.dumps(original.get(check), sort_keys=True)}",
                               f"rust     {json.dumps(rust.get(check), sort_keys=True)}"]
    # The CLI sessions each server started for its checks (arguments only).
    if original["launches"] != rust["launches"]:
        problems["cli launches"] = [f"original {original['launches']}", f"rust     {rust['launches']}"]
    return problems


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--scenario", action="append", help="run only this scenario (repeatable)")
    parser.add_argument("--dump", type=Path, help="write each server's outcome here")
    args = parser.parse_args()
    xfail = json.loads(XFAIL.read_text()) if XFAIL.exists() else {}
    failures, unexpected_passes = [], []
    selected = [s for s in SCENARIOS if not args.scenario or s["name"] in args.scenario]
    for scenario in selected:
        with tempfile.TemporaryDirectory(prefix="t3-status-") as directory:
            temporary = Path(directory)
            outcomes = {}
            for name, command in (("original", shlex.split(args.original_command)),
                                  ("rust", shlex.split(args.rust_command))):
                outcomes[name] = run_server(args, command, temporary / name, scenario, temporary)
            if args.dump:
                args.dump.mkdir(parents=True, exist_ok=True)
                (args.dump / (scenario["name"].replace(" ", "-") + ".json")).write_text(
                    json.dumps(outcomes, indent=1, sort_keys=True))
            problems = compare(outcomes)
            expected = xfail.get(scenario["name"], {})
            for check, lines in problems.items():
                if check in expected:
                    print(f"XFAIL {scenario['name']}: {check}: {expected[check]}")
                    continue
                failures.append(scenario["name"])
                print(f"FAIL {scenario['name']}: {check}")
                for line in lines:
                    print(f"  {line[:4000]}")
            for check in expected:
                if check not in problems:
                    unexpected_passes.append(f"{scenario['name']}: {check}")
            if not problems:
                print(f"ok   {scenario['name']}")
    for entry in unexpected_passes:
        print(f"XPASS {entry} (remove it from {XFAIL.name})")
    if failures or unexpected_passes:
        sys.exit(1)
    print(f"provider status parity: {len(selected)} scenarios match")


if __name__ == "__main__":
    main()
