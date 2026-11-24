#!/usr/bin/env python3
"""Compare SSH device hosts on the reference and Rust servers, against a fake `ssh` on loopback.

`scripts/fake_ssh.py` stands in for ssh. A host's remote commands run on this machine under the
host's own fake home, which holds a fake Android SDK, the pinned Node as the host's `node` and
`npm`, and the device tools the reference installs there. One host resolves to this machine, one
cannot be resolved, one refuses its host key, and one runs Windows. Each server gets its own
hosts and homes, seeded identically.

Each server is checked for:
- the hosts it lists once `deviceHosts` names them: the ones that are not this machine, unprobed;
- `device.testHost` for every kind of host, with the reference's probe and its failure messages;
- `device.list` with `inspectOnly`, which probes each host;
- removing a host from settings;
- every `ssh` it ran: its arguments, and the bootstrap script it sent (by digest), with this
  server's owner name on the host, checked against `ownerFor`.
Nothing may reach the harness's refusing proxy besides the startup fetches both servers make.
"""
import argparse
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import secrets
import shlex
import shutil
import stat
from tempfile import TemporaryDirectory
from time import monotonic, sleep

from client_initial_sync_parity import socket
from client_thread_parity import scenario_env
from device_tools_parity import FAKE_HUB_SOURCE, STARTUP_FETCHES, seed_fake_android_sdk, write_executable
from parity_harness import REFUSED, assert_nothing_escaped, encode, managed_server, recv_frame

HERE = Path(__file__).resolve().parent
HUB_VERSION = "0.26.1"


def call(url, cookie, tag, payload):
    """The whole exit of one RPC: its value, or the failure the server answered."""
    with socket(url, cookie) as ws:
        ws.send(encode({"_tag": "Request", "id": "1", "tag": tag, "payload": payload, "headers": []}).decode())
        while True:
            frame = json.loads(recv_frame(ws, timeout=60))
            if frame.get("_tag") == "Exit":
                return frame["exit"]


def seed_remote_home(home: Path, reference_root: Path, request_log: Path):
    """What a prepared device host has: Node, npm and ffmpeg on its login PATH, an Android SDK
    (the device tools suite's fakes), and the device hub the reference installs under
    ~/.t3/device/tools (the device tools suite's fake hub). The fakes log to `request_log`."""
    bin_dir = home / ".local" / "bin"
    bin_dir.mkdir(parents=True)
    for tool in ("node", "npm"):
        found = shutil.which(tool)
        if found is None:
            raise RuntimeError(f"{tool} is not on PATH")
        (bin_dir / tool).symlink_to(found)
    staging = home.parent / f"{home.name}-staging"
    sdk, fake_bin = seed_fake_android_sdk(staging, request_log)
    (home / "Android").mkdir()
    shutil.move(str(sdk), str(home / "Android" / "Sdk"))
    shutil.move(str(fake_bin / "ffmpeg"), str(bin_dir / "ffmpeg"))
    versions = json.loads((reference_root / "apps/server/src/device/deviceToolVersions.json").read_text())
    hub = home / ".t3" / "device" / "tools" / f"expo-device-hub@{versions['expo-device-hub']}"
    entry = hub / "node_modules" / "expo-device-hub" / "dist" / "server" / "cli.mjs"
    entry.parent.mkdir(parents=True)
    # The hub runs under the host's fresh SSH environment, so its log path is written into it.
    entry.write_text(FAKE_HUB_SOURCE.replace(
        "process.env.T3_FAKE_DEVICE_LOG", json.dumps(str(request_log))))
    (hub / ".install-complete").write_text(versions["expo-device-hub"])


def stop_remote_hubs(home: Path):
    """Hubs a bootstrap started run detached, as on a real host; end any a server left running."""
    for record in home.glob(".t3/device/hosts/*/hub.json"):
        try:
            os.kill(json.loads(record.read_text())["pid"], 15)
        except (OSError, ValueError, KeyError):
            pass


def owner_for(state_dir: Path, host_id: str) -> str:
    """The reference's `ownerFor`."""
    try:
        environment = (state_dir / "environment-id").read_text()
    except OSError:
        environment = str(state_dir)
    return hashlib.sha256(f"{environment}\0{state_dir}\0{host_id}".encode()).hexdigest()[:24]


def tunnels(log: Path):
    """This server's live `ssh -L` processes (fake_ssh.py, found by the log it writes to)."""
    found = []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            argv = (proc / "cmdline").read_bytes().split(b"\0")
            environ = (proc / "environ").read_bytes().split(b"\0")
        except OSError:
            continue
        if any(arg.endswith(b"fake_ssh.py") for arg in argv) and b"-L" in argv and \
                f"T3_FAKE_SSH_LOG={log}".encode() in environ:
            found.append(int(proc.name))
    return found


def wait_for_reconnect(log: Path, before: int, seconds=60):
    """Until the server has opened a new tunnel after `before` of them."""
    deadline = monotonic() + seconds
    while monotonic() < deadline:
        calls = [json.loads(line) for line in log.read_text().splitlines()]
        if sum(1 for c in calls if "-L" in c["args"]) > before and tunnels(log):
            sleep(1)
            return True
        sleep(0.2)
    return False


def drop_volatile(value):
    """Session timestamps differ between runs; keep that they are there."""
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "openedAt" and isinstance(item, str):
                value[key] = "<time>"
            else:
                drop_volatile(item)
    elif isinstance(value, list):
        for item in value:
            drop_volatile(item)


def wait_hosts(url, cookie, ids, seconds=30):
    """The device state once it lists `ids` and has settled: a host change publishes more than
    once (removing a host publishes again after the host has stopped)."""
    deadline = monotonic() + seconds
    previous = None
    while True:
        state = call(url, cookie, "device.list", {})
        listed = [host["id"] for host in state.get("value", {}).get("hosts", [])]
        if (listed == ids and state == previous) or monotonic() > deadline:
            return state
        previous = state if listed == ids else None
        sleep(0.5)


HOSTS = [
    {"id": "linux", "label": "Linux box", "target": "linux-box"},
    {"id": "myself", "label": "This one", "target": "self-host"},
    {"id": "gone", "label": "Gone", "kind": "ssh", "target": "gone.invalid", "port": 2222},
]


def run_server(name, command, reference_root, directory):
    root = Path(directory, name)
    root.mkdir()
    home = root / "remote-linux"
    request_log = root / "remote-requests.jsonl"
    seed_remote_home(home, reference_root, request_log)
    fake_bin = root / "fake-ssh-bin"
    write_executable(fake_bin / "ssh", f"#!/bin/sh\nexec python3 {shlex.quote(str(HERE / 'fake_ssh.py'))} \"$@\"\n")
    hosts_file = root / "ssh-hosts.json"
    hosts_file.write_text(json.dumps({
        "linux-box": {"home": str(home)},
        "self-host": {"hostname": "127.0.0.1"},
        "gone.invalid": {"fail": "resolve"},
        "locked": {"fail": "host_key"},
        "win-box": {"windows": True},
    }))
    log = root / "ssh.jsonl"
    env, _, _ = scenario_env(root, "ssh", root / "server", {"name": "ssh", "scenario": None}, set())
    env["PATH"] = f"{fake_bin}{os.pathsep}{env['PATH']}"
    env["T3_FAKE_SSH_HOSTS"] = str(hosts_file)
    env["T3_FAKE_SSH_LOG"] = str(log)
    server_root = root / "server"
    server_root.mkdir()
    answers = []
    with ExitStack() as stack:
        url, cookie = managed_server(stack, shlex.split(command), server_root, secrets.token_urlsafe(32),
                                     secrets.token_bytes(32), env=env)
        call(url, cookie, "server.updateSettings", {"patch": {"deviceHosts": HOSTS}})
        answers.append(("hosts after settings", wait_hosts(url, cookie, ["local", "linux", "gone"])))
        for label, payload in [
            ("testHost remote", {"id": "t1", "label": "Linux box", "target": "linux-box"}),
            ("testHost this machine", {"id": "t2", "label": "Me", "target": "self-host"}),
            ("testHost unresolvable", {"id": "t3", "label": "Gone", "target": "gone.invalid"}),
            ("testHost host key", {"id": "t4", "label": "Locked", "target": "locked", "port": 2222,
                                   "identityFile": "/nonexistent/id_fake"}),
            ("testHost windows", {"id": "t5", "label": "Win", "target": "win-box"}),
        ]:
            answers.append((label, call(url, cookie, "device.testHost", payload)))
        answers.append(("inspect", call(url, cookie, "device.list", {"inspectOnly": True})))
        call(url, cookie, "server.updateSettings", {"patch": {"deviceHosts": HOSTS[:1]}})
        answers.append(("hosts after removal", wait_hosts(url, cookie, ["local", "linux"])))
        # Starting the host: its hub over the bootstrap and an ssh -L tunnel, then its devices.
        answers.append(("enable (starts the host)", call(url, cookie, "device.configure", {"enabled": True})))
        for label, payload in [
            ("open booted emulator", {"threadId": "thread-ssh", "hostId": "linux", "deviceId": "emulator-5554",
                                      "platform": "android"}),
            ("open unbooted AVD", {"threadId": "thread-ssh", "hostId": "linux", "deviceId": "Pixel_Unbooted",
                                   "platform": "android"}),
            ("open missing device", {"threadId": "thread-ssh", "hostId": "linux", "deviceId": "nope",
                                     "platform": "android"}),
        ]:
            answers.append((label, call(url, cookie, "device.open", payload)))
        answers.append(("close with shutdown", call(url, cookie, "device.close", {
            "threadId": "thread-ssh", "hostId": "linux", "deviceId": "emulator-5556", "shutdown": True})))
        answers.append(("list", call(url, cookie, "device.list", {})))
        # A dropped connection: the host reconnects on its own.
        opened = sum(1 for line in log.read_text().splitlines() if '"-L"' in line)
        for pid in tunnels(log):
            os.kill(pid, 15)
        answers.append(("reconnected", {"reconnected": wait_for_reconnect(log, opened)}))
        answers.append(("list after reconnect", call(url, cookie, "device.list", {})))
        answers.append(("retry host", call(url, cookie, "device.list", {"retryHostId": "linux"})))
        answers.append(("disable (stops the host)", call(url, cookie, "device.configure", {"enabled": False})))
    stop_remote_hubs(home)
    requests = [json.loads(line) for line in request_log.read_text().splitlines()] if request_log.exists() else []
    calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
    state_dir = server_root / "userdata"
    owners = {}
    for entry in calls:
        if "owner" in entry:
            owners.setdefault(entry["owner"], []).append(entry.get("mode"))
    text = json.dumps({"answers": answers, "calls": calls, "requests": requests})
    for owner in owners:
        text = text.replace(owner, "<owner>")
    text = text.replace(str(root), "<root>")
    result = json.loads(text)
    # ssh runs concurrently (probes of several hosts), so the calls are compared as a multiset.
    # `ssh -G` (is this host this machine?) only reads ssh's configuration, and the reference runs
    # it again for each echo of a settings change, so those are compared as a set.
    lookups = {json.dumps(entry, sort_keys=True) for entry in result["calls"] if "-G" in entry["args"]}
    result["calls"] = sorted(
        [json.loads(entry) for entry in lookups]
        + [entry for entry in result["calls"] if "-G" not in entry["args"]],
        key=lambda entry: json.dumps(entry, sort_keys=True))
    expected = {owner_for(state_dir, host["id"]) for host in HOSTS + [{"id": f"t{i}"} for i in range(1, 6)]}
    for entry in result["answers"]:
        drop_volatile(entry[1])
    result["owner check"] = sorted("ownerFor" if owner in expected else f"unexpected owner {owner}" for owner in owners)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--reference-root", type=Path, required=True)
    parser.add_argument("--dump", type=Path, help="write both servers' results as JSON")
    args = parser.parse_args()
    results = {}
    with TemporaryDirectory() as directory:
        for name, command in (("original", args.original_command), ("rust", args.rust_command)):
            results[name] = run_server(name, command, args.reference_root, directory)
    assert_nothing_escaped()
    refused = sorted({entry.split(" ", 1)[1] for entry in REFUSED} - STARTUP_FETCHES)
    if args.dump:
        args.dump.write_text(json.dumps(results, indent=1, sort_keys=True))
    if refused:
        print(f"FAIL requests besides startup fetches reached the refusing proxy: {refused}")
        return 1
    failures, total = 0, 0
    original, rust = results["original"], results["rust"]
    for (label, left), (_, right) in zip(original["answers"], rust["answers"], strict=True):
        total += 1
        same = left == right
        failures += not same
        print(f"{'PASS' if same else 'FAIL'} {label}")
        if not same:
            print(f"  original {json.dumps(left)[:3000]}\n  rust     {json.dumps(right)[:3000]}")
    for key in ("calls", "requests", "owner check"):
        total += 1
        same = original[key] == rust[key]
        failures += not same
        print(f"{'PASS' if same else 'FAIL'} ssh {key} ({len(original[key])} vs {len(rust[key])})")
        if not same:
            print(f"  original {json.dumps(original[key], indent=1)[:6000]}\n  rust     {json.dumps(rust[key], indent=1)[:6000]}")
    print(f"{total - failures}/{total} SSH device host checks matched across the reference and the Rust server")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
