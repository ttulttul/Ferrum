#!/usr/bin/env python3
"""Compare how two T3 servers run the T3 Connect relay client (cloudflared).

Each scenario starts a server on a scratch home seeded with a fake managed tunnel
(`userdata/secrets/cloud-endpoint-runtime-config.bin` and, when confirmed, the
`cloud-endpoint-confirmed-origin.bin` marker for the server's port) and points
`T3CODE_CLOUDFLARED_PATH` at `scripts/fake_cloudflared.py`, which never contacts
Cloudflare. It compares, with secrets and pids replaced by placeholders:

- whether a connector starts, and the argv, environment changes, stdin and process
  group it starts with;
- `userdata/logs/relay-client.ndjson`, entry by entry;
- `cloud.getRelayClientStatus`;
- restarts after the connector exits, and that stopping the server (SIGTERM to the
  server alone) stops the connector.

No relay URL or credential is seeded, so neither server calls T3 Connect.
"""
import argparse
import contextlib
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time

from websockets.sync.client import connect

from parity_harness import encode, free_port, ready, session_cookie

FAKE = Path(__file__).resolve().parent / "fake_cloudflared.py"
# Shaped like a connector token, but never valid anywhere.
FAKE_TOKEN = "eyJhIjoiZmFrZS1hY2NvdW50IiwidCI6ImZha2UtdHVubmVsIiwicyI6ImZha2Utc2VjcmV0In0"
TUNNEL_ID = "00000000-0000-4000-8000-00000000c0de"
TUNNEL_NAME = "t3coderelay-managedendpoint-parity-0000000000000000"
PUBLIC_CONFIG = {
    "T3CODE_RELAY_URL": "https://relay.invalid",
    "T3CODE_CLERK_PUBLISHABLE_KEY": "pk_test_Y2xlcmsuaW52YWxpZCQ",
    "T3CODE_CLERK_CLI_OAUTH_CLIENT_ID": "parity-fake-client",
}
RUNTIME_CONFIG = {
    "providerKind": "cloudflare_tunnel",
    "connectorToken": FAKE_TOKEN,
    "tunnelId": TUNNEL_ID,
    "tunnelName": TUNNEL_NAME,
}


def write_secret(root, name, value):
    directory = root / "userdata" / "secrets"
    directory.mkdir(parents=True, mode=0o700, exist_ok=True)
    path = directory / f"{name}.bin"
    path.write_bytes(value if isinstance(value, bytes) else value.encode())
    path.chmod(0o600)


def seed(root, port, scenario):
    if scenario == "not-linked":
        return
    write_secret(root, "cloud-endpoint-runtime-config", json.dumps(RUNTIME_CONFIG, separators=(",", ":")))
    if scenario in ("confirmed", "restart", "publish-only"):
        marker = {"config": RUNTIME_CONFIG, "origin": {"localHttpHost": "127.0.0.1", "localHttpPort": port}}
        write_secret(root, "cloud-endpoint-confirmed-origin", json.dumps(marker, separators=(",", ":")))
    if scenario == "other-port":
        marker = {"config": RUNTIME_CONFIG, "origin": {"localHttpHost": "127.0.0.1", "localHttpPort": port + 1}}
        write_secret(root, "cloud-endpoint-confirmed-origin", json.dumps(marker, separators=(",", ":")))
    if scenario == "publish-only":
        write_secret(root, "cloud-cli-desired-link", "publish_only")


@contextlib.contextmanager
def server(prefix, root, port, secret, signing_key, env):
    secrets_dir = root / "userdata" / "secrets"
    secrets_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
    (secrets_dir / "mcp-oauth-signing-key.bin").write_bytes(signing_key)
    url = f"http://127.0.0.1:{port}"
    envelope = dict(mode="desktop", noBrowser=True, host="127.0.0.1", port=port, t3Home=str(root),
                    desktopBootstrapToken=secret, tailscaleServeEnabled=False, tailscaleServePort=443)
    with tempfile.TemporaryFile() as bootstrap:
        bootstrap.write(encode(envelope) + b"\n")
        bootstrap.seek(0)
        base_env = {k: os.environ[k] for k in ("PATH", "LANG", "LC_ALL") if k in os.environ}
        process = subprocess.Popen(
            prefix + ["--bootstrap-fd", str(bootstrap.fileno())],
            start_new_session=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=root,
            pass_fds=(bootstrap.fileno(),),
            env={**base_env, **env, "HOME": str(root), "T3CODE_HOME": str(root)},
        )
        try:
            ready(url, process)
            yield process, url
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            # Anything the server left in its group (a connector included) belongs to this run.
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)


def rpc(url, cookie, tag, payload=None):
    with connect(url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2",
                 additional_headers={"Cookie": cookie}, proxy=None, compression=None) as socket:
        socket.send(encode({"_tag": "Request", "id": "1", "tag": tag, "payload": payload or {}, "headers": []}).decode())
        return json.loads(socket.recv(timeout=20))


def read_jsonl(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def wait_for(predicate, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.1)
    return predicate()


def alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    try:
        with open(f"/proc/{pid}/stat") as handle:
            return handle.read().rsplit(")", 1)[1].split()[0] != "Z"
    except OSError:
        return False


# Names the reference's own modules set on process.env, so every child it spawns inherits
# them; they are not part of the relay client's spawn (see tests/relay-client-spawn.json).
REFERENCE_PROCESS_ENV = {"CLAUDE_AGENT_SDK_VERSION", "NoDefaultCurrentDirectoryInExePath"}


def normalize_start(entry, root):
    entry = dict(entry)
    entry.pop("pid", None)
    entry["envAdded"] = [name for name in entry.get("envAdded", []) if name not in REFERENCE_PROCESS_ENV]
    if entry.get("tunnelToken") == FAKE_TOKEN:
        entry["tunnelToken"] = "<token>"
    entry["cwd"] = entry.get("cwd", "").replace(str(root), "<root>")
    return entry


OUTPUT_NOISE = [
    (re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z "), "<time> "),
    (re.compile(r"connection=[0-9a-f-]{36}"), "connection=<uuid>"),
    (re.compile(r"127\.0\.0\.1:\d+/metrics"), "127.0.0.1:<port>/metrics"),
]


def normalize_log(entry):
    message = entry.get("message")
    shape = {
        "level": entry.get("level"),
        "keys": list(entry.keys()),
        "annotations": entry.get("annotations"),
        "spans": entry.get("spans"),
        "timestamp": bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", entry.get("timestamp", ""))),
        "fiberId": bool(re.fullmatch(r"#\d+", entry.get("fiberId", ""))),
    }
    if isinstance(message, list) and message:
        shape["text"] = message[0]
        attributes = dict(message[1]) if len(message) > 1 and isinstance(message[1], dict) else {}
        if "pid" in attributes:
            attributes["pid"] = "<pid>"
        if isinstance(attributes.get("output"), str):
            output = attributes["output"]
            for pattern, replacement in OUTPUT_NOISE:
                output = pattern.sub(replacement, output)
            attributes["output"] = output.replace(FAKE_TOKEN, "<token>")
        if "cause" in attributes:
            attributes["cause"] = "<cause>"
        if "uptimeMillis" in attributes:
            attributes["uptimeMillis"] = "<millis>"
        shape["attributes"] = attributes
    else:
        shape["text"] = message
    return shape


def run_scenario(name, prefix, root, credentials):
    port = free_port()
    seed(root, port, name)
    binary = root / "tools-bin" / "cloudflared"
    binary.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(FAKE, binary)
    binary.chmod(0o755)
    record = binary.parent / "fake-cloudflared.jsonl"
    env = {"T3CODE_CLOUDFLARED_PATH": str(binary)}
    if name == "publish-only":
        # A desired link is read only when the build has T3 Connect's public config, as the
        # published build does; these hosts can never resolve, so nothing reaches T3 Connect.
        env.update(PUBLIC_CONFIG)
    if name == "restart":
        (binary.parent / "fake-cloudflared-options.json").write_text(json.dumps({"exitAfter": 1, "exitTimes": 2}))
    log_path = root / "userdata" / "logs" / "relay-client.ndjson"
    result = {}
    with server(prefix, root, port, *credentials, env) as (process, url):
        cookie = session_cookie(url, credentials[0])
        result["getRelayClientStatus"] = rpc(url, cookie, "cloud.getRelayClientStatus")
        if name in ("confirmed", "restart"):
            expected = 3 if name == "restart" else 1
            wait_for(lambda: len([e for e in read_jsonl(record) if e["event"] == "start"]) >= expected, 30)
            wait_for(lambda: sum("registered" in json.dumps(e["message"]) for e in read_jsonl(log_path)) >= expected, 15)
        else:
            time.sleep(4)
        starts = [e for e in read_jsonl(record) if e["event"] == "start"]
        running = [e["pid"] for e in starts if alive(e["pid"])]
        result["startsBeforeShutdown"] = len(starts)
        result["runningBeforeShutdown"] = len(running)
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=20)
            result["serverExited"] = True
        except subprocess.TimeoutExpired:
            result["serverExited"] = False
        result["connectorsLeftRunning"] = sum(alive(pid) for pid in running)
    events = read_jsonl(record)
    result["starts"] = [normalize_start(e, root) for e in events if e["event"] == "start"]
    result["signals"] = [e["signal"] for e in events if e["event"] == "signal"]
    result["exits"] = [e["code"] for e in events if e["event"] == "exit"]
    result["relayClientLog"] = [normalize_log(e) for e in read_jsonl(log_path)]
    result["status"] = result.pop("getRelayClientStatus")
    if isinstance(result["status"].get("exit"), dict):
        value = result["status"]["exit"].get("value")
        if isinstance(value, dict) and "executablePath" in value:
            value["executablePath"] = value["executablePath"].replace(str(root), "<root>")
    return result


SCENARIOS = ["not-linked", "confirmed", "restart", "unconfirmed", "other-port", "publish-only"]


def diff(left, right, path=""):
    if type(left) is not type(right):
        return [f"{path}: {json.dumps(left)} != {json.dumps(right)}"]
    if isinstance(left, dict):
        out = []
        for key in list(dict.fromkeys([*left, *right])):
            if key not in left or key not in right:
                out.append(f"{path}.{key}: {'missing in original' if key not in left else 'missing in rust'}")
            else:
                out.extend(diff(left[key], right[key], f"{path}.{key}"))
        return out
    if isinstance(left, list):
        out = [f"{path}: length {len(left)} != {len(right)}"] if len(left) != len(right) else []
        for index, (a, b) in enumerate(zip(left, right)):
            out.extend(diff(a, b, f"{path}[{index}]"))
        return out
    return [] if left == right else [f"{path}: {json.dumps(left)} != {json.dumps(right)}"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--scenario", action="append", choices=SCENARIOS)
    parser.add_argument("--dump", help="write both results as JSON to this path")
    args = parser.parse_args()
    if not sys.platform.startswith("linux"):
        print("relay client parity reads /proc and runs on Linux only")
        return 0
    credentials = secrets.token_urlsafe(32), secrets.token_bytes(32)
    failures = 0
    dump = {}
    for name in args.scenario or SCENARIOS:
        results = []
        for label, command in (("original", args.original_command), ("rust", args.rust_command)):
            with tempfile.TemporaryDirectory(prefix=f"relay-{label}-") as temporary:
                results.append(run_scenario(name, shlex.split(command), Path(temporary), credentials))
        dump[name] = {"original": results[0], "rust": results[1]}
        differences = diff(*results)
        if differences:
            failures += 1
            print(f"FAIL {name}")
            for line in differences:
                print(f"  {line}")
        else:
            print(f"PASS {name} ({results[0]['startsBeforeShutdown']} connector starts)")
    if args.dump:
        Path(args.dump).write_text(json.dumps(dump, indent=2, sort_keys=True))
    print(f"{len(dump) - failures}/{len(dump)} relay client scenarios matched")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
