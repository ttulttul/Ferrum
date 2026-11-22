#!/usr/bin/env python3
"""Compare HTTP responses and authenticated Effect RPC frames from two T3 servers."""
import argparse
import base64
import contextlib
import http.client as http_client
import json
import os
from pathlib import Path
import secrets
import shlex
import signal
import socket
import subprocess
import tempfile
import threading
import time
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
TRACE = "00-0123456789abcdef0123456789abcdef-1234567890abcdef-01"


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


def http(base, method, path, body=None, headers=None):
    url = urlsplit(base)
    connection = (http_client.HTTPSConnection if url.scheme == "https" else http_client.HTTPConnection)(
        url.hostname, url.port, timeout=10
    )
    try:
        connection.request(method, url.path.rstrip("/") + path, body, headers or {})
        response = connection.getresponse()
        return response.status, response.headers, response.read()
    finally:
        connection.close()


def ready(url, process=None):
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        if process and process.poll() is not None:
            raise RuntimeError(f"server exited before readiness ({process.returncode})")
        try:
            if http(url, "GET", "/ws")[0] == 426:
                return
        except (OSError, http_client.HTTPException):
            pass
        time.sleep(.1)
    raise RuntimeError(f"{url} did not answer protocol negotiation")


@contextlib.contextmanager
def launch(command, url, **kwargs):
    if not command:
        ready(url)
        yield
        return
    process = subprocess.Popen(command, start_new_session=True, stdout=subprocess.DEVNULL, **kwargs)
    try:
        ready(url, process)
        yield
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
        # Provider discovery can leave children running after the server exits.
        # Reap this test's process group before disposing of its temporary home.
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)


_HANDED_OUT = set()
_HANDED_OUT_LOCK = threading.Lock()


def free_port():
    """A port nothing listens on, never the same one twice in this process, so servers that
    suites start side by side (run_concurrently) do not race for one."""
    while True:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        with _HANDED_OUT_LOCK:
            if port not in _HANDED_OUT:
                _HANDED_OUT.add(port)
                return port


def run_concurrently(tasks, jobs):
    """Call each of `tasks` (functions of no arguments) with up to `jobs` running at once, and
    return their results in order; the first exception raised is raised here once all are done.
    Every server and temporary home a task starts must be its own."""
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max(1, jobs)) as pool:
        futures = [pool.submit(task) for task in tasks]
    return [future.result() for future in futures]


def session_cookie(url, secret):
    status, headers, _ = http(url, "POST", "/api/auth/browser-session",
                              encode({"credential": secret}), {"Content-Type": "application/json"})
    if status != 200 or not headers.get("set-cookie"):
        raise RuntimeError(f"browser bootstrap failed on {url} (HTTP {status})")
    return headers["set-cookie"].split(";", 1)[0]


# The relay client a server would report from PATH (`cloud.getRelayClientStatus`).
RELAY_TOOLS = {"cloudflared"}
# Agent CLIs a server may probe or start. managed_server hides them, so a local run sees a host
# without them, as CI does, and never starts a developer's real codex or claude.
PROVIDER_CLIS = {"codex", "claude", "cursor-agent", "opencode", "gemini", "grok", "kiro-cli", "droid", "amp",
                 "copilot", "qwen"}


def isolated_git_env(directory):
    """Git settings for parity processes: no system config, and a global config this run owns.

    Host settings such as init.defaultBranch, url.*.insteadOf, commit signing or a newer system
    default would otherwise reach the seeded fixtures and both servers.
    """
    config = Path(directory) / "gitconfig"
    if not config.exists():
        config.parent.mkdir(parents=True, exist_ok=True)
        config.write_text("# Written by the parity harness; host Git configuration is not read.\n")
    return {"GIT_CONFIG_GLOBAL": str(config), "GIT_CONFIG_NOSYSTEM": "1"}


def path_without(names, scratch, path=None):
    """PATH with every directory that holds one of `names` replaced by links to its other entries."""
    directories = []
    for index, directory in enumerate((os.environ.get("PATH", "") if path is None else path).split(os.pathsep)):
        folder = Path(directory)
        if not folder.is_dir():
            continue
        if not any((folder / name).exists() for name in names):
            directories.append(directory)
            continue
        shim = Path(scratch) / f"path-{index}"
        shim.mkdir(parents=True, exist_ok=True)
        for entry in folder.iterdir():
            if entry.name not in names and not (shim / entry.name).exists():
                # Servers started side by side may share `scratch`.
                with contextlib.suppress(FileExistsError):
                    (shim / entry.name).symlink_to(entry)
        directories.append(str(shim))
    return os.pathsep.join(directories)


def managed_server(stack, prefix, root, secret, signing_key, env=None):
    """Launches one server in `root`. `env` adds variables (or replaces PATH) for the child.

    Without a PATH in `env`, the child's PATH hides the provider CLIs (PROVIDER_CLIS)."""
    secrets_dir = root / "userdata" / "secrets"
    secrets_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
    key_file = secrets_dir / "mcp-oauth-signing-key.bin"
    if not key_file.exists():
        key_file.write_bytes(signing_key)
        key_file.chmod(0o600)
    port = free_port()
    url = f"http://127.0.0.1:{port}"
    envelope = dict(mode="desktop", noBrowser=True, host="127.0.0.1", port=port, t3Home=str(root),
                    desktopBootstrapToken=secret, tailscaleServeEnabled=False, tailscaleServePort=443)
    bootstrap = stack.enter_context(tempfile.TemporaryFile())
    bootstrap.write(encode(envelope) + b"\n")
    bootstrap.seek(0)
    # Each child receives its own state directory, home and bootstrap descriptor.
    # Do not pass developer provider credentials into parity processes.
    child_env = {k: os.environ[k] for k in ("PATH", "LANG", "LC_ALL", "SYSTEMROOT") if k in os.environ}
    if "PATH" not in (env or {}):
        child_env["PATH"] = path_without(PROVIDER_CLIS, root / ".parity-path")
    stack.enter_context(launch(prefix + ["--bootstrap-fd", str(bootstrap.fileno())], url,
                               env={**child_env, **(env or {}), "HOME": str(root), "T3CODE_HOME": str(root)},
                               cwd=root, pass_fds=(bootstrap.fileno(),)))
    return url, session_cookie(url, secret)


def check(fixture, servers, sockets):
    if "frame" in fixture:
        if "setupFrame" in fixture:
            setup = encode(fixture["setupFrame"]).decode()
            for ws in sockets:
                ws.send(setup)
                reply = json.loads(ws.recv(timeout=10))
                if reply.get("exit", {}).get("_tag") != "Success":
                    raise RuntimeError(f"{fixture['name']} setup failed: {reply!r}")
        frame = encode(fixture["frame"]).decode()
        for ws in sockets:
            ws.send(frame)
        values = [ws.recv(timeout=10) for ws in sockets]
        return values[0] == values[1], [repr(v)[:1600] for v in values]
    responses = []
    for url, cookie in servers:
        headers = {"traceparent": TRACE, **fixture.get("headers", {})}
        if fixture.get("authenticated"):
            headers["Cookie"] = cookie
        body = fixture.get("body")
        if body is not None:
            headers["content-type"] = "application/json"
            body = encode(body)
        if "bodyBase64" in fixture:
            body = base64.b64decode(fixture["bodyBase64"], validate=True)
            headers.setdefault("content-type", "application/octet-stream")
        status, actual, data = http(url, fixture["method"], fixture["path"], body, headers)
        responses.append((status, [(k, actual.get(k)) for k in fixture.get("compareHeaders", [])], data))
    return responses[0] == responses[1], [repr(v)[:1600] for v in responses]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-url", default="http://127.0.0.1:3773")
    parser.add_argument("--rust-url", default="http://127.0.0.1:43773")
    parser.add_argument("--original-command")
    parser.add_argument("--rust-command")
    parser.add_argument("--managed", action="store_true", help="launch each command in an isolated desktop environment")
    parser.add_argument("--fixtures", type=Path)
    parser.add_argument("--check-required-fields", action="store_true",
                        help="also compare missing-field validation for registered RPCs and orchestration commands")
    args = parser.parse_args()
    fixtures = json.loads((args.fixtures or ROOT / "tests/parity-fixtures.json").read_text())
    if args.managed and not args.fixtures:
        for name in ("provider-parity.json", "github-voice-parity.json", "host-parity.json"):
            fixtures.extend(json.loads((ROOT / "tests" / name).read_text()))
    if args.check_required_fields:
        for rpc in json.loads((ROOT / "contracts/rpc.json").read_text()):
            if rpc.get("required"):
                fixtures.append({"name": rpc["tag"] + " missing fields", "frame": {
                    "_tag": "Request", "id": "validation", "tag": rpc["tag"], "payload": {}, "headers": []}})
        for tag in json.loads((ROOT / "contracts/commands.json").read_text()):
            fixtures.append({"name": tag + " missing command fields", "frame": {
                "_tag": "Request", "id": "validation", "tag": "orchestration.dispatchCommand",
                "payload": {"type": tag}, "headers": []}})
    with contextlib.ExitStack() as stack:
        if args.managed:
            if not args.original_command or not args.rust_command:
                parser.error("--managed requires both launch commands (use absolute executable paths)")
            root = Path(stack.enter_context(tempfile.TemporaryDirectory()))
            secret = secrets.token_urlsafe(32)
            signing_key = secrets.token_bytes(32)
            git_repo = root / "repo"
            subprocess.run(["git", "init", "-qb", "main", git_repo], check=True)
            git = ["git", "-C", str(git_repo), "-c", "user.email=test@example.test", "-c", "user.name=Parity"]
            subprocess.run(git + ["commit", "--allow-empty", "-qm", "Fixture"], check=True)
            (git_repo / "untracked.txt").write_text("test data")
            for fixture in fixtures:
                if "frame" in fixture and fixture["frame"].get("payload", {}).get("cwd") == "$PARITY_REPO":
                    fixture["frame"]["payload"]["cwd"] = str(git_repo)
            servers = [managed_server(stack, shlex.split(cmd), root / name, secret, signing_key)
                       for name, cmd in [("original", args.original_command), ("rust", args.rust_command)]]
        else:
            servers = []
            for url, cmd, credential_var in [(args.original_url, args.original_command, "T3_PARITY_ORIGINAL_BOOTSTRAP"),
                                             (args.rust_url, args.rust_command, "T3_PARITY_RUST_BOOTSTRAP")]:
                stack.enter_context(launch(shlex.split(cmd) if cmd else None, url))
                secret = os.environ.get(credential_var)
                servers.append((url, session_cookie(url, secret) if secret else None))
        sockets = []
        if any("frame" in f for f in fixtures):
            from websockets.sync.client import connect
            for url, cookie in servers:
                if not cookie:
                    parser.error("RPC fixtures require managed servers or both parity bootstrap environment variables")
                sockets.append(stack.enter_context(connect(url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2",
                                additional_headers={"Cookie": cookie}, proxy=None, compression=None)))
        failures = 0
        for fixture in fixtures:
            passed, values = check(fixture, servers, sockets)
            if not passed:
                failures += 1
                print(f"FAIL {fixture['name']}\n  original {values[0]}\n  rust     {values[1]}")
            else:
                print(f"PASS {fixture['name']}")
        print(f"{len(fixtures) - failures}/{len(fixtures)} fixtures matched")
        return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
