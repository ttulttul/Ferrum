#!/usr/bin/env python3
"""Compare HTTP responses and authenticated Effect RPC frames from two T3 servers."""
import argparse
import base64
import contextlib
import http.client as http_client
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import signal
import socket
import sys
import subprocess
import tempfile
import threading
import time
from urllib.parse import urlsplit

from parity_gaps import Gaps

ROOT = Path(__file__).resolve().parents[1]
TRACE = "00-0123456789abcdef0123456789abcdef-1234567890abcdef-01"


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


def http(base, method, path, body=None, headers=None, timeout=10):
    url = urlsplit(base)
    connection = (http_client.HTTPSConnection if url.scheme == "https" else http_client.HTTPConnection)(
        url.hostname, url.port, timeout=timeout
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
            # A request the reference accepts before it serves is never answered; probe again.
            if http(url, "GET", "/ws", timeout=0.5)[0] == 426:
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
    # parity-env: callers pass the server_env their server needs
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
# Agent CLIs a server may probe or start, under every name either server looks one up by. server_env
# hides them, so a local run sees a host without them, as CI does, and never starts a developer's
# real codex or claude. Fakes a suite writes into its temporary directories stay visible.
PROVIDER_CLIS = {"codex", "claude", "cursor-agent", "cursor", "opencode", "gemini", "grok", "kiro-cli", "droid",
                 "amp", "copilot", "qwen", "fx", "muse", "pi", "agy", "antigravity", "agy_acp_server.par"}


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
    """PATH with every directory that holds one of `names` replaced by links to its other entries.

    A program the suites wrote themselves (one whose real file is in the system temporary directory
    or the repository's fixtures, such as a fake CLI) stays visible; a link to a host program does
    not, even from a directory in the temporary directory."""
    directories = []
    for index, directory in enumerate((os.environ.get("PATH", "") if path is None else path).split(os.pathsep)):
        folder = Path(directory)
        if not folder.is_dir():
            continue
        hidden = host_programs(folder, names)
        if not hidden:
            directories.append(directory)
            continue
        shim = Path(scratch) / f"path-{index}"
        shim.mkdir(parents=True, exist_ok=True)
        for entry in folder.iterdir():
            if entry.name not in hidden and not (shim / entry.name).exists():
                # Servers started side by side may share `scratch`.
                with contextlib.suppress(FileExistsError):
                    (shim / entry.name).symlink_to(entry)
        directories.append(str(shim))
    return os.pathsep.join(directories)


def host_programs(folder, names):
    """Those of `names` in `folder` that are the host's own programs rather than the suites'."""
    return {name for name in names if (Path(folder) / name).exists() and not harness_owned(Path(folder) / name)}


def harness_owned(path):
    """Whether the real file of `path` is the suites' own: in the system temporary directory, where
    suites write their homes and fakes, or in the repository's test fixtures."""
    resolved = os.path.realpath(path)
    for owned in (os.path.realpath(tempfile.gettempdir()), str(ROOT / "tests" / "fixtures")):
        if resolved == owned or resolved.startswith(owned + os.sep):
            return True
    return False


# The login shell both servers read PATH from at start-up and on refresh (`$SHELL -ilc`). It never
# reads /etc/profile or the developer's own profile, which can put ~/.local/bin, nvm or Homebrew
# (and their real codex and claude) back on PATH: only a `.profile` a suite wrote into the server's
# temporary home. Named `bash` because the servers pick the capture syntax by the shell's name.
LOGIN_SHELL = """#!/bin/sh
# Written by the parity harness (scripts/parity_harness.py).
case "$1" in
  -[!-]*c*)
    exec {bash} --noprofile --norc -c '[ -r "$HOME/.profile" ] && . "$HOME/.profile"; eval "$1"' bash "$2" ;;
esac
exec {bash} --noprofile --norc "$@"
"""
_LOGIN_SHELL_DIR = None
_LOGIN_SHELL_LOCK = threading.Lock()


def login_shell():
    """The path of this process's parity login shell (LOGIN_SHELL), written on first use."""
    global _LOGIN_SHELL_DIR
    with _LOGIN_SHELL_LOCK:
        if _LOGIN_SHELL_DIR is None:
            import atexit
            import shutil
            bash = shutil.which("bash", path=os.defpath + os.pathsep + "/bin:/usr/bin")
            if not bash:
                raise RuntimeError("the parity login shell needs bash")
            directory = Path(tempfile.mkdtemp(prefix="t3-parity-shell-"))
            shell = directory / "bash"
            shell.write_text(LOGIN_SHELL.format(bash=shlex.quote(bash)))
            shell.chmod(0o755)
            atexit.register(shutil.rmtree, directory, ignore_errors=True)
            _LOGIN_SHELL_DIR = directory
        return str(_LOGIN_SHELL_DIR / "bash")


# Every server a suite starts sends its HTTP(S) through this loopback proxy, which forwards
# nothing: it records each target and refuses it, so no parity run reaches a real host. Loopback
# fakes are exempt (NO_PROXY). Both servers otherwise fetch the upstream model manifest from
# raw.githubusercontent.com at start-up and have no switch for it; refused, both keep the bundled
# copy, which is the same file (contracts/model-manifest.json is the reference's), so what the
# suites compare does not change.
REFUSED = []
_REFUSING_PROXY = None
_REFUSING_PROXY_LOCK = threading.Lock()


def refusing_proxy():
    """The URL of this process's refusing proxy (see REFUSED), started on first use."""
    global _REFUSING_PROXY
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Refuse(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def refuse(self):
            REFUSED.append(f"{self.command} {self.path}")
            self.send_response(403)
            self.send_header("Content-Length", "0")
            self.send_header("Connection", "close")
            self.end_headers()

        do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = do_HEAD = do_OPTIONS = do_CONNECT = refuse

    with _REFUSING_PROXY_LOCK:
        if _REFUSING_PROXY is None:
            server = ThreadingHTTPServer(("127.0.0.1", 0), Refuse)
            server.daemon_threads = True
            threading.Thread(target=server.serve_forever, daemon=True).start()
            _REFUSING_PROXY = f"http://127.0.0.1:{server.server_port}"
            import atexit
            atexit.register(report_refused)
        return _REFUSING_PROXY


def report_refused():
    """Names, once a suite ends, the off-loopback targets its servers tried and were refused."""
    targets = sorted({entry.split(" ", 1)[1] for entry in REFUSED})
    if targets:
        print(f"refused {len(REFUSED)} off-loopback request(s) from the servers: {', '.join(targets)}",
              file=sys.stderr)


# Every server environment server_env built, and whether its HTTP(S) proxy was on loopback.
OFFLINE_SERVERS = []


def assert_nothing_escaped():
    """Fails unless every server a suite started sent its HTTP(S) through a loopback proxy (the
    harness's refusing proxy, or a suite's own recorder): nothing then reaches a real host. What
    was refused is reported, not failed on, since a refused request never left the machine."""
    assert OFFLINE_SERVERS, "no server was started through server_env"
    off = [proxy for proxy in OFFLINE_SERVERS if urlsplit(proxy or "").hostname not in ("127.0.0.1", "localhost", "::1")]
    assert not off, f"a server ran without a loopback proxy: {off}"


def mask_port(text, port, mark="<port>"):
    """`text` with `port` masked where it is a port: after the `:` of a host or URL, and not
    followed by another digit. A plain replace would also mask the same digits inside a hash
    or an id, which changes from run to run."""
    return re.sub(rf"(?<=:){int(port)}(?!\d)", mark, text)


def offline_env():
    """Variables that keep a server off the network: no telemetry to ingest.t3.codes (the
    reference's T3CODE_TELEMETRY_ENABLED; the Rust server sends none), none from the host CLIs a
    server runs (gh, az), and HTTP(S) through the refusing proxy, honoured by Node
    (NODE_USE_ENV_PROXY), reqwest, git, npm, Chromium and the gh CLI. Loopback is exempt."""
    proxy = refusing_proxy()
    loopback = "127.0.0.1,localhost,.localhost,::1,[::1]"
    env = {"T3CODE_TELEMETRY_ENABLED": "false", "NODE_USE_ENV_PROXY": "1",
           # Host CLIs a server may run (gh, az) have telemetry and update checks of their own.
           "GH_TELEMETRY": "false", "DO_NOT_TRACK": "1", "GH_NO_UPDATE_NOTIFIER": "1",
           "AZURE_CORE_COLLECT_TELEMETRY": "false",
           # No session bus: gh would wait on a desktop keyring there (the reference turns the bus
           # off only where none exists, #23824), so both servers see gh fail fast as in CI.
           "DBUS_SESSION_BUS_ADDRESS": "disabled:"}
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
        env[name] = env[name.lower()] = proxy
    env["NO_PROXY"] = env["no_proxy"] = loopback
    return env


def split_env_prefix(command):
    """`command` without a leading `env NAME=value ...`, and those variables, so server_env sees them."""
    command = list(command)
    variables = {}
    if command and command[0] == "env":
        command.pop(0)
        while command and "=" in command[0] and not command[0].startswith("-"):
            name, value = command.pop(0).split("=", 1)
            variables[name] = value
    return command, variables


def server_env(home, env=None):
    """The whole environment for a T3 server (or one of its CLI commands) a suite starts in `home`.

    Every suite builds a server's environment here, so both servers get the same one and neither
    can find a real provider CLI: not on PATH (PROVIDER_CLIS are hidden unless a suite wrote them),
    not through the login shell (LOGIN_SHELL), and not under the developer's home. Neither sends
    telemetry or reaches a real host either (offline_env). `env` adds variables or replaces PATH;
    nothing else reaches the child, so no developer provider credential does either. scripts/check_parity_shards.py fails a suite that starts a
    server without it.
    """
    env = dict(env or {})
    if "SHELL" in env:
        raise ValueError("server_env sets SHELL to the parity login shell; write a .profile in the home instead")
    child = {k: os.environ[k] for k in ("LANG", "LC_ALL", "SYSTEMROOT") if k in os.environ}
    path = env.pop("PATH", os.environ.get("PATH", ""))
    # A suite may set its own proxy (a recorder of its own) or telemetry; otherwise neither server
    # reaches the network.
    child.update(offline_env())
    child.update(env)
    OFFLINE_SERVERS.append(child.get("HTTPS_PROXY"))
    child["PATH"] = path_without(PROVIDER_CLIS, Path(home) / ".parity-path", path)
    child.setdefault("HOME", str(home))
    child.setdefault("T3CODE_HOME", str(home))
    child["SHELL"] = login_shell()
    for directory in child["PATH"].split(os.pathsep):
        found = directory and host_programs(directory, PROVIDER_CLIS)
        if found:
            raise RuntimeError(f"a real provider CLI is still on the server PATH: {directory} has {sorted(found)}")
    return child


def managed_server(stack, prefix, root, secret, signing_key, env=None, stderr_path=None):
    """Launches one server in `root`. `env` adds variables (or replaces PATH) for the child; see
    server_env, which also takes the variables of a leading `env NAME=value` in `prefix`."""
    secrets_dir = root / "userdata" / "secrets"
    secrets_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
    key_file = secrets_dir / "mcp-oauth-signing-key.bin"
    if not key_file.exists():
        key_file.write_bytes(signing_key)
        key_file.chmod(0o600)
    prefix, variables = split_env_prefix(prefix)
    # The port is free when picked, but a slow-starting server leaves a window in which another
    # process (another suite on the same machine, or a server a suite started) can take it and
    # answer: then the bootstrap is refused, or our server exits for want of the port. Each such
    # attempt is stopped and the server relaunched on a fresh port.
    for attempt in range(3):
        port = free_port()
        url = f"http://127.0.0.1:{port}"
        envelope = dict(mode="desktop", noBrowser=True, host="127.0.0.1", port=port, t3Home=str(root),
                        desktopBootstrapToken=secret, tailscaleServeEnabled=False, tailscaleServePort=443)
        attempt_stack = contextlib.ExitStack()
        try:
            bootstrap = attempt_stack.enter_context(tempfile.TemporaryFile())
            bootstrap.write(encode(envelope) + b"\n")
            bootstrap.seek(0)
            # Each child receives its own state directory, home and bootstrap descriptor.
            stderr = None
            if stderr_path is not None:
                stderr_file = attempt_stack.enter_context(Path(stderr_path).open("w"))
                stderr = stderr_file
            attempt_stack.enter_context(launch(prefix + ["--bootstrap-fd", str(bootstrap.fileno())], url,
                                               env=server_env(root, {**variables, **(env or {}), "HOME": str(root),
                                                                     "T3CODE_HOME": str(root)}),
                                               cwd=root, pass_fds=(bootstrap.fileno(),), stderr=stderr))
            cookie = session_cookie(url, secret)
        except RuntimeError as error:
            attempt_stack.close()
            lost_port = "browser bootstrap failed" in str(error) or "exited before readiness" in str(error)
            if attempt == 2 or not lost_port:
                raise
            print(f"relaunching a server on a new port after: {error}", file=sys.stderr)
            continue
        stack.enter_context(attempt_stack)
        return url, cookie


_VOLATILE = [
    # A server's clock (server.probe, getConfig), read a few milliseconds apart on each server.
    (re.compile(r'("serverTime"\s*:\s*")[^"]*(")'), r"\1<time>\2"),
    # The terminal manager's random id, new each process.
    (re.compile(r'("managerId"\s*:\s*")[0-9a-f-]{36}(")'), r"\1<uuid>\2"),
]


def volatile_masked(value):
    """`value` (text, bytes or anything repr-able as JSON text) with per-process values masked."""
    if isinstance(value, bytes):
        value = value.decode("utf-8", "replace")
    if not isinstance(value, str):
        return value
    for pattern, replacement in _VOLATILE:
        value = pattern.sub(replacement, value)
    return value


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
        values = [volatile_masked(ws.recv(timeout=10)) for ws in sockets]
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
            # parity-env: git, not a T3 server
            subprocess.run(["git", "init", "-qb", "main", git_repo], check=True)
            git = ["git", "-C", str(git_repo), "-c", "user.email=test@example.test", "-c", "user.name=Parity"]
            # parity-env: git, not a T3 server
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
                # A launched server still gets a temporary home and never the host's provider CLIs.
                home = Path(stack.enter_context(tempfile.TemporaryDirectory()))
                stack.enter_context(launch(shlex.split(cmd) if cmd else None, url, env=server_env(home)))
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
        failures = differing = 0
        gaps = Gaps("parity_harness")
        for fixture in fixtures:
            passed, values = check(fixture, servers, sockets)
            if not passed:
                differing += 1
                failures += gaps.differs(fixture["name"], [f"original {values[0]}", f"rust     {values[1]}"])
            else:
                print(f"PASS {fixture['name']}")
        failures += gaps.finish(ran={fixture["name"] for fixture in fixtures})
        print(f"{len(fixtures) - differing}/{len(fixtures)} fixtures matched")
        return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
