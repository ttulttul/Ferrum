#!/usr/bin/env python3
"""Drive both servers through the pinned reference client and compare every call.

Each server runs on 127.0.0.1 in its own temporary home, seeded with the same
fixture: a git checkout with a bare `origin`, an ignored build directory, an
untracked and a modified file, and a plain directory. client_rpc_driver.mjs then
connects the way the 0.1.10 desktop renderer does (bearer session from the
bootstrap credential, websocket ticket, `Origin: t3code://app`) and runs the
plan in client_rpc_cases.json through the client runtime's own RPC session and
typed HTTP client: connect-time subscriptions, project and thread lifecycle
with live shell/thread/config subscribers, terminals, VCS, attachments,
preview, settings and auth. Each step must end the same way on both servers
after normalizing paths, ids and timestamps.

Afterwards a set of read-only calls is replayed over a raw websocket and the
exact reply frames are compared, so envelope encoding is checked as well as the
decoded values.

Known Rust gaps are listed in client_rpc_parity_xfail.json. The run fails on any
other mismatch, and also when a listed case passes (remove it from the list) or
no longer exists.

    python3 scripts/client_rpc_parity.py --original-command "node ref/apps/server/src/bin.ts" \\
        --rust-command target/debug/t3code-server --reference-root ref
"""
import argparse
import base64
import contextlib
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import subprocess
import sys
import tempfile
import time
import urllib.request

from parity_harness import RELAY_TOOLS, encode, isolated_git_env, managed_server, path_without, run_concurrently, session_cookie

ROOT = Path(__file__).resolve().parents[1]
GIT_ENV = {"GIT_AUTHOR_NAME": "Parity", "GIT_AUTHOR_EMAIL": "parity@example.test",
           "GIT_COMMITTER_NAME": "Parity", "GIT_COMMITTER_EMAIL": "parity@example.test",
           "GIT_AUTHOR_DATE": "2026-11-11T10:12:13Z", "GIT_COMMITTER_DATE": "2026-11-11T10:12:13Z"}
# Read-only calls replayed over a raw socket once the plan has run; the frames must match exactly.
RAW_REPLAYS = [
    ("server.probe", {}),
    ("peerLinks.list", {}),
    ("projects.listEntries", {"cwd": "$REPO"}),
    ("projects.listEntries", {"cwd": "$HOME/does-not-exist"}),
    ("projects.readFile", {"cwd": "$REPO", "relativePath": "README.md"}),
    ("projects.readFile", {"cwd": "$REPO", "relativePath": "nope.txt"}),
    ("projects.searchEntries", {"cwd": "$REPO", "query": "read", "limit": 10}),
    ("vcs.listRefs", {"cwd": "$REPO"}),
    ("orchestration.getThreadTitles", {"threadIds": ["thread:client-rpc-a", "thread:absent"]}),
    ("orchestration.getThreadFamily", {"threadId": "thread:client-rpc-a"}),
    ("orchestration.getOlderSettledThreads", {"before": {"settledAt": "2030-01-01T00:00:00.000Z"}, "limit": 10}),
    ("orchestration.getTurnDiff", {"threadId": "thread:client-rpc-a", "fromTurnCount": 0, "toTurnCount": 0}),
    ("preview.list", {"threadId": "thread:client-rpc-a"}),
    ("pullRequests.listStats", {"refs": []}),
    # Every provider auth needs account, host and detail; the 0.1.10 client rejects the reply without them.
    ("server.discoverSourceControl", {}),
    ("attachments.createUploadUrl", {"type": "file", "name": "large.bin", "mimeType": "application/octet-stream", "sizeBytes": 52_428_801}),
    ("attachments.createUploadUrl", {"name": "large.png", "mimeType": "image/png", "sizeBytes": 10_485_761}),
    ("attachments.createUploadUrl", {"type": "file", "name": "empty.bin", "mimeType": "application/octet-stream", "sizeBytes": 0}),
]
ISO_TIME = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-]\d\d:\d\d)")
UUID = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.I)
# Random ids and tokens: long, and with several digits, unlike schema tags such as OrchestrationV2...Error.
LONG_TOKEN = re.compile(r"(?<![\w$])(?=(?:[A-Za-z_-]*\d){3})[A-Za-z0-9_-]{20,}(?![\w])")
# Values that vary by run or host, not by implementation.
VOLATILE_KEYS = re.compile(
    r"(?:^|_)(?:\w*At|\w*Ms|\w*Time|timestamp|expires\w*|pid|port|uptime\w*|\w*Bytes|bytes|size|"
    r"mtime\w*|ctime\w*|atime\w*|ino|dev|nlink|uid|gid|version|serverVersion|cpu\w*|memory\w*|load\w*|"
    r"rss|heap\w*|elapsed\w*|duration\w*|latency\w*|hostname|osRelease|"
    # Event counts differ by implementation; the client only feeds them back to the same server.
    r"sequence|\w*Sequence)$", re.I)


def seed_fixture(home, isolation):
    """The same workspace in each server's home. Commit dates are fixed, so commit ids match.

    `isolation` is the Git configuration both servers run with, so host settings never shape
    the fixture.
    """
    def git(*args, cwd):
        # parity-env: git, not a T3 server
        subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True,
                       env={**os.environ, **GIT_ENV, **isolation, "HOME": str(home)})

    remote, repo, plain = home / "remote.git", home / "repo", home / "plain"
    plain.mkdir(parents=True)
    (plain / "notes.txt").write_text("not a repository\n")
    git("init", "-q", "--bare", "-b", "main", str(remote), cwd=home)
    git("init", "-q", "-b", "main", str(repo), cwd=home)
    (repo / "README.md").write_text("# Parity fixture\n\nA parity line for content search.\n")
    (repo / "src").mkdir()
    (repo / "src/lib.rs").write_text("fn alpha() {}\nfn beta() {}\n")
    (repo / ".gitignore").write_text("target/\n*.log\n")
    git("add", "-A", cwd=repo)
    git("commit", "-qm", "Initial fixture", cwd=repo)
    (repo / "docs").mkdir()
    (repo / "docs/guide.md").write_text("Guide with a parity mention.\n")
    git("add", "-A", cwd=repo)
    git("commit", "-qm", "Add guide", cwd=repo)
    git("remote", "add", "origin", str(remote), cwd=repo)
    git("push", "-q", "-u", "origin", "main", cwd=repo)
    (repo / "target").mkdir()
    (repo / "target/build.out").write_text("ignored\n")
    (repo / "build.log").write_text("ignored log\n")
    (repo / "scratch.txt").write_text("untracked\n")
    # A 1x1 PNG the client can preview through assets.createUrl.
    (repo / "logo.png").write_bytes(bytes.fromhex(
        "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c489"
        "0000000d4944415478da63f8cfc0f01f0005000201a5f1d6a90000000049454e44ae426082"))
    (repo / "src/lib.rs").write_text("fn alpha() {}\nfn beta() {}\nfn gamma() {}\n")
    # The published parent advances after its child branches. Review the child's
    # commit and dirty files against their common ancestor.
    stack = home / "stack"
    git("clone", "-q", str(remote), str(stack), cwd=home)
    git("switch", "-qc", "parent", cwd=stack)
    for name in ("both", "before", "removed"):
        (stack / name).write_text(f"base {name}\n")
    git("add", ".", cwd=stack)
    git("commit", "-qm", "Parent", cwd=stack)
    git("push", "-qu", "origin", "parent", cwd=stack)
    git("switch", "-qc", "topic", cwd=stack)
    git("mv", "before", "moved", cwd=stack)
    git("rm", "-q", "removed", cwd=stack)
    git("commit", "-qm", "Child", cwd=stack)
    git("switch", "-q", "parent", cwd=stack)
    (stack / "both").write_text("parent advanced\n")
    git("commit", "-qam", "Advance parent", cwd=stack)
    git("push", "-q", "origin", "parent", cwd=stack)
    git("switch", "-q", "topic", cwd=stack)
    git("config", "branch.topic.t3-merge-base", "parent", cwd=stack)
    (stack / "both").write_text("working copy\n")
    (stack / "new").write_text("untracked\n")
    # The managed worktree basename includes the repository's absolute common directory.
    identity = f"{(repo / '.git').resolve()}\0feature/client-rpc"
    worktree_hash = hashlib.sha256(identity.encode()).hexdigest()[:16]
    return {"WORKTREE_HASH": worktree_hash, "HOME": str(home), "REPO": str(repo), "PLAIN": str(plain), "REMOTE": str(remote)}


def seed_asset_grants(home, key):
    """Known signatures with expired claims for otherwise readable/writable attachments."""
    def signed(claims):
        def b64(raw):
            return base64.urlsafe_b64encode(raw).rstrip(b"=")
        encoded = b64(encode(claims))
        return (encoded + b"." + b64(hmac.digest(key, encoded, "sha256"))).decode()

    secrets_dir = home / "userdata/secrets"
    secrets_dir.mkdir(parents=True)
    key_file = secrets_dir / "asset-access-signing-key.bin"
    key_file.write_bytes(key)
    key_file.chmod(0o600)
    attachment_id = "pending-aaaaaaaa-bbbb-cccc-dddd-000000000000-txt"
    attachments = home / "userdata/attachments"
    attachments.mkdir()
    (attachments / f"{attachment_id}.txt").write_bytes(b"x")
    common = dict(version=1, attachmentId=attachment_id, expiresAt=1)
    upload = signed(dict(common, kind="attachment-upload", type="file", name="x.txt", mimeType="text/plain", sizeBytes=1))
    asset = signed(dict(common, kind="attachment", download=True))
    return {"EXPIRED_UPLOAD": f"/api/attachments/upload/{upload}", "EXPIRED_ASSET": f"/api/assets/{asset}/x.txt"}


def inspect_grant(value, key, before, after):
    """Compare the actual signed claims, URL filename and TTL without reporting bearer URLs."""
    parts = value["relativeUrl"].split("/")
    upload = parts[1:4] == ["api", "attachments", "upload"]
    encoded, signature = parts[4 if upload else 3].split(".")
    expected = base64.urlsafe_b64encode(hmac.digest(key, encoded.encode(), "sha256")).rstrip(b"=").decode()
    if not hmac.compare_digest(expected, signature):
        raise ValueError("invalid grant signature")
    claims = json.loads(base64.urlsafe_b64decode(encoded + "==="))
    expires = claims.pop("expiresAt")
    ttl = (10 if upload else 60) * 60_000
    bucket = 30 * 60_000 if claims.get("mimeType") == "text/html" and not upload else 1
    def expiry(now):
        return ((now + ttl + bucket - 1) // bucket) * bucket
    if expires != value["expiresAt"] or expires % bucket or not expiry(before) <= expires <= expiry(after):
        raise ValueError("incorrect grant expiry")
    return {**{k: v for k, v in value.items() if k not in ("relativeUrl", "expiresAt")},
            "claims": claims, "route": parts[1:4] if upload else parts[1:3], "filename": None if upload else parts[4]}


def run_driver(args, url, credential, variables):
    env = {**os.environ, "T3_CLIENT_RPC_CREDENTIAL": credential, "T3_CLIENT_RPC_VARS": json.dumps(variables),
           "T3_CLIENT_RPC_REPLY_MS": str(args.reply_ms)}
    # parity-env: the Node client driver, not a T3 server
    result = subprocess.run([args.node, str(ROOT / "scripts/client_rpc_driver.mjs"), args.reference_root, url,
                             str(args.cases)], capture_output=True, text=True, env=env, timeout=900)
    lines = [line for line in result.stdout.splitlines() if line.startswith("{")]
    if not lines:
        raise RuntimeError(f"client driver produced no result (exit {result.returncode}):\n{result.stderr[-4000:]}")
    return json.loads(lines[-1])


class Normalizer:
    """Maps one server's run-specific strings onto shared placeholders."""

    def __init__(self, variables, environment_id=None):
        self.replacements = sorted(((v, f"${k}") for k, v in variables.items() if k in ("HOME", "REPO", "PLAIN",
                                                                                         "REMOTE", "WORKTREE_HASH")),
                                   key=lambda item: -len(item[0]))
        self.replacements += [(os.path.realpath(v), p) for v, p in self.replacements if os.path.realpath(v) != v]
        self.environment_id = environment_id
        self.ids = {}

    def placeholder(self, kind, value):
        return self.ids.setdefault((kind, value), f"<{kind}{sum(1 for k, _ in self.ids if k == kind) + 1}>")

    def text(self, value):
        for raw, placeholder in self.replacements:
            value = value.replace(raw, placeholder)
        if self.environment_id:
            value = value.replace(self.environment_id, "$ENV")
        value = ISO_TIME.sub("$TIME", value)
        value = re.sub(r"t3_session_\d+", "t3_session_$PORT", value)
        value = UUID.sub(lambda m: self.placeholder("uuid", m.group(0).lower()), value)
        value = re.sub(r"([?&](?:token|wsTicket|ticket|signature|sig|expires)=)[^&\s\"]+", r"\1<redacted>", value)
        value = LONG_TOKEN.sub(lambda m: self.placeholder("token", m.group(0)), value)
        return value

    def __call__(self, value, key=""):
        if isinstance(value, dict):
            return {k: self(v, k) for k, v in value.items()}
        if isinstance(value, list):
            return [self(v, key) for v in value]
        if isinstance(value, str):
            return value if key in ("_tag", "type", "kind", "diffHash") else self.text(value)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and VOLATILE_KEYS.search(key):
            return "<number>"
        return value


def shape(value):
    if isinstance(value, dict):
        return {k: shape(v) for k, v in sorted(value.items())}
    if isinstance(value, list):
        return [] if not value else [shape(value[0])]
    if value is None:
        return None
    return type(value).__name__


def signature(value):
    if isinstance(value, list):
        return f"array({'empty' if not value else signature(value[0])})"
    if not isinstance(value, dict):
        return type(value).__name__
    for key in ("type", "_tag", "kind"):
        if isinstance(value.get(key), str):
            return f"{key}={value[key]}"
    return "{" + ",".join(sorted(value)) + "}"


def view(step, mode):
    """What is compared for one step under its compare mode."""
    if step is None:
        return None
    outcome = step["outcome"]
    value = step.get("value")
    if mode == "outcome":
        return {"outcome": outcome}
    if mode == "signature":
        return {"outcome": outcome, "signature": step.get("signature")}
    if mode == "kinds":
        if isinstance(value, dict) and isinstance(value.get("items"), list):
            return {"outcome": outcome, "items": [signature(item) for item in value["items"]],
                    **{k: v for k, v in value.items() if k != "items"}}
        return {"outcome": outcome, "signature": step.get("signature")}
    if mode == "shape":
        if outcome != "reply":
            return {"outcome": outcome, "signature": step.get("signature")}
        shaped = shape(value)
        pricing = shaped.get("pricing") if isinstance(shaped, dict) else None
        if isinstance(pricing, dict) and pricing.get("fetchedAt") in (None, "str"):
            # The reference fetches its usage rates from GitHub at request time, and leaves
            # fetchedAt null when that fetch fails, so it depends on the network, not the server.
            pricing["fetchedAt"] = "str or null"
        return {"outcome": outcome, "shape": shaped}
    return {"outcome": outcome, "signature": step.get("signature"), "value": value}


def plan_modes(cases):
    modes = {}

    def visit(step):
        for child in step.get("parallel", []):
            visit(child)
        if "name" in step:
            modes[step["name"]] = step.get("compare", "value")
    for step in json.loads(cases.read_text())["steps"]:
        visit(step)
    return modes


def raw_frames(url, cookie, variables, normalizer_args):
    """Replays RAW_REPLAYS with numeric ids, as the 0.1.10 client numbers its requests."""
    from websockets.sync.client import connect
    replies = []
    with connect(url.replace("http", "ws", 1) + "/ws?orchestrationProtocol=2", additional_headers={
            "Cookie": cookie, "Origin": "t3code://app"}, proxy=None, compression=None, max_size=None) as ws:
        for index, (tag, payload) in enumerate(RAW_REPLAYS):
            payload = json.loads(json.dumps(payload).replace("$REPO", variables["REPO"])
                                 .replace("$HOME", variables["HOME"]))
            ws.send(encode({"_tag": "Request", "id": index + 1, "tag": tag, "payload": payload,
                            "headers": []}).decode())
            try:
                frame = json.loads(ws.recv(timeout=15))
                for reason in frame.get("exit", {}).get("cause", []) if isinstance(frame.get("exit"), dict) else []:
                    reason.pop("fiberId", None)
                replies.append((f"raw.{tag}#{index + 1}", Normalizer(*normalizer_args)(frame)))
            except TimeoutError:
                replies.append((f"raw.{tag}#{index + 1}", "no reply within 15 s"))
    return replies


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--reference-root", required=True,
                        help="checkout of the pinned upstream with client-runtime dependencies installed")
    parser.add_argument("--node", default="node")
    parser.add_argument("--cases", type=Path, default=ROOT / "scripts/client_rpc_cases.json")
    parser.add_argument("--xfail", type=Path, default=ROOT / "scripts/client_rpc_parity_xfail.json")
    parser.add_argument("--reply-ms", type=int, default=8000, help="wait for each reply or stream value")
    parser.add_argument("--verbose", action="store_true", help="print both sides of expected failures too")
    parser.add_argument("--dump", type=Path, help="write both servers' raw driver results here")
    parser.add_argument("--jobs", type=int, default=2, help="2 runs both servers at once, 1 one after the other")
    args = parser.parse_args()
    xfail = json.loads(args.xfail.read_text())["cases"]
    modes = plan_modes(args.cases)
    credential, signing_key = secrets.token_urlsafe(32), secrets.token_bytes(32)
    asset_key = secrets.token_bytes(32)
    started = time.monotonic()
    results, frames = {}, {}
    with tempfile.TemporaryDirectory(prefix="t3-client-rpc-") as temporary:
        root = Path(temporary)
        # Both servers and the fixture see the same Git configuration, written here, and a PATH
        # without relay tooling, so the host's setup cannot decide a comparison.
        isolation = {**isolated_git_env(root / "git"), "PATH": path_without(RELAY_TOOLS, root / "bin")}
        def run(name, command):
            # Each server has its own home, port and driver, so both run side by side.
            home = root / name
            home.mkdir()
            variables = {**seed_fixture(home, isolation), **seed_asset_grants(home, asset_key)}
            with contextlib.ExitStack() as server:
                url, _ = managed_server(server, shlex.split(command), home, credential, signing_key, env=isolation)
                before = time.time_ns() // 1_000_000
                result = run_driver(args, url, credential, variables)
                after = time.time_ns() // 1_000_000
                for step in result["steps"]:
                    if modes.get(step["name"]) == "grant" and step["outcome"] == "reply":
                        step["value"] = inspect_grant(step["value"], asset_key, before, after)
                with urllib.request.urlopen(url + "/.well-known/t3/environment", timeout=10) as response:
                    environment_id = json.load(response)["environmentId"]
                # Placeholders are numbered per step, so an extra id in one step does not shift the next.
                results[name] = {step["name"]: {**step, "value": Normalizer(variables, environment_id)(step.get("value"))}
                                 for step in result["steps"]}
                # The plan revokes every other client, the launch cookie included, so sign in again.
                frames[name] = raw_frames(url, session_cookie(url, credential), variables,
                                          (variables, environment_id))
            print(f"{name}: {len(result['steps'])} client steps in {result.get('totalMs')} ms", flush=True)

        run_concurrently([lambda: run("original", args.original_command), lambda: run("rust", args.rust_command)],
                         args.jobs)
        if args.dump:
            args.dump.write_text(json.dumps(results, indent=1))

    passed = expected = 0
    problems = []
    names = list(dict.fromkeys([*results["original"], *results["rust"]]))
    checks = [(name, view(results["original"].get(name), modes.get(name, "value")),
               view(results["rust"].get(name), modes.get(name, "value"))) for name in names]
    checks += [(name, original, rust) for (name, original), (_, rust) in zip(frames["original"], frames["rust"])]
    for name, original, rust in checks:
        same = original is not None and original == rust
        # A client that never connects or a driver failure is never parity.
        if name in ("environment.initialSync", "driver") and (rust or {}).get("outcome") != "reply":
            same = False
        known = xfail.get(name)
        if same and known:
            problems.append(name)
            print(f"XPASS {name}: passes now, remove from xfail ({known['reason']})")
        elif same:
            passed += 1
            print(f"PASS  {name}")
        elif known:
            expected += 1
            print(f"XFAIL {name}: {known['reason']}")
            if args.verbose:
                print(f"  original {json.dumps(original)[:1500]}\n  rust     {json.dumps(rust)[:1500]}")
        else:
            problems.append(name)
            print(f"FAIL  {name}\n  original {json.dumps(original)[:3000]}\n  rust     {json.dumps(rust)[:3000]}")
    stale = sorted(set(xfail) - {name for name, _, _ in checks})
    for name in stale:
        problems.append(name)
        print(f"STALE {name}: listed in xfail but not run; remove from xfail")
    print(f"{passed} passed, {expected} expected failures, {len(problems)} unexpected "
          f"({len(checks)} checks, {time.monotonic() - started:.0f} s)")
    return int(bool(problems))


if __name__ == "__main__":
    sys.exit(main())
