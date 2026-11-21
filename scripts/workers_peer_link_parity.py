#!/usr/bin/env python3
"""Link to the native Rust server and to the WebAssembly build served locally
by `wrangler dev` the way the pinned reference's "link another machine" does
(scripts/peer_link_sign_in.mjs), and compare every step.

Each server has linked environments turned on and gives a peer-link pairing
code; the reference's sign-in then registers, approves with the code,
exchanges it for a token, and calls /mcp as a linked server does: reads,
then a project, a launched thread, a wait and a read. Each tool answer must
decode with the reference's result schema; turns themselves are not
compared, since the WebAssembly build has no workspace.

  python3 scripts/workers_peer_link_parity.py --rust-command target/debug/t3code-server \\
      --reference-root reference --wrangler "npx --yes wrangler@4.166.0"

`workers/build` must already hold the `worker-build --release` output.
"""
import argparse
import contextlib
import json
import os
import secrets
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from client_initial_sync_parity import rpc  # noqa: E402
from parity_harness import encode, free_port, http, managed_server, session_cookie  # noqa: E402
from workers_initial_sync_parity import wrangler_dev  # noqa: E402

LINKED = {"linkedEnvironmentsEnabled": True}


def pairing_code(url, cookie, access):
    status, _, body = http(url, "POST", "/api/auth/pairing-token",
                           encode({"purpose": "peer-link", "access": access}),
                           {"Cookie": cookie, "Content-Type": "application/json"})
    if status != 200:
        raise RuntimeError(f"pairing-token failed on {url}: HTTP {status} {body[:400]!r}")
    return json.loads(body)["credential"]


def sign_in(args, url, cookie, access, root):
    env = {**os.environ, "T3_PEER_LINK_CODE": pairing_code(url, cookie, access), "T3_PEER_LINK_ACCESS": access,
           "T3_PEER_LINK_ROOT": str(root)}
    done = subprocess.run([args.node, str(ROOT / "scripts/peer_link_sign_in.mjs"), args.reference_root, url],
                          env=env, capture_output=True, text=True, timeout=120)
    if done.stderr:
        print(done.stderr[-4000:], file=sys.stderr)
    return json.loads(done.stdout.strip().splitlines()[-1])


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rust-command", required=True, help="native t3code-server command")
    parser.add_argument("--reference-root", required=True,
                        help="checkout of the pinned upstream with dependencies installed")
    parser.add_argument("--wrangler", default="npx --yes wrangler@4.166.0")
    parser.add_argument("--node", default="node")
    args = parser.parse_args()
    credential, signing_key = secrets.token_urlsafe(32), secrets.token_bytes(32)
    with tempfile.TemporaryDirectory() as temporary, contextlib.ExitStack() as stack:
        root = Path(temporary)
        log_path = root / "wrangler.log"
        log = stack.enter_context(open(log_path, "wb"))
        try:
            native = managed_server(stack, shlex.split(args.rust_command), root / "native", credential, signing_key)
            rpc(*native, "server.updateSettings", {"patch": LINKED})
            url = stack.enter_context(wrangler_dev(args.wrangler, free_port(), credential, log,
                                                   [f"T3_INITIAL_SETTINGS:{json.dumps(LINKED)}"]))
            wasm = (url, session_cookie(url, credential))
            project = root / "project"
            project.mkdir()
            outcomes = {name: sign_in(args, *server, "auto", project)
                        for name, server in (("native", native), ("wasm", wasm))}
        except Exception:
            log.flush()
            print(log_path.read_text(errors="replace")[-6000:], file=sys.stderr)
            raise
    failures = 0
    expected, actual = outcomes["native"]["steps"], outcomes["wasm"]["steps"]
    for index in range(max(len(expected), len(actual))):
        a = expected[index] if index < len(expected) else None
        b = actual[index] if index < len(actual) else None
        same = a == b and a is not None and a.get("ok")
        failures += not same
        name = (a or b or {}).get("name")
        print(f"{'PASS' if same else 'FAIL'} {name}" + ("" if same else f"\n  native: {a}\n  wasm:   {b}"))
    total = max(len(expected), len(actual))
    print(f"{total - failures}/{total} peer-link steps matched native")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
