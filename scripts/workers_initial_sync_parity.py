#!/usr/bin/env python3
"""Compare the pinned client's initial sync against the native Rust server and
the WebAssembly build served locally by `wrangler dev`.

The native server is the reference here ("original" in the step comparison);
the workers build must answer every step the same way. Only the empty
environment is compared: the WebAssembly build has no local workspace yet
(project.create needs a workspace).

  python3 scripts/workers_initial_sync_parity.py --rust-command target/debug/t3code-server \\
      --reference-root reference --wrangler "npx --yes wrangler@4.166.0"

`workers/build` must already hold the `worker-build --release` output.
"""
import argparse
import contextlib
import secrets
import shlex
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from client_initial_sync_parity import compare_steps, run_client  # noqa: E402
from parity_harness import free_port, managed_server, session_cookie  # noqa: E402


@contextlib.contextmanager
def wrangler_dev(command, port, credential, log):
    # --var keeps the per-run credential out of wrangler.toml; it only reaches
    # the local development server.
    argv = shlex.split(command) + ["dev", "--port", str(port), "--ip", "127.0.0.1",
                                   "--var", f"T3_BOOTSTRAP_CREDENTIAL:{credential}"]
    process = subprocess.Popen(argv, cwd=ROOT / "workers", stdout=log, stderr=subprocess.STDOUT,
                               stdin=subprocess.DEVNULL, start_new_session=True)
    url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 180
        while True:
            if process.poll() is not None:
                raise RuntimeError(f"wrangler dev exited with {process.returncode}")
            try:
                urllib.request.urlopen(f"{url}/.well-known/t3/environment", timeout=2).read()
                break
            except Exception:
                if time.monotonic() > deadline:
                    raise RuntimeError("wrangler dev did not answer within 180 s")
                time.sleep(1)
        yield url
    finally:
        with contextlib.suppress(ProcessLookupError):
            import os
            import signal
            os.killpg(process.pid, signal.SIGTERM)
        with contextlib.suppress(subprocess.TimeoutExpired):
            process.wait(timeout=15)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rust-command", required=True, help="native t3code-server command")
    parser.add_argument("--reference-root", required=True,
                        help="checkout of the pinned upstream with dependencies installed")
    parser.add_argument("--wrangler", default="npx --yes wrangler@4.166.0")
    parser.add_argument("--node", default="node")
    parser.add_argument("--timeout-ms", type=int, default=20000)
    parser.add_argument("--reply-ms", type=int, default=5000)
    args = parser.parse_args()
    credential, signing_key = secrets.token_urlsafe(32), secrets.token_bytes(32)
    with tempfile.TemporaryDirectory() as temporary, contextlib.ExitStack() as stack:
        root = Path(temporary)
        log_path = root / "wrangler.log"
        log = stack.enter_context(open(log_path, "wb"))
        try:
            native = managed_server(stack, shlex.split(args.rust_command), root / "native", credential, signing_key)
            url = stack.enter_context(wrangler_dev(args.wrangler, free_port(), credential, log))
            wasm = (url, session_cookie(url, credential))
            outcomes = {"original": run_client(args, *native), "rust": run_client(args, *wasm)}
        except Exception:
            log.flush()
            print(log_path.read_text(errors="replace")[-6000:], file=sys.stderr)
            raise
        failures, total = compare_steps("workers", outcomes)
        if failures:
            log.flush()
            print(log_path.read_text(errors="replace")[-6000:], file=sys.stderr)
    print(f"{total - failures}/{total} initial sync steps matched native")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
