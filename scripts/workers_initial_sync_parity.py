#!/usr/bin/env python3
"""Compare the pinned client's initial sync against the native Rust server and
the WebAssembly build served locally by `wrangler dev`.

The native server is the reference here ("original" in the step comparison);
the workers build must answer every step the same way. The empty environment
is compared first, then one seeded over RPC with a project and a thread, so
the thread subscription runs too. Steps that read the project's checkout are
reported but not compared until a workspace is connected.

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

from client_initial_sync_parity import PROJECT, THREAD, compare_steps, rpc, run_client  # noqa: E402
from parity_harness import free_port, managed_server, session_cookie  # noqa: E402


@contextlib.contextmanager
def wrangler_dev(command, port, credential, log, variables=()):
    # --var keeps the per-run credential out of wrangler.toml; it only reaches
    # the local development server.
    argv = shlex.split(command) + ["dev", "--port", str(port), "--ip", "127.0.0.1",
                                   "--var", f"T3_BOOTSTRAP_CREDENTIAL:{credential}"]
    for variable in variables:
        argv += ["--var", variable]
    # parity-env: wrangler running the Workers build, not a T3 server
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


# Seeded steps that read the project's checkout. The workers build answers
# them as unavailable until a workspace is connected, so they are reported
# but not compared.
WORKSPACE_STEPS = {"subscribeTerminalMetadata", "subscribeVcsBranch", "subscribeVcsStatus"}


def seed(url, cookie, root):
    """A project with one thread, made over RPC on either server."""
    rpc(url, cookie, "projects.mutate", {
        "type": "project.create", "commandId": "seed-project", "projectId": PROJECT,
        "title": "Initial sync", "workspaceRoot": str(root)})
    rpc(url, cookie, "orchestration.dispatchCommand", {
        "type": "thread.create", "commandId": "seed-thread", "threadId": THREAD, "projectId": PROJECT,
        "title": "Draft", "modelSelection": {"instanceId": "codex", "model": "fixture"},
        "runtimeMode": "full-access", "interactionMode": "default", "branch": None, "worktreePath": None,
        "createdBy": "user", "creationSource": "web"})
    return {"cwd": str(root), "threadId": THREAD}


def without_workspace_steps(outcomes):
    for name, result in outcomes.items():
        for step in result["steps"]:
            if step["name"] in WORKSPACE_STEPS:
                print(f"SKIP seeded {step['name']} ({name}): {step['outcome']} {step.get('signature', '')}")
    return {name: {**result, "steps": [step for step in result["steps"] if step["name"] not in WORKSPACE_STEPS]}
            for name, result in outcomes.items()}


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
            repo = root / "repo"
            # parity-env: git, not a T3 server
            subprocess.run(["git", "init", "-qb", "main", str(repo)], check=True)
            seeded = without_workspace_steps({
                "original": run_client(args, *native, seed(*native, repo)),
                "rust": run_client(args, *wasm, seed(*wasm, repo)),
            })
        except Exception:
            log.flush()
            print(log_path.read_text(errors="replace")[-6000:], file=sys.stderr)
            raise
        failures, total = compare_steps("workers", outcomes)
        seeded_failures, seeded_total = compare_steps("workers seeded", seeded)
        if failures or seeded_failures:
            log.flush()
            print(log_path.read_text(errors="replace")[-6000:], file=sys.stderr)
    print(f"{total - failures}/{total} initial sync steps matched native")
    print(f"{seeded_total - seeded_failures}/{seeded_total} seeded steps matched native")
    return int(failures != 0 or seeded_failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
