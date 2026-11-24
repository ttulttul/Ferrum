#!/usr/bin/env python3
"""Compare `browser setup` (the reference's `t3 browser setup`, upstream PreviewBrowserHost) on
both servers: what each prints, and what each asks the host to do.

Each case runs the command with `--base-dir` on a fresh home, with fake `apparmor_parser`,
`apt-get`, `apt-cache` and `fc-match` first on PATH. The fakes only log their arguments, so
nothing is installed and nothing is downloaded. The home holds no browser, or a fake
`chrome-headless-shell` that fails to load a shared library, as a host missing libnss3 does.

- As this user, on this host: no browser, then the browser with a missing library.
- In a fake host (`fake_browser_host.sh`: user namespaces, an overlay over /etc, and the AppArmor
  user-namespace restriction forced on or off): as a user whose host blocks the sandbox; as
  root, which writes and loads the AppArmor profile and installs the libraries and fonts with
  apt; as root when the profile is already there (reloaded as is); and as root on a ready host
  (the root warning).

Compared: the output (with each server's own command masked as `<setup>`), the exit code, the
fakes' calls, and the profile written. Where user namespaces are unavailable (GitHub's runners,
for one), the fake-host cases are skipped with the reason.
"""
import argparse
import json
from pathlib import Path
import re
import shlex
import subprocess
import tempfile

from parity_harness import server_env

HERE = Path(__file__).resolve().parent
FAKE_HOST = HERE / "fake_browser_host.sh"
VERSION_SOURCE = "apps/server/src/preview/PreviewBrowser.ts"
SETUP = re.compile(r'sudo (?:env "PATH=\$PATH" \S+|\S+) browser setup')
FAKES = {
    "apparmor_parser": 'echo "apparmor_parser $*" >> "$FAKE_HOST_LOG"',
    "apt-get": 'echo "apt-get $*" >> "$FAKE_HOST_LOG"',
    # apt offers the t64 library names, as Ubuntu 24.04 does.
    "apt-cache": 'case "$2" in *t64) echo "  Candidate: 1.0";; *) echo "  Candidate: (none)";; esac',
    "fc-match": 'printf %s "$FAKE_FONT"',
}
MISSING_LIBRARY = ('#!/bin/sh\necho "$0: error while loading shared libraries: libnss3.so: cannot open '
                   'shared object file: No such file or directory" >&2\nexit 127\n')
CASES = [
    # name, fake host (role, restricted) or None, browser installed, font, profile already on disk
    ("no browser", None, False, "/usr/share/fonts/fake.ttf", False),
    ("browser missing a library", None, True, "/usr/share/fonts/fake.ttf", False),
    ("user on a host that blocks the sandbox", ("user", "1"), False, "/usr/share/fonts/fake.ttf", False),
    ("root sets up a blocked host with libraries and fonts missing", ("root", "1"), True, "", False),
    ("root reloads a profile already on disk", ("root", "1"), False, "/usr/share/fonts/fake.ttf", True),
    ("root on a ready host", ("root", "0"), False, "/usr/share/fonts/fake.ttf", False),
]


def fake_host_works(root):
    """Whether `fake_browser_host.sh` runs on this machine."""
    work = root / "probe"
    work.mkdir(parents=True)
    try:
        # parity-env: probes the namespace wrapper with `id`, not a T3 server
        found = subprocess.run([str(FAKE_HOST), "root", "1", "id", "-u"], capture_output=True, timeout=30,
                               env={"PATH": "/usr/bin:/bin", "FAKE_HOST_WORK": str(work)})
    except (OSError, subprocess.TimeoutExpired):
        return False
    return found.returncode == 0 and found.stdout.strip() == b"0"


def run_case(command, root, version, case):
    name, host, browser, font, profile_on_disk = case
    root.mkdir(parents=True)
    home = root / "home"
    if browser:
        target = home / "tools/chrome-headless-shell/linux64" / version / "chrome-headless-shell"
        target.parent.mkdir(parents=True)
        target.write_text(MISSING_LIBRARY)
        target.chmod(0o755)
    home.mkdir(exist_ok=True)
    fakes = root / "fakes"
    fakes.mkdir()
    for tool, body in FAKES.items():
        (fakes / tool).write_text(f"#!/bin/sh\n{body}\n")
        (fakes / tool).chmod(0o755)
    log = root / "calls.log"
    log.touch()
    work = root / "host"
    env = server_env(home, {"PATH": f"{fakes}:{server_env(home)['PATH']}", "FAKE_HOST_LOG": str(log),
                            "FAKE_FONT": font, "FAKE_HOST_WORK": str(work),
                            "FAKE_PROFILE_ON_DISK": "1" if profile_on_disk else "0"})
    argv = command + ["browser", "setup", "--base-dir", str(home)]
    if host:
        argv = [str(FAKE_HOST), *host, *argv]
    # parity-env: the environment comes from server_env above
    done = subprocess.run(argv, capture_output=True, text=True, timeout=120, env=env)
    output = SETUP.sub("<setup>", done.stdout + done.stderr).replace(str(root), "<root>")
    written = work / "up/apparmor.d/t3-chrome-headless-shell"
    return {"exit": done.returncode, "output": output.splitlines(),
            "calls": log.read_text().splitlines(),
            "profile": written.read_text() if written.exists() else None}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--reference-root", type=Path, required=True)
    args = parser.parse_args()
    source = (args.reference_root / VERSION_SOURCE).read_text()
    version = re.search(r'export const VERSION = "([^"]+)"', source).group(1)
    failures = compared = 0
    with tempfile.TemporaryDirectory(prefix="t3-browser-setup-parity-") as directory:
        directory = Path(directory)
        fake_host = fake_host_works(directory)
        for index, case in enumerate(CASES):
            if case[1] and not fake_host:
                print(f"SKIP {case[0]}: this machine has no unprivileged user namespaces for the fake host")
                continue
            outcomes = {name: run_case(shlex.split(command), directory / f"{index}-{name}", version, case)
                        for name, command in (("original", args.original_command), ("rust", args.rust_command))}
            compared += 1
            same = outcomes["original"] == outcomes["rust"]
            print(f"{'PASS' if same else 'FAIL'} {case[0]}")
            if not same:
                failures += 1
                for name in ("original", "rust"):
                    print(f"  {name:8} {json.dumps(outcomes[name])}")
    print(f"{compared - failures}/{compared} browser setup checks matched across the reference and the Rust server")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
