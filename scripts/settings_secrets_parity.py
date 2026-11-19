#!/usr/bin/env python3
"""Compare secret storage and device credential consumers against the TypeScript server."""

import argparse
from contextlib import ExitStack
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import secrets
import shlex
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from unittest.mock import patch

from websockets.sync.client import connect

from parity_harness import encode, http, managed_server
from settings_mutation_parity import comparator, paired


def run_secrets(original_command, rust_command):
    with TemporaryDirectory() as workspace, ExitStack() as stack:
        root = Path(workspace)
        calls = []

        class Identity(BaseHTTPRequestHandler):
            def do_GET(self):
                calls.append((self.path, self.headers.get("Authorization")))
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"name":"fixture-agent","role":"agent"}')

            def log_message(self, *args):
                pass

        upstream = stack.enter_context(ThreadingHTTPServer(("127.0.0.1", 0), Identity))
        Thread(target=upstream.serve_forever, daemon=True).start()
        stack.callback(upstream.shutdown)
        gateway = f"http://127.0.0.1:{upstream.server_port}"
        # Both runtimes execute the real provider path against a deterministic local CLI.
        binary = root / "op"
        binary.write_text("""#!/bin/sh
[ "$#" = 5 ] && [ "$1" = --cache=false ] && [ "$2" = read ] &&
[ "$3" = --account=fixture.example ] && [ "$4" = --no-newline ] &&
[ "$5" = 'op://fixture/device/credential' ] || exit 2
printf %s https://fixture.invalid
""")
        binary.chmod(0o700)
        # Use the real OpenSSH config expander with an isolated config. The shim permits
        # only -G, so adding a configured remote host cannot connect to another machine.
        ssh_path = shutil.which("ssh")
        if ssh_path is None:
            raise RuntimeError(
                "OpenSSH is required for the SSH device settings fixtures"
            )
        config = root / "ssh-config"
        config.write_text("""Host loop
  Hostname 127.2.3.4
Host v6
  Hostname ::1
Host mapped
  Hostname ::ffff:7f00:1
Host dns-loop
  Hostname localhost
Host forwarded
  Hostname 127.0.0.1
  Port 2200
Host routed
  Hostname 127.0.0.1
  ProxyJump example.invalid
Host command-proxy
  Hostname 127.0.0.1
  ProxyCommand false
Host remote
  Hostname 192.0.2.3
Host unresolved
  Hostname fixture-missing.invalid
""")
        ssh = root / "ssh"
        ssh.write_text(
            "#!/bin/sh\nfor flag do if [ \"$flag\" = '-G' ]; then exec "
            + shlex.quote(ssh_path)
            + " -F "
            + shlex.quote(str(config))
            + ' "$@"; fi; done\nexit 2\n'
        )
        ssh.chmod(0o700)
        stack.enter_context(
            patch.dict(os.environ, {"PATH": f"{root}{os.pathsep}{os.environ['PATH']}"})
        )
        sockets, homes = paired(stack, root, (original_command, rust_command))
        index = 0

        def rpc(label, method, value, success=True):
            nonlocal index
            index += 1
            request = encode(
                dict(
                    _tag="Request", id=str(index), tag=method, payload=value, headers=[]
                )
            ).decode()
            for socket in sockets:
                socket.send(request)
            left, right = [socket.recv(timeout=15) for socket in sockets]
            if left != right:
                at = next(
                    (i for i, (a, b) in enumerate(zip(left, right)) if a != b),
                    min(len(left), len(right)),
                )
                raise AssertionError(
                    f"{label}: byte mismatch at {at}\noriginal: {left[at : at + 240]}\nrust: {right[at : at + 240]}"
                )
            result = json.loads(left)["exit"]
            assert (result["_tag"] == "Success") == success, (
                f"{label}: unexpected result {result}"
            )
            print("PASS", label)
            return result.get("value")

        def files(home):
            directory = home / "userdata"
            secrets = directory / "secrets"
            prefixes = (
                "provider-config-",
                "provider-env-",
                "source-control-",
                "device-host-",
                "simlock-device-host-",
                "voice-cloud-",
                "github-token-",
                "bitbucket-",
            )
            return {
                **{
                    f.name: f.read_bytes()
                    for f in secrets.iterdir()
                    if f.name.startswith(prefixes)
                },
                "settings.json": (directory / "settings.json").read_bytes(),
            }

        def compare_files(label):
            left, right = map(files, homes)
            if left != right:
                for name in sorted(left.keys() | right.keys()):
                    if left.get(name) != right.get(name):
                        raise AssertionError(
                            f"{label}: different {name}\noriginal: {left.get(name)!r}\nrust: {right.get(name)!r}"
                        )
            print("PASS", label)
            return left

        aliases = [
            "loop",
            "v6",
            "mapped",
            "dns-loop",
            "forwarded",
            "routed",
            "command-proxy",
            "remote",
            "unresolved",
        ]
        hosts = [{"id": name, "label": name, "target": name} for name in aliases]
        hosts.append(
            {"id": "alternate", "label": "Alternate", "target": "loop", "port": 2222}
        )
        hosts.append(
            {
                "id": "default-port",
                "label": "Standard",
                "target": "forwarded",
                "port": 22,
            }
        )
        hosts[0]["identityFile"] = "/fixture/keys/has spaces"
        snapshot = rpc(
            "filter local SSH aliases on settings update",
            "server.updateSettings",
            {"patch": {"deviceHosts": hosts}},
        )
        assert [h["id"] for h in snapshot["deviceHosts"]] == [
            "forwarded",
            "routed",
            "command-proxy",
            "remote",
            "unresolved",
            "alternate",
        ]
        compare_files("persist filtered SSH aliases in order")
        local = rpc(
            "SSH test for a local alias returns this machine",
            "device.testHost",
            hosts[0],
        )
        assert local["id"] == "local" and local["kind"] == "local"
        rpc(
            "clear SSH fixtures",
            "server.updateSettings",
            {"patch": {"deviceHosts": []}},
        )
        ref = {"provider": "vault", "reference": "op://fixture/device/credential"}
        unreadable = {"provider": "vault", "reference": "op://fixture/device/missing"}
        initial = {
            "secretProviders": {
                "vault": {
                    "kind": "one_password",
                    "label": "Fixture vault",
                    "config": {"account": "fixture.example"},
                }
            },
            "providerInstances": {
                "worker": {
                    "driver": "opencode",
                    "config": {"serverPassword": "fixture-password"},
                    "environment": [
                        {"name": "HIDDEN", "sensitive": True, "value": "fixture-env"},
                        {"name": "PUBLIC", "sensitive": False, "value": "public"},
                        {"name": "REF", "value": "", "sensitive": True, "secret": ref},
                    ],
                }
            },
            "sourceControlHosts": {
                "github": {
                    "tokens": {
                        "literal.example": "fixture-github",
                        "vault.example": ref,
                    }
                }
            },
            "easDeviceHosts": [
                {
                    "kind": "eas",
                    "id": "expo",
                    "label": "Cloud",
                    "accessToken": unreadable,
                }
            ],
            "simlockDeviceHosts": [
                {
                    "kind": "simlock",
                    "id": "phone",
                    "label": "Test",
                    "gatewayUrl": gateway,
                    "agentToken": ref,
                }
            ],
        }
        snapshot = rpc(
            "save literal and referenced credentials",
            "server.updateSettings",
            {"patch": initial},
        )
        stored = compare_files("sparse settings and secret file bytes")
        for literal in (b"fixture-password", b"fixture-env", b"fixture-github"):
            assert literal not in stored["settings.json"] and literal in stored.values()
        assert snapshot["sourceControlHosts"]["github"]["tokens"] == {
            "literal.example": "••••••",
            "vault.example": "••••••",
        }
        for source in (
            {},
            {"provider": "vault"},
            {"provider": "bad id", "reference": "x"},
            {"provider": "vault", "reference": 42},
            {"provider": "vault", "reference": "a\nb"},
        ):
            rpc(
                "validate a secret-check input",
                "server.checkSecretSource",
                {"source": source},
                False,
            )
        unknown = rpc(
            "check missing provider",
            "server.checkSecretSource",
            {
                "source": {
                    "provider": "other",
                    "reference": "op://fixture/example/field",
                }
            },
        )
        assert unknown["_tag"] == "Failed" and '"other"' in unknown["detail"]
        fresh = rpc(
            "resolve named secret without disclosing it",
            "server.checkSecretSource",
            {"source": ref},
        )
        assert fresh == {"_tag": "Resolved"}
        identity = rpc(
            "use saved 1Password Simlock reference",
            "device.simlockIdentity",
            {"gatewayUrl": gateway, "hostId": "phone"},
        )
        assert identity == {
            "gatewayUrl": gateway,
            "signedInAs": "fixture-agent",
            "tokenSource": "settings",
        }
        # Re-lock the fixture vault. A device keeps an already-read credential until refreshed.
        binary.write_text('#!/bin/sh\nprintf x >> "$HOME/failed-reads"\nexit 2\n')
        failed = rpc(
            "fresh secret check reports the newly locked store",
            "server.checkSecretSource",
            {"source": ref},
        )
        assert failed == {
            "_tag": "Failed",
            "detail": 'Could not read this secret from "Fixture vault". Check the provider\'s settings and that its store is unlocked.',
        }
        cached = rpc(
            "cached Simlock identity survives a locked vault",
            "device.simlockIdentity",
            {"gatewayUrl": gateway, "hostId": "phone"},
        )
        assert cached == identity
        identity = rpc(
            "entered token overrides a saved reference",
            "device.simlockIdentity",
            {"gatewayUrl": gateway, "hostId": "phone", "agentToken": "fixture-entered"},
        )
        assert identity["tokenSource"] == "entered"
        rpc(
            "other gateway cannot receive the saved token",
            "device.simlockIdentity",
            {"gatewayUrl": "http://127.0.0.1:1", "hostId": "phone"},
            False,
        )
        rpc(
            "unreadable saved Expo reference",
            "device.easAccount",
            {"hostId": "expo"},
            False,
        )
        assert (
            calls
            == [("/v1/identity", "Bearer https://fixture.invalid")] * 4
            + [("/v1/identity", "Bearer fixture-entered")] * 2
        )
        rpc(
            "save the cached device reference as a speech key",
            "server.updateSettings",
            {"patch": {"cloudSpeechCredentials": {"openai": ref}}},
        )
        options = encode(
            {
                "speechLanguage": "auto",
                "speechTranslateToEnglish": False,
                "speechCustomWords": [],
                "speechRemoveFillerWords": True,
            }
        )
        recording = len(options).to_bytes(4, "little") + options + bytes(4)
        answers = []
        for socket, home in zip(sockets, homes):
            attempts = home / "failed-reads"
            before = attempts.stat().st_size if attempts.exists() else 0
            port = socket.remote_address[1]
            status, _, body = http(
                f"http://127.0.0.1:{port}",
                "POST",
                "/api/voice/cloud/openai/transcribe",
                recording,
                {
                    "Cookie": socket.request.headers["Cookie"],
                    "Content-Type": "application/octet-stream",
                },
            )
            answers.append((status, body))
            assert attempts.stat().st_size == before + 1
        assert (
            answers[0]
            == answers[1]
            == (
                422,
                b'{"_tag":"CloudSpeechError","provider":"openai","reason":"unreadable-key"}',
            )
        )
        print("PASS voice upload re-reads its vault reference after it locks")
        rpc(
            "remove vault references",
            "server.updateSettings",
            {"patch": {"secretProviders": {"vault": None}}},
        )
        stored = compare_files("removed reference storage")
        assert b"op://fixture/" not in stored["settings.json"]
        rpc(
            "remove provider and GitHub literals",
            "server.updateSettings",
            {
                "patch": {"sourceControlHosts": {"github": {"tokens": {}}}},
                "providerInstanceMutation": {
                    "operation": "remove",
                    "instanceId": "worker",
                },
            },
        )
        stored = compare_files("removed literal secret files")
        assert all(
            v not in stored.values()
            for v in (b"fixture-env", b"fixture-password", b"fixture-github")
        )

    with TemporaryDirectory() as workspace:
        root = Path(workspace)
        seed = encode(
            {
                "projectSettingsFolded": True,
                "github": {"tokens": {"example.test": "••••••"}},
                "cloudSpeechCredentials": {"openai": "fixture-inline"},
            }
        )
        legacy_file = "github-token-ZXhhbXBsZS50ZXN0.bin"

        def unavailable(state):
            secret_dir = state / "secrets"
            secret_dir.mkdir(parents=True)
            (secret_dir / legacy_file).mkdir()
            (secret_dir / "voice-cloud-openai.bin").mkdir()
            (state / "settings.json").write_bytes(seed)

        commands = (original_command, rust_command)
        with ExitStack() as processes:
            sockets, homes = paired(processes, root, commands, unavailable)
            result = comparator(sockets)(
                "serve settings while credential migration is blocked",
                "server.getSettings",
                {},
            )
            assert result["cloudSpeechCredentials"]["openai"] == "••••••"
            for home in homes:
                assert (home / "userdata/settings.json").read_bytes() == seed
        for home in homes:
            directory = home / "userdata/secrets"
            (directory / "voice-cloud-openai.bin").rmdir()
            legacy = directory / legacy_file
            legacy.rmdir()
            legacy.write_bytes(b"fixture-legacy")
        with ExitStack() as processes:
            sockets, homes = paired(processes, root, commands)
            result = comparator(sockets)(
                "retry legacy and inline migration after storage repair",
                "server.getSettings",
                {},
            )
            assert (
                result["sourceControlHosts"]["github"]["tokens"]["example.test"]
                == "••••••"
            )
            copies = [files(home) for home in homes]
            assert copies[0] == copies[1]
            assert copies[0]["voice-cloud-openai.bin"] == b"fixture-inline"
            assert b"fixture-legacy" in copies[0].values()
            assert legacy_file not in copies[0]
            print("PASS deferred migration settings and credential bytes")
    # The error contract includes the absolute state path, so replay these failures in the same directory.
    with TemporaryDirectory() as workspace:
        root = Path(workspace) / "server"
        signing_key = secrets.token_bytes(32)
        session = secrets.token_urlsafe(32)
        outputs = []
        for command in (original_command, rust_command):
            with ExitStack() as stack:
                url, cookie = managed_server(
                    stack, shlex.split(command), root, session, signing_key
                )
                ws = stack.enter_context(
                    connect(
                        url.replace("http:", "ws:") + "/ws?orchestrationProtocol=2",
                        additional_headers={"Cookie": cookie},
                        proxy=None,
                    )
                )
                records = []
                for slot, patch_input in (
                    (
                        "provider-env-d29ya2Vy-S0VZ",
                        {
                            "providerInstances": {
                                "worker": {
                                    "driver": "codex",
                                    "config": {},
                                    "environment": [
                                        {
                                            "name": "KEY",
                                            "sensitive": True,
                                            "value": "fixture-only",
                                        }
                                    ],
                                }
                            }
                        },
                    ),
                    (
                        "voice-cloud-openai",
                        {"cloudSpeechCredentials": {"openai": "fixture-only"}},
                    ),
                ):
                    path = root / "userdata" / "secrets" / (slot + ".bin")
                    path.mkdir()
                    ws.send(
                        encode(
                            dict(
                                _tag="Request",
                                id=slot,
                                tag="server.updateSettings",
                                payload={"patch": patch_input},
                                headers=[],
                            )
                        ).decode()
                    )
                    frame = ws.recv(timeout=15)
                    result = json.loads(frame)["exit"]
                    assert result["cause"][0]["error"]["operation"] == "read-secret"
                    records.append(frame)
                    path.rmdir()
                outputs.append(records)
            shutil.rmtree(root)
        assert outputs[0] == outputs[1], f"Storage failures differed: {outputs}"
        print(
            "PASS read-secret failure frames for a provider environment variable and a speech key"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    run_secrets(args.original_command, args.rust_command)
