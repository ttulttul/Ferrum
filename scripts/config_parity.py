#!/usr/bin/env python3
"""Compare unmodified settings RPC frames, settings.json and credential sidecars."""
import argparse
from contextlib import ExitStack
import json
from pathlib import Path
import secrets
import shlex
import shutil
from tempfile import TemporaryDirectory

from websockets.sync.client import connect

from parity_harness import encode, managed_server
from settings_syntax_parity import malformed_settings
from managed_policy_parity import run_policy
from settings_secrets_parity import run_secrets
from settings_mutation_parity import run_mutations, run_migration

GET = "server.getSettings"
UPDATE = "server.updateSettings"
SECRET = "fixture-test-key-only"
REFERENCE = {"provider": "vault", "reference": "op://fixture/item/field"}
MARKER = "••••••"


def socket(stack, base, cookie, references=False):
    query = "/ws?orchestrationProtocol=2"
    if references:
        query += "&clientCapabilities=extra,settingsSecretReferences"
    return stack.enter_context(connect(base.replace("http:", "ws:", 1) + query,
                                       additional_headers={"Cookie": cookie}, proxy=None))


def frame(ws, label, method, payload):
    ws.send(encode(dict(_tag="Request", id=label, tag=method, payload=payload, headers=[])).decode())
    return ws.recv(timeout=15).encode()


def identical(label, originals):
    left, right = originals
    if left != right:
        i = next((i for i, pair in enumerate(zip(left, right)) if pair[0] != pair[1]),
                 min(len(left), len(right)))
        raise AssertionError(
            f"{label} differs at byte {i}\n"
            f"TS   {left[i:i+360]!r}\nRust {right[i:i+360]!r}\n"
            f"TS full {left!r}\nRust full {right!r}"
        )


def disk(state):
    return (state / "settings.json").read_bytes() if (state / "settings.json").exists() else b""


def secret_files(state):
    # Other services own independent secrets, including auth signing keys.
    prefixes = ("bitbucket-", "github-token-", "source-control-", "voice-cloud-",
                "provider-instance-", "provider-env-", "usage-limit-source-", "text-generator-")
    return {p.name: p.read_bytes() for p in (state / "secrets").glob("*.bin") if p.name.startswith(prefixes)}


def paired(commands, root, initial=None):
    roots = [root / name for name in ("original", "rust")]
    states = [d / "userdata" for d in roots]
    for state in states:
        state.mkdir(parents=True)
        if initial:
            (state / "actual.json").write_text(initial)
            (state / "settings.json").symlink_to("actual.json")
    credentials = secrets.token_urlsafe(32), secrets.token_bytes(32)
    with ExitStack() as stack:
        servers = [managed_server(stack, shlex.split(c), d, *credentials) for c, d in zip(commands, roots)]
        sockets = {refs: [socket(stack, *sv, refs) for sv in servers] for refs in (True, False)}

        def compare(label, method, payload, references=True, expect="Success"):
            packets = [frame(ws, label, method, payload) for ws in sockets[references]]
            identical(label, packets)
            if SECRET.encode() in packets[0] or b"opaque-fixture-key" in packets[0]:
                raise AssertionError(f"{label} exposed a credential")
            reply = json.loads(packets[0])["exit"]
            if expect and reply["_tag"] != expect:
                raise AssertionError(f"{label} unexpected {reply}")
            identical(label + " persisted JSON", [disk(s) for s in states])
            files = [secret_files(s) for s in states]
            if files[0] != files[1]:
                raise AssertionError(f"{label} secret files differ: {list(files[0])} / {list(files[1])}")
            return reply.get("value")

        settings = compare("get", GET, {})
        if initial:
            assert settings["worktreesDirectory"] == "/fixture"
            assert settings["environmentIcon"] is None and "defaultThreadEnvMode" not in settings
            assert settings["hiddenThreadTags"] == ["work"]
            assert settings["sourceControlWritingStyle"]["customInstructions"] == "house style"
            assert settings["cloudSpeechCredentials"]["openai"] == MARKER
            assert all((s / "settings.json").is_symlink() for s in states)
            assert SECRET.encode() not in disk(states[0])
            assert SECRET.encode() in secret_files(states[0]).values()
        else:
            assert disk(states[0]) == b""
            descriptor = json.loads((Path(__file__).resolve().parents[1] / "contracts/settings.json").read_text())
            nodes = descriptor["nodes"]
            patch_schema = nodes[nodes[descriptor["request"]]["fields"][0][1]]
            for field, _ in patch_schema["fields"]:
                # A nested invalid record hits more than a primitive type mismatch.
                compare("schema-" + field, UPDATE, {"patch": {field: {"!": 127}}}, expect=None)
            run_mutations(compare)
        compare("JS numeric key order and number formatting", UPDATE, {"patch":{"sourceControlHosts":{
            "custom":{"nested":{"42":1.0,"2":1e-7,"15":1e-6,"001":1e21,"big":9007199254740993}}}}})
        compare("store-literal", UPDATE, {"patch": {"cloudSpeechCredentials": {"openai": SECRET}}})
        assert SECRET.encode() not in disk(states[0])
        cfg = compare("set-reference", UPDATE, {"patch": {
            "secretProviders": {"vault": {"kind": "one_password", "label": "Fixture", "config": {"account": "demo"}}},
            "cloudSpeechCredentials": {"openai": REFERENCE}}})
        assert cfg["cloudSpeechCredentials"]["openai"] == REFERENCE
        cfg = compare("legacy-read", GET, {}, False)
        assert cfg["cloudSpeechCredentials"]["openai"] == MARKER
        compare("legacy-echo", UPDATE, {"patch": {"cloudSpeechCredentials": {"openai": MARKER}}}, False)
        assert compare("kept-reference", GET, {})["cloudSpeechCredentials"]["openai"] == REFERENCE
        cfg = compare("remove-provider", UPDATE, {"patch": {"secretProviders": {"vault": None}}})
        assert cfg["cloudSpeechCredentials"]["openai"] == ""
        compare("array-errors", UPDATE, {"patch": {"hiddenThreadTags": [5]}}, expect="Failure")
        compare("set-theme-through-client", UPDATE, {"patch": {"defaultTheme": "ignored", "unexpected": True}})
        if initial:
            assert all((s / "settings.json").is_symlink() for s in states)
    # Reload real on-disk state in both processes, using the same inputs as before.
    with ExitStack() as stack:
        servers = [managed_server(stack, shlex.split(c), d, *credentials) for c, d in zip(commands, roots)]
        identical("restart", [frame(socket(stack, *sv, True), "restart", GET, {}) for sv in servers])


def same_path_errors(commands, root):
    # settingsPath is on the wire. Reuse the absolute path between isolated runs.
    def capture(command):
        root.mkdir()
        state = root / "userdata"
        state.mkdir()
        invalid = b'{"fastWorktreeCheckout":"no"}'
        (state / "settings.json").write_bytes(invalid)
        with ExitStack() as stack:
            base, cookie = managed_server(stack, shlex.split(command), root,
                                          secrets.token_urlsafe(32), secrets.token_bytes(32))
            ws = socket(stack, base, cookie, True)
            frames = [frame(ws, "invalid-read", GET, {}),
                      frame(ws, "invalid-write", UPDATE, {"patch": {"defaultAutoPull": True}})]
            fallback = json.loads(frames[0])["exit"]["value"]
            assert fallback["continueThreadsAfterServerUpdate"] is False
            assert fallback["usageHistoryRetentionDays"] == "unlimited"
            assert disk(state) == invalid
            frames.extend(malformed_settings(ws, state, frame))
            (state / "settings.json").write_text('{"worktreesDirectory":"/repaired"}')
            frames.append(frame(ws, "repair", UPDATE, {"patch": {"defaultAutoPull": True}}))
            for kind, config in (("future", {}), ("one_password", {"account": "-invalid"})):
                frames.append(frame(ws, "bad-provider", UPDATE, {"patch": {"secretProviders": {
                    "vault": {"kind": kind, "label": "test", "config": config}}}}))
            assert all(json.loads(f)["exit"]["_tag"] == "Failure" for f in frames[-2:])
            # Disk and wire use different representations for non-finite durations.
            for special in ("NaN", "Infinity", "-Infinity"):
                (state / "settings.json").write_text("{}")
                frames.append(frame(ws, "duration-" + special, UPDATE, {"patch":{"automaticGitFetchInterval":special}}))
                assert json.loads(frames[-1])["exit"]["_tag"]=="Success"
                frames.append(disk(state))
            file = state / "settings.json"
            file.unlink()
            file.mkdir()
            frames.append(frame(ws,"filesystem-read-error",UPDATE,{"patch":{}}))
            assert json.loads(frames[-1])["exit"]["_tag"]=="Failure"
        shutil.rmtree(root)
        return frames
    for pair in zip(*(capture(c) for c in commands)):
        identical("same-path settings error/recovery", pair)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    args = parser.parse_args()
    commands = args.original_command, args.rust_command
    initial = '''{
      // Legacy settings and JSONC syntax.
      "environmentIcon":"future", "defaultThreadEnvMode":"future", "hiddenThreadTags":[1,"work"],
      "worktreesDirectory":" /fixture ", "sourceControlWritingStyle":" house style ",
      "responseStreamingMode":"token","cloudSpeechCredentials":{"openai":"fixture-test-key-only"},
      "secretProviders":{"unknown":{"label":"Future","kind":"other","config":{"password":"opaque-fixture-key"}}},
    }'''
    with TemporaryDirectory() as directory:
        root = Path(directory)
        paired(commands, root / "fresh")
        paired(commands, root / "seeded", initial)
        same_path_errors(commands, root / "common")
    run_migration(*commands)
    run_secrets(*commands)
    run_policy(*commands)
    print("Matched raw settings frames, persisted JSON, secret migrations, restarts and typed errors.")


if __name__ == "__main__":
    main()
