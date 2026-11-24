#!/usr/bin/env python3
"""Compare the device MCP tools (device_list, device_open, device_screenshot and device_close) on the reference and Rust servers.

Runs both servers side by side on loopback with identical fakes:
- a fake Android SDK (ANDROID_HOME with platform-tools/adb, emulator/emulator,
  cmdline-tools/latest/bin/avdmanager) plus a fake ffmpeg reporting libx264;
- a fake expo-device-hub seeded into each server's tools directory, serving
  /readyz, /api/devices, /api/devices/boot and /api/devices/shutdown.

Four phases exercise both tools through a fake Codex provider turn calling /mcp:
1. off: default settings (device support off) -> both tools report agent device access is off.
2. ready: device support + agent access on -> device_list starts the fake hub, probes emulator
   camera flags, lists booted and unbooted AVDs, resolves a host by case-insensitive label,
   rejects an unknown host; device_close treats an unopened device as closed and rejects
   unknown hosts and blank ids.
3. sessions: two devices opened on the thread (one already booted, one booted through the fake
   hub) -> device_list shows both open sessions; device_close removes one without shutting it
   down, then closes and shuts down the second.
4. fallback: fake hub reports an avdmanager listing error -> both servers fall back to adb
   discovery (adb devices -l, shell getprop, emu avd name) plus emulator -list-avds.

Compared: every MCP tool answer, every request sent to the fake hub and fake Android SDK,
and that nothing escaped the harness's refusing proxy.
"""
import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import secrets
import re
import shlex
import stat
import subprocess
from tempfile import TemporaryDirectory
from time import monotonic, sleep

from client_thread_parity import FIXTURES, cli_launch, rpc, scenario_env, seed_turn
from parity_harness import REFUSED, assert_nothing_escaped, managed_server

# Startup fetches both servers attempt, which the harness's refusing proxy refuses.
STARTUP_FETCHES = {"raw.githubusercontent.com:443", "registry.npmjs.org:443"}

FAKE_HUB_SOURCE = r"""import http from "node:http";
import fs from "node:fs";

const args = process.argv.slice(2);
const flag = (name, fallback) => {
  const i = args.indexOf(name);
  return i >= 0 && args[i + 1] ? args[i + 1] : fallback;
};
const port = Number(flag("--port", "0"));
const host = flag("--host", "127.0.0.1");
const logFile = process.env.T3_FAKE_DEVICE_LOG;
const log = (entry) => {
  if (logFile) fs.appendFileSync(logFile, JSON.stringify(entry) + "\n");
};

const normalizedArgs = [];
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--port" && i + 1 < args.length) {
    normalizedArgs.push("--port", "<port>");
    i++;
  } else {
    normalizedArgs.push(args[i]);
  }
}
log({
  kind: "hub.start",
  args: normalizedArgs,
  emulatorExtraArgs: process.env.EXPO_DEVICE_HUB_EMULATOR_EXTRA_ARGS ?? null,
  hasAndroidHome: Boolean(process.env.ANDROID_HOME),
  forceColor: process.env.FORCE_COLOR ?? null,
  noColor: process.env.NO_COLOR ?? null,
});

let emulators = [
  {
    id: "emulator-5554",
    name: "Pixel_9",
    version: "Android 15.0",
    platform: "android",
    booted: true,
    physical: false,
  },
];

const readBody = (req) =>
  new Promise((resolve) => {
    let raw = "";
    req.on("data", (chunk) => (raw += chunk));
    req.on("end", () => {
      try {
        resolve(raw ? JSON.parse(raw) : {});
      } catch {
        resolve({});
      }
    });
  });

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url || "/", `http://${host}:${port}`);
  if (req.method === "GET" && url.pathname === "/readyz") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true }));
    return;
  }
  if (req.method === "GET" && url.pathname === "/api/devices") {
    log({ kind: "hub.http", method: "GET", path: "/api/devices" });
    const modeFile = process.env.T3_FAKE_HUB_MODE;
    if (modeFile && fs.existsSync(modeFile)) {
      const mode = JSON.parse(fs.readFileSync(modeFile, "utf8"));
      if (mode.avdmanagerFailed) {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(
          JSON.stringify({
            simulators: [],
            emulators: [],
            errors: [
              {
                message: "[android-utils] Failed to run `avdmanager list avd`",
                error: "Error: avdmanager exited 1\n    at probe (/fake/hub.js:1:1)",
              },
            ],
          }),
        );
        return;
      }
    }
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ simulators: [], emulators }));
    return;
  }
  if (req.method === "POST" && url.pathname === "/api/devices/boot") {
    const body = await readBody(req);
    log({ kind: "hub.http", method: "POST", path: "/api/devices/boot", body });
    const serial = "emulator-5556";
    if (!emulators.some((d) => d.id === serial)) {
      emulators.push({
        id: serial,
        name: body.name || body.id,
        version: "Android 15.0",
        platform: "android",
        booted: true,
        physical: false,
      });
    }
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true, id: serial, serial }));
    return;
  }
  if (req.method === "POST" && url.pathname.endsWith("/api/screenshot")) {
    const device = url.searchParams.get("device");
    log({ kind: "hub.http", method: "POST", path: url.pathname, device });
    if (device === "emulator-5556") {
      res.writeHead(500, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "capture failed" }));
      return;
    }
    // A 3x2 PNG with adb warnings around it, as `adb exec-out screencap -p` can print.
    const png = Buffer.from(
      "89504e470d0a1a0a0000000d49484452000000030000000208020000001216f14d0000001049444154789c63f8cfc0f01f0418000017f402fe5a7a2d7a0000000049454e44ae426082",
      "hex",
    );
    res.writeHead(200, { "content-type": "image/png" });
    res.end(Buffer.concat([Buffer.from("WARNING: multiple displays\n"), png, Buffer.from("\ntrailer")]));
    return;
  }
  if (req.method === "POST" && url.pathname === "/api/devices/shutdown") {
    const body = await readBody(req);
    log({ kind: "hub.http", method: "POST", path: "/api/devices/shutdown", body });
    emulators = emulators.filter((d) => d.id !== body.id);
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true }));
    return;
  }
  res.writeHead(404, { "content-type": "application/json" });
  res.end(JSON.stringify({ error: "not_found" }));
});

server.listen(port, host);
process.on("SIGTERM", () => server.close(() => process.exit(0)));
"""


FAKE_AGENT_DEVICE_SOURCE = r"""import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";

const args = process.argv.slice(2);
const logFile = process.env.T3_FAKE_DEVICE_LOG;
const log = (entry) => {
  if (logFile) fs.appendFileSync(logFile, JSON.stringify(entry) + "\n");
};
const stateDir = process.env.AGENT_DEVICE_STATE_DIR;

if (args[0] === "__daemon") {
  const dir = args[1];
  const token = "fake-daemon-token";
  const server = http.createServer((req, res) => {
    let raw = "";
    req.on("data", (chunk) => (raw += chunk));
    req.on("end", () => {
      const entry = { kind: "agent.http", method: req.method, path: req.url };
      if (req.method === "POST") {
        entry.authorized = req.headers.authorization === `Bearer ${token}`;
        try { entry.body = JSON.parse(raw); } catch { entry.body = raw; }
      }
      log(entry);
      if (req.url === "/health") {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ ok: true }));
      } else if (req.url === "/rpc") {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ jsonrpc: "2.0", id: entry.body?.id ?? null, result: { ok: true } }));
      } else {
        res.writeHead(404);
        res.end();
      }
    });
  });
  server.listen(0, "127.0.0.1", () => {
    fs.writeFileSync(
      path.join(dir, "daemon.json"),
      JSON.stringify({ httpPort: server.address().port, token, pid: process.pid, version: "0.21.41" }),
    );
  });
  // A backstop; the harness kills the daemon when each phase ends.
  setTimeout(() => process.exit(0), 300_000).unref();
} else {
  const env = {};
  for (const name of [
    "AGENT_DEVICE_DAEMON_SERVER_MODE",
    "AGENT_DEVICE_DAEMON_IDLE_TIMEOUT_MS",
    "AGENT_DEVICE_NO_UPDATE_NOTIFIER",
    "AGENT_DEVICE_DAEMON_BASE_URL",
    "AGENT_DEVICE_CONFIG",
    "ELECTRON_RUN_AS_NODE",
    "FORCE_COLOR",
    "NO_COLOR",
  ]) env[name] = process.env[name] ?? null;
  env.hasStateDir = Boolean(stateDir);
  env.hasAndroidHome = Boolean(process.env.ANDROID_HOME);
  log({ kind: "agent.cli", args, env });
  if (args[0] === "devices" && stateDir) {
    fs.mkdirSync(stateDir, { recursive: true });
    const file = path.join(stateDir, "daemon.json");
    if (!fs.existsSync(file)) {
      spawn(process.execPath, [process.argv[1], "__daemon", stateDir], {
        detached: true,
        stdio: "ignore",
        env: process.env,
      }).unref();
      const deadline = Date.now() + 10_000;
      while (!fs.existsSync(file) && Date.now() < deadline) {
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 50);
      }
    }
    console.log(JSON.stringify({ success: true, data: { devices: [] } }));
  } else if (args[0] === "daemon" && args[1] === "stop") {
    const dir = args[args.indexOf("--state-dir") + 1];
    try {
      const recorded = JSON.parse(fs.readFileSync(path.join(dir, "daemon.json"), "utf8"));
      process.kill(recorded.pid, "SIGTERM");
      fs.rmSync(path.join(dir, "daemon.json"), { force: true });
    } catch {}
  } else {
    console.log(JSON.stringify({ success: true, data: { command: args[0] ?? null } }));
  }
}
"""


def write_executable(path: Path, content: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def seed_fake_android_sdk(root: Path, log_path: Path):
    sdk = root / "android-sdk"
    bin_dir = root / "fake-bin"
    write_executable(
        sdk / "emulator" / "emulator",
        f"""#!/usr/bin/env python3
import json, sys
with open({str(log_path)!r}, "a") as f:
    f.write(json.dumps({{"kind": "sdk", "tool": "emulator", "args": sys.argv[1:]}}) + "\\n")
if sys.argv[1:] in (["-help-camera-front"], ["-help-camera-back"]):
    print("valid modes: emulated, webcam0")
elif sys.argv[1:] == ["-list-avds"]:
    print("Pixel_9")
    print("Pixel_Unbooted")
""",
    )
    write_executable(
        sdk / "platform-tools" / "adb",
        f"""#!/usr/bin/env python3
import json, sys
with open({str(log_path)!r}, "a") as f:
    f.write(json.dumps({{"kind": "sdk", "tool": "adb", "args": sys.argv[1:]}}) + "\\n")
args = sys.argv[1:]
if args == ["devices", "-l"]:
    print("List of devices attached")
    print("emulator-5554          device product:sdk_gphone64 model:Pixel_9")
elif args[-2:] == ["shell", "getprop"]:
    print("[ro.kernel.qemu]: [1]")
    print("[ro.build.version.release]: [15]")
elif args[-3:] == ["emu", "avd", "name"]:
    print("Pixel_9")
    print("OK")
""",
    )
    write_executable(
        sdk / "cmdline-tools" / "latest" / "bin" / "avdmanager",
        "#!/bin/sh\nexit 0\n",
    )
    write_executable(
        bin_dir / "ffmpeg",
        f"""#!/usr/bin/env python3
import json, sys
with open({str(log_path)!r}, "a") as f:
    f.write(json.dumps({{"kind": "bin", "tool": "ffmpeg", "args": sys.argv[1:]}}) + "\\n")
if "-encoders" in sys.argv[1:]:
    print("Encoders:")
    print(" V..... libx264              libx264 H.264 / AVC / MPEG-4 AVC / MPEG-4 part 10")
""",
    )
    return sdk, bin_dir


def seed_fake_hub(root: Path, version: str):
    install_dir = root / "tools" / "expo-device-hub" / version
    entry = install_dir / "node_modules" / "expo-device-hub" / "dist" / "server" / "cli.mjs"
    entry.parent.mkdir(parents=True, exist_ok=True)
    entry.write_text(FAKE_HUB_SOURCE)
    (install_dir / ".install-complete").write_text(f"{version}\n")


def tool_versions(reference_root: Path) -> dict:
    return json.loads((reference_root / "apps/server/src/device/deviceToolVersions.json").read_text())


def hub_version_for(reference_root: Path) -> str:
    return tool_versions(reference_root)["expo-device-hub"]


def seed_fake_agent_device(root: Path, version: str):
    install_dir = root / "tools" / "agent-device" / version
    entry = install_dir / "node_modules" / "agent-device" / "bin" / "agent-device.mjs"
    entry.parent.mkdir(parents=True, exist_ok=True)
    entry.write_text(FAKE_AGENT_DEVICE_SOURCE)
    (install_dir / ".install-complete").write_text(f"{version}\n")


def stop_fake_daemons(root: Path):
    """Kill fake agent-device daemons this phase started (they run detached, as the real one does)."""
    for daemon in root.rglob("device/agent-device/daemon.json"):
        try:
            os.kill(json.loads(daemon.read_text())["pid"], 15)
        except (OSError, ValueError, KeyError):
            pass


def wait_answers(log: Path, count: int, seconds: int = 120):
    calls = Path(f"{log}.mcp")
    deadline = monotonic() + seconds
    lines = []
    while monotonic() < deadline:
        lines = calls.read_text().splitlines() if calls.exists() else []
        if len(lines) >= count:
            break
        sleep(0.1)
    if len(lines) < count:
        raise RuntimeError(f"the fake CLI completed {len(lines)} of {count} calls")
    return [json.loads(line)["answer"] for line in lines]


def normalize_answer(answer: dict, home: Path):
    result = answer.get("result", answer)
    out = {"isError": result.get("isError"), "rpcError": answer.get("error")}
    parts = []
    for part in result.get("content", []):
        if part.get("type", "text") != "text":
            parts.append(part)
            continue
        text = part.get("text", "")
        try:
            parts.append(json.loads(text))
        except json.JSONDecodeError:
            parts.append(text)
    out["content"] = parts
    if "structuredContent" in result:
        out["structuredContent"] = result["structuredContent"]
    text = json.dumps(out, sort_keys=True)
    for side in ("original", "rust"):
        text = text.replace(f"{home}/{side}/", "<home>/<server>/")
    text = text.replace(str(home), "<home>")
    return json.loads(text)


NODE_IN_LAUNCHER = re.compile(r'const child = spawn\(("(?:[^"\\]|\\.)*")')


def launcher_files(root: Path):
    """Every agent-device launcher a server wrote, with the node it runs as `<node>`."""
    files = {}
    for path in sorted(root.rglob("device/bin/*")) + sorted(root.rglob("device/targets/*/*")):
        text = path.read_text()
        match = NODE_IN_LAUNCHER.search(text)
        if match:
            text = text.replace(match.group(1), '"<node>"')
        if path.name == "agent-device":
            node = text.split("\n", 2)[1].split(" ")[2]
            text = text.replace(node, "'<node>'")
        files[str(path.relative_to(root))] = text
    return files


def run_phase(name, phase, command, reference_root, directory, plan, *, enable_devices, open_devices=(), avdmanager_failed=False, run_launcher=False, list_launchers=False, configure=None, provider="codex", launch=False):
    root = Path(directory, name, phase)
    root.mkdir(parents=True, exist_ok=True)
    request_log = root / "fake-requests.jsonl"
    sdk, fake_bin = seed_fake_android_sdk(root, request_log)
    seed_fake_hub(root, hub_version_for(reference_root))
    seed_fake_agent_device(root, tool_versions(reference_root)["agent-device"])
    hub_mode_file = root / "hub-mode.json"
    if avdmanager_failed:
        hub_mode_file.write_text(json.dumps({"avdmanagerFailed": True}))

    claude = provider == "claude"
    scenario = {
        "name": f"device tools {phase}",
        "instance": "claudeAgent" if claude else "codex",
        "model": "claude-opus-5-5" if claude else "fixture",
        "scenario": "claude_turn.json" if claude else "codex_turn.json",
    }
    env, _, log = scenario_env(Path(directory, name), phase, root, scenario, {provider})
    env["ANDROID_HOME"] = str(sdk)
    env.pop("ANDROID_SDK_ROOT", None)
    env["PATH"] = f"{fake_bin}{os.pathsep}{env['PATH']}"
    env["T3_FAKE_DEVICE_LOG"] = str(request_log)
    env["T3_FAKE_HUB_MODE"] = str(hub_mode_file)
    env.pop("EXPO_DEVICE_HUB_EMULATOR_EXTRA_ARGS", None)
    env.pop("SERVE_EMU_FFMPEG", None)

    turn = json.loads((FIXTURES / scenario["scenario"]).read_text())
    started = next(
        i for i, step in enumerate(turn["steps"])
        if step.get("send", {}).get("method") == "turn/started"
        or step.get("send", {}).get("subtype") == "init"
    )
    steps = [{"mcp": {"tool": tool, "arguments": arguments}} for tool, arguments in plan]
    if claude:
        # Claude Code calls the hooks the server registered: SubagentStart (hook_0, only with the
        # device capability) as a native subagent starts, and Stop (after it) as the turn ends.
        hooks = [("SubagentStart", {"agent_id": "agent-1", "agent_type": "general-purpose"})] if enable_devices else []
        hooks.append(("Stop", {"stop_hook_active": False}))
        steps[:0] = [
            step
            for index, (event, extra) in enumerate(hooks)
            for step in (
                {"send": {"type": "control_request", "request_id": f"hook-{event}", "request": {
                    "subtype": "hook_callback", "callback_id": f"hook_{index}", "tool_use_id": None,
                    "input": {"hook_event_name": event, "session_id": "s", **extra}}}},
                {"expect": "control_response"},
            )
        ]
    turn["steps"][started + 1 : started + 1] = steps
    fixture = root / "device-turn.json"
    fixture.write_text(json.dumps(turn))
    env["FAKE_PROVIDER_SCENARIO"] = str(fixture)

    with ExitStack() as stack:
        url, cookie = managed_server(
            stack,
            shlex.split(command),
            root,
            secrets.token_urlsafe(32),
            secrets.token_bytes(32),
            env=env,
        )
        if enable_devices:
            rpc(
                url,
                cookie,
                "server.updateSettings",
                {"patch": {"enableDeviceSupport": True, "enableAgentDeviceAccess": True}},
            )
        seeded = seed_turn(url, cookie, Path(directory, name, f"{phase}-checkouts"), scenario)
        for device_id in open_devices:
            rpc(
                url,
                cookie,
                "device.open",
                {
                    "threadId": seeded["threadId"],
                    "hostId": "local",
                    "deviceId": device_id,
                    "platform": "android",
                },
            )
        if configure:
            rpc(url, cookie, "device.configure", configure[0])
        rpc(url, cookie, "orchestration.dispatchCommand", seeded["dispatch"])
        raw_answers = wait_answers(log, len(plan))
        answers = [normalize_answer(a, Path(directory)) for a in raw_answers]
        if launch:
            # The agent instructions the CLI was launched with (Codex's turn context, Claude Code's
            # system prompt append), compared as the thread suite compares them.
            answers.append(cli_launch(log, Path(directory)))
            plan = [*plan, ("instructions", {})]
        if claude:
            # Claude Code's initialize hooks and the server's answers to their callbacks.
            seen = []
            for line in Path(f"{log}.messages").read_text().splitlines():
                message = json.loads(json.loads(line)["line"])
                request = message.get("request") or {}
                if request.get("subtype") == "initialize":
                    seen.append({"initialize hooks": request.get("hooks")})
                if message.get("type") == "control_response":
                    seen.append({"control_response": message.get("response")})
            answers.append(seen)
            plan = [*plan, ("hooks", {})]
        launched = None
        if configure:
            # Turning agent access off stops the daemon with its own CLI.
            rpc(url, cookie, "device.configure", configure[1])
            deadline = monotonic() + 15
            while monotonic() < deadline and not any(
                '"stop"' in line for line in request_log.read_text().splitlines()
            ):
                sleep(0.1)
        if run_launcher:
            # Run the first launcher device_open returned, as an agent would.
            opened = next(
                json.loads(a["result"]["content"][0]["text"])
                for (tool, _), a in zip(plan, raw_answers)
                if tool == "device_open" and not a["result"].get("isError")
            )
            launcher = opened["agentDevice"]["command"]
            if Path(launcher).exists():
                # parity-env: the agent-device launcher a server wrote, which runs the fake agent-device
                done = subprocess.run(
                    [launcher, "snapshot", "-i"], capture_output=True, text=True, timeout=60,
                    env={k: v for k, v in env.items() if not k.startswith("AGENT_DEVICE_")},
                )
                launched = {"code": done.returncode, "stdout": done.stdout, "stderr": done.stderr}
            else:
                launched = {"missing": True}
            launched["files"] = launcher_files(root)
        elif list_launchers:
            launched = {"files": launcher_files(root)}
        # Snapshot fake requests before ExitStack sends SIGTERM to the process group
        # (which stops the hub child simultaneously and can trigger the reference's hub supervisor).
        requests = (
            [json.loads(line) for line in request_log.read_text().splitlines() if line.strip()]
            if request_log.exists()
            else []
        )

    stop_fake_daemons(root)
    text = json.dumps(requests)
    for side in ("original", "rust"):
        text = text.replace(f"{Path(directory)}/{side}/", "<home>/<server>/")
    requests = json.loads(text.replace(str(Path(directory)), "<home>"))
    if launched is not None:
        text = json.dumps(launched)
        for side in ("original", "rust"):
            text = text.replace(f"{Path(directory)}/{side}/", "<home>/<server>/")
        answers.append(json.loads(text.replace(str(Path(directory)), "<home>")))
        plan = [*plan, ("launcher", {})]
    return [f"{phase}:{t}" for t, _ in plan], answers, requests


def run_server(name, command, reference_root, directory):
    phases = [
        (
            "off",
            [("device_list", {}), ("device_open", {}), ("device_screenshot", {}), ("device_close", {})],
            dict(enable_devices=False, launch=True),
        ),
        (
            "ready",
            [
                ("device_list", {}),
                ("device_list", {"hostId": "THIS MACHINE"}),
                ("device_list", {"hostId": "unknown-host"}),
                ("device_close", {"deviceId": "Pixel_9"}),
                ("device_close", {"hostId": "unknown-host"}),
                ("device_close", {"deviceId": "   "}),
            ],
            dict(enable_devices=True, launch=True),
        ),
        (
            "sessions",
            [
                ("device_list", {}),
                ("device_close", {"deviceId": "emulator-5554"}),
                ("device_list", {}),
                ("device_close", {"deviceId": "emulator-5556", "shutdown": True}),
                ("device_list", {}),
            ],
            dict(enable_devices=True, open_devices=("emulator-5554", "Pixel_Unbooted")),
        ),
        (
            "agent",
            [
                ("device_screenshot", {}),
                ("device_open", {}),
                ("device_open", {"deviceId": "emulator-5554", "hostId": "this machine"}),
                ("device_screenshot", {}),
                ("device_screenshot", {"deviceId": "emulator-5554", "hostId": "local"}),
                ("device_screenshot", {"deviceId": "   "}),
                ("device_open", {"deviceId": "nope"}),
                ("device_open", {"platform": "ios"}),
                ("device_list", {}),
            ],
            dict(enable_devices=True, run_launcher=True),
        ),
        (
            "agent-close",
            [
                ("device_open", {}),
                ("device_close", {"deviceId": "emulator-5554"}),
                ("device_screenshot", {"deviceId": "emulator-5554"}),
                ("device_open", {"deviceId": "Pixel_Unbooted"}),
                ("device_screenshot", {}),
                ("device_close", {"shutdown": True}),
                ("device_list", {}),
            ],
            dict(enable_devices=True, list_launchers=True),
        ),
        (
            "agent-access",
            [("device_list", {})],
            dict(
                enable_devices=True,
                configure=({"agentAccessEnabled": True}, {"agentAccessEnabled": False}),
            ),
        ),
        (
            "claude",
            [("device_list", {"hostId": "local"})],
            dict(enable_devices=True, provider="claude", launch=True),
        ),
        (
            "claude-off",
            [("device_list", {})],
            dict(enable_devices=False, provider="claude", launch=True),
        ),
        (
            "fallback",
            [("device_list", {})],
            dict(enable_devices=True, avdmanager_failed=True),
        ),
    ]
    only = os.environ.get("DEVICE_PARITY_PHASES")
    if only:
        phases = [entry for entry in phases if entry[0] in only.split(",")]
    labels, answers, requests = [], [], {}
    for phase, plan, kwargs in phases:
        p_labels, p_answers, p_reqs = run_phase(
            name, phase, command, reference_root, directory, plan, **kwargs
        )
        labels.extend(p_labels)
        answers.extend(p_answers)
        requests[phase] = p_reqs
    return labels, answers, requests


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--reference-root", type=Path, required=True)
    parser.add_argument("--dump", type=Path, help="write both servers' answers and fake requests as JSON")
    args = parser.parse_args()

    results = {}
    with TemporaryDirectory() as directory:
        for name, command in (("original", args.original_command), ("rust", args.rust_command)):
            labels, ans, reqs = run_server(name, command, args.reference_root, directory)
            results[name] = {"answers": ans, "requests": reqs}

    assert_nothing_escaped()
    refused = sorted({entry.split(" ", 1)[1] for entry in REFUSED} - STARTUP_FETCHES)
    if refused:
        print(f"FAIL requests besides startup fetches reached the refusing proxy: {refused}")
        return 1

    if args.dump:
        args.dump.write_text(json.dumps(results, indent=1, sort_keys=True))

    failures = 0
    for index, (label, left, right) in enumerate(
        zip(labels, results["original"]["answers"], results["rust"]["answers"], strict=True)
    ):
        same = left == right
        failures += not same
        print(f"{'PASS' if same else 'FAIL'} {index + 1} {label}")
        if not same:
            print(f"  original {json.dumps(left)[:3000]}\n  rust     {json.dumps(right)[:3000]}")

    for phase in results["original"]["requests"]:
        left_req = results["original"]["requests"][phase]
        right_req = results["rust"]["requests"][phase]
        req_same = left_req == right_req
        failures += not req_same
        print(f"{'PASS' if req_same else 'FAIL'} requests:{phase} ({len(left_req)} vs {len(right_req)})")
        if not req_same:
            print(f"  original {json.dumps(left_req, indent=1)}")
            print(f"  rust     {json.dumps(right_req, indent=1)}")

    total = len(labels) + len(results["original"]["requests"])
    print(f"{total - failures}/{total} device tool checks matched across the reference and the Rust server")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
