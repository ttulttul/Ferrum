import { spawn } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { connect } from "node:net";
const entry = {{ENTRY}};
const bound = {{BOUND}};
const refused = new Map([["cdp", ["--platform", "--udid", "--config", "--session"]], ["connection", ["--platform", "--udid"]]]);
const managed = ["connect", "disconnect", "auth", "daemon"];
const args = process.argv.slice(2);
const end = args.indexOf("--");
const options = end < 0 ? args : args.slice(0, end);
const valued = new Set({{VALUED}});
let name = "";
for (let index = 0; index < args.length; index++) {
  const arg = args[index];
  if (arg === "--") { name = args[index + 1] ?? ""; break; }
  if (!arg.startsWith("-")) { name = arg; break; }
  if (!arg.includes("=") && valued.has(arg)) index++;
}
const informational = args.length === 0 || ["help", "--help", "-h", "--version", "version"].includes(args[0]) || options.includes("--help") || options.includes("-h");
if (!informational && managed.includes(name)) {
  console.error("T3 Code manages this command's connection to the device, so `" + name + "` is not available through it.");
  process.exit(1);
}
if (!informational && name === "device") {
  console.error("`device status` reads this machine's device claims, not those on the device's host, so it is not available through this command. Quote the DEVICE_IN_USE error instead.");
  process.exit(1);
}
const skip = refused.get(name) ?? [];
if (!informational) for (let index = 0; index + 1 < bound.length; index += 2) if (!skip.includes(bound[index]) && !options.some(arg => arg === bound[index] || arg.startsWith(bound[index] + "="))) options.push(bound[index], bound[index + 1]);
const command = end < 0 ? options : [...options, ...args.slice(end)];
const hasValue = flag => { const index = options.indexOf(flag); return options.some(arg => arg.startsWith(flag + "=") && arg.length > flag.length + 1) || (index >= 0 && !!options[index + 1] && !options[index + 1].startsWith("--")); };
if (!informational && !skip.includes("--session") && !(hasValue("--config") && hasValue("--session"))) {
  console.error("Call device_open first and run the agent-device command it returns.");
  process.exit(1);
}
if (!existsSync(entry)) {
  console.error({{STALE}});
  process.exit(1);
}
const valueOf = flag => { const inline = options.find(arg => arg.startsWith(flag + "=")); if (inline) return inline.slice(flag.length + 1); const index = options.indexOf(flag); return index >= 0 ? options[index + 1] : undefined; };
const endpoint = () => { try { const url = new URL(valueOf("--daemon-base-url") ?? JSON.parse(readFileSync(valueOf("--config"), "utf8")).daemonBaseUrl); return ["127.0.0.1", "localhost", "[::1]"].includes(url.hostname) && url.port ? url : undefined; } catch { return undefined; } };
const local = informational || refused.has(name) ? undefined : endpoint();
if (local) {
  const failure = await new Promise(resolve => {
    const socket = connect({ host: local.hostname.replace(/^\[|\]$/g, ""), port: Number(local.port) });
    socket.setTimeout(3000, () => { socket.destroy(); resolve("ETIMEDOUT"); });
    socket.once("connect", () => { socket.destroy(); resolve(undefined); });
    socket.once("error", error => resolve(error.code ?? "ERROR"));
  });
  if (failure === "EPERM" || failure === "EACCES") {
    console.error("This shell cannot reach T3 Code's device connection at " + local.host + " (connect " + failure + "): a network sandbox is blocking localhost. Run this command again with network access, outside the sandbox.");
    process.exit(1);
  }
  if (failure) {
    console.error("T3 Code's device connection at " + local.host + " is not reachable from this shell (connect " + failure + "). Call device_open again to reconnect; if that succeeds and this repeats, this shell runs in a network sandbox, so run the command with network access, outside the sandbox.");
    process.exit(1);
  }
}
const env = { ...process.env, ELECTRON_RUN_AS_NODE: "1" };
delete env.AGENT_DEVICE_DAEMON_BASE_URL;
delete env.AGENT_DEVICE_DAEMON_AUTH_TOKEN;
delete env.AGENT_DEVICE_CONFIG;
const child = spawn({{NODE}}, [entry, ...command], { stdio: "inherit", env });
child.on("error", error => { console.error(error.message); process.exitCode = 1; });
child.on("exit", code => { process.exitCode = code ?? 1; });
