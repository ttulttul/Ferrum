// The pinned reference's SSH device host scripts, which Rust's SSH device hosts run as they are:
// the shell prelude every remote command starts with (`remoteDeviceEnvironment`), and the Node
// bootstrap it feeds the host's `node` on stdin (`remoteDeviceScript`), with its owner and mode
// left as `"{{OWNER}}"` and `"{{MODE}}"` (JSON strings) for Rust to fill.
//
//   node scripts/reference_device_scripts.mjs <reference checkout>  > device_ssh.json
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

const root = resolve(process.argv[2]);
const ssh = await import(
  pathToFileURL(resolve(root, "apps/server/src/device/sshDeviceScript.ts")).href
);
const script = ssh.remoteDeviceScript("{{OWNER}}", "{{MODE}}");
for (const hole of ['"{{OWNER}}"', '"{{MODE}}"']) {
  if (script.split(hole).length !== 2) {
    throw new Error(`remoteDeviceScript no longer embeds ${hole} exactly once`);
  }
}
const scripts = {
  environment: ssh.remoteDeviceEnvironment,
  script,
};
process.stdout.write(`${JSON.stringify(scripts, null, 1)}\n`);
