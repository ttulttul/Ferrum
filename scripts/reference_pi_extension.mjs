// The pinned reference's Pi extension (PI_T3_MCP_EXTENSION_SOURCE), which a Pi turn's process loads
// with `--extension`: T3's MCP tools, the runtime mode's approval hook, the system prompt
// instructions and the service tier command. Rust writes these exact bytes, as the reference
// rewrites a file that differs by any byte.
//
//   node scripts/reference_pi_extension.mjs <reference checkout>  > pi_extension.ts
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

const root = resolve(process.argv[2]);
const source = await import(
  pathToFileURL(resolve(root, "packages/provider-pi/src/server/mcpExtensionSource.ts")).href
);
process.stdout.write(source.PI_T3_MCP_EXTENSION_SOURCE);
