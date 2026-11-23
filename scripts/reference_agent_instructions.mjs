// The pinned reference's agent instructions, as its provider adapters send them: the orchestration
// sections, the browser and device tool text, the runtime and pull request linking text
// (buildRuntimeInstructions), and Claude Code's background-wait and send_to_user text. Rust sends
// them as they are, leaving out each line that names a tool it does not serve yet
// (src/agent_instructions.rs).
//
//   node scripts/reference_agent_instructions.mjs <reference checkout>  > instructions.json
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

const root = resolve(process.argv[2]);
const load = (path) => import(pathToFileURL(resolve(root, path)).href);
const orchestration = await load("packages/provider-core/src/server/orchestrationInstructions.ts");
const runtime = await load("packages/provider-core/src/server/runtimeInstructions.ts");

// buildRuntimeInstructions with a marker harness and no model: the sentence around them.
const MARK = "\u0000HARNESS\u0000";
const runtimeInfo = runtime.buildRuntimeInstructions({ harness: MARK, pullRequestLinking: false });
const [runtimeInfoPrefix, runtimeInfoSuffix] = runtimeInfo.split(`${MARK} harness`);

// Claude Code's own text is module-private in its adapter: read it from the source.
const claudeAdapter = readFileSync(
  resolve(root, "apps/server/src/orchestration-v2/Adapters/ClaudeAdapterV2.ts"),
  "utf8",
);
const constant = (name) => {
  const match = claudeAdapter.match(new RegExp(`const ${name} = \`([\\s\\S]*?)\`;`));
  if (match === null || match[1].includes("${")) {
    throw new Error(`${name} is not a plain template literal in ClaudeAdapterV2.ts`);
  }
  return match[1].replaceAll("\\`", "`");
};

const instructions = {
  orchestrationSections: [...orchestration.T3_CODE_ORCHESTRATION_SECTIONS],
  orchestration: orchestration.T3_CODE_ORCHESTRATION_INSTRUCTIONS,
  browserTools: orchestration.T3_CODE_BROWSER_TOOL_INSTRUCTIONS,
  deviceTools: orchestration.T3_CODE_DEVICE_TOOL_INSTRUCTIONS,
  deviceToolsWithToolSearch: orchestration.T3_CODE_DEVICE_TOOL_INSTRUCTIONS_WITH_TOOL_SEARCH,
  runtimeInfoPrefix,
  runtimeInfoSuffix,
  pullRequestLinking: runtime.PULL_REQUEST_LINKING_INSTRUCTIONS,
  claudeBackgroundWait: constant("CLAUDE_BACKGROUND_WAIT_INSTRUCTIONS"),
  claudeSendToUser: constant("CLAUDE_SEND_TO_USER_INSTRUCTIONS"),
};
process.stdout.write(`${JSON.stringify(instructions, null, 1)}\n`);
