#!/usr/bin/env node
// Decodes every message the fake Codex CLI sends in tests/fixtures/providers/codex_*.json and
// status_codex_*.json (scripted steps and "replies" by method) with the pinned reference's
// app-server schemas, so the reference's client accepts what the parity suite replays to it.
//
//   node scripts/codex_fixture_schemas.mjs <reference-checkout>
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import fs from "node:fs";
import path from "node:path";

const [reference] = process.argv.slice(2);
if (!reference) {
  console.error("usage: codex_fixture_schemas.mjs <reference-checkout>");
  process.exit(2);
}
const pkg = path.join(path.resolve(reference), "packages/effect-codex-app-server");
const require = createRequire(path.join(pkg, "package.json"));
const Schema = await import(pathToFileURL(require.resolve("effect/Schema")).href);
const meta = await import(pathToFileURL(path.join(pkg, "src/_generated/meta.gen.ts")).href);
const fixtures = path.join(path.dirname(new URL(import.meta.url).pathname), "../tests/fixtures/providers");

let problems = 0;
for (const name of fs.readdirSync(fixtures).filter((file) => /^(status_)?codex_.*\.json$/.test(file)).sort()) {
  const scenario = JSON.parse(fs.readFileSync(path.join(fixtures, name), "utf8"));
  for (const [method, result] of Object.entries(scenario.replies ?? {})) {
    const schema = meta.CLIENT_REQUEST_RESPONSES[method];
    if (!schema) {
      problems += 1;
      console.log(`${name}: no reference schema for reply to ${method}`);
      continue;
    }
    try {
      Schema.decodeUnknownSync(schema)(result, { errors: "all" });
    } catch (error) {
      problems += 1;
      console.log(`${name}: reply to ${method} does not decode:\n${String(error.message).slice(0, 2000)}`);
    }
  }
  for (const steps of scenario.steps ? [scenario.steps] : scenario.variants.map((variant) => variant.steps)) {
    let expected = null;
    for (const step of steps) {
      if (step.expect) expected = step.expect;
      const message = step.send;
      if (!message) continue;
      let schema;
      let label;
      if (message.method && message.id !== undefined) {
        [schema, label] = [meta.SERVER_REQUEST_PARAMS[message.method], `request ${message.method}`];
      } else if (message.method) {
        [schema, label] = [meta.SERVER_NOTIFICATION_PARAMS[message.method], `notification ${message.method}`];
      } else if (message.result !== undefined) {
        [schema, label] = [meta.CLIENT_REQUEST_RESPONSES[expected], `reply to ${expected}`];
      } else {
        continue;
      }
      if (!schema) {
        problems += 1;
        console.log(`${name}: no reference schema for ${label}`);
        continue;
      }
      try {
        Schema.decodeUnknownSync(schema)(message.params ?? message.result, { errors: "all" });
      } catch (error) {
        problems += 1;
        console.log(`${name}: ${label} does not decode:\n${String(error.message).slice(0, 2000)}`);
      }
    }
  }
}
console.log(problems === 0 ? "every Codex fixture message decodes" : `${problems} Codex fixture messages do not decode`);
process.exit(problems === 0 ? 0 : 1);
