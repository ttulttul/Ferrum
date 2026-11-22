#!/usr/bin/env node
// Decodes a thread projection (JSON on stdin) with the pinned reference's
// OrchestrationV2ThreadProjectionJson schema, the one its ProjectionStore
// decodes stored rows with, and prints every error. Exit 1 when any.
//
//   node scripts/decode_v2_projection.mjs <reference-checkout> < projection.json
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import path from "node:path";
import { readFileSync } from "node:fs";

const [reference] = process.argv.slice(2);
const contracts = path.join(path.resolve(reference), "packages/contracts");
const require = createRequire(path.join(contracts, "package.json"));
const load = (specifier) => import(pathToFileURL(require.resolve(specifier)).href);
const Schema = await load("effect/Schema");
const { OrchestrationV2ThreadProjectionJson } = await import(
  pathToFileURL(path.join(contracts, "src/orchestrationV2.ts")).href
);

const input = JSON.parse(readFileSync(0, "utf8"));
try {
  Schema.decodeUnknownSync(OrchestrationV2ThreadProjectionJson)(input, { errors: "all" });
  console.log("ok");
} catch (error) {
  console.log(String(error.message ?? error));
  process.exit(1);
}
