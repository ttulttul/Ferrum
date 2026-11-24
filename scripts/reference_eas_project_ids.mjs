// The pinned reference's answers for finding a workspace's Expo project (findEasProjectId in
// apps/server/src/device/easProjectId.ts) over a corpus of workspaces: the reference's own test
// cases and variants of each rule its static scan of app configs follows. Rust's port
// (src/local_services/eas_project_id.rs) is tested against every answer.
//
//   node scripts/reference_eas_project_ids.mjs <reference checkout>  > eas_project_ids.json
//
// A file is a string, `{ "hex": ... }` for bytes that are not UTF-8, `{ "pad": [text, length,
// tail] }` for text padded with spaces to `length` UTF-16 units and then `tail`, or
// `{ "directory": true }` for a directory where a file would be.
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import * as NodePath from "node:path";
import { pathToFileURL } from "node:url";

const server = NodePath.join(NodePath.resolve(process.argv[2]), "apps/server");
const requireFromServer = createRequire(NodePath.join(server, "package.json"));
const fromServer = (specifier) => import(pathToFileURL(requireFromServer.resolve(specifier)).href);
const Effect = await fromServer("effect/Effect");
const Option = await fromServer("effect/Option");
const NodeServices = await fromServer("@effect/platform-node/NodeServices");
const { findEasProjectId } = await import(
  pathToFileURL(NodePath.join(server, "src/device/easProjectId.ts")).href
);

const A = "d763fcb8-d37c-41ea-a773-b54a0ab4a454";
const B = "54d70eaf-b0a3-477a-a9cd-2cf81a5c84c9";
const appJson = (id) => JSON.stringify({ expo: { extra: { eas: { projectId: id } } } });
const config = (source, json = appJson(A)) => ({ "app.config.js": source, "app.json": json });
const literal = (source) => ({ "app.config.js": source });

const cases = [
  // The reference's tests (easProjectId.test.ts).
  ["a literal project id in an app config below apps", {
    "apps/mobile/app.config.ts": `import { writeFileSync } from "node:fs";
      writeFileSync("/nonexistent/config-ran", "");
      export default { extra: { eas: { projectId: "${A}" } } };`,
  }],
  ["app.json alone", { "app.json": appJson(B) }],
  ["a computed project id", {
    "app.config.ts": `const parts = ${JSON.stringify(A.split("-"))};
      export default { extra: { eas: { projectId: parts.join("-") } } };`,
  }],
  ["one project id whatever else the config holds", {
    "app.config.js": `export default {
        assetBundlePatterns: ["**/*"],
        extra: { eas: { projectId: "${A}" }, firebase: { projectId: "my-app-12345" } },
      };
      /** docs */`,
    "app.json": appJson(A),
  }],
  ["a project only some builds use", {
    "app.config.ts": `export default ({ config }) =>
        process.env.APP_VARIANT === "preview"
          ? { ...config, extra: { ...config.extra, eas: { projectId: "${B}" } } }
          : config;`,
    "app.json": appJson(A),
  }],
  ["a comment keeping an old project id", literal(`// projectId: "${B}" (the old production project)
        export default { extra: { eas: { projectId: "${A}" } } };`)],
  ["one app sets its project from a variable", {
    "apps/mobile/app.config.ts": `export default {
        extra: { eas: { projectId: process.env.EAS_PROJECT_ID } },
      };`,
    "apps/kiosk/app.json": appJson(B),
  }],
  ["the id changed after naming it", literal(`export default ({ config }) => {
        config.extra = { eas: { projectId: "${A}" } };
        if (process.env.APP_ENV === "development") {
          config.extra.eas.projectId = process.env.DEV_PROJECT_ID;
        }
        return config;
      };`)],
  ["a spread after the project id", {
    "apps/mobile/app.config.js": `import { eas } from "./app.shared.js";
        export default { extra: { eas: { projectId: "${A}", ...eas } } };`,
  }],
  ["assigned over after naming the id", literal(`export default ({ config }) => {
        config.extra = { eas: { projectId: "${A}" } };
        Object.assign(config.extra.eas, JSON.parse(process.env.EAS_OVERRIDES ?? "{}"));
        return config;
      };`)],
  ["another file spread over app.json", config(`module.exports = ({ config }) => ({ ...config, ...require("./app.local.js") });`)],
  ["a dynamic config that spreads nothing", config(`export default { name: "Mobile", slug: "mobile" };`)],
  ["more extra entries than are read", config(`module.exports = ({ config }) => ({ ...config, extra: { ...config.extra, ${"extra: { ...config.extra, ".repeat(40)}${"}".repeat(40)} } });`)],
  ["extra and eas entries that spread their own", config(`module.exports = ({ config }) => ({
        ...config,
        extra: { ...config?.["extra"], eas: { ...config.extra?.eas, build: { experimental: true } }, other: 1 },
      });`)],
  ["a typed variant config spreading parts", config(`/** @param {import('expo/config').ConfigContext} context */
      module.exports = ({ config }) => ({
        ...config,
        ios: { ...config.ios, infoPlist: { ...config?.ios.infoPlist, CFBundleDisplayName: "Dev" } },
        plugins: [...config.plugins, "expo-dev-client"],
      });`)],
  ["a config typed in JSDoc and line comments", config(`// Types: import('expo/config').ExpoConfig
      /**
       * The dev variant of the app.
       * @param {import('expo/config').ConfigContext} context
       */
      module.exports = ({ config }) => ({ ...config, name: "Dev" });`)],
  ["a part spread over the top", config(`module.exports = ({ config }) => ({ ...config, ...config.preview });`,
    JSON.stringify({ expo: { extra: { eas: { projectId: A } }, preview: { extra: { eas: { projectId: B } } } } }))],
  ["a part wrapped under expo", config(`module.exports = ({ config }) =>
        process.env.APP_VARIANT === "preview" ? { expo: { ...config.preview } } : { ...config };`)],
  ["a part spread into an expo entry", config(`module.exports = ({ config }) => ({ ...config, expo: { ...config.preview.expo } });`)],
  ["a part spread into a computed key", config(`module.exports = ({ config }) => ({ ...config, [[...config.keys]]: config.preview });`)],
  ["a template line that looks like a comment", config(`module.exports = ({ config }) => {
        const banner = \`
/* \${(config.extra = config.preview.extra)} */\`;
        return { ...config, name: banner };
      };`)],
  ["a project id that follows spreads", literal(`export default ({ config }) => ({
        ...config,
        extra: { ...config.extra, eas: { ...(config.extra?.eas ?? {}), projectId: "${A}" } },
      });`)],
  ["an app.json only JSON5 reads", {
    "apps/mobile/app.json": `{
        // the mobile app
        "expo": { "extra": { "eas": { "projectId": "${B}" } }, },
      }`,
    "apps/admin/app.json": appJson(A),
  }],
  ["only the first static config", { "app.config.json": JSON.stringify({ expo: { name: "Mobile" } }), "app.json": appJson(A) }],
  ["an unwrapped static config and a wrapped one", {
    "apps/mobile/app.config.json": JSON.stringify({ extra: { eas: { projectId: A } } }),
    "apps/kiosk/app.json": appJson(B),
  }],
  ["app.json when the config never mentions a project", { "app.config.js": `export default ({ config }) => ({ ...config, name: "Mobile" });`, "app.json": appJson(B) }],
  ["a non-LF line terminator inside a joined id", {
    "apps/mobile/app.config.ts": `export default { extra: { eas: { projectId: "${A.slice(0, 9)}" // prefix + "${A.slice(9)}" } } };`,
    "apps/kiosk/app.json": appJson(B),
  }],
  ["an unreadable app.json", { "apps/two/app.json": appJson(B), "apps/one/app.json": { directory: true } }],
  ["an unreadable app.config.json", { "apps/two/app.json": appJson(B), "apps/one/app.config.json": { directory: true } }],
  ["apps naming different projects", { "apps/one/app.json": appJson(A), "apps/two/app.json": appJson(B) }],
  ["no app", { "README.md": "no app" }],

  // Where apps are: the root, and directories (only) below apps/ and packages/.
  ["the same project in several apps", { "apps/one/app.json": appJson(A), "packages/two/app.json": appJson(A.toUpperCase()), "app.json": appJson(A) }],
  ["a file below apps is not an app", { "apps/app.json": appJson(B), "app.json": appJson(A) }],
  ["apps two levels down are not read", { "apps/group/one/app.json": appJson(B), "app.json": appJson(A) }],
  ["apps is a file", { apps: "not a directory", "app.json": appJson(A) }],
  ["an unreadable dynamic config", { "app.config.ts": { directory: true }, "app.json": appJson(A) }],
  ["the first dynamic config only", { "app.config.ts": `export default {};`, "app.config.js": `export default { extra: { eas: { projectId: "${A}" } } };` }],
  ["an .mts config", { "app.config.mts": `export default { extra: { eas: { projectId: "${A}" } } };` }],
  ["a .cjs config", { "app.config.cjs": `module.exports = { extra: { eas: { projectId: "${A}" } } };` }],

  // Quoted entries.
  ["single quotes", literal(`export default { extra: { eas: { projectId: '${A}' } } };`)],
  ["a template literal", literal(`export default { extra: { eas: { projectId: \`${A}\` } } };`)],
  ["an interpolating template", literal(`export default { extra: { eas: { projectId: \`\${process.env.ID}\` } } };`)],
  ["a quoted key", literal(`export default { "extra": { "eas": { "projectId": "${A}" } } };`)],
  ["a single-quoted key", literal(`export default { extra: { eas: { 'projectId' : '${A}' } } };`)],
  ["mismatched quotes", literal(`export default { extra: { eas: { projectId: "${A}' } } };`)],
  ["an upper-case key", literal(`export default { extra: { eas: { PROJECTID: "${A}" } } };`)],
  ["an upper-case id", literal(`export default { extra: { eas: { projectId: "${A.toUpperCase()}" } } };`)],
  ["a non-ASCII letter folding to the key", literal(`export default { extra: { eas: { projectıd: "${A}" } } };`)],
  ["a key inside a longer name", literal(`export default { extra: { eas: { myprojectId: "${A}" } } };`)],
  ["a mention as part of a longer name", literal(`const projectIds = 1; export default { extra: { eas: { projectId: "${A}" } } };`)],
  ["a mention with an underscore suffix", literal(`const projectId_x = 1; export default { extra: { eas: { projectId: "${A}" } } };`)],
  ["the id at the end of the file", literal(`export default { extra: { eas: { projectId: "${A}"`)],
  ["a semicolon after the id", literal(`const x = { projectId: "${A}" };`)],
  ["a bracket after the id", literal(`const x = [{ projectId: "${A}"}]`)],
  ["a parenthesis after the id", literal(`f({ projectId: "${A}"})`)],
  ["a block comment before the separator", literal(`export default { extra: { eas: { projectId: "${A}" /* prod */ } } };`)],
  ["an unclosed block comment", literal(`export default { extra: { eas: { projectId: "${A}" /* prod } } };`)],
  ["a line comment before the separator", literal(`export default { extra: { eas: { projectId: "${A}" // prod
      } } };`)],
  ["a line comment ending the file", literal(`export default { extra: { eas: { projectId: "${A}" // prod`)],
  ["a CR after a line comment", literal(`export default { extra: { eas: { projectId: "${A}" // prod\r} } };`)],
  ["a U+2029 after a line comment", literal(`export default { extra: { eas: { projectId: "${A}" // prod } } };`)],
  ["a no-break space before the separator", literal(`export default { extra: { eas: { projectId: "${A}" } } };`)],
  ["a byte order mark before the separator", literal(`export default { extra: { eas: { projectId: "${A}"﻿} } };`)],
  ["a next-line control before the separator", literal(`export default { extra: { eas: { projectId: "${A}"\u0085} } };`)],
  ["an operator after the id", literal(`export default { extra: { eas: { projectId: "${A}" + "" } } };`)],
  ["a ternary after the id", literal(`export default { extra: { eas: { projectId: "${A}" ? "${A}" : "${B}" } } };`)],
  ["a newline inside the quotes", literal(`export default { extra: { eas: { projectId: "${A}\n" } } };`)],
  ["a non-UUID quoted id", literal(`export default { extra: { eas: { projectId: "my-project" } } };`)],
  ["a non-UUID id with a spread config", config(`module.exports = ({ config }) => ({ ...config, extra: { ...config.extra, eas: { ...config.extra.eas, projectId: "dev" } } });`)],
  ["two different literal ids", literal(`export default { a: { projectId: "${A}" }, b: { projectId: "${B}" } };`)],
  ["the same literal id twice", literal(`export default { a: { projectId: "${A}" }, b: { projectId: "${A.toUpperCase()}" } };`)],
  ["a literal id and a different app.json", { "app.config.js": `export default { extra: { eas: { projectId: "${A}" } } };`, "app.json": appJson(B) }],
  ["32 mentions", literal(`export default { extra: { eas: { projectId: "${A}" } } };\n${`/* projectId: "${A}", */\n`.repeat(31)}`)],
  ["33 mentions", literal(`export default { extra: { eas: { projectId: "${A}" } } };\n${`/* projectId: "${A}", */\n`.repeat(32)}`)],
  ["a config at the length limit", literal({ pad: [`export default { extra: { eas: { projectId: "${A}" } } };`, 256 * 1024, ""] })],
  ["a config over the length limit", literal({ pad: [`export default { extra: { eas: { projectId: "${A}" } } };`, 256 * 1024 + 1, ""] })],
  ["a config at the limit counted in UTF-16", literal({ pad: [`export default { extra: { eas: { projectId: "${A}" } } }; //`, 256 * 1024 - 2, "😀"] })],
  ["a config over the limit counted in UTF-16", literal({ pad: [`export default { extra: { eas: { projectId: "${A}" } } }; //`, 256 * 1024 - 1, "😀"] })],

  // Assignments and loads that may replace the id.
  ["Object.assign spaced", literal(`export default { extra: { eas: { projectId: "${A}" } } }; Object . assign({}, {});`)],
  ["a bare assign call", literal(`export default { extra: { eas: { projectId: "${A}" } } }; assign (a, b);`)],
  ["assignment as a word inside another", literal(`export default { extra: { eas: { projectId: "${A}" } } }; reassign(a);`)],
  ["a comparison with extra", literal(`export default { extra: { eas: { projectId: "${A}" } } }; if (a.extra == b) {}`)],
  ["an assignment to eas", literal(`export default { extra: { eas: { projectId: "${A}" } } }; x.eas = {};`)],
  ["an assignment to a bracketed eas", literal(`export default { extra: { eas: { projectId: "${A}" } } }; x[ 'eas' ] = {};`)],
  ["an arrow after extra", literal(`export default { extra: { eas: { projectId: "${A}" } } }; const f = (o) => o.extra => 1;`)],
  ["a delete", literal(`export default { extra: { eas: { projectId: "${A}" } } }; delete x.y;`)],
  ["deleted as a word", literal(`export default { extra: { eas: { projectId: "${A}" } } }; const deleted = 1;`)],
  ["a require", literal(`export default { extra: { eas: { projectId: "${A}" } } }; const x = require ("./x");`)],
  ["a dynamic import", literal(`export default { extra: { eas: { projectId: "${A}" } } }; await import("./x.js");`)],
  ["an import of Expo's config type", literal(`/** @type {import("expo/config").ExpoConfig} */ export default { extra: { eas: { projectId: "${A}" } } };`)],
  ["a spaced import of Expo's config type", literal(`/** @type {import ( 'expo/config' ) . ExpoConfig} */ export default { extra: { eas: { projectId: "${A}" } } };`)],
  ["an import of Expo's config with mismatched quotes", literal(`/** @type {import("expo/config').ExpoConfig} */ export default { extra: { eas: { projectId: "${A}" } } };`)],
  ["an import of Expo's config used lower-case", literal(`/** @type {import("expo/config").default} */ export default { extra: { eas: { projectId: "${A}" } } };`)],
  ["a static import", literal(`import x from "./x"; export default { extra: { eas: { projectId: "${A}" } } };`)],
  ["a later spread anywhere", literal(`export default { extra: { eas: { projectId: "${A}" } } }; const y = [...x];`)],
  ["a spread only before the id", literal(`const y = [...x]; export default { extra: { eas: { projectId: "${A}" } } };`)],

  // Spreads of config, its parts and its holders (app.json's id carried through).
  ["a spread of config with a comment", config(`module.exports = ({ config }) => ({ .../* all */ config, name: "x" });`)],
  ["a spread of config in parentheses", config(`module.exports = ({ config }) => ({ ...(config), name: "x" });`)],
  ["a spread of config with a line comment", config(`module.exports = ({ config }) => ({ ... // all\n config, name: "x" });`)],
  ["a spread of a longer name", config(`module.exports = ({ configs }) => ({ ...configs });`)],
  ["a spread of config$", config(`module.exports = ({ config$ }) => ({ ...config$ });`)],
  ["a spread of a config with a non-ASCII letter", config(`module.exports = ({ configé }) => ({ ...configé });`)],
  ["a spread of config with an escape", config(`module.exports = ({ config }) => ({ ...config\\u0041 });`)],
  ["a spread of a config call", config(`module.exports = ({ config }) => ({ ...config() });`)],
  ["a spread of config optional", config(`module.exports = ({ config }) => ({ ...config?.x });`)],
  ["a spread of config with a default", config(`module.exports = ({ config }) => ({ ...config ?? {} });`)],
  ["a spread of config conditional", config(`module.exports = ({ config }) => ({ ...config ? config : {} });`)],
  ["a spread of config indexed", config(`module.exports = ({ config }) => ({ ...config[0] });`)],
  ["a spread of config.extra at the top", config(`module.exports = ({ config }) => ({ ...config, ...config.extra });`)],
  ["a spread of config.extra.eas at the top", config(`module.exports = ({ config }) => ({ ...config, ...config.extra.eas });`)],
  ["a spread of config.extra.other", config(`module.exports = ({ config }) => ({ ...config, extra: { ...config.extra.other } });`)],
  ["a spread of config.extraStuff", config(`module.exports = ({ config }) => ({ ...config, extraStuff: { ...config.extraStuff } });`)],
  ["a part spread under another key", config(`module.exports = ({ config }) => ({ ...config, android: { ...config.ios } });`)],
  ["a part spread under its own quoted key", config(`module.exports = ({ config }) => ({ ...config, "ios": { ...config.ios } });`)],
  ["a part spread second in its entry", config(`module.exports = ({ config }) => ({ ...config, ios: { a: 1, ...config.ios } });`)],
  ["a deep part spread", config(`module.exports = ({ config }) => ({ ...config, infoPlist: { ...config.ios.infoPlist } });`)],
  ["a part spread with comments", config(`module.exports = ({ config }) => ({ ...config, ios: { /* keep */ ...config.ios } });`)],
  ["a part spread after a newline key", config(`module.exports = ({ config }) => ({ ...config,\n  ios:\n  {\n    ...config.ios } });`)],
  ["a part with a digit-led name", config(`module.exports = ({ config }) => ({ ...config, x1: { ...config.x1 } });`)],
  ["a part with a dollar name", config(`module.exports = ({ config }) => ({ ...config, $ios: { ...config.$ios } });`)],
  ["a part with a non-ASCII name", config(`module.exports = ({ config }) => ({ ...config, réglages: { ...config.réglages } });`)],
  ["a part opener after a non-brace", config(`module.exports = ({ config }) => ({ ...config, ios: f({ ...config.ios }) });`)],
  ["a part spread into a plugins array", config(`module.exports = ({ config }) => ({ ...config, plugins: [ ...config.plugins ] });`)],
  ["extra spread from config's extra in brackets", config(`module.exports = ({ config }) => ({ ...config, extra: { ...config["extra"] } });`)],
  ["extra spread from optional brackets", config(`module.exports = ({ config }) => ({ ...config, extra: { ...config?.['extra'] } });`)],
  ["extra spread from a spaced holder", config(`module.exports = ({ config }) => ({ ...config, extra: { ...config . extra } });`)],
  ["extra spread from a property of the holder", config(`module.exports = ({ config }) => ({ ...config, extra: { ...config.extra.x } });`)],
  ["extra spread from a holder with a default", config(`module.exports = ({ config }) => ({ ...config, extra: { ...(config.extra ?? {}) } });`)],
  ["extra spread from a holder called", config(`module.exports = ({ config }) => ({ ...config, extra: { ...config.extra() } });`)],
  ["extra spread from the whole config", config(`module.exports = ({ config }) => ({ ...config, extra: { ...config } });`)],
  ["extra not an object", config(`module.exports = ({ config }) => ({ ...config, extra: config.extra });`)],
  ["extra spreading second", config(`module.exports = ({ config }) => ({ ...config, extra: { a: 1, ...config.extra } });`)],
  ["eas spread from extra's holder", config(`module.exports = ({ config }) => ({ ...config, extra: { ...config.extra, eas: { ...config.extra } } });`)],
  ["eas spread in brackets", config(`module.exports = ({ config }) => ({ ...config, extra: { ...config.extra, eas: { ...config["extra"]["eas"] } } });`)],
  ["a quoted extra key", config(`module.exports = ({ config }) => ({ ...config, "extra": { ...config.extra } });`)],
  ["a computed extra key", config(`module.exports = ({ config }) => ({ ...config, ["extra"]: { ...config.extra } });`)],
  ["a computed extra key not spreading", config(`module.exports = ({ config }) => ({ ...config, ["extra"]: {} });`)],
  ["a shorthand extra", config(`module.exports = ({ config, extra }) => ({ ...config, extra });`)],
  ["a shorthand eas", config(`module.exports = ({ config, eas }) => ({ ...config, eas, name: "x" });`)],
  ["a member named extra", config(`module.exports = ({ config }) => ({ ...config, name: o.extra, x: 1 });`)],
  ["a word ending in extra", config(`module.exports = ({ config }) => ({ ...config, name: myextra });`)],
  ["a dollar before eas", config(`module.exports = ({ config }) => ({ ...config, name: $eas });`)],
  ["an unclosed comment after a spread", config(`module.exports = ({ config }) => ({ ...config, extra: { .../* x config.extra } });`)],
  ["a spread with no config at all", config(`module.exports = () => ({ ...defaults });`)],

  // Static configs.
  ["app.json with a byte order mark", { "app.json": "﻿" + appJson(A) }],
  ["a dynamic config with a byte order mark", literal(`﻿export default { extra: { eas: { projectId: "${A}" } } };`)],
  ["app.json that is not UTF-8", { "app.json": { hex: Buffer.from(`{"expo": {"name": "x`).toString("hex") + "ff" + Buffer.from(`", "extra": {"eas": {"projectId": "${A}"}}}}`).toString("hex") } }],
  ["a dynamic config that is not UTF-8", { "app.config.js": { hex: Buffer.from(`export default { extra: { eas: { projectId: "${A}" } } }; // `).toString("hex") + "ff" } }],
  ["app.json unwrapped", { "app.json": JSON.stringify({ extra: { eas: { projectId: A } } }) }],
  ["app.json with expo and a top-level extra", { "app.json": JSON.stringify({ expo: { name: "x" }, extra: { eas: { projectId: A } } }) }],
  ["app.json with an invalid top-level extra", { "app.json": JSON.stringify({ expo: { extra: { eas: { projectId: A } } }, extra: 5 }) }],
  ["app.json whose project id is a number", { "app.json": JSON.stringify({ expo: { extra: { eas: { projectId: 5 } } } }) }],
  ["app.json whose expo is null", { "app.json": JSON.stringify({ expo: null, extra: { eas: { projectId: A } } }) }],
  ["app.json whose expo is an array", { "app.json": JSON.stringify({ expo: [], extra: { eas: { projectId: A } } }) }],
  ["app.json whose extra is an array", { "app.json": JSON.stringify({ expo: { extra: [] } }) }],
  ["app.json that is an array", { "app.json": "[]" }],
  ["app.json that is null", { "app.json": "null" }],
  ["app.json that is a string", { "app.json": JSON.stringify(A) }],
  ["app.json with a non-UUID id", { "app.json": appJson("my-project") }],
  ["app.json with an id and a newline", { "app.json": appJson(`${A}\n`) }],
  ["app.json with duplicate keys", { "app.json": `{"expo": {"extra": {"eas": {"projectId": "${B}"}}}, "expo": {"extra": {"eas": {"projectId": "${A}"}}}}` }],
  ["app.json with trailing text", { "app.json": `${appJson(A)} x` }],
  ["app.json with surrounding whitespace", { "app.json": `\n ${appJson(A)} \n` }],
  ["an empty app.json", { "app.json": "" }],
  ["an empty app.config.json before app.json", { "app.config.json": "{}", "app.json": appJson(A) }],
];

const results = [];
for (const [name, files] of cases) {
  if (!files) throw new Error(`case ${name} has no files`);
  const root = mkdtempSync(NodePath.join(tmpdir(), "t3-eas-project-"));
  try {
    for (const [file, content] of Object.entries(files)) {
      const path = NodePath.join(root, file);
      mkdirSync(NodePath.dirname(path), { recursive: true });
      if (typeof content === "string") writeFileSync(path, content);
      else if (content.directory) mkdirSync(path, { recursive: true });
      else if (content.pad) writeFileSync(path, content.pad[0].padEnd(content.pad[1], " ") + content.pad[2]);
      else writeFileSync(path, Buffer.from(content.hex, "hex"));
    }
    const found = await Effect.runPromise(
      findEasProjectId(root).pipe(Effect.provide(NodeServices.layer)),
    );
    results.push({ case: name, files, expected: Option.getOrNull(found) });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}
process.stdout.write(`${JSON.stringify(results, null, 1)}\n`);
