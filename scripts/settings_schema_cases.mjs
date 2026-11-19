// Generate an independent corpus with the pinned server's actual settings codecs.
import { createRequire } from "node:module";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
const root = resolve(process.argv[2]);
const require = createRequire(resolve(root, "packages/contracts/package.json"));
const S = await import(pathToFileURL(require.resolve("effect/Schema")));
const { ServerSettings } = await import(
  pathToFileURL(resolve(root, "packages/contracts/src/settings.ts"))
);
const { WsRpcGroup } = await import(
  pathToFileURL(resolve(root, "packages/contracts/src/rpc.ts"))
);
const update = WsRpcGroup.requests.get("server.updateSettings").payloadSchema;
const cases = [
  null,
  true,
  4,
  -1.5,
  "",
  "  x  ",
  "bad!",
  "/tmp",
  "off",
  "local",
  "Infinity",
  "-Infinity",
  "NaN",
  [],
  ["ok", 0],
  {},
  { "!": 127 },
  { future: true },
  [{ id: "foo", value: true }],
];
// Generate populated values from every nested branch, then exercise invalid leaf
// types in those contexts. The Rust implementation never supplies expected results.
function samples(ast, depth = 0) {
  const kind = ast._tag;
  switch (kind) {
    case "Union":
      return ast.types
        .flatMap((t) => samples(t, depth))
        .filter((x) => x !== undefined);
    case "Literal":
      return [ast.literal];
    case "String":
      return ["  test  ", "", "local"];
    case "Boolean":
      return [true];
    case "Declaration":
    case "Number":
      return [1, 0, 99.5];
    case "Unknown":
      return [{ future: 1 }];
    case "Null":
      return [null];
    case "Undefined":
      return [];
    case "Arrays":
      return samples(ast.rest[0], depth + 1).map((x) => [x]);
    case "Objects": {
      const props = ast.propertySignatures;
      if (!props.length && ast.indexSignatures.length) {
        return samples(ast.indexSignatures[0].type, depth + 1).map((x) => ({
          demo: x,
        }));
      }
      const base = Object.fromEntries(
        props.map((p) => [p.name, samples(p.type, depth + 1)[0]]),
      );
      const variants = props.flatMap((p) =>
        [null, "bad", ...samples(p.type, depth + 1)].map((x) => ({
          ...base,
          [p.name]: x,
        })),
      );
      return [base, ...variants];
    }
    default:
      throw Error(kind);
  }
}
const legacy = {
  defaultModelSelection: [
    {
      provider: "  codex ",
      model: " m ",
      options: { speed: true, " other ": " hi ", ignored: 0 },
    },
    { model: "m" },
    { instanceId: "foo", model: "m", options: null },
  ],
  providerInstances: [
    {
      cli: {
        driver: "codex",
        environment: [
          { name: "TEST", secret: { provider: "vault", reference: "foo" } },
        ],
      },
    },
  ],
  speechProjectCustomWords: [[{ term: "é", aliases: ["e\u0301"] }]],
};
for (const [mode, codec, fields] of [
  ["settings", ServerSettings, ServerSettings.fields],
  ["request", S.toCodecJson(update), update.fields.patch.fields],
]) {
  const decode = S.decodeUnknownSync(codec),
    encode = S.encodeSync(mode === "request" ? update : codec);
  for (const key of Object.keys(fields)) {
    for (const encoded of new Set(
      [...cases, ...samples(fields[key].ast), ...(legacy[key] ?? [])].map(
        JSON.stringify,
      ),
    )) {
      const value = JSON.parse(encoded);
      const input =
        mode === "settings" ? { [key]: value } : { patch: { [key]: value } };
      let outcome;
      try {
        const wire = encode(decode(input));
        const view = mode === "settings" ? wire : wire.patch;
        outcome =
          view[key] !== undefined ? { value: view[key] } : { omitted: true };
      } catch (e) {
        outcome = { error: e.message };
      }
      console.log(
        JSON.stringify({ mode, key, input, outcome }, (_, v) =>
          typeof v === "number" && !Number.isFinite(v) ? String(v) : v,
        ),
      );
    }
  }
}
