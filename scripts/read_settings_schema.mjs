// Development-time extraction. Production reads the checked-in descriptors using Rust.
import { createRequire } from "node:module";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
const root = resolve(process.argv[2]);
const source = (path) => import(pathToFileURL(resolve(root, path)));
const { ServerSettings, ServerSettingsPatch, DEFAULT_SERVER_SETTINGS } =
  await source("packages/contracts/src/settings.ts");
const { WsRpcGroup } = await source("packages/contracts/src/rpc.ts");
const require = createRequire(resolve(root, "packages/contracts/package.json"));
const S = await import(pathToFileURL(require.resolve("effect/Schema")));
const nodes = [],
  ids = new Map(),
  seen = new WeakMap();
function describe(ast) {
  if (seen.has(ast)) return seen.get(ast);
  const schema = S.make(ast);
  const field = S.Struct({ x: schema });
  const roundTrip = (input) =>
    S.encodeSync(field)(S.decodeUnknownSync(field)(input));
  const node = { kind: ast._tag };
  if (ast.encoding?.length) node.source = describe(ast.encoding.at(-1).to);
  try {
    const data = S.decodeUnknownSync(field)({ x: null });
    if (data.x === undefined) node.nullOmitted = true;
    else if (data.x !== null) node.nullValue = S.encodeSync(field)(data).x;
  } catch {}
  if (ast._tag === "Literal") node.value = ast.literal;
  if (ast.propertySignatures)
    node.fields = ast.propertySignatures.map((p) => [p.name, describe(p.type)]);
  if (ast.indexSignatures?.length)
    node.record = ast.indexSignatures.map((p) => [
      describe(p.parameter),
      describe(p.type),
    ]);
  if (ast.types) node.members = ast.types.map(describe);
  if (ast.rest) node.item = describe(ast.rest[0]);
  if (ast._tag === "String") {
    try {
      node.trim = roundTrip({ x: "  demo  " }).x === "demo";
    } catch {
      node.trim = !!ast.encoding;
    }
    if (!node.trim) delete node.trim;
  }
  try {
    const result = roundTrip({});
    if (Object.hasOwn(result, "x")) node.default = result.x;
    else node.optional = true;
  } catch {}
  if (ast.checks?.length)
    node.checks = ast.checks.map((c) => {
      const a = c.annotations ?? {};
      return [
        a.representation?.id?.split("/").at(-1) ?? "filter",
        a.representation?.payload ?? {},
        a.expected ?? "",
      ];
    });
  // Effect reports different expected members after narrowing a union by JSON type.
  const probes = [
    ["legacyStyle", "  custom  ", (r) => r?.mode === "custom"],
    [
      "legacyOptions",
      { foo: true },
      (r) => Array.isArray(r) && r[0]?.id === "foo",
    ],
    [
      "model",
      { provider: "probe", model: "test" },
      (r) => r?.instanceId === "probe",
    ],
    [
      "tags",
      ["foo", 32],
      (r) => Array.isArray(r) && r.length === 1 && r[0] === "foo",
    ],
    [
      "environment",
      { name: "A", secret: { provider: "test", reference: "foo" } },
      (r) => r?.sensitive === true && r?.secret?.reference === "foo",
    ],
  ];
  for (const [name, x, test] of probes) {
    try {
      if (test(roundTrip({ x }).x)) (node.transforms ??= []).push(name);
    } catch {}
  }
  try {
    const probe = S.decodeUnknownSync(field)({ x: "__invalid value__" });
    if (probe.x === undefined && ast._tag !== "Unknown") node.fallback = "omit";
  } catch {}
  const errors = {};
  for (const [type, x] of Object.entries({
    string: "__invalid value__",
    number: -1.25,
    boolean: true,
    null: null,
    object: {},
    array: [],
  })) {
    try {
      const result = roundTrip({ x });
      if (
        type === "string" &&
        result.x !== x &&
        result.x == null &&
        ast._tag !== "Unknown"
      )
        node.fallback = result.x === null ? "null" : "omit";
    } catch (e) {
      const [message, path] = e.message.split("\n  at ");
      if (path === '["x"]') errors[type] = message;
    }
  }
  if (Object.keys(errors).length) node.errors = errors;
  const key = JSON.stringify(node);
  if (!ids.has(key)) {
    ids.set(key, nodes.length);
    nodes.push(node);
  }
  seen.set(ast, ids.get(key));
  return ids.get(key);
}
const settings = describe(ServerSettings.ast);
const patch = describe(ServerSettingsPatch.ast);
const request = describe(
  S.toCodecJson(WsRpcGroup.requests.get("server.updateSettings").payloadSchema)
    .ast,
);
const secretCheck = describe(
  S.toCodecJson(
    WsRpcGroup.requests.get("server.checkSecretSource").payloadSchema,
  ).ast,
);
// One descriptor per line keeps upstream schema changes reviewable.
const header = {
  settings,
  patch,
  request,
  secretCheck,
  defaults: S.encodeSync(ServerSettings)(DEFAULT_SERVER_SETTINGS),
};
console.log(
  JSON.stringify(header).slice(0, -1) +
    ',\n"nodes":[\n' +
    nodes.map((n) => JSON.stringify(n)).join(",\n") +
    "\n]}",
);
