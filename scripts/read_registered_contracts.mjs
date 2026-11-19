// Development-time extraction from the same registered Effect schemas as the server.
import { createRequire } from "node:module";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

const root = resolve(process.argv[2]);
const { WsRpcGroup } = await import(pathToFileURL(resolve(root, "packages/contracts/src/rpc.ts")));
const require = createRequire(resolve(root, "packages/contracts/package.json"));
const Schema = await import(pathToFileURL(require.resolve("effect/Schema")));
const fields = {};
for (const [tag, rpc] of WsRpcGroup.requests) {
  const ast = Schema.toEncoded(Schema.toCodecJson(rpc.payloadSchema)).ast;
  if (ast._tag === "Objects") {
    fields[tag] = ast.propertySignatures
      .filter(property => !property.type.context?.isOptional)
      .map(property => property.name);
  }
}
const http = [];
for (const [source, name] of [
  ["packages/contracts/src/environmentHttp.ts", "EnvironmentHttpApi"],
  ["packages/contracts/src/relay.ts", "RelayApi"],
]) {
  const api = (await import(pathToFileURL(resolve(root, source))))[name];
  for (const group of Object.values(api.groups)) {
    for (const [name, endpoint] of Object.entries(group.endpoints)) {
      http.push({source, name, method: endpoint.method, path: endpoint.path,
                 auth: [...endpoint.middlewares].map(m => m.key).filter(k => k.endsWith("Auth"))});
    }
  }
}
process.stdout.write(JSON.stringify({http, fields}));
