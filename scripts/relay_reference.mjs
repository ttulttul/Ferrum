// Test adapter for the upstream Cloudflare relay. Bind its actual Effect HTTP handlers
// to a loopback Node listener, supplying local identity and storage at the service boundary.
import { createServer } from "node:http";
import { createRequire } from "node:module";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
const [, , checkout, port] = process.argv;
const root = resolve(checkout, "infra/relay");
const require = createRequire(resolve(root, "package.json"));
const pkg = (name) => import(pathToFileURL(name === "@t3tools/contracts/relay" ? resolve(checkout, "packages/contracts/src/relay.ts") : require.resolve(name)));
const source = (path) => import(pathToFileURL(resolve(root, "src", path + ".ts")));
const [Effect, Layer, HttpApi, Builder, Router, Server, contracts, api, config, db, devices, links, tokens, linker, endpoints, limits, chat, credentials, inbox] =
  await Promise.all([
    ...["effect/Effect", "effect/Layer", "effect/http-api/HttpApi", "effect/http-api/HttpApiBuilder",
        "effect/http/HttpRouter", "effect/http/HttpServer", "@t3tools/contracts/relay"].map(pkg),
    ...["http/Api", "Config", "db", "agentActivity/Devices", "environments/EnvironmentLinks",
        "auth/RelayTokens", "environments/EnvironmentLinker", "environments/ManagedEndpointProvider",
        "environments/ManagedTunnelLimits", "t3chat/T3ChatAccounts", "environments/EnvironmentCredentials",
        "hooks/HookInbox"].map(source),
  ]);
const Redacted = await pkg("effect/Redacted");
const settings = Layer.succeed(config.RelayConfiguration, {
  cloudMintPrivateKey: Redacted.make(process.env.CLOUD_MINT_PRIVATE_KEY),
  relayIssuer: process.env.RELAY_ISSUER, cloudMintPublicKey: process.env.CLOUD_MINT_PUBLIC_KEY,
});
const localDb = Layer.mock(db.RelayDb, { execute: () => Effect.succeed([]) });
const auth = Layer.succeed(contracts.RelayClientAuth, {
  clientBearer: (effect) => Effect.provideService(effect, contracts.RelayClientPrincipal, {userId: "fixture", token: "fixture"}),
});
const client = api.layerClientApi.pipe(
  Router.provideRequest(Layer.mergeAll(...[credentials.EnvironmentCredentials, links.EnvironmentLinks,
    endpoints.ManagedEndpointProvider, db.RelayTransactions, inbox.HookInbox].map((tag) => Layer.mock(tag, {})))),
  Layer.provide(Layer.mergeAll(settings, Layer.mock(tokens.RelayTokens, {}),
    Layer.mock(linker.EnvironmentLinker, {}), Layer.mock(endpoints.ManagedEndpointProvider, {}),
    Layer.mock(devices.Devices, { listForUser: () => Effect.succeed([]) }),
    Layer.mock(links.EnvironmentLinks, { listForUser: () => Effect.succeed([]) }),
    Layer.mock(limits.ManagedTunnelLimits, {getOverride: () => Effect.succeed(null)}),
    Layer.mock(chat.T3ChatAccounts, {available: false}))),
  Layer.provideMerge(auth),
);
const nodeCrypto = await pkg("@effect/platform-node/NodeCrypto");
const [mint, dpop, registrations] = await Promise.all(
  ["auth/CloudMintSigner", "auth/DpopProofs", "agentActivity/MobileRegistrations"].map(source));
const tokenVerifier = tokens.layer.pipe(Layer.provide(mint.layer), Layer.provide(settings), Layer.provide(nodeCrypto.layer));
const spent = new Set();
// Use the reference signature verifier with a deterministic in-memory INSERT ON CONFLICT result.
const replayStore = Layer.mock(db.RelayDb, {
  insert: () => ({values: (v) => ({onConflictDoNothing: () => ({returning: () => Effect.sync(() => {
    const key = JSON.stringify([v.thumbprint,v.jti]);
    if (spent.has(key)) return [];
    spent.add(key);
    return [{jti:v.jti}];
  })})})}),
});
const mobile = api.layerMobileApi.pipe(Layer.provide(Layer.mergeAll(
  Layer.mock(registrations.MobileRegistrations, {getAgentActivitySnapshot: () => Effect.succeed({aggregate:null})}),
  dpop.layer.pipe(Layer.provide(replayStore)),
)), Layer.provideMerge(api.layerDpopClientAuth.pipe(Layer.provide(tokenVerifier))));
const groups = contracts.RelayApi.groups;
const app = Router.toWebHandler(Builder.layer(HttpApi.make("RelayFixture").add(groups.health, groups.metadata, groups.client, groups.mobile)).pipe(
  Layer.provide(Layer.mergeAll(client, mobile, api.layerHealthApi.pipe(Layer.provide(localDb)),
    api.layerMetadataApi.pipe(Layer.provide(settings)))), Layer.provide(nodeCrypto.layer), Layer.provide(Server.layerServices),
), {disableLogger: true, middleware: effect => api.traceRelayHttpRequestWith(effect, Layer.empty)});
const server = createServer(async (req, res) => {
  try {
    const response = await app.handler(new Request(process.env.RELAY_ISSUER + req.url, {headers: req.headers,method:req.method}));
    res.writeHead(response.status, Object.fromEntries(response.headers));
    res.end(Buffer.from(await response.arrayBuffer()));
  } catch (error) { console.error(error); res.writeHead(500).end(); }
});
server.listen(Number(port), "127.0.0.1");
