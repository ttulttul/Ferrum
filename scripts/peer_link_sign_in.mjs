#!/usr/bin/env node
// Links to one server the way the pinned reference's "link another machine"
// does: its PeerLinks sign-in over the reference's own HTTP API client (so its
// schemas decode every answer), then its PeerMcpClient's MCP handshake and a
// few tool calls.
//
//   node scripts/peer_link_sign_in.mjs <reference-checkout> <http-base-url>
//
// Environment:
//   T3_PEER_LINK_CODE    a peer-link pairing code from the server (kept out of the process listing)
//   T3_PEER_LINK_ACCESS  the access to ask for (default "auto")
//   T3_PEER_LINK_ROOT    optional: an existing folder to register as a project, then launch,
//                        wait on and read a thread there
//
// Prints one JSON object on stdout. Exits 1 when a step fails.
import { createHash, randomBytes } from "node:crypto";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import path from "node:path";

const [reference, base] = process.argv.slice(2);
if (!reference || !base) {
  console.error("usage: peer_link_sign_in.mjs <reference-checkout> <http-base-url>");
  process.exit(2);
}
const code = process.env.T3_PEER_LINK_CODE ?? "";
const access = process.env.T3_PEER_LINK_ACCESS ?? "auto";

const runtimeDir = path.join(path.resolve(reference), "packages/client-runtime");
const require = createRequire(path.join(runtimeDir, "package.json"));
const load = (specifier) => import(pathToFileURL(require.resolve(specifier)).href);

const Effect = await load("effect/Effect");
const Redacted = await load("effect/Redacted");
const HttpApi = await load("effect/http-api/HttpApi");
const HttpApiClient = await load("effect/http-api/HttpApiClient");
const FetchHttpClient = await load("effect/http/FetchHttpClient");
// The contracts package exports TypeScript source for `import` only.
const contracts = await import(
  pathToFileURL(path.join(path.resolve(reference), "packages/contracts/src/index.ts")).href
);
const { EnvironmentHttpApi, MCP_PEER_LINK_SOFTWARE_ID } = contracts;
const Schema = await load("effect/Schema");
const root = process.env.T3_PEER_LINK_ROOT;
// The result schemas the linking server decodes these tools' answers with.
const decoders = {
  orchestrator_capabilities: contracts.OrchestratorMcpCapabilitiesResult,
  t3_thread_list: contracts.OrchestratorMcpThreadListResult,
  t3_thread_read: contracts.OrchestratorMcpThreadReadResult,
  t3_thread_wait: contracts.OrchestratorMcpThreadWaitResult,
  t3_thread_send: contracts.OrchestratorMcpThreadSendResult,
  t3_thread_interrupt: contracts.OrchestratorMcpThreadInterruptResult,
};
const decodes = (name, value) => {
  const schema = decoders[name];
  if (schema === undefined) return null;
  try {
    Schema.decodeUnknownSync(schema)(value);
    return true;
  } catch (error) {
    return String(error?.message ?? error).slice(0, 600);
  }
};

// As PeerLinks.ts builds it.
const PeerHttpApi = HttpApi.make(EnvironmentHttpApi.identifier)
  .add(EnvironmentHttpApi.groups.metadata)
  .add(EnvironmentHttpApi.groups.mcpOAuth)
  .annotateMerge(EnvironmentHttpApi.annotations);
const LINK_REDIRECT_URI = "http://127.0.0.1/t3code-peer-link";
const PROTOCOL_VERSION = "2025-06-18";

const b64url = (bytes) => Buffer.from(bytes).toString("base64url");
const report = { steps: [] };
const step = (name, value) => report.steps.push({ name, ...value });

const signIn = Effect.gen(function* () {
  const client = yield* HttpApiClient.make(PeerHttpApi, { baseUrl: base });
  const descriptor = yield* client.metadata.descriptor();
  step("descriptor", {
    ok: true,
    linkLimits:
      descriptor.capabilities.mcpModeLimitHeader === true && descriptor.capabilities.linkFence === true,
  });
  const registered = yield* client.mcpOAuth.register({
    payload: {
      client_name: "T3 Code · parity",
      redirect_uris: [LINK_REDIRECT_URI],
      software_id: MCP_PEER_LINK_SOFTWARE_ID,
    },
  });
  step("register", { ok: true, redirectUris: registered.redirect_uris });
  const verifier = b64url(randomBytes(32));
  const challenge = b64url(createHash("sha256").update(verifier).digest());
  const state = b64url(randomBytes(16));
  const resource = `${new URL(base).origin}/mcp`;
  const decided = yield* client.mcpOAuth.decision({
    headers: {},
    payload: {
      authorization: {
        response_type: "code",
        client_id: registered.client_id,
        redirect_uri: LINK_REDIRECT_URI,
        code_challenge: challenge,
        code_challenge_method: "S256",
        state: Redacted.make(state),
        resource,
      },
      decision: { _tag: "pairing-code", access, code },
    },
  });
  const redirect = new URL(decided.redirectTo);
  const granted = redirect.searchParams.get("code");
  step("decision", { ok: granted !== null && redirect.searchParams.get("state") === state });
  const issued = yield* client.mcpOAuth.token({
    payload: {
      grant_type: "authorization_code",
      code: Redacted.make(granted ?? ""),
      redirect_uri: LINK_REDIRECT_URI,
      client_id: registered.client_id,
      code_verifier: Redacted.make(verifier),
      resource,
    },
  });
  step("token", { ok: true, tokenType: issued.token_type, scope: issued.scope });
  return issued.access_token;
});

const token = await Effect.runPromise(
  signIn.pipe(Effect.provide(FetchHttpClient.layer)),
).catch((error) => {
  step("failed", { ok: false, error: String(error?.message ?? error) });
  return null;
});

// As PeerMcpClient calls a peer: initialize, notifications/initialized, tools/call.
if (token !== null) {
  const limit = { "t3-mode-limit": `${access === "read-only" ? "approval-required" : access}/default` };
  const post = async (body, session) => {
    const response = await fetch(new URL("/mcp", base), {
      method: "POST",
      headers: {
        accept: "application/json, text/event-stream",
        authorization: `Bearer ${token}`,
        "content-type": "application/json",
        ...limit,
        ...(session ? { "mcp-session-id": session, "mcp-protocol-version": PROTOCOL_VERSION } : {}),
      },
      body: JSON.stringify(body),
    });
    const text = await response.text();
    return { status: response.status, session: response.headers.get("mcp-session-id"), text };
  };
  const initialized = await post({
    jsonrpc: "2.0",
    id: 0,
    method: "initialize",
    params: { protocolVersion: PROTOCOL_VERSION, capabilities: {}, clientInfo: { name: "t3code-peer", version: "1" } },
  });
  step("initialize", { ok: initialized.status === 200 && initialized.session !== null, status: initialized.status });
  const session = initialized.session;
  const notified = await post({ jsonrpc: "2.0", method: "notifications/initialized" }, session);
  step("initialized", { ok: notified.status === 202, status: notified.status });
  const listed = await post({ jsonrpc: "2.0", id: 1, method: "tools/list", params: {} }, session);
  const tools = JSON.parse(listed.text).result?.tools?.map((tool) => tool.name) ?? [];
  step("tools/list", { ok: listed.status === 200, has: ["t3_project_list", "t3_thread_launch", "t3_thread_read", "t3_thread_wait"].filter((name) => tools.includes(name)) });
  const call = async (name, args) => {
    const called = await post(
      { jsonrpc: "2.0", id: 2, method: "tools/call", params: { name, arguments: args } },
      session,
    );
    const result = JSON.parse(called.text).result;
    const value = result?.structuredContent ?? {};
    const decoded = decodes(name, value);
    step(name, {
      ok: called.status === 200 && result?.isError === false && decoded !== false && typeof decoded !== "string",
      keys: Object.keys(value).sort(),
      ...(decoded === null ? {} : { decodes: decoded }),
      ...(result?.isError ? { failure: result.content?.[0]?.text } : {}),
    });
    return value;
  };
  await call("t3_project_list", {});
  await call("orchestrator_capabilities", {});
  await call("t3_thread_list", { allProjects: true });
  if (root) {
    const project = await call("t3_project_create", { title: "Peer link", workspaceRoot: root });
    // Below full access the link cannot register a project: it uses the one the user has.
    const projectId = project.id ?? process.env.T3_PEER_LINK_PROJECT;
    const launched = await call("t3_thread_launch", {
      projectId,
      title: "Peer link",
      message: "hello",
      modelSelection: { instanceId: "codex", model: "fixture" },
    });
    // The turn's outcome depends on the server's workspace; only the answers' shapes count.
    const waited = await call("t3_thread_wait", { threadId: launched.threadId, timeoutMs: 5000 });
    report.steps.at(-1).keys = Object.keys(waited).filter((key) => key !== "next").sort();
    await call("t3_thread_read", { threadId: launched.threadId, afterPosition: -1 });
    // The thread this link started is the link's to change; the user's own thread is not,
    // unless the link has full access.
    const userThread = process.env.T3_PEER_LINK_USER_THREAD;
    for (const [whose, threadId] of [["launched", launched.threadId], ["user's", userThread]]) {
      if (threadId === undefined) continue;
      const tagged = async (name, args) => {
        await call(name, args);
        report.steps.at(-1).name = `${name} (${whose} thread)`;
      };
      await tagged("t3_thread_update", { threadId, action: "rename", title: "Peer link renamed" });
      await tagged("t3_thread_configure", { threadId, modelSelection: { instanceId: "codex", model: "fixture-2" } });
      await tagged("t3_thread_organize", { threadId, action: "pin" });
      // A fenced link is refused before the queue is looked at; with full access the
      // queue answers that the run is not queued, so only a fenced link asks.
      if (process.env.T3_PEER_LINK_ACCESS !== "full-access")
        await tagged("t3_queue", { action: "cancel", threadId, queuedRunId: "run-not-queued" });
      await tagged("t3_thread_interrupt", { threadId });
    }
  }
}

console.log(JSON.stringify(report));
process.exit(report.steps.every((s) => s.ok) ? 0 : 1);
