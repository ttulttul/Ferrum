#!/usr/bin/env node
// Drives one server through the pinned reference client runtime, the way the
// 0.1.10 desktop renderer does, and runs a plan of RPC and HTTP calls.
//
//   node scripts/client_rpc_driver.mjs <reference-checkout> <http-base-url> <plan.json>
//
// Authentication follows the desktop: the bootstrap credential is exchanged at
// /oauth/token for a bearer session (bootstrapRemoteBearerSession), the socket
// gets a ticket from /api/auth/websocket-ticket/rpc
// (resolveRemoteWebSocketConnectionUrl), and HTTP calls carry the bearer token.
// Every request carries the renderer's `Origin: t3code://app`.
//
// RPCs go through RpcSession's client, so request encoding (numeric ids, Ack,
// frame parts, interrupts) and reply decoding are the client's own. HTTP calls
// go through the client's typed HttpApiClient (executeAuthenticatedEnvironmentHttpRequest).
//
// Environment:
//   T3_CLIENT_RPC_CREDENTIAL  desktop bootstrap credential (kept out of the process listing)
//   T3_CLIENT_RPC_VARS        JSON object of $NAME placeholders substituted into inputs
//   T3_CLIENT_RPC_REPLY_MS    wait for each reply or stream value (default 8000)
//   T3_CLIENT_RPC_TRACE=1     log frames to stderr
//
// Prints one JSON object {steps: [...]} on stdout.
import { createRequire } from "node:module";
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";
import path from "node:path";

const [reference, base, planPath] = process.argv.slice(2);
if (!reference || !base || !planPath) {
  console.error("usage: client_rpc_driver.mjs <reference-checkout> <http-base-url> <plan.json>");
  process.exit(2);
}
const credential = process.env.T3_CLIENT_RPC_CREDENTIAL;
const vars = JSON.parse(process.env.T3_CLIENT_RPC_VARS ?? "{}");
const replyMs = Number(process.env.T3_CLIENT_RPC_REPLY_MS ?? 8000);
const trace = process.env.T3_CLIENT_RPC_TRACE === "1";
const plan = JSON.parse(readFileSync(planPath, "utf8"));

const runtimeDir = path.join(path.resolve(reference), "packages/client-runtime");
const require = createRequire(path.join(runtimeDir, "package.json"));
const load = (specifier) => import(pathToFileURL(require.resolve(specifier)).href);
const loadSource = (relative) => import(pathToFileURL(path.join(runtimeDir, "src", relative)).href);

const Cause = await load("effect/Cause");
const Effect = await load("effect/Effect");
const DateTime = await load("effect/DateTime");
const Fiber = await load("effect/Fiber");
const Layer = await load("effect/Layer");
const Option = await load("effect/Option");
const Queue = await load("effect/Queue");
const Redacted = await load("effect/Redacted");
const Stream = await load("effect/Stream");
const Socket = await load("effect/socket/Socket");
const FetchHttpClient = await load("effect/http/FetchHttpClient");
const RpcSession = await loadSource("rpc/session.ts");
const Remote = await loadSource("authorization/remote.ts");
const HttpAuth = await loadSource("state/environmentHttpAuth.ts");
const Compatibility = await loadSource("connection/compatibility.ts");

const ORIGIN = "t3code://app";
const rendererFetch = (input, init = {}) =>
  globalThis.fetch(input, { ...init, headers: { ...Object.fromEntries(new Headers(init.headers)), Origin: ORIGIN } });
const webSocket = (url, options) => {
  const socket = new globalThis.WebSocket(url, {
    protocols: typeof options === "string" || Array.isArray(options) ? options : options?.protocols,
    headers: { Origin: ORIGIN },
  });
  if (trace) {
    const send = socket.send.bind(socket);
    socket.send = (data) => {
      process.stderr.write(`>> ${String(data).slice(0, 600)}\n`);
      send(data);
    };
    socket.addEventListener("message", (event) => process.stderr.write(`<< ${String(event.data).slice(0, 600)}\n`));
  }
  return socket;
};

// Credentials never reach the output: they are random per server and must not be printed.
const SECRET_KEYS = /^(access_token|refresh_token|credential|token|ticket|secret|pairingCode|uploadToken|wsTicket)$/i;

/** Plain JSON for a decoded value: what the client's state would hold. */
function plain(value, depth = 0) {
  if (depth > 60) return "<deep>";
  if (value === undefined) return null;
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") return Number.isFinite(value) ? value : String(value);
  if (typeof value === "bigint") return `${value}n`;
  if (typeof value === "function") return "<function>";
  if (DateTime.isDateTime(value)) return DateTime.formatIso(value);
  if (value instanceof Date) return value.toISOString();
  if (Redacted.isRedacted(value)) return "<redacted>";
  if (Option.isOption(value)) return Option.isSome(value) ? { _some: plain(value.value, depth + 1) } : { _none: true };
  if (value instanceof Uint8Array) return `<bytes ${value.length}>`;
  if (value instanceof Map) return Object.fromEntries([...value].map(([k, v]) => [String(k), plain(v, depth + 1)]));
  if (value instanceof Set) return [...value].map((v) => plain(v, depth + 1));
  if (Array.isArray(value)) return value.map((v) => plain(v, depth + 1));
  const out = {};
  for (const key of Object.keys(value).sort()) {
    if (key === "stack" || key === "cause" && value instanceof Error) continue;
    out[key] = SECRET_KEYS.test(key) && typeof value[key] === "string" ? "<secret>" : plain(value[key], depth + 1);
  }
  if (value instanceof Error && out._tag === undefined && value.name) out._error = value.name;
  return out;
}

/** The discriminator a client switches on. */
function signature(value) {
  if (Array.isArray(value)) return `array(${value.length === 0 ? "empty" : signature(value[0])})`;
  if (value === null || typeof value !== "object") return typeof value;
  for (const key of ["type", "_tag", "kind"]) if (typeof value[key] === "string") return `${key}=${value[key]}`;
  return `{${Object.keys(value).sort().join(",")}}`;
}

const outputs = {};
/** Replaces "$NAME" placeholders and "$step:<name>.<path>" references to earlier outputs. */
function substitute(value) {
  if (typeof value === "string") {
    if (value.startsWith("$step:")) {
      // Step names contain dots, so the longest recorded name that prefixes the reference wins.
      const reference = value.slice("$step:".length);
      const name = Object.keys(outputs).filter((key) => reference.startsWith(`${key}.`))
        .sort((a, b) => b.length - a.length)[0];
      if (name === undefined) throw new Error(`no earlier reply for ${value}`);
      return reference.slice(name.length + 1).split(".").reduce((node, key) => node?.[key], outputs[name]);
    }
    if (value === "$NOW") return new Date().toISOString();
    return value.replace(/\$([A-Z][A-Z0-9_]*)/g, (whole, name) => (name in vars ? vars[name] : whole));
  }
  if (Array.isArray(value)) return value.map(substitute);
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, substitute(v)]));
  return value;
}

/** Converts the plan's JSON input to the client's decoded form where the schema wants richer types. */
function clientInput(value) {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    if (value.$dateTime) return DateTime.makeUnsafe(value.$dateTime === "now" ? Date.now() : value.$dateTime);
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, clientInput(v)]));
  }
  if (Array.isArray(value)) return value.map(clientInput);
  return value;
}

const started = performance.now();
const elapsed = () => Math.round(performance.now() - started);
const steps = [];

/** Runs an effect and records how it ended: a reply, an error, a defect, or silence. */
const record = (step, effect, limitMs = replyMs) =>
  effect.pipe(
    Effect.timeoutOption(limitMs),
    Effect.matchCauseEffect({
      onSuccess: (outcome) =>
        Effect.sync(() => {
          if (Option.isNone(outcome)) {
            steps.push({ name: step.name, call: step.call, outcome: "silent", atMs: elapsed() });
            return undefined;
          }
          const value = plain(outcome.value);
          outputs[step.name] = value;
          steps.push({ name: step.name, call: step.call, outcome: "reply", signature: signature(value), value,
                       atMs: elapsed() });
          return outcome.value;
        }),
      onFailure: (cause) =>
        Effect.sync(() => {
          const failure = Cause.findErrorOption(cause);
          const error = Option.isSome(failure) ? failure.value : Cause.squash(cause);
          const value = plain(error);
          steps.push({
            name: step.name,
            call: step.call,
            outcome: Cause.hasInterruptsOnly?.(cause) ? "interrupted" : Option.isSome(failure) ? "error" : "defect",
            signature: String(error?._tag ?? error?.name ?? "unknown"),
            value,
            message: String(error?.message ?? error).slice(0, 600),
            atMs: elapsed(),
          });
          return undefined;
        }),
    }),
  );

/**
 * A stream collected until `until` matches (or `take` items), ending in the client's interrupt.
 * Shell and thread subscriptions ask for a completion marker; refreshes that only fill in project
 * identities are dropped, as client_initial_sync.mjs does.
 */
function collect(stream, spec) {
  const take = spec.take ?? 1;
  let s = stream;
  if (spec.dropIdentityRefreshes)
    s = s.pipe(Stream.filter((item) => !(item.kind === "snapshot" && item.resolvedRepositoryIdentityRoots !== undefined)));
  if (spec.until) s = s.pipe(Stream.takeUntil((item) => matches(plain(item), spec.until)));
  else if (spec.untilText) {
    let seen = "";
    s = s.pipe(Stream.takeUntil((item) => (seen += JSON.stringify(plain(item))).includes(spec.untilText)));
  }
  else s = s.pipe(Stream.take(take));
  return s.pipe(
    Stream.runCollect,
    Effect.map((items) => {
      const values = Array.from(items);
      if (spec.until && !(values.length && matches(plain(values.at(-1)), spec.until)))
        return { items: values, ended: "before match" };
      if (spec.untilText) return JSON.stringify(values.map((v) => plain(v))).includes(spec.untilText)
        ? { items: values.length, sawText: true } : { items: values, ended: "before text" };
      if (!spec.until && values.length < take) return { items: values, ended: "early" };
      return { items: values };
    }),
  );
}

/** Whether a plain value has every field of `pattern` (a shallow-by-path match). */
function matches(value, pattern) {
  if (pattern === null || typeof pattern !== "object") return value === pattern;
  if (value === null || typeof value !== "object") return false;
  return Object.entries(pattern).every(([key, expected]) => matches(value[key], expected));
}

const program = Effect.gen(function* () {
  // Desktop: exchange the bootstrap credential for a bearer session.
  let accessToken;
  const bearer = yield* record(
    { name: "auth.bootstrapBearerSession", call: "POST /oauth/token" },
    Remote.bootstrapRemoteBearerSession({
      httpBaseUrl: base,
      credential,
      clientMetadata: { label: "T3 Code Desktop", deviceType: "desktop" },
    }).pipe(
      Effect.tap((session) => Effect.sync(() => (accessToken = session.access_token))),
      // Only the token's kind and scope are compared; the token itself stays in this process.
      Effect.map((session) => ({ token_type: session.token_type, scope: session.scope ?? null,
                                 hasAccessToken: typeof session.access_token === "string" })),
    ),
  );
  if (bearer === undefined) return;
  yield* record(
    { name: "auth.sessionState", call: "GET /api/auth/session" },
    Remote.fetchRemoteSessionState({ httpBaseUrl: base, bearerToken: accessToken }),
  );
  const socketUrl = yield* record(
    { name: "auth.webSocketTicket", call: "POST /api/auth/websocket-ticket/:use" },
    Remote.resolveRemoteWebSocketConnectionUrl({
      wsBaseUrl: base.replace(/^http/, "ws"),
      httpBaseUrl: base,
      bearerToken: accessToken,
      clientMetadata: { label: "T3 Code Desktop", deviceType: "desktop", surface: "desktop" },
    }).pipe(Effect.map((url) => Compatibility.appendOrchestrationProtocol(url.toString()))),
  );
  // The step records a URL; the value compared is only whether a ticket was attached.
  steps.at(-1).value = typeof socketUrl === "string" && socketUrl.includes("wsTicket=") ? "ticketed" : steps.at(-1).value;
  steps.at(-1).signature = steps.at(-1).value === "ticketed" ? "string" : steps.at(-1).signature;
  if (socketUrl === undefined) return;

  const descriptor = yield* Effect.promise(() =>
    rendererFetch(new URL("/.well-known/t3/environment", base)).then((r) => r.json()),
  );
  vars.ENV = descriptor.environmentId;
  const prepared = {
    environmentId: descriptor.environmentId,
    label: "parity",
    httpBaseUrl: base,
    socketUrl,
    httpAuthorization: { _tag: "Bearer", token: accessToken },
    target: { _tag: "PrimaryConnectionTarget", environmentId: descriptor.environmentId },
  };
  const factory = yield* RpcSession.make({ environmentThemes: true, usageLimitSources: true, usageLimitsCommand: true });
  const session = yield* factory.connect(prepared);
  const config = yield* record(
    { name: "environment.initialSync", call: "subscribeServerConfig" },
    session.ready.pipe(Effect.andThen(session.initialConfig), Effect.map((c) => ({ keys: Object.keys(c).sort() }))),
    20000,
  );
  if (config === undefined) return;
  const client = session.client;

  const http = (step) =>
    HttpAuth.executeAuthenticatedEnvironmentHttpRequest({
      prepared,
      signer: Option.none(),
      group: step.group,
      method: step.method,
      url: (urls) => urls[step.endpoint](substitute(step.args ?? {})),
      timeoutMs: replyMs,
      request: ({ client: api, headers }) =>
        api[step.endpoint]({
          ...substitute(step.args ?? {}),
          headers: { ...headers, ...(step.orchestrationHeader ? { "x-t3-orchestration-protocol": "2" } : {}) },
          ...(step.responseOnly ? { responseMode: "response-only" } : {}),
        }),
    });

  const running = new Map();
  const run = (step) => {
    const call = step.rpc ?? (step.http ? `${step.method} ${step.path}` : undefined);
    const named = { ...step, call };
    if (step.rpc) {
      const input = clientInput(substitute(step.input ?? {}));
      const method = client[step.rpc];
      if (typeof method !== "function") {
        steps.push({ name: step.name, call, outcome: "defect", signature: "NoSuchClientMethod", atMs: elapsed() });
        return Effect.void;
      }
      if (step.stream) {
        const spec = step.stream;
        if (spec.background) {
          // Kept open while later steps run; a later {"await": name} step collects it.
          return Effect.gen(function* () {
            const queue = yield* Queue.unbounded();
            const fiber = yield* method(input).pipe(
              Stream.runForEach((item) => Queue.offer(queue, item)),
              Effect.exit,
              Effect.forkScoped,
            );
            running.set(step.name, { queue, fiber, spec, call });
            // The client treats a subscription as live once its first value arrives; mutations
            // made before then could land in the initial snapshot instead of as follow-up events.
            const first = yield* Queue.take(queue).pipe(Effect.timeoutOption(spec.timeoutMs ?? replyMs));
            steps.push({ name: step.name, call, outcome: Option.isSome(first) ? "reply" : "silent",
                         signature: Option.isSome(first) ? signature(plain(first.value)) : undefined, atMs: elapsed() });
          });
        }
        return record(named, collect(method(input), spec), spec.timeoutMs ?? step.timeoutMs ?? replyMs);
      }
      return record(named, method(input), step.timeoutMs ?? replyMs);
    }
    if (step.http) return record(named, http(step), step.timeoutMs ?? replyMs);
    if (step.await) {
      // Reads a background stream's items until the pattern arrives, then interrupts it,
      // as the client does when a view unmounts.
      const entry = running.get(step.await);
      if (entry === undefined) {
        steps.push({ name: step.name, call: step.await, outcome: "defect", signature: "NotStarted", atMs: elapsed() });
        return Effect.void;
      }
      const stop = Effect.suspend(() => (running.delete(step.await) ? Fiber.interrupt(entry.fiber) : Effect.void));
      return record({ ...step, call: entry.call }, Effect.gen(function* () {
        const items = [];
        for (;;) {
          const item = yield* Queue.take(entry.queue);
          items.push(item);
          if (step.untilText ? JSON.stringify(items.map((v) => plain(v))).includes(step.untilText)
              : !step.until || matches(plain(item), step.until)) break;
        }
        running.delete(step.await);
        yield* Fiber.interrupt(entry.fiber);
        // Interrupting the subscription must end the stream; the exit itself is the client's.
        return { items: step.lastOnly ? [items.at(-1)] : items };
      }), step.timeoutMs ?? replyMs).pipe(Effect.ensuring(stop));
    }
    if (step.pause) return Effect.sleep(step.pause);
    if (step.fetch) {
      // A request the client builds by hand, such as the PUT to an attachment upload URL.
      const spec = substitute(step.fetch);
      return record({ ...step, call: step.call ?? `${spec.method} ${spec.path ?? "url"}` }, Effect.promise(async () => {
        const url = new URL(spec.url ?? spec.path, base);
        const response = await rendererFetch(url, {
          method: spec.method,
          headers: { ...(spec.bearer === false ? {} : { Authorization: `Bearer ${accessToken}` }), ...(spec.headers ?? {}) },
          body: spec.repeatBytes ? Buffer.alloc(spec.repeatBytes, "x") :
                spec.bodyBase64 ? Buffer.from(spec.bodyBase64, "base64") : spec.body,
        });
        let body;
        if (spec.binary) {
          body = Buffer.from(await response.arrayBuffer()).toString("base64");
        } else {
          const text = await response.text();
          try { body = JSON.parse(text); } catch { body = spec.bodyText ? text : `<${text.length} bytes>`; }
        }
        const headers = spec.responseHeaders?.map((key) => [key, response.headers.get(key)]);
        return { status: response.status, contentType: response.headers.get("content-type"), body,
                 ...(headers ? { headers: Object.fromEntries(headers) } : {}) };
      }), step.timeoutMs ?? replyMs);
    }
    throw new Error(`unknown step ${JSON.stringify(step)}`);
  };

  for (const step of plan.steps) {
    if (step.parallel) {
      yield* Effect.all(step.parallel.map(run), { concurrency: "unbounded", discard: true });
    } else {
      yield* run(step);
    }
  }
  for (const [name, entry] of running) {
    steps.push({ name, call: entry.call, outcome: "defect", signature: "NeverAwaited", atMs: elapsed() });
    yield* Fiber.interrupt(entry.fiber);
  }
}).pipe(
  Effect.scoped,
  Effect.provide(FetchHttpClient.layer),
  Effect.provideService(FetchHttpClient.Fetch, rendererFetch),
  Effect.provide(Layer.succeed(Socket.WebSocketConstructor, webSocket)),
);

const exit = await Effect.runPromiseExit(program);
if (exit._tag === "Failure") {
  steps.push({ name: "driver", outcome: "defect", signature: "DriverFailure", message: String(Cause.pretty(exit.cause)).slice(0, 2000) });
}
// Exit only once the pipe has taken the whole result; open sockets would otherwise keep Node alive.
process.stdout.write(JSON.stringify({ totalMs: elapsed(), steps }) + "\n", () => process.exit(0));
