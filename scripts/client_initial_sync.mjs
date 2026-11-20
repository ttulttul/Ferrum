#!/usr/bin/env node
// Runs the pinned reference client's connection against one server: the RPC
// session's initial sync, then the RPCs and HTTP reads a freshly connected
// 0.1.10 web or desktop client issues in its first seconds.
//
//   node scripts/client_initial_sync.mjs <reference-checkout> <http-base-url>
//
// Environment:
//   T3_CLIENT_SYNC_COOKIE      session cookie (kept out of the process listing)
//   T3_CLIENT_SYNC_SEED        optional JSON {"cwd": ..., "threadId": ...} of a seeded
//                              project and thread, enabling the RPCs sidebar rows issue
//   T3_CLIENT_SYNC_TIMEOUT_MS  limit for the initial sync (default 20000)
//   T3_CLIENT_SYNC_STREAM_MS   wait for each later reply or first stream value (default 5000)
//   T3_CLIENT_SYNC_TRACE=1     log every frame to stderr
//
// Prints one JSON object on stdout. Exits 1 when the initial sync fails.
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import path from "node:path";

const [reference, base] = process.argv.slice(2);
if (!reference || !base) {
  console.error("usage: client_initial_sync.mjs <reference-checkout> <http-base-url>");
  process.exit(2);
}
const cookie = process.env.T3_CLIENT_SYNC_COOKIE;
const seed = process.env.T3_CLIENT_SYNC_SEED ? JSON.parse(process.env.T3_CLIENT_SYNC_SEED) : null;
const syncTimeoutMs = Number(process.env.T3_CLIENT_SYNC_TIMEOUT_MS ?? 20000);
const replyTimeoutMs = Number(process.env.T3_CLIENT_SYNC_STREAM_MS ?? 5000);
const trace = process.env.T3_CLIENT_SYNC_TRACE === "1";

const runtimeDir = path.join(path.resolve(reference), "packages/client-runtime");
const require = createRequire(path.join(runtimeDir, "package.json"));
const load = (specifier) => import(pathToFileURL(require.resolve(specifier)).href);
const loadSource = (relative) => import(pathToFileURL(path.join(runtimeDir, "src", relative)).href);

const Cause = await load("effect/Cause");
const Effect = await load("effect/Effect");
const DateTime = await load("effect/DateTime");
const Layer = await load("effect/Layer");
const Option = await load("effect/Option");
const Stream = await load("effect/Stream");
const Socket = await load("effect/socket/Socket");
const FetchHttpClient = await load("effect/http/FetchHttpClient");
const RpcSession = await loadSource("rpc/session.ts");
const ShellSnapshotLoader = await loadSource("state/shellSnapshotHttp.ts");

const authHeaders = cookie ? { Cookie: cookie } : {};
const descriptor = await (await fetch(new URL("/.well-known/t3/environment", base))).json();
const socketUrl = new URL("/ws?orchestrationProtocol=2", base.replace(/^http/, "ws")).toString();

// The desktop and web clients use the platform WebSocket and fetch; these add the cookie a
// browser would send.
const webSocket = (url, options) => {
  const socket = new globalThis.WebSocket(url, {
    protocols: typeof options === "string" || Array.isArray(options) ? options : options?.protocols,
    headers: authHeaders,
  });
  if (trace) {
    const send = socket.send.bind(socket);
    socket.send = (data) => {
      process.stderr.write(`>> ${String(data).slice(0, 400)}\n`);
      send(data);
    };
    socket.addEventListener("message", (event) =>
      process.stderr.write(`<< ${String(event.data).slice(0, 400)}\n`),
    );
  }
  return socket;
};
const cookieFetch = (input, init = {}) =>
  globalThis.fetch(input, { ...init, headers: { ...Object.fromEntries(new Headers(init.headers)), ...authHeaders } });

// The options apps/web/src/connection/runtime.ts gives the 0.1.10 client.
const sessionOptions = { environmentThemes: true, usageLimitSources: true, usageLimitsCommand: true };
const started = performance.now();
const elapsed = () => Math.round(performance.now() - started);
const results = { environmentId: descriptor.environmentId, steps: [] };

/** The discriminator a client switches on, so a renamed event shape shows as a difference. */
function signature(value) {
  if (Option.isOption?.(value)) return Option.isSome(value) ? `some:${signature(value.value)}` : "none";
  if (Array.isArray(value)) return `array(${value.length === 0 ? "empty" : signature(value[0])})`;
  if (value === null || typeof value !== "object") return typeof value;
  for (const key of ["type", "_tag", "kind"]) if (typeof value[key] === "string") return `${key}=${value[key]}`;
  return `{${Object.keys(value).sort().join(",")}}`;
}

/** Records how one step ended: a reply, an error, or silence within the limit. */
const record = (name, effect, limitMs) =>
  effect.pipe(
    Effect.timeoutOption(limitMs),
    Effect.matchCauseEffect({
      onSuccess: (outcome) =>
        Effect.sync(() => {
          results.steps.push(
            Option.isNone(outcome)
              ? { name, outcome: "silent", atMs: elapsed() }
              : { name, outcome: "reply", signature: signature(outcome.value), atMs: elapsed() },
          );
          return Option.getOrUndefined(outcome);
        }),
      onFailure: (cause) =>
        Effect.sync(() => {
          // A reply the client cannot decode is a defect, as are transport failures.
          const failure = Cause.findErrorOption(cause);
          const error = Option.isSome(failure) ? failure.value : Cause.squash(cause);
          results.steps.push({
            name,
            outcome: Option.isSome(failure) ? "error" : "defect",
            signature: String(error?._tag ?? error?.name ?? "unknown"),
            error: String(error?.message ?? error).slice(0, 500),
            atMs: elapsed(),
          });
          return undefined;
        }),
    }),
  );

/** A stream's first value is what the client waits for before it shows anything. */
const firstValue = (stream) =>
  Stream.runHead(stream).pipe(Effect.map((head) => (Option.isSome(head) ? head.value : "ended")));

/**
 * Shell and thread subscriptions ask for a completion marker, and the client counts them live
 * only once `synchronized` arrives (state/shell.ts, state/threads.ts). Records the items up to
 * it. Enrichment refreshes, snapshots that carry `resolvedRepositoryIdentityRoots`, only fill in
 * project identities and are not counted.
 */
const untilSynchronized = (stream) =>
  stream.pipe(
    Stream.filter((item) => !(item.kind === "snapshot" && item.resolvedRepositoryIdentityRoots !== undefined)),
    Stream.takeUntil((item) => item.kind === "synchronized"),
    Stream.runCollect,
    Effect.map((items) => {
      const kinds = Array.from(items, (item) => item.kind);
      return kinds.at(-1) === "synchronized" ? { type: kinds.join(",") } : "ended before synchronized";
    }),
  );

const program = Effect.gen(function* () {
  const factory = yield* RpcSession.make(sessionOptions);
  const prepared = {
    environmentId: descriptor.environmentId,
    label: "parity",
    httpBaseUrl: base,
    socketUrl,
    httpAuthorization: null,
    target: { _tag: "PrimaryConnectionTarget", environmentId: descriptor.environmentId },
  };
  const session = yield* factory.connect(prepared);
  // ConnectionDriver.connect waits on exactly this before the environment counts as connected.
  const config = yield* record(
    "environment.initialSync",
    session.ready.pipe(Effect.andThen(session.initialConfig)),
    syncTimeoutMs,
  );
  if (config === undefined) return;
  yield* record("server.probe", session.probe, replyTimeoutMs);

  const client = session.client;
  // EnvironmentShellState: the HTTP snapshot first, then the socket resumes after it.
  const omitIdleSubagents = config.shellThreadFamilies === true;
  const omission = {
    ...(omitIdleSubagents ? { omitIdleSubagents: true } : {}),
    ...(config.shellOlderSettled === true && omitIdleSubagents ? { omitOlderSettled: true } : {}),
  };
  const loader = yield* ShellSnapshotLoader.ShellSnapshotLoader;
  const shell = yield* record(
    "GET /api/orchestration/shell",
    loader.load(prepared, omission).pipe(
      Effect.flatMap((answer) =>
        Option.isSome(answer) ? Effect.succeed(answer.value) : Effect.fail({ _tag: "ShellSnapshotUnavailable" }),
      ),
    ),
    replyTimeoutMs,
  );
  const shellInput =
    shell?.snapshotSequence === undefined
      ? { ...omission, requestCompletionMarker: true }
      : {
          afterSequence: shell.snapshotSequence,
          ...(shell.projectIdentities === undefined ? {} : { knownProjectIdentities: shell.projectIdentities }),
          ...omission,
          requestCompletionMarker: true,
        };
  const activity = {
    environmentId: descriptor.environmentId,
    clientId: "client-initial-sync-parity",
    clientKind: "web",
    visible: true,
    focused: true,
    recentlyInteracted: true,
    appState: "active",
    scopes: [{ type: "provider-status" }],
    ttlMs: 45000,
    observedAt: DateTime.nowUnsafe(),
  };
  const later = [
    ["orchestration.subscribeShell", untilSynchronized(client["orchestration.subscribeShell"](shellInput))],
    ["subscribeServerLifecycle", firstValue(client.subscribeServerLifecycle({}))],
    ["subscribeDeviceState", firstValue(client.subscribeDeviceState({}))],
    ["subscribeProjectClones", firstValue(client.subscribeProjectClones({}))],
    ["subscribeAuthAccess", firstValue(client.subscribeAuthAccess({}))],
    ["server.reportClientActivity", client["server.reportClientActivity"](activity)],
    [
      "preview.reportProfiles",
      client["preview.reportProfiles"]({ profiles: [], defaultProfileId: "default", recordingFrameRate: 30 }),
    ],
    ["server.refreshProviders", client["server.refreshProviders"]({})],
  ];
  if (seed !== null) {
    // Sidebar rows of a project's threads.
    later.push(
      ["subscribeTerminalMetadata", firstValue(client.subscribeTerminalMetadata({}))],
      ["subscribeVcsBranch", firstValue(client.subscribeVcsBranch({ cwd: seed.cwd }))],
      ["subscribeVcsStatus", firstValue(client.subscribeVcsStatus({ cwd: seed.cwd }))],
      ["pullRequests.subscribeRefreshes", firstValue(client["pullRequests.subscribeRefreshes"]({}))],
      [
        "orchestration.subscribeThread",
        untilSynchronized(
          client["orchestration.subscribeThread"]({
            threadId: seed.threadId,
            requestCompletionMarker: true,
            acceptBoundedSnapshot: true,
            acceptDeferredWork: true,
            acceptCompactTurnItems: true,
            acceptCompactIds: true,
            acceptDedupedBodies: true,
          }),
        ),
      ],
    );
  }
  // The client issues these together on one socket.
  yield* Effect.all(
    later.map(([name, effect]) => record(name, effect, replyTimeoutMs)),
    { concurrency: "unbounded", discard: true },
  );
}).pipe(
  Effect.scoped,
  Effect.provide(ShellSnapshotLoader.layer),
  Effect.provide(FetchHttpClient.layer),
  Effect.provideService(FetchHttpClient.Fetch, cookieFetch),
  Effect.provide(Layer.succeed(Socket.WebSocketConstructor, webSocket)),
);

await Effect.runPromise(program);
results.totalMs = elapsed();
results.steps.sort((left, right) => left.name.localeCompare(right.name));
const sync = results.steps.find((s) => s.name === "environment.initialSync");
results.ok = sync?.outcome === "reply";
process.stdout.write(JSON.stringify(results) + "\n");
process.exit(results.ok ? 0 : 1);
