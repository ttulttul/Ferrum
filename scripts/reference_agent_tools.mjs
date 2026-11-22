// The agent tools the pinned reference's /mcp server advertises, as `tools/list` returns them:
// built the way its AgentContextBudget test builds them, from `McpHttpServer.layerTools` with
// mocked handler services (registration only stores definitions; no handler runs).
//
//   node scripts/reference_agent_tools.mjs <reference checkout>  > tools.json
import { createRequire } from "node:module";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

const root = resolve(process.argv[2]);
const server = resolve(root, "apps/server/src");
const load = (path) => import(pathToFileURL(resolve(server, path)).href);
const require = createRequire(resolve(root, "apps/server/package.json"));
const pkg = async (name) => import(pathToFileURL(require.resolve(name)).href);

const Effect = await pkg("effect/Effect");
const Layer = await pkg("effect/Layer");
const Context = await pkg("effect/Context");
const { McpServer, McpSchema } = await pkg("effect/ai");
const NodeCrypto = await pkg("@effect/platform-node/NodeCrypto");
const NodeServices = await pkg("@effect/platform-node/NodeServices");
const { McpProviderSessions } = await import(
  pathToFileURL(resolve(root, "packages/provider-core/src/server/McpProviderSessions.ts")).href
);

const mocked = [
  ["assets/AssetSigner.ts", "AssetSigner"],
  ["device/DeviceService.ts", "DeviceService"],
  ["provider/DocumentText.ts", "DocumentText"],
  ["environmentTheme.ts", "EnvironmentThemeService"],
  ["vcs/GitVcsDriver.ts", "GitVcsDriver"],
  ["git/GitWorkflowService.ts", "GitWorkflowService"],
  ["issue/IssueService.ts", "IssueService"],
  ["peer/PeerForwarding.ts", "PeerForwarding"],
  ["peer/PeerLinkRequests.ts", "PeerLinkRequests"],
  ["peer/PeerLinks.ts", "PeerLinks"],
  ["mcp/PreviewAutomationBroker.ts", "PreviewAutomationBroker"],
  ["preview/PreviewBrowser.ts", "PreviewBrowser"],
  ["preview/Manager.ts", "PreviewManager"],
  ["project/ProjectCloneTracker.ts", "ProjectCloneTracker"],
  ["project/ProjectService.ts", "ProjectService"],
  ["project/ProjectSetupScriptRunner.ts", "ProjectSetupScriptRunner"],
  ["orchestration-v2/ProviderAdapterRegistry.ts", "ProviderAdapterRegistryV2"],
  ["orchestration-v2/Orchestrator.ts", "OrchestratorV2"],
  ["provider/ProviderRegistry.ts", "ProviderRegistry"],
  ["pullRequest/PullRequestReviewGuideService.ts", "PullRequestReviewGuideService"],
  ["pullRequest/PullRequestService.ts", "PullRequestService"],
  ["peer/RemoteDelegation.ts", "RemoteDelegation"],
  ["scheduledTasks/ScheduledTaskService.ts", "ScheduledTaskService"],
  ["secrets/SecretRequests.ts", "SecretRequests"],
  ["environment/ServerEnvironment.ts", "ServerEnvironment"],
  ["auth/ServerSecretStore.ts", "ServerSecretStore"],
  ["serverSettings.ts", "ServerSettingsService"],
  ["sourceControl/SourceControlRepositoryService.ts", "SourceControlRepositoryService"],
  ["orchestration-v2/ThreadIssueService.ts", "ThreadIssueService"],
  ["orchestration-v2/ThreadLaunchService.ts", "ThreadLaunchService"],
  ["orchestration-v2/ThreadManagementService.ts", "ThreadManagementService"],
  ["orchestration-v2/ThreadSearch.ts", "ThreadSearch"],
  ["vcs/VcsStatusBroadcaster.ts", "VcsStatusBroadcaster"],
];
const layers = [Layer.mock(McpProviderSessions)({})];
for (const [path, name] of mocked) layers.push(Layer.mock((await load(path))[name])({}));
const { ManagedProjectFolders } = await load("project/ManagedProjectFolders.ts");
layers.push(Layer.mock(ManagedProjectFolders)({ namedProjectsRoot: "" }));
const { ServerConfig } = await load("config.ts");
layers.push(ServerConfig.layerTest(process.cwd(), { prefix: "t3-agent-tools-" }));

const McpHttpServer = await load("mcp/McpHttpServer.ts");
const services = Layer.mergeAll(...layers).pipe(
  Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
);
const advertised = McpHttpServer.layerTools.pipe(
  Layer.provideMerge(McpHttpServer.layerInProcessMcpServer),
  Layer.provide(services),
);
// `tools/list` sends a tool's annotation title as its title too (effect's McpServer.addTool), and
// lists a tool only to the callers its `McpSchema.EnabledWhen` predicate admits.
const listing = await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const mcp = yield* McpServer.McpServer;
      return mcp.tools.map(({ tool, annotations }) => {
        const wire = JSON.parse(JSON.stringify(tool));
        const title = wire.title ?? wire.annotations?.title;
        const enabledWhen = Context.getOrUndefined(annotations, McpSchema.EnabledWhen);
        return {
          tool: title === undefined ? wire : { ...wire, title },
          listedTo: enabledWhen === undefined ? "everyone" : enabledWhen.name,
        };
      });
    }).pipe(Effect.provide(advertised)),
  ),
);
const tools = listing.map(({ tool }) => tool);
const listedTo = Object.fromEntries(listing.map(({ tool, listedTo }) => [tool.name, listedTo]));
process.stdout.write(`${JSON.stringify({ tools, listedTo }, null, 1)}\n`);
