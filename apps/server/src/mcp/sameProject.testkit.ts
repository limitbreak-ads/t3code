import * as ManagedFolders from "../project/ManagedProjectFolders.ts";
import * as CloneTracker from "../project/ProjectCloneTracker.ts";
import * as Repositories from "../sourceControl/SourceControlRepositoryService.ts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect } from "@effect/vitest";
import {
  EnvironmentId,
  NodeId,
  OrchestrationV2Subagent,
  type OrchestrationV2ThreadProjection,
  ProviderDriverKind,
  ProjectId,
  ProviderInstanceId,
  ScheduledTask,
  ThreadId,
  RuntimeRequestId,
  TurnItemId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
// oxlint-disable-next-line t3code/no-raw-mcp-registration -- this test fixture calls tools registered by McpHttpServer; it registers none.
import { McpSchema, McpServer } from "effect/ai";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ThreadLaunch from "../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadSearch from "../orchestration-v2/ThreadSearch.ts";
import * as ProviderAdapters from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ScheduledTasks from "../scheduledTasks/ScheduledTaskService.ts";
import * as Project from "../project/ProjectService.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as DocumentText from "../provider/DocumentText.ts";
import * as Git from "../vcs/GitVcsDriver.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ServerSecrets from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as RemoteDelegation from "../peer/RemoteDelegation.ts";
import * as PeerForwarding from "../peer/PeerForwarding.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import * as PeerLinkRequests from "../peer/PeerLinkRequests.ts";
import * as McpHttpServer from "./McpHttpServer.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";
import { idleThreadProjection, liveThreadShell } from "./McpToolAccess.testkit.ts";

export const own = liveThreadShell(ThreadId.make("own"));
export const sibling = liveThreadShell(ThreadId.make("sibling"));
export const foreign = {
  ...liveThreadShell(ThreadId.make("foreign")),
  projectId: ProjectId.make("other"),
};
export const deleted = { ...sibling, id: ThreadId.make("deleted"), deletedAt: sibling.createdAt };
const shells = [own, sibling, foreign, deleted];
const decodeTask = Schema.decodeUnknownSync(ScheduledTask);
const task = (id: string, projectId = own.projectId, threadId: ThreadId | null = null) =>
  decodeTask({
    id,
    projectId,
    threadId,
    title: id,
    prompt: "Check the build",
    enabled: true,
    schedule: { type: "interval", everyMs: 3600000 },
    workspaceStrategy: { type: "root" },
    modelSelection: own.modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    createdBy: "agent",
    creationSource: "mcp",
    nextRunAt: null,
    lastRunStatus: "never",
    lastRunAt: null,
    lastRunThreadId: null,
    lastRunError: null,
    runCount: 0,
    createdAt: "2026-10-01T00:00:00.000Z",
    updatedAt: "2026-10-01T00:00:00.000Z",
  });
const tasks = [
  task("own-task"),
  task("foreign-task", foreign.projectId),
  task("moved-binding", own.projectId, foreign.id),
  task("missing-binding", own.projectId, ThreadId.make("missing")),
];
const delegated = (id: string, remote = false) =>
  OrchestrationV2Subagent.make({
    id: NodeId.make(id),
    threadId: own.id,
    runId: null,
    parentNodeId: NodeId.make("parent"),
    origin: "app_owned",
    createdBy: "agent",
    driver: ProviderDriverKind.make("codex"),
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerThreadId: null,
    childThreadId: remote ? null : foreign.id,
    ...(remote
      ? {
          remoteChild: {
            environmentId: EnvironmentId.make("remote"),
            threadId: foreign.id,
            label: "Remote",
          },
        }
      : {}),
    nativeTaskRef: null,
    prompt: "Task",
    title: null,
    model: "gpt-5",
    status: "completed",
    result: "Private result",
    startedAt: null,
    completedAt: null,
    updatedAt: own.updatedAt,
  });
const scope = (external = false): McpInvocationContext.McpInvocationScope => ({
  environmentId: EnvironmentId.make("here"),
  requestNamespace: "test",
  issuedAt: 0,
  capabilities: new Set(["orchestration"]),
  thread: external
    ? undefined
    : {
        threadId: own.id,
        providerSessionId: "session",
        providerInstanceId: ProviderInstanceId.make("codex"),
      },
  client: external ? { sessionId: "engine", label: "Engine", access: "full-access" } : undefined,
});
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "test", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "test", version: "1" },
  },
  getClient: Effect.die("unused"),
});
export const fixture = (
  caller: typeof own | null = own,
  options: {
    shells?: typeof shells;
    transfers?: OrchestrationV2ThreadProjection["contextTransfers"];
    timeline?: OrchestrationV2ThreadProjection["visibleTurnItems"];
    messages?: OrchestrationV2ThreadProjection["messages"];
  } = {},
) => {
  const fixtureShells = options.shells ?? shells;
  const effects: string[] = [];
  const answers = {
    projectId: "answer-project",
    taskId: "answer-task",
    nested: { threadId: "answer-thread" },
  };
  const questionProjection = (id: ThreadId) => ({
    ...idleThreadProjection(fixtureShells.find((s) => s.id === id) ?? own),
    contextTransfers: options.transfers ?? [],
    runtimeRequests: [
      {
        id: RuntimeRequestId.make("question"),
        nodeId: NodeId.make("root"),
        providerTurnId: null,
        nativeRequestRef: null,
        kind: "user_input" as const,
        status: "pending" as const,
        responseCapability: { type: "message" as const },
        createdAt: own.createdAt,
        resolvedAt: null,
      },
    ],
    turnItems: [
      {
        id: TurnItemId.make("question-item"),
        threadId: id,
        runId: null,
        nodeId: null,
        providerThreadId: null,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 0,
        status: "completed" as const,
        title: null,
        startedAt: own.createdAt,
        completedAt: own.updatedAt,
        updatedAt: own.updatedAt,
        type: "user_input_request" as const,
        requestId: RuntimeRequestId.make("question"),
        questions: [],
      },
    ],
  });
  const threads = Layer.mock(ThreadManagement.ThreadManagementService)({
    getTimelinePage: () =>
      Effect.succeed({
        items: options.timeline ?? [],
        totalItems: options.timeline?.length ?? 0,
        hasMore: false,
      }),
    getThreadShell: (id) =>
      Effect.succeed(id === own.id ? caller : (fixtureShells.find((s) => s.id === id) ?? null)),
    getThreadRecords: (id) =>
      Effect.succeed({
        ...idleThreadProjection(fixtureShells.find((s) => s.id === id) ?? own),
        messages: options.messages ?? [],
        subagents: id === own.id ? [delegated("moved-child"), delegated("remote-child", true)] : [],
      }),
    getProjectThreadRecords: ({ threadId }) => Effect.succeed(questionProjection(threadId)),
    listProjectThreads: ({ projectId }) =>
      Effect.succeed(
        fixtureShells.filter(
          (s) => s.deletedAt === null && (projectId === undefined || s.projectId === projectId),
        ),
      ),
    awaitBackgroundTasks: (input) =>
      Effect.sync(() => {
        expect(input).toEqual({
          threadId: own.id,
          taskIds: ["provider:background-123"],
          awaited: true,
        });
        effects.push("background");
        return { matched: input.taskIds, unmatched: [] };
      }),
    dispatch: (command) =>
      Effect.sync(() => {
        if (command.type === "runtime-request.respond") expect(command.answers).toEqual(answers);
        effects.push("dispatch");
        return { sequence: 7, storedEvents: [] };
      }),
    stopWorkflow: () =>
      Effect.sync(() => {
        effects.push("workflow");
      }),
  });
  const scheduled = Layer.mock(ScheduledTasks.ScheduledTaskService)({
    list: () => Effect.succeed({ tasks }),
    upsert: (input) =>
      Effect.sync(() => {
        effects.push("upsert");
        return {
          task: decodeTask({
            ...task(input.id ?? "created", input.projectId),
            ...input,
            id: input.id ?? "created",
            createdBy: input.createdBy ?? "agent",
            creationSource: input.creationSource ?? "mcp",
            threadId: input.threadId ?? null,
          }),
        };
      }),
    delete: (input) =>
      Effect.sync(() => {
        effects.push("delete");
        return { id: input.id };
      }),
    runNow: (input) =>
      Effect.sync(() => {
        effects.push("run");
        return { task: tasks.find((t) => t.id === input.id)!, threadId: null };
      }),
  });
  const layer = Layer.mergeAll(
    McpHttpServer.layerThreadToolkit,
    McpHttpServer.layerOrchestratorToolkit,
    McpHttpServer.layerAttachmentToolkit,
    McpHttpServer.layerProjectRegistration,
  )
    .pipe(
      Layer.provideMerge(McpServer.McpServer.layer),
      Layer.provide(threads),
      Layer.provide(scheduled),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(Layer.mock(ThreadLaunch.ThreadLaunchService)({})),
      Layer.provide(
        Layer.mock(ThreadSearch.ThreadSearch)({
          searchThreads: (input) =>
            Effect.succeed({
              matches: fixtureShells
                .filter(
                  (s) =>
                    s.deletedAt === null &&
                    (input.projectId === undefined || s.projectId === input.projectId),
                )
                .map((s) => ({
                  threadId: s.id,
                  projectId: s.projectId,
                  title: s.title,
                  source: "title" as const,
                })),
            }),
        }),
      ),
      Layer.provide(
        Layer.mock(ManagedFolders.ManagedProjectFolders)({ namedProjectsRoot: "/projects" }),
      ),
      Layer.provide(Layer.mock(CloneTracker.ProjectCloneTracker)({})),
      Layer.provide(Layer.mock(Repositories.SourceControlRepositoryService)({})),
      Layer.provide(Layer.mock(ProviderRegistry.ProviderRegistry)({})),
      Layer.provide(Layer.mock(ProviderAdapters.ProviderAdapterRegistryV2)({})),
      Layer.provide(Layer.mock(Git.GitVcsDriver)({})),
      Layer.provide(Layer.mock(SecretRequests.SecretRequests)({})),
      Layer.provide(Layer.mock(RemoteDelegation.RemoteDelegation)({})),
    )
    .pipe(
      Layer.provide(Layer.mock(PeerLinks.PeerLinks)({})),
      Layer.provide(Layer.mock(PeerLinkRequests.PeerLinkRequests)({})),
      Layer.provide(
        Layer.mock(PeerForwarding.PeerForwarding)({
          call: () =>
            Effect.sync(() => {
              effects.push("forward");
            }).pipe(Effect.andThen(Effect.die("Unexpected remote forwarding"))),
        }),
      ),
      Layer.provide(
        Layer.mock(Project.ProjectService)({
          getById: (id) =>
            Effect.succeedSome({
              id,
              title: "Project",
              workspaceRoot: "/workspace",
              defaultModelSelection: own.modelSelection,
              scripts: [],
              createdAt: "2026-10-01T00:00:00.000Z",
              updatedAt: "2026-10-01T00:00:00.000Z",
              deletedAt: null,
            }),
        }),
      ),
      Layer.provide(Layer.mock(DocumentText.DocumentText)({})),
      Layer.provide(Layer.mock(ServerSecrets.ServerSecretStore)({})),
      Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "same-project-" })),
      Layer.provide(NodeCrypto.layer),
      Layer.provide(NodeServices.layer),
    );
  return { layer, effects, answers };
};
export const call = (name: string, args: Record<string, unknown>, external = false) =>
  McpServer.McpServer.pipe(
    Effect.flatMap((server) => server.callTool({ name, arguments: args })),
    Effect.provideService(McpInvocationContext.McpInvocationContext, scope(external)),
    Effect.provideService(McpSchema.McpServerClient, client),
  );
export const failure = (result: McpSchema.CallToolResult) =>
  result.content[0]?.type === "text" ? JSON.parse(result.content[0].text) : undefined;
export const withToggle = <A, E, R>(value: string | undefined, effect: Effect.Effect<A, E, R>) =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      const old = process.env.T3CODE_MCP_SAME_PROJECT_ONLY;
      if (value === undefined) delete process.env.T3CODE_MCP_SAME_PROJECT_ONLY;
      else process.env.T3CODE_MCP_SAME_PROJECT_ONLY = value;
      return old;
    }),
    () => effect,
    (old) =>
      Effect.sync(() => {
        if (old === undefined) delete process.env.T3CODE_MCP_SAME_PROJECT_ONLY;
        else process.env.T3CODE_MCP_SAME_PROJECT_ONLY = old;
      }),
  );
