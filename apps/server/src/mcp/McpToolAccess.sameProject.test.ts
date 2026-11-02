import * as ManagedFolders from "../project/ManagedProjectFolders.ts";
import * as CloneTracker from "../project/ProjectCloneTracker.ts";
import * as Repositories from "../sourceControl/SourceControlRepositoryService.ts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  NodeId,
  OrchestrationV2Subagent,
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

const own = liveThreadShell(ThreadId.make("own"));
const sibling = liveThreadShell(ThreadId.make("sibling"));
const foreign = {
  ...liveThreadShell(ThreadId.make("foreign")),
  projectId: ProjectId.make("other"),
};
const deleted = { ...sibling, id: ThreadId.make("deleted"), deletedAt: sibling.createdAt };
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
const fixture = (caller: typeof own | null = own) => {
  const effects: string[] = [];
  const answers = {
    projectId: "answer-project",
    taskId: "answer-task",
    nested: { threadId: "answer-thread" },
  };
  const questionProjection = (id: ThreadId) => ({
    ...idleThreadProjection(shells.find((s) => s.id === id) ?? own),
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
    getThreadShell: (id) =>
      Effect.succeed(id === own.id ? caller : (shells.find((s) => s.id === id) ?? null)),
    getThreadRecords: (id) =>
      Effect.succeed({
        ...idleThreadProjection(shells.find((s) => s.id === id) ?? own),
        subagents: id === own.id ? [delegated("moved-child"), delegated("remote-child", true)] : [],
      }),
    getProjectThreadRecords: ({ threadId }) => Effect.succeed(questionProjection(threadId)),
    listProjectThreads: ({ projectId }) =>
      Effect.succeed(
        shells.filter(
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
              matches: shells
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
const call = (name: string, args: Record<string, unknown>, external = false) =>
  McpServer.McpServer.pipe(
    Effect.flatMap((server) => server.callTool({ name, arguments: args })),
    Effect.provideService(McpInvocationContext.McpInvocationContext, scope(external)),
    Effect.provideService(McpSchema.McpServerClient, client),
  );
const failure = (result: McpSchema.CallToolResult) =>
  result.content[0]?.type === "text" ? JSON.parse(result.content[0].text) : undefined;
const withToggle = <A, E, R>(value: string | undefined, effect: Effect.Effect<A, E, R>) =>
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
const cases: Array<[string, Record<string, unknown>]> = [
  ...["task_status", "task_cancel"].flatMap((name) =>
    ["moved-child", "remote-child"].map(
      (taskId) => [name, { taskId }] as [string, Record<string, unknown>],
    ),
  ),
  ["t3_thread_send", { threadId: foreign.id, message: "Message" }],
  ["t3_thread_update", { threadId: foreign.id, action: "rename", title: "Changed" }],
  ...["t3_thread_interrupt", "t3_thread_read", "t3_thread_wait"].map(
    (name) => [name, { threadId: foreign.id }] as [string, Record<string, unknown>],
  ),
  ["t3_thread_launch", { projectId: foreign.projectId, title: "Launch", message: "Run" }],
  ["t3_thread_launch", { scratch: true, title: "Launch", message: "Run" }],
  ...Object.entries({
    t3_thread_configure: { threadId: own.id },
    t3_thread_organize: { threadId: own.id, action: "pin" },
    t3_thread_transfer: { threadId: own.id, action: "status" },
    t3_pending_request: { threadId: own.id, action: "list" },
    t3_workflow_stop: { threadId: own.id, subagentId: "workflow" },
    schedule_task: { scheduledTaskId: "own-task" },
    list_scheduled_tasks: {},
    delete_scheduled_task: { scheduledTaskId: "own-task" },
  }).map(
    ([name, args]) =>
      [name, { ...args, environmentId: "remote" }] as [string, Record<string, unknown>],
  ),

  [
    "t3_thread_send_attachments",
    {
      threadId: foreign.id,
      attachments: [
        { type: "image", id: "uploaded", name: "image.png", mimeType: "image/png", sizeBytes: 1 },
      ],
    },
  ],
  ...["list", "read", "edit", "cancel", "reorder", "promote_to_steer"].map(
    (action) => ["t3_queue", { threadId: foreign.id, action }] as [string, Record<string, unknown>],
  ),
  ...["list", "read", "respond"].map(
    (action) =>
      ["t3_pending_request", { threadId: foreign.id, action }] as [string, Record<string, unknown>],
  ),
  ["t3_thread_configure", { threadId: foreign.id }],
  ["t3_thread_configure", { threadId: foreign.id, modelSelection: own.modelSelection }],
  ["t3_thread_organize", { threadId: foreign.id, action: "pin" }],
  ...["status", "fork"].map(
    (action) =>
      ["t3_thread_transfer", { threadId: foreign.id, action }] as [string, Record<string, unknown>],
  ),
  ["t3_thread_transfer", { threadId: own.id, targetThreadId: foreign.id, action: "merge_back" }],
  ["t3_workflow_stop", { threadId: foreign.id, subagentId: "workflow" }],
  ...["foreign-task", "moved-binding", "missing-binding", "missing-task"].flatMap(
    (id) =>
      [
        ["run_scheduled_task_now", { taskId: id }],
        ["delete_scheduled_task", { scheduledTaskId: id }],
        ["schedule_task", { scheduledTaskId: id, prompt: "Changed" }],
      ] as Array<[string, Record<string, unknown>]>,
  ),
  [
    "schedule_task",
    {
      projectId: foreign.projectId,
      prompt: "Run",
      schedule: { type: "interval", everyMs: 3600000 },
    },
  ],
  ["t3_queue", { threadId: "missing", action: "list" }],
  ["t3_thread_configure", { threadId: deleted.id }],
  ["t3_queue", { threadId: own.id, action: "list", environmentId: "remote" }],
  ["run_scheduled_task_now", { taskId: "own-task", environmentId: "remote" }],
  ["delegate_task", { task: "Review", target: { environmentId: "remote" } }],
  [
    "create_threads",
    { threads: [{ title: "Child", message: "Run", target: { projectId: foreign.projectId } }] },
  ],
];
it.effect.each(cases)("production target refusal: %s %o", ([name, args]) => {
  const f = fixture();
  return withToggle(
    "1",
    Effect.gen(function* () {
      const result = yield* call(name, args);
      expect(failure(result)).toMatchObject({
        code: "capability_denied",
        message: expect.stringMatching(/same-project only$/),
      });
      expect(f.effects).toEqual([]);
    }),
  ).pipe(Effect.provide(f.layer));
});
it.effect("production scheduled list scopes project and omits moved or missing bindings", () => {
  const f = fixture();
  return withToggle(
    "1",
    Effect.gen(function* () {
      for (const args of [{}, { projectId: foreign.projectId }]) {
        const result = yield* call("list_scheduled_tasks", args);
        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          tasks: [{ scheduledTaskId: "own-task" }],
        });
      }
    }),
  ).pipe(Effect.provide(f.layer));
});
it.effect.each([
  "t3_thread_configure",
  "t3_queue",
  "t3_pending_request",
  "t3_thread_organize",
  "t3_thread_transfer",
  "run_scheduled_task_now",
  "delete_scheduled_task",
  "schedule_task",
  "list_scheduled_tasks",
])("production external/unset compatibility: %s", (name) => {
  const f = fixture();
  const args =
    name === "t3_thread_organize"
      ? { threadId: foreign.id, action: "pin" }
      : name === "t3_thread_transfer"
        ? { threadId: foreign.id, action: "status" }
        : name === "run_scheduled_task_now"
          ? { taskId: "foreign-task" }
          : name === "list_scheduled_tasks"
            ? { projectId: foreign.projectId }
            : name === "schedule_task" || name === "delete_scheduled_task"
              ? { scheduledTaskId: "foreign-task", prompt: "Changed" }
              : { threadId: foreign.id, action: "list" };
  return Effect.gen(function* () {
    const before = yield* withToggle(undefined, call(name, args, true));
    expect(before.isError).toBe(false);
    const after = yield* withToggle("1", call(name, args, true));
    expect(after).toEqual(before);
    const disabled = yield* withToggle("0", call(name, args));
    const unset = yield* withToggle(undefined, call(name, args));
    expect(unset).toEqual(disabled);
  }).pipe(Effect.provide(f.layer));
});

it.effect(
  "production discovery scopes projects, thread pagination and search before returning ids",
  () => {
    const f = fixture();
    return withToggle(
      "1",
      Effect.gen(function* () {
        const projects = yield* call("t3_project_list", {});
        expect(projects.isError).toBe(false);
        expect(projects.structuredContent).toMatchObject({
          projects: [{ id: own.projectId }],
          nextCursor: null,
        });
        const empty = yield* call("t3_project_list", { cursor: 1 });
        expect(empty.structuredContent).toMatchObject({ projects: [], nextCursor: null });
        const first = yield* call("t3_thread_list", { allProjects: true, limit: 1 });
        expect(first.isError).toBe(false);
        expect(first.structuredContent).toMatchObject({
          threads: [{ threadId: own.id }],
          total: 2,
          nextCursor: 1,
        });
        const last = yield* call("t3_thread_list", {
          projectId: foreign.projectId,
          cursor: 1,
          limit: 1,
        });
        expect(last.structuredContent).toMatchObject({
          threads: [{ threadId: sibling.id }],
          total: 2,
          nextCursor: null,
        });
        const search = yield* call("t3_thread_search", {
          projectId: foreign.projectId,
          query: "thread",
        });
        expect(search.isError).toBe(false);
        expect(search.structuredContent).toMatchObject({
          matches: [{ threadId: own.id }, { threadId: sibling.id }],
        });
      }),
    ).pipe(Effect.provide(f.layer));
  },
);
it.effect("production schedule creation and bound updates stay in the caller project", () => {
  const f = fixture();
  return withToggle(
    "1",
    Effect.gen(function* () {
      const created = yield* call("schedule_task", {
        prompt: "Run",
        schedule: { type: "interval", everyMs: 60000 },
      });
      expect(created.isError).toBe(false);
      expect(created.structuredContent).toMatchObject({
        projectId: own.projectId,
        boundThreadId: own.id,
      });
      const updated = yield* call("schedule_task", {
        scheduledTaskId: "own-task",
        prompt: "Changed",
      });
      expect(updated.isError).toBe(false);
      const removed = yield* call("delete_scheduled_task", { scheduledTaskId: "own-task" });
      expect(removed.isError).toBe(false);
      expect(f.effects).toEqual(["upsert", "upsert", "delete"]);
    }),
  ).pipe(Effect.provide(f.layer));
});

it.effect.each([null, { ...own, deletedAt: own.createdAt }])(
  "production guard fails closed for missing or deleted callers: %o",
  (caller) => {
    const f = fixture(caller);
    return withToggle(
      "1",
      Effect.gen(function* () {
        const result = yield* call("t3_thread_configure", { threadId: sibling.id });
        expect(failure(result)).toMatchObject({
          code: "capability_denied",
          message: expect.stringMatching(/same-project only$/),
        });
        expect(f.effects).toEqual([]);
      }),
    ).pipe(Effect.provide(f.layer));
  },
);
it.effect.each(["t3_thread_send_attachments", "t3_workflow_stop"])(
  "production attachment/workflow external and unset compatibility: %s",
  (name) => {
    const f = fixture();
    const args =
      name === "t3_workflow_stop"
        ? { threadId: foreign.id, subagentId: "workflow" }
        : {
            threadId: foreign.id,
            attachments: [
              {
                type: "image",
                id: "missing-upload",
                name: "image.png",
                mimeType: "image/png",
                sizeBytes: 1,
              },
            ],
          };
    return Effect.gen(function* () {
      const external = yield* withToggle(undefined, call(name, args, true));
      expect(yield* withToggle("1", call(name, args, true))).toEqual(external);
      expect(yield* withToggle(undefined, call(name, args))).toEqual(
        yield* withToggle("0", call(name, args)),
      );
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect.each(["background", "answers"])(
  "production payload is not a resource reference: %s",
  (kind) => {
    const f = fixture();
    return withToggle(
      "1",
      Effect.gen(function* () {
        if (kind === "background") {
          const background = yield* call("t3_background_wait", {
            taskIds: ["provider:background-123"],
          });
          expect(background.isError).toBe(false);
          expect(background.structuredContent).toEqual({
            matched: ["provider:background-123"],
            unmatched: [],
          });
          expect(f.effects).toEqual(["background"]);
        } else {
          const response = yield* call("t3_pending_request", {
            action: "respond",
            requestId: "question",
            answers: f.answers,
          });
          expect(response.isError).toBe(false);
          expect(f.effects).toEqual(["dispatch"]);
        }
      }),
    ).pipe(Effect.provide(f.layer));
  },
);
