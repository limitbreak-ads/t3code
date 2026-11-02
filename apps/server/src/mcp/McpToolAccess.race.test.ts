import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  EventId,
  OrchestratorMcpFailure,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  ScheduledTaskId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Exit from "effect/Exit";
import * as TestClock from "effect/testing/TestClock";
import * as ThreadLaunch from "../orchestration-v2/ThreadLaunchService.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ManagedFolders from "../project/ManagedProjectFolders.ts";
import * as ManagedPolicy from "../policy/ManagedPolicy.ts";
import { McpSchema, McpServer, Tool, Toolkit } from "effect/ai";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { DispatchModeLimit } from "../orchestration-v2/DispatchModeLimit.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ProjectAccess from "../orchestration-v2/ProjectAccessScope.ts";
import * as ScheduledTasks from "../scheduledTasks/ScheduledTaskService.ts";
import * as ThreadSearch from "../orchestration-v2/ThreadSearch.ts";
import { liveThreadShell } from "./McpToolAccess.testkit.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ProviderReplayHarness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as McpHttpServer from "./McpHttpServer.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";
import * as McpToolAccess from "./McpToolAccess.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

const instanceId = ProviderInstanceId.make("codex");
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process needed for metadata commands"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const layerDatabase = SqlitePersistence.layerMemory;
const layerOrchestrator = Layer.mergeAll(
  layerDatabase,
  ProjectStore.layer.pipe(Layer.provide(layerDatabase)),
  ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "mcp-mode-race" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    { databaseLayer: layerDatabase, runEffectWorker: false },
  ),
);

const RenameTool = Tool.make("rename", {
  parameters: Schema.Struct({ threadId: ThreadId }),
  success: Schema.Struct({ renamed: Schema.Boolean }),
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies: [
    McpInvocationContext.McpInvocationContext,
    ThreadManagement.ThreadManagementService,
  ],
});
const RenameToolkit = Toolkit.make(RenameTool);

const supervisedClient: McpInvocationContext.McpInvocationScope = {
  environmentId: EnvironmentId.make("environment"),
  requestNamespace: "client:race",
  thread: undefined,
  client: { sessionId: "race", label: "Claude Code", access: "approval-required" },
  capabilities: new Set(["orchestration"]),
  issuedAt: 0,
};

const mcpClient = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "mcp-race", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "mcp-race", version: "1" },
  },
  getClient: Effect.die("unused"),
});

const decodeOutcome = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Union([
      Schema.Struct({ renamed: Schema.Boolean }),
      Schema.Struct({ code: Schema.String }),
    ]),
  ),
);

const renameRacingTheUser = (threadId: ThreadId, userRaises: boolean) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const raised = yield* Ref.make(false);
    const handlers = McpToolAccess.toLayer(RenameToolkit, {
      rename: McpToolAccess.writesThreads(
        (input) => [input.threadId],
        (input) =>
          Effect.gen(function* () {
            if (userRaises) {
              yield* orchestrator
                .dispatch({
                  type: "thread.runtime-mode.set",
                  commandId: CommandId.make("user-raise"),
                  threadId: input.threadId,
                  runtimeMode: "full-access",
                })
                .pipe(Effect.provideService(DispatchModeLimit, undefined), Effect.orDie);
              yield* Ref.set(raised, true);
            }
            yield* threads
              .dispatch({
                type: "thread.metadata.update",
                commandId: CommandId.make(`agent-rename:${userRaises}`),
                threadId: input.threadId,
                title: "Renamed by the agent",
              })
              .pipe(
                Effect.mapError(
                  () =>
                    new OrchestratorMcpFailure({
                      code: "orchestration_error",
                      message: "The rename failed.",
                    }),
                ),
              );
            return { renamed: true };
          }),
      ),
    });
    const outcome = yield* McpServer.McpServer.pipe(
      Effect.flatMap((server) => server.callTool({ name: "rename", arguments: { threadId } })),
      Effect.flatMap(({ content }) =>
        decodeOutcome(content.map((part) => (part.type === "text" ? part.text : "")).join("")),
      ),
      Effect.provideService(McpInvocationContext.McpInvocationContext, supervisedClient),
      Effect.provideService(McpSchema.McpServerClient, mcpClient),
      Effect.provide(
        McpHttpServer.toolkitRegistration(RenameToolkit, handlers).pipe(
          Layer.provideMerge(McpServer.McpServer.layer),
        ),
      ),
    );
    assert.equal(yield* Ref.get(raised), userRaises);
    return outcome;
  });

const createSupervisedThread = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make(`create:${threadId}`),
      threadId,
      projectId: ProjectId.make("project:mcp-race"),
      title: "Before",
      modelSelection: { instanceId, model: "gpt-5" },
      runtimeMode: "approval-required",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
  });

it.layer(layerOrchestrator)("writesThreads against a mode raise", (it) => {
  it.effect("refuses the write when the user raises the thread after the check", () =>
    Effect.gen(function* () {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const threadId = ThreadId.make("thread:race-raised");
      yield* createSupervisedThread(threadId);
      const outcome = yield* renameRacingTheUser(threadId, true);
      assert.deepEqual(outcome, { code: "runtime_mode_escalation_denied" });
      const shell = yield* projections.getThreadShell(threadId);
      assert.equal(shell?.runtimeMode, "full-access");
      assert.equal(shell?.title, "Before");
    }).pipe(
      Effect.provide(
        ThreadManagement.layer.pipe(
          Layer.provide(ServerSettings.layerTest()),
          Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase))),
        ),
      ),
    ),
  );
});

it.layer(layerOrchestrator)("production project move race", (it) => {
  it.effect.each(["none", "legacy-target", "caller", "target"] as const)(
    "checks project pinning and dispatch locking after preflight: move=%s",
    (move) =>
      Effect.gen(function* () {
        const moved = move === "legacy-target";
        const previous = process.env.T3CODE_MCP_SAME_PROJECT_ONLY;
        process.env.T3CODE_MCP_SAME_PROJECT_ONLY = "1";
        try {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const threads = yield* ThreadManagement.ThreadManagementService;
          const projects = yield* ProjectStore.ProjectStoreV2;
          const callerId = ThreadId.make(`caller:${move}`);
          const targetId = ThreadId.make(`target:${move}`);
          const ownProject = ProjectId.make("project:mcp-race");
          const foreignProject = ProjectId.make(`foreign:${move}`);
          for (const projectId of [ownProject, foreignProject]) {
            const at = "2026-03-24T00:00:00.000Z";
            yield* projects.apply({
              sequence: 1,
              eventId: EventId.make(`create:${projectId}`),
              aggregateKind: "project",
              aggregateId: projectId,
              occurredAt: at,
              commandId: null,
              causationEventId: null,
              correlationId: null,
              metadata: {},
              type: "project.created",
              payload: {
                projectId,
                title: "Project",
                workspaceRoot: "/workspace",
                defaultModelSelection: null,
                scripts: [],
                createdAt: at,
                updatedAt: at,
              },
            });
          }
          yield* createSupervisedThread(callerId);
          yield* createSupervisedThread(targetId);
          let dispatched = false;
          const intercepted = ThreadManagement.ThreadManagementService.of({
            ...threads,
            getThreadShell: (id) =>
              id === callerId
                ? Effect.succeed({ ...liveThreadShell(callerId), projectId: ownProject })
                : threads.getThreadShell(id),
            dispatch: (command) =>
              Effect.gen(function* () {
                dispatched = true;
                if (move === "caller" || move === "target") {
                  const id = move === "caller" ? callerId : targetId;
                  const attempt = yield* orchestrator
                    .dispatch({
                      type: "thread.project.adopt-folder",
                      commandId: CommandId.make(`move:${id}`),
                      threadId: id,
                      projectId: foreignProject,
                    })
                    .pipe(
                      Effect.provideService(ProjectAccess.ProjectAccessScope, undefined),
                      Effect.provideService(DispatchModeLimit, undefined),
                      Effect.result,
                    );
                  assert.equal(attempt._tag, "Failure");
                  if (attempt._tag === "Failure")
                    assert.match(String(attempt.failure.cause), /pinned.*same-project only/);
                  assert.equal((yield* threads.getThreadShell(id))?.projectId, ownProject);
                }
                if (moved) {
                  delete process.env.T3CODE_MCP_SAME_PROJECT_ONLY;
                  yield* orchestrator
                    .dispatch({
                      type: "thread.project.adopt-folder",
                      commandId: CommandId.make(`move:${targetId}`),
                      threadId: targetId,
                      projectId: foreignProject,
                    })
                    .pipe(
                      Effect.provideService(ProjectAccess.ProjectAccessScope, undefined),
                      Effect.provideService(DispatchModeLimit, undefined),
                    );
                  process.env.T3CODE_MCP_SAME_PROJECT_ONLY = "1";
                }
                return yield* threads.dispatch(command);
              }),
          });
          const result = yield* McpServer.McpServer.pipe(
            Effect.flatMap((server) =>
              server.callTool({
                name: "t3_thread_organize",
                arguments: { threadId: targetId, action: "pin" },
              }),
            ),
            Effect.provideService(McpInvocationContext.McpInvocationContext, {
              ...supervisedClient,
              client: undefined,
              thread: {
                threadId: callerId,
                providerInstanceId: instanceId,
                providerSessionId: "session",
              },
            }),
            Effect.provideService(McpSchema.McpServerClient, mcpClient),
            Effect.provide(
              McpHttpServer.layerThreadToolkit.pipe(
                Layer.provideMerge(McpServer.McpServer.layer),
                Layer.provide(Layer.succeed(ThreadManagement.ThreadManagementService, intercepted)),
                Layer.provide(Layer.mock(ThreadSearch.ThreadSearch)({})),
                Layer.provide(Layer.mock(ScheduledTasks.ScheduledTaskService)({})),
                Layer.provide(NodeCrypto.layer),
              ),
            ),
          );
          assert.equal(dispatched, true);
          assert.equal(result.isError, moved);
          const shell = yield* threads.getThreadShell(targetId);
          assert.equal(shell?.projectId, moved ? foreignProject : ownProject);
          if (moved) {
            const content = result.content[0];
            assert.equal(content?.type, "text");
            if (content?.type === "text")
              assert.deepEqual(JSON.parse(content.text), {
                _tag: "OrchestratorMcpFailure",
                code: "capability_denied",
                message: "The requested target is unavailable in this project; same-project only",
              });
            assert.equal(shell?.pinnedAt == null, true);
          } else assert.equal(shell?.pinnedAt != null, true);
        } finally {
          if (previous === undefined) delete process.env.T3CODE_MCP_SAME_PROJECT_ONLY;
          else process.env.T3CODE_MCP_SAME_PROJECT_ONLY = previous;
        }
      }).pipe(
        Effect.provide(
          ThreadManagement.layer.pipe(
            Layer.provide(ServerSettings.layerTest()),
            Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase))),
          ),
        ),
      ),
  );
});

it.layer(layerOrchestrator)("project pinning and deferred schedules", (it) => {
  it.effect.each(["1", "0", undefined])("project moves with toggle %s", (toggle) =>
    Effect.gen(function* () {
      const previous = process.env.T3CODE_MCP_SAME_PROJECT_ONLY;
      if (toggle === undefined) delete process.env.T3CODE_MCP_SAME_PROJECT_ONLY;
      else process.env.T3CODE_MCP_SAME_PROJECT_ONLY = toggle;
      try {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const projects = yield* ProjectStore.ProjectStoreV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const ownProject = ProjectId.make("project:mcp-race");
        const foreignProject = ProjectId.make(`destination:${toggle}`);
        const threadId = ThreadId.make(`scheduled:${toggle}`);
        for (const projectId of [ownProject, foreignProject]) {
          const at = "2026-03-24T00:00:00.000Z";
          yield* projects.apply({
            sequence: 1,
            eventId: EventId.make(`project:${projectId}`),
            aggregateKind: "project",
            aggregateId: projectId,
            occurredAt: at,
            commandId: null,
            causationEventId: null,
            correlationId: null,
            metadata: {},
            type: "project.created",
            payload: {
              projectId,
              title: "Project",
              workspaceRoot: "/workspace",
              defaultModelSelection: null,
              scripts: [],
              createdAt: at,
              updatedAt: at,
            },
          });
        }
        yield* createSupervisedThread(threadId);
        const sent = yield* Ref.make<ProjectId[]>([]);
        const tick = yield* Ref.make(Effect.void);
        const schedulerLayer = ScheduledTasks.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              ManagedPolicy.layerTest(),
              NodeCrypto.layer,
              Layer.succeed(Scheduler.Scheduler, {
                register: (name, work) =>
                  Effect.gen(function* () {
                    const context = yield* Effect.context<Effect.Services<typeof work>>();
                    if (name === "scheduled-tasks")
                      yield* Ref.set(tick, work.pipe(Effect.provideContext(context), Effect.orDie));
                  }),
              }),
              Layer.mock(SecretRequests.SecretRequests)({}),
              Layer.mock(ManagedFolders.ManagedProjectFolders)({
                namedProjectsRoot: "/projects",
                isScratchProject: () => Effect.succeed(false),
              }),
              Layer.mock(ThreadLaunch.ThreadLaunchService)({}),
              Layer.mock(ThreadManagement.ThreadManagementService)({
                getThreadShell: (id) => orchestrator.getThreadShell(id),
                sendToThread: (input) =>
                  Ref.update(sent, (ids) => [...ids, input.projectId]).pipe(Effect.as({} as never)),
              }),
            ),
          ),
        );
        yield* Effect.gen(function* () {
          const scheduler = yield* ScheduledTasks.ScheduledTaskService;
          const { task } = yield* scheduler.upsert({
            id: ScheduledTaskId.make(`scheduled-task:${toggle}`),
            projectId: ownProject,
            threadId,
            title: "Deferred",
            prompt: "Check",
            enabled: true,
            schedule: { type: "interval", everyMs: 60000 },
            workspaceStrategy: { type: "root" },
            modelSelection: { instanceId, model: "gpt-5" },
            runtimeMode: "approval-required",
            interactionMode: "default",
          });
          const move = yield* orchestrator
            .dispatch({
              type: "thread.project.adopt-folder",
              commandId: CommandId.make(`adopt:${toggle}`),
              threadId,
              projectId: foreignProject,
            })
            .pipe(Effect.exit);
          assert.equal(Exit.isFailure(move), toggle === "1");
          const shell = yield* projections.getThreadShell(threadId);
          assert.equal(shell?.projectId, toggle === "1" ? ownProject : foreignProject);
          if (toggle === "1") {
            assert.equal(yield* ProjectAccess.ProjectAccessScope, undefined);
            yield* TestClock.adjust("60 seconds");
            yield* Effect.flatten(Ref.get(tick));
            assert.deepEqual(yield* Ref.get(sent), [ownProject]);
            const saved = (yield* scheduler.list()).tasks.find((current) => current.id === task.id);
            assert.equal(saved?.runCount, 1);
            assert.equal(saved?.lastRunStatus, "succeeded");
            assert.equal(saved?.projectId, ownProject);
            assert.equal(saved?.threadId, threadId);
          }
        }).pipe(Effect.provide(schedulerLayer));
      } finally {
        if (previous === undefined) delete process.env.T3CODE_MCP_SAME_PROJECT_ONLY;
        else process.env.T3CODE_MCP_SAME_PROJECT_ONLY = previous;
      }
    }),
  );
});
