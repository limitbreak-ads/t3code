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
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
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
  it.effect.each([false, true])(
    "checks the project under the dispatch lock after preflight: moved=%s",
    (moved) =>
      Effect.gen(function* () {
        const previous = process.env.T3CODE_MCP_SAME_PROJECT_ONLY;
        process.env.T3CODE_MCP_SAME_PROJECT_ONLY = "1";
        try {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const threads = yield* ThreadManagement.ThreadManagementService;
          const projects = yield* ProjectStore.ProjectStoreV2;
          const callerId = ThreadId.make(`caller:${moved}`);
          const targetId = ThreadId.make(`target:${moved}`);
          const ownProject = ProjectId.make("project:mcp-race");
          const foreignProject = ProjectId.make(`foreign:${moved}`);
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
                if (moved)
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
