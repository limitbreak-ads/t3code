import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  OrchestratorMcpFailure,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { McpSchema, McpServer, Tool, Toolkit } from "effect/ai";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as McpHttpServer from "./McpHttpServer.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";
import * as McpToolAccess from "./McpToolAccess.ts";
import { liveThreadShell } from "./McpToolAccess.testkit.ts";

const own = liveThreadShell(ThreadId.make("own"));
const sibling = liveThreadShell(ThreadId.make("sibling"));
const foreign = {
  ...liveThreadShell(ThreadId.make("foreign")),
  projectId: ProjectId.make("other"),
};
const parameters = Schema.Struct({
  threadId: Schema.optional(ThreadId),
  projectId: Schema.optional(ProjectId),
  allProjects: Schema.optional(Schema.Boolean),
  environmentId: Schema.optional(EnvironmentId),
  scratch: Schema.optional(Schema.Boolean),
  target: Schema.optional(
    Schema.Struct({
      projectId: Schema.optional(ProjectId),
      environmentId: Schema.optional(EnvironmentId),
    }),
  ),
  threads: Schema.optional(
    Schema.Array(
      Schema.Struct({
        target: Schema.optional(Schema.Struct({ projectId: Schema.optional(ProjectId) })),
      }),
    ),
  ),
});
const definition = {
  parameters,
  success: Schema.Struct({ ids: Schema.Array(ThreadId) }),
  failure: OrchestratorMcpFailure,
  failureMode: "return" as const,
  dependencies: [
    McpInvocationContext.McpInvocationContext,
    ThreadManagement.ThreadManagementService,
  ],
};
const toolkit = Toolkit.make(
  Tool.make("t3_thread_send", definition),
  Tool.make("t3_thread_update", definition),
  Tool.make("t3_thread_interrupt", definition),
  Tool.make("t3_thread_read", definition),
  Tool.make("t3_thread_wait", definition),
  Tool.make("t3_thread_list", definition),
  Tool.make("t3_thread_search", definition),
  Tool.make("t3_thread_launch", definition),
  Tool.make("create_threads", definition),
  Tool.make("delegate_task", definition),
);
const run = () => Effect.succeed({ ids: [foreign.id] });
const list = (input: typeof parameters.Type) =>
  Effect.succeed({
    ids: [own, sibling, foreign]
      .filter(
        (s) =>
          input.allProjects || input.projectId === undefined || s.projectId === input.projectId,
      )
      .map((s) => s.id),
  });
const serverLayer = McpHttpServer.toolkitRegistration(
  toolkit,
  McpToolAccess.toLayer(toolkit, {
    t3_thread_send: McpToolAccess.writesThreads((p) => [p.threadId], run),
    t3_thread_update: McpToolAccess.writesThreads((p) => [p.threadId], run),
    t3_thread_interrupt: McpToolAccess.writesThreads((p) => [p.threadId], run),
    t3_thread_read: McpToolAccess.reads(run),
    t3_thread_wait: McpToolAccess.reads(run),
    t3_thread_list: McpToolAccess.reads(list),
    t3_thread_search: McpToolAccess.reads(list),
    t3_thread_launch: McpToolAccess.startsThreads(() => ({}), run, "stamped"),
    create_threads: McpToolAccess.actsAsCaller(run),
    delegate_task: McpToolAccess.actsAsCaller(run),
  }),
).pipe(
  Layer.provideMerge(McpServer.McpServer.layer),
  Layer.provideMerge(
    Layer.mock(ThreadManagement.ThreadManagementService)({
      getThreadShell: (id) =>
        Effect.succeed([own, sibling, foreign].find((s) => s.id === id) ?? null),
    }),
  ),
);
const scope = (external: boolean): McpInvocationContext.McpInvocationScope => ({
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
const decodeOutcome = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Union([
      Schema.Struct({ ids: Schema.Array(ThreadId) }),
      Schema.Struct({ code: Schema.String, message: Schema.String }),
    ]),
  ),
);
const denied = "capability_denied";
it.effect.each([
  ...[
    "t3_thread_send",
    "t3_thread_update",
    "t3_thread_interrupt",
    "t3_thread_read",
    "t3_thread_wait",
  ].map((tool) => [tool, { threadId: foreign.id }, false, "1", denied] as const),
  ["t3_thread_send", { threadId: foreign.id, environmentId: "remote" }, false, "1", denied],
  ["t3_thread_send", { threadId: sibling.id }, false, "1", [foreign.id]],
  ["t3_thread_send", { threadId: foreign.id }, true, "1", [foreign.id]],
  ["t3_thread_send", { threadId: foreign.id }, false, undefined, [foreign.id]],
  ["t3_thread_send", { threadId: foreign.id }, false, "0", [foreign.id]],
  ["t3_thread_list", { allProjects: true }, false, "1", [own.id, sibling.id]],
  ["t3_thread_list", { projectId: foreign.projectId }, false, "1", [own.id, sibling.id]],
  ["t3_thread_search", { projectId: foreign.projectId }, false, "1", [own.id, sibling.id]],
  ["t3_thread_list", { allProjects: true }, true, "1", [own.id, sibling.id, foreign.id]],
  ["t3_thread_launch", { projectId: foreign.projectId }, false, "1", denied],
  ["t3_thread_launch", { projectId: own.projectId }, false, "1", [foreign.id]],
  ["t3_thread_launch", { scratch: true }, false, "1", denied],
  ["delegate_task", { target: { projectId: foreign.projectId } }, false, "1", denied],
  ["delegate_task", { target: { environmentId: "remote" } }, false, "1", denied],
  [
    "create_threads",
    { threads: [{}, { target: { projectId: foreign.projectId } }] },
    false,
    "1",
    denied,
  ],
  [
    "create_threads",
    { threads: [{ target: { projectId: own.projectId } }] },
    false,
    "1",
    [foreign.id],
  ],
] as const)(
  "same-project guard %s %o external=%s env=%s",
  ([name, args, external, env, expected]) =>
    Effect.gen(function* () {
      const previous = process.env.T3CODE_MCP_SAME_PROJECT_ONLY;
      if (env === undefined) delete process.env.T3CODE_MCP_SAME_PROJECT_ONLY;
      else process.env.T3CODE_MCP_SAME_PROJECT_ONLY = env;
      try {
        const server = yield* McpServer.McpServer;
        const result = yield* server.callTool({ name, arguments: args });
        const outcome = yield* decodeOutcome(
          result.content.map((p) => (p.type === "text" ? p.text : "")).join(""),
        );
        expect("code" in outcome ? outcome.code : outcome.ids).toEqual(expected);
        if ("code" in outcome) {
          expect(outcome.message).toContain("same-project only");
          if ("threadId" in args && !("environmentId" in args)) {
            expect(outcome.message).toBe(
              `Thread ${foreign.id} is in another project; same-project only`,
            );
          }
        }
      } finally {
        if (previous === undefined) delete process.env.T3CODE_MCP_SAME_PROJECT_ONLY;
        else process.env.T3CODE_MCP_SAME_PROJECT_ONLY = previous;
      }
    }).pipe(
      Effect.provideService(McpInvocationContext.McpInvocationContext, scope(external)),
      Effect.provideService(McpSchema.McpServerClient, client),
      Effect.provide(serverLayer),
    ),
);
