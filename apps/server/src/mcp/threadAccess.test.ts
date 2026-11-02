import { expect, it } from "@effect/vitest";
import { EnvironmentId, ProjectId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";
import { liveThreadShell } from "./McpToolAccess.testkit.ts";
import { readThread } from "./threadAccess.ts";

const bareId = ThreadId.make("5d0b4a3e-8c1f-4a52-9d37-2f6c7e1b9a04");
const client: McpInvocationContext.McpInvocationScope = {
  environmentId: EnvironmentId.make("environment:thread-access"),
  requestNamespace: "client:thread-access",
  thread: undefined,
  client: { sessionId: "thread-access", label: "Claude Code", access: "full-access" },
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
};

const readBare = (existing: ReadonlyArray<ThreadId>) =>
  readThread(bareId).pipe(
    Effect.flip,
    Effect.provideService(McpInvocationContext.McpInvocationContext, client),
    Effect.provide(
      Layer.mock(ThreadManagement.ThreadManagementService)({
        getThreadShell: (threadId) =>
          Effect.succeed(existing.includes(threadId) ? liveThreadShell(threadId) : null),
      }),
    ),
  );

it.effect("points a bare uuid at the mcp: thread it belongs to", () =>
  Effect.gen(function* () {
    const error = yield* readBare([ThreadId.make(`mcp:${bareId}`)]);

    expect(error.code).toBe("thread_not_found");
    expect(error.message).toBe(`The thread was not found. Did you mean \`mcp:${bareId}\`?`);
  }),
);

it.effect.each([true, false])(
  "same-project suggestions never disclose a foreign canonical id: own=%s",
  (sameProject) => {
    const own = liveThreadShell(ThreadId.make("caller"));
    return Effect.acquireUseRelease(
      Effect.sync(() => {
        const old = process.env.T3CODE_MCP_SAME_PROJECT_ONLY;
        process.env.T3CODE_MCP_SAME_PROJECT_ONLY = "1";
        return old;
      }),
      () =>
        Effect.gen(function* () {
          const error = yield* readThread(bareId).pipe(Effect.flip);
          expect(error.code).toBe("thread_not_found");
          expect(error.message).toBe(
            sameProject
              ? `The thread was not found. Did you mean \`mcp:${bareId}\`?`
              : "The thread was not found.",
          );
        }),
      (old) =>
        Effect.sync(() => {
          if (old === undefined) delete process.env.T3CODE_MCP_SAME_PROJECT_ONLY;
          else process.env.T3CODE_MCP_SAME_PROJECT_ONLY = old;
        }),
    ).pipe(
      Effect.provideService(McpInvocationContext.McpInvocationContext, {
        ...client,
        thread: {
          threadId: own.id,
          providerInstanceId: own.providerInstanceId!,
          providerSessionId: "session",
        },
      }),
      Effect.provide(
        Layer.mock(ThreadManagement.ThreadManagementService)({
          getThreadShell: (id) =>
            Effect.succeed(
              id === own.id
                ? own
                : id === `mcp:${bareId}`
                  ? {
                      ...liveThreadShell(id),
                      projectId: sameProject ? own.projectId : ProjectId.make("foreign"),
                    }
                  : null,
            ),
        }),
      ),
    );
  },
);
