import type { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { ProjectAccessScope } from "../orchestration-v2/ProjectAccessScope.ts";
import type { ThreadManagementService } from "../orchestration-v2/ThreadManagementService.ts";

export const auditOutputThreadReferences = Effect.fn("mcp.auditOutputThreadReferences")(function* (
  threads: Pick<ThreadManagementService["Service"], "getThreadShell">,
  threadIds: ReadonlyArray<ThreadId | null | undefined>,
) {
  const boundary = yield* ProjectAccessScope;
  if (boundary === undefined)
    return { scoped: false, allows: (_id: ThreadId | null | undefined) => true };
  const allowed = new Set<ThreadId>();
  for (const id of new Set(threadIds)) {
    if (id == null) continue;
    const shell = yield* threads.getThreadShell(id).pipe(Effect.orElseSucceed(() => null));
    // Output redaction must not mark the entire invocation as a refused input.
    if (shell?.deletedAt === null && shell.projectId === boundary.projectId) allowed.add(id);
  }
  return {
    scoped: true,
    allows: (id: ThreadId | null | undefined) => id == null || allowed.has(id),
  };
});
