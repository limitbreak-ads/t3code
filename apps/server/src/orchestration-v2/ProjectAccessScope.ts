import type { ProjectId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";

/** Invocation-local boundary; dispatch rechecks it under the target's command lock. */
export const ProjectAccessScope = Context.Reference<
  | {
      readonly projectId: ProjectId;
      readonly callerThreadId: ThreadId;
      readonly refused: Ref.Ref<boolean>;
    }
  | undefined
>("t3/orchestration-v2/ProjectAccessScope", { defaultValue: () => undefined });

export const allowsProject = Effect.fn("ProjectAccessScope.allowsProject")(function* (
  projectId: ProjectId | undefined,
) {
  const scope = yield* ProjectAccessScope;
  if (scope === undefined || scope.projectId === projectId) return true;
  yield* Ref.set(scope.refused, true);
  return false;
});
