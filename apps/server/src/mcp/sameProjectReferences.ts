import type { EnvironmentId, ProjectId, ThreadId } from "@t3tools/contracts";
import type { Tool } from "effect/ai";
import type { OrchestratorToolkit } from "./toolkits/orchestrator/tools.ts";
import type { ThreadToolkit } from "./toolkits/thread/tools.ts";
import type { ProjectToolkit } from "./toolkits/project/tools.ts";
import type { AttachmentToolkit } from "./toolkits/attachment/tools.ts";
import type { WorktreeToolkit } from "./toolkits/worktree/tools.ts";
import type { PullRequestsToolkit } from "./toolkits/pullRequests/tools.ts";
import type { EnvironmentToolkit } from "./toolkits/environment/tools.ts";

type Tools = typeof OrchestratorToolkit.tools &
  typeof ThreadToolkit.tools &
  typeof ProjectToolkit.tools &
  typeof AttachmentToolkit.tools &
  typeof WorktreeToolkit.tools &
  typeof PullRequestsToolkit.tools &
  typeof EnvironmentToolkit.tools;

type References = {
  readonly environmentId?: EnvironmentId | undefined;
  readonly projectId?: ProjectId | undefined;
  readonly threadIds?: ReadonlyArray<ThreadId> | undefined;
  readonly scheduledTaskId?: string | undefined;
  readonly delegatedTaskId?: string | undefined;
  readonly scratch?: boolean | undefined;
};

const threadReference = ({
  threadId,
  environmentId,
}: {
  readonly threadId?: ThreadId | undefined;
  readonly environmentId?: EnvironmentId | undefined;
}) => [{ environmentId, threadIds: threadId === undefined ? [] : [threadId] }];
const projectReference = ({
  projectId,
  environmentId,
}: {
  readonly projectId?: ProjectId | undefined;
  readonly environmentId?: EnvironmentId | undefined;
}) => [{ projectId, environmentId }];
const environmentReference = ({
  environmentId,
}: {
  readonly environmentId?: EnvironmentId | undefined;
}) => [{ environmentId }];

// Select resource fields from production schemas; provider ids and data payloads are opaque.
const resolvers = {
  t3_thread_read: threadReference,
  t3_thread_send: threadReference,
  t3_thread_wait: threadReference,
  t3_thread_interrupt: threadReference,
  t3_thread_update: threadReference,
  t3_workflow_stop: threadReference,
  t3_thread_organize: threadReference,
  t3_queue: threadReference,
  t3_pending_request: threadReference,
  t3_thread_configure: threadReference,
  t3_worktree_list: threadReference,
  link_pull_request: threadReference,
  link_pull_requests: threadReference,
  unlink_pull_request: threadReference,
  unlink_pull_requests: threadReference,
  list_thread_pull_requests: threadReference,
  watch_pull_request: threadReference,
  unwatch_pull_request: threadReference,
  t3_thread_send_attachments: ({ threadId }) => [
    { threadIds: threadId === undefined ? [] : [threadId] },
  ],
  t3_project_read: projectReference,
  t3_project_update: projectReference,
  t3_project_delete: projectReference,
  t3_thread_list: projectReference,
  t3_thread_search: projectReference,
  list_scheduled_tasks: projectReference,
  orchestrator_capabilities: environmentReference,
  t3_project_list: environmentReference,
  t3_project_create: environmentReference,
  t3_project_clone: environmentReference,
  t3_environment_link: environmentReference,
  t3_environment_unlink: environmentReference,
  t3_environment_read: environmentReference,
  t3_environment_preferences_update: environmentReference,
  write_pull_request_guides: environmentReference,
  read_pull_request_guide: environmentReference,
  t3_thread_launch: ({ projectId, environmentId, scratch }) => [
    { projectId, environmentId, scratch },
  ],
  t3_thread_transfer: ({ threadId, targetThreadId, environmentId }) => [
    {
      environmentId,
      threadIds: [threadId, targetThreadId].filter((id) => id !== undefined),
    },
  ],
  delegate_task: ({ target }) =>
    target === undefined
      ? []
      : [{ projectId: target.projectId, environmentId: target.environmentId }],
  create_threads: ({ threads }) =>
    threads.flatMap(({ target }) =>
      target === undefined
        ? []
        : [{ projectId: target.projectId, environmentId: target.environmentId }],
    ),
  schedule_task: ({ scheduledTaskId, projectId, environmentId }) => [
    { scheduledTaskId, projectId, environmentId },
  ],
  delete_scheduled_task: ({ scheduledTaskId, environmentId }) => [
    { scheduledTaskId, environmentId },
  ],
  run_scheduled_task_now: ({ taskId, environmentId }) => [
    { scheduledTaskId: taskId, environmentId },
  ],
  task_status: ({ taskId }) => [{ delegatedTaskId: taskId }],
  task_cancel: ({ taskId }) => [{ delegatedTaskId: taskId }],
  t3_background_wait: () => [],
  request_secret: () => [],
  t3_environment_links: () => [],
  t3_attachment_prepare_upload: () => [],
  t3_attachment_discard: () => [],
  t3_worktree_handoff: () => [],
  t3_worktree_status: () => [],
} satisfies {
  [Name in keyof Tools]: (input: Tool.Parameters<Tools[Name]>) => ReadonlyArray<References>;
};

export const sameProjectReferences = (name: string, params: object): ReadonlyArray<References> => {
  const resolve = (
    resolvers as Record<string, ((input: object) => ReadonlyArray<References>) | undefined>
  )[name];
  return resolve?.(params) ?? [];
};
