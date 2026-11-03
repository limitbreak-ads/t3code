import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import {
  own,
  sibling,
  foreign,
  deleted,
  fixture,
  call,
  failure,
  withToggle,
} from "./sameProject.testkit.ts";

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
