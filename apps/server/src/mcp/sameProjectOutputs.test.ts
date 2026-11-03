import { expect, it } from "@effect/vitest";
import {
  ContextTransferId,
  MessageId,
  NodeId,
  TurnItemId,
  ThreadId,
  type OrchestrationV2ContextTransfer,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import {
  own,
  sibling,
  foreign,
  deleted,
  fixture,
  call,
  withToggle,
} from "./sameProject.testkit.ts";

const missing = ThreadId.make("missing-parent");

it.effect.each([foreign.id, missing, deleted.id, sibling.id, null])(
  "production list/read audits historical parent %s",
  (parentThreadId) => {
    // The child moved into this project before the guard was enabled; lineage still names its old parent.
    const child = {
      ...own,
      lineage: { ...own.lineage, parentThreadId, relationshipToParent: "fork" as const },
    };
    const f = fixture(child, { shells: [child, sibling, foreign, deleted] });
    return Effect.gen(function* () {
      for (const name of ["t3_thread_list", "t3_thread_read"]) {
        const args =
          name === "t3_thread_list" ? { projectId: own.projectId } : { threadId: own.id };
        const baseline = yield* withToggle(undefined, call(name, args));
        expect(baseline.isError).toBe(false);
        expect(yield* withToggle("0", call(name, args))).toEqual(baseline);
        const external = yield* withToggle(undefined, call(name, args, true));
        expect(yield* withToggle("1", call(name, args, true))).toEqual(external);
        const result = yield* withToggle("1", call(name, args));
        expect(result.isError).toBe(false);
        const redacted = parentThreadId !== null && parentThreadId !== sibling.id;
        const expected = {
          parentThreadId: redacted ? null : parentThreadId,
          ...(redacted ? { redacted: true } : {}),
        };
        expect(result.structuredContent).toMatchObject(
          name === "t3_thread_list"
            ? { threads: [expected, { threadId: sibling.id }] }
            : { thread: expected },
        );
        if (redacted) expect(JSON.stringify(result)).not.toContain(parentThreadId);
        else expect(result).toEqual(baseline);
      }
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect("production transfer status audits both endpoints and counts omissions", () => {
  const transfers = [
    [own.id, sibling.id],
    [foreign.id, own.id],
    [own.id, foreign.id],
    [missing, own.id],
    [own.id, missing],
    [deleted.id, own.id],
  ].map(([sourceThreadId, targetThreadId], index): OrchestrationV2ContextTransfer => ({
    id: ContextTransferId.make(`transfer-${index}`),
    type: "fork",
    sourceThreadId: sourceThreadId!,
    targetThreadId: targetThreadId!,
    sourcePoint: { threadId: sourceThreadId! },
    basePoint: null,
    sourceProviderInstanceId: null,
    targetProviderInstanceId: null,
    targetRunId: null,
    status: "pending",
    resolution: null,
    createdBy: "user",
    error: null,
    createdAt: own.createdAt,
    updatedAt: own.updatedAt,
    consumedAt: null,
  }));
  const f = fixture(own, { transfers });
  return Effect.gen(function* () {
    const args = { action: "status", threadId: own.id };
    const baseline = yield* withToggle(undefined, call("t3_thread_transfer", args));
    expect(baseline.isError).toBe(false);
    expect(baseline.structuredContent).toMatchObject({
      transfers: transfers.map(({ id, sourceThreadId, targetThreadId, status }) => ({
        id,
        sourceThreadId,
        targetThreadId,
        status,
      })),
    });
    expect(yield* withToggle("0", call("t3_thread_transfer", args))).toEqual(baseline);
    expect(yield* withToggle("1", call("t3_thread_transfer", args, true))).toEqual(baseline);
    const result = yield* withToggle("1", call("t3_thread_transfer", args));
    expect(result.isError).toBe(false);
    expect(result.structuredContent).toEqual({
      transfers: [
        { id: "transfer-0", sourceThreadId: own.id, targetThreadId: sibling.id, status: "pending" },
      ],
      omittedCount: 5,
    });
    for (const id of [foreign.id, missing, deleted.id])
      expect(JSON.stringify(result)).not.toContain(id);
  }).pipe(Effect.provide(f.layer));
});

it.effect.each([foreign.id, missing, deleted.id, sibling.id])(
  "production read audits timeline source and message sender %s",
  (reference) => {
    const messageId = MessageId.make("message");
    const itemId = TurnItemId.make("item");
    const item = {
      id: itemId,
      threadId: own.id,
      runId: null,
      nodeId: null,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: 0,
      status: "completed" as const,
      title: null,
      startedAt: null,
      completedAt: null,
      updatedAt: own.updatedAt,
      type: "assistant_message" as const,
      messageId,
      text: "Copied message",
      streaming: false,
    };
    const f = fixture(own, {
      timeline: [
        {
          position: 0,
          visibility: "inherited",
          sourceThreadId: reference,
          sourceItemId: itemId,
          item,
        },
        { position: 1, visibility: "local", sourceThreadId: own.id, sourceItemId: itemId, item },
      ],
      messages: [
        {
          id: messageId,
          threadId: own.id,
          runId: null,
          nodeId: null,
          role: "assistant",
          text: "Copied message",
          attachments: [],
          streaming: false,
          createdBy: "agent",
          creationSource: "mcp",
          senderThreadId: reference,
          createdAt: own.createdAt,
          updatedAt: own.updatedAt,
        },
      ],
    });
    return Effect.gen(function* () {
      const args = { threadId: own.id };
      const baseline = yield* withToggle(undefined, call("t3_thread_read", args));
      expect(baseline.isError).toBe(false);
      expect(yield* withToggle("0", call("t3_thread_read", args))).toEqual(baseline);
      expect(yield* withToggle("1", call("t3_thread_read", args, true))).toEqual(baseline);
      const result = yield* withToggle("1", call("t3_thread_read", args));
      expect(result.isError).toBe(false);
      const redacted = reference !== sibling.id;
      expect(result.structuredContent).toMatchObject({
        items: [
          { sourceThreadId: redacted ? null : reference, ...(redacted ? { redacted: true } : {}) },
          {
            sourceThreadId: own.id,
            senderThreadId: redacted ? null : reference,
            ...(redacted ? { redacted: true } : {}),
          },
        ],
      });
      if (redacted) expect(JSON.stringify(result)).not.toContain(reference);
      else expect(result).toEqual(baseline);
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect.each(
  (["fork", "thread_created"] as const).flatMap((type) =>
    [foreign.id, missing, deleted.id, sibling.id].map((targetThreadId) => ({
      type,
      targetThreadId,
    })),
  ),
)(
  "production activity/direct-item read audits $type target $targetThreadId",
  ({ type, targetThreadId }) => {
    const itemId = TurnItemId.make("activity-item");
    const base = {
      id: itemId,
      threadId: own.id,
      runId: null,
      nodeId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: 0,
      status: "completed" as const,
      title: null,
      startedAt: null,
      completedAt: null,
      updatedAt: own.updatedAt,
      targetThreadId,
    };
    const item =
      type === "fork"
        ? { ...base, type, source: { type: "node" as const, nodeId: NodeId.make("source-node") } }
        : {
            ...base,
            type,
            providerThreadId: null,
            targetRunId: null,
            targetProviderInstanceId: own.modelSelection.instanceId,
            targetModel: own.modelSelection.model,
          };
    const f = fixture(own, {
      timeline: [
        { position: 0, visibility: "local", sourceThreadId: own.id, sourceItemId: itemId, item },
      ],
    });
    const originalText =
      type === "fork"
        ? `Forked to thread ${targetThreadId}.`
        : `Created thread ${targetThreadId} with ${own.modelSelection.instanceId} (${own.modelSelection.model}).`;
    const redacted = targetThreadId !== sibling.id;
    const safeText = !redacted
      ? originalText
      : type === "fork"
        ? "Forked to a thread outside this project."
        : "Created a thread outside this project.";
    return Effect.gen(function* () {
      for (const selection of [{ view: "activity" }, { itemId }]) {
        const args = { threadId: own.id, ...selection };
        const baseline = yield* withToggle(undefined, call("t3_thread_read", args));
        expect(baseline.isError).toBe(false);
        expect(baseline.structuredContent).toMatchObject({ items: [{ text: originalText }] });
        expect(yield* withToggle("0", call("t3_thread_read", args))).toEqual(baseline);
        expect(yield* withToggle("1", call("t3_thread_read", args, true))).toEqual(baseline);
        const result = yield* withToggle("1", call("t3_thread_read", args));
        expect(result.isError).toBe(false);
        expect(result.structuredContent).toMatchObject({
          items: [{ text: safeText, ...(redacted ? { redacted: true } : {}) }],
        });
        if (redacted) expect(JSON.stringify(result)).not.toContain(targetThreadId);
        else expect(result).toEqual(baseline);
      }
      const paged = yield* withToggle(
        "1",
        call("t3_thread_read", {
          threadId: own.id,
          itemId,
          textOffset: 12,
          maxCharsPerItem: 20,
        }),
      );
      expect(paged.isError).toBe(false);
      expect(paged.structuredContent).toMatchObject({
        items: [
          {
            text: safeText.slice(12, 32),
            textTruncated: safeText.length > 32,
            nextTextOffset: safeText.length > 32 ? 32 : null,
            ...(redacted ? { redacted: true } : {}),
          },
        ],
      });
    }).pipe(Effect.provide(f.layer));
  },
);
