import { describe, expect, it } from "vitest";
import {
  buildSessionMemoryLogicalPath,
  buildTranscriptLogicalPath,
  countZeroGMemorySyncManifestFiles,
  createEmptyZeroGMemorySyncManifest,
  createEmptyZeroGMemorySyncState,
  createZeroGMemorySyncPointer,
  createZeroGMemorySyncStreamId,
  getZeroGMemorySyncPointerKeyBytes,
  parseZeroGMemorySyncLogicalPath,
  setZeroGMemorySyncStateFile,
  shouldUploadZeroGMemorySyncCandidate,
  upsertZeroGMemorySyncManifestFile,
} from "./storage-sync.js";

describe("0G memory sync helpers", () => {
  it("derives a stable stream id from the wallet address", () => {
    const first = createZeroGMemorySyncStreamId("0xAbC123");
    const second = createZeroGMemorySyncStreamId(" 0xabc123 ");

    expect(first).toMatch(/^0x[0-9a-f]{64}$/);
    expect(first).toBe(second);
  });

  it("encodes the pointer key as bytes", () => {
    expect(Buffer.from(getZeroGMemorySyncPointerKeyBytes()).toString("utf8")).toBe(
      "latest-manifest",
    );
  });

  it("builds and parses transcript logical paths", () => {
    const logicalPath = buildTranscriptLogicalPath("Main", "demo.jsonl");

    expect(logicalPath).toBe("agents/main/sessions/demo.jsonl");
    expect(parseZeroGMemorySyncLogicalPath(logicalPath)).toEqual({
      kind: "session-transcript",
      agentId: "main",
      fileName: "demo.jsonl",
    });
  });

  it("builds and parses memory logical paths", () => {
    const logicalPath = buildSessionMemoryLogicalPath("Ops", "2026-04-11-demo.md");

    expect(logicalPath).toBe("agents/ops/memory/2026-04-11-demo.md");
    expect(parseZeroGMemorySyncLogicalPath(logicalPath)).toEqual({
      kind: "session-memory",
      agentId: "ops",
      fileName: "2026-04-11-demo.md",
    });
  });

  it("returns null for unsupported logical paths", () => {
    expect(parseZeroGMemorySyncLogicalPath("agents/main/other/demo.txt")).toBeNull();
    expect(parseZeroGMemorySyncLogicalPath("memory/demo.md")).toBeNull();
  });

  it("marks candidates dirty when state is missing or stale", () => {
    const logicalPath = buildTranscriptLogicalPath("main", "session.jsonl");
    const candidate = {
      kind: "session-transcript" as const,
      agentId: "main",
      localPath: "/tmp/session.jsonl",
      fileName: "session.jsonl",
      logicalPath,
      size: 42,
      mtimeMs: 1234,
      sha256: "abc",
    };

    expect(
      shouldUploadZeroGMemorySyncCandidate({
        candidate,
        state: createEmptyZeroGMemorySyncState("0xabc"),
      }),
    ).toBe(true);

    const syncedState = setZeroGMemorySyncStateFile({
      state: createEmptyZeroGMemorySyncState("0xabc"),
      file: {
        logicalPath,
        size: 42,
        mtimeMs: 1234,
        sha256: "abc",
        rootHash: "0xroot",
        txHash: "0xtx",
        uploadedAt: "2026-04-11T00:00:00.000Z",
      },
    });

    expect(shouldUploadZeroGMemorySyncCandidate({ candidate, state: syncedState })).toBe(false);
    expect(
      shouldUploadZeroGMemorySyncCandidate({
        candidate: { ...candidate, sha256: "def" },
        state: syncedState,
      }),
    ).toBe(true);
  });

  it("upserts manifest files and counts them", () => {
    const manifest = createEmptyZeroGMemorySyncManifest("0xAbC123", "2026-04-11T00:00:00.000Z");
    const updated = upsertZeroGMemorySyncManifestFile({
      manifest,
      timestamp: "2026-04-11T01:00:00.000Z",
      file: {
        kind: "session-memory",
        agentId: "main",
        logicalPath: buildSessionMemoryLogicalPath("main", "memory.md"),
        size: 16,
        mtimeMs: 4567,
        sha256: "hash",
        rootHash: "0xroot",
        txHash: "0xtx",
        uploadedAt: "2026-04-11T01:00:00.000Z",
      },
    });

    expect(updated.walletAddress).toBe("0xabc123");
    expect(updated.updatedAt).toBe("2026-04-11T01:00:00.000Z");
    expect(countZeroGMemorySyncManifestFiles(updated)).toBe(1);
  });

  it("creates pointer payloads with normalized wallet addresses", () => {
    expect(
      createZeroGMemorySyncPointer({
        walletAddress: " 0xAbC123 ",
        manifestRootHash: "0xroot",
        updatedAt: "2026-04-11T00:00:00.000Z",
        fileCount: 3,
      }),
    ).toEqual({
      version: 1,
      walletAddress: "0xabc123",
      manifestRootHash: "0xroot",
      updatedAt: "2026-04-11T00:00:00.000Z",
      fileCount: 3,
    });
  });
});
