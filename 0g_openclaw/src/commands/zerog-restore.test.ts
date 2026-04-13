import { describe, expect, it, vi } from "vitest";
import { createNonExitingRuntime } from "../runtime.js";
import { zeroGRestoreCommand } from "./zerog-restore.js";

describe("zeroGRestoreCommand", () => {
  it("prints the restored file count and manifest root on success", async () => {
    const runtime = createNonExitingRuntime();
    const log = vi.spyOn(runtime, "log").mockImplementation(() => {});

    await zeroGRestoreCommand(runtime, {
      loadConfig: () => ({}),
      restoreZeroGMemorySyncIfAvailable: vi.fn(async () => ({
        skipped: false,
        restoredCount: 2,
        skippedCount: 0,
        failedCount: 0,
        manifestRootHash: "0xmanifest",
      })),
    });

    expect(log).toHaveBeenCalledWith("Restored 2 0G memory files.\nManifest root: 0xmanifest");
  });

  it("prints a no-remote message when nothing has been published yet", async () => {
    const runtime = createNonExitingRuntime();
    const log = vi.spyOn(runtime, "log").mockImplementation(() => {});

    const result = await zeroGRestoreCommand(runtime, {
      loadConfig: () => ({}),
      restoreZeroGMemorySyncIfAvailable: vi.fn(async () => ({
        skipped: true,
        reason: "no-remote-manifest",
        restoredCount: 0,
        skippedCount: 0,
        failedCount: 0,
        manifestRootHash: null,
      })),
    });

    expect(result.reason).toBe("no-remote-manifest");
    expect(log).toHaveBeenCalledWith("No remote 0G memory manifest found for this wallet.");
  });

  it("fails clearly when no KV read endpoint is configured", async () => {
    const runtime = createNonExitingRuntime();

    await expect(
      zeroGRestoreCommand(runtime, {
        loadConfig: () => ({}),
        restoreZeroGMemorySyncIfAvailable: vi.fn(async () => ({
          skipped: true,
          reason: "kv-read-unavailable",
          restoredCount: 0,
          skippedCount: 0,
          failedCount: 0,
          manifestRootHash: null,
        })),
      }),
    ).rejects.toThrow(
      "0G memory restore requires OPENCLAW_0G_KV_RPC_URL to point to a KV node RPC endpoint.",
    );
  });
});
