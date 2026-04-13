import { describe, expect, it, vi } from "vitest";
import { createNonExitingRuntime } from "../runtime.js";
import { zeroGSyncCommand } from "./zerog-sync.js";

describe("zeroGSyncCommand", () => {
  it("prints a no-op message when no files changed", async () => {
    const runtime = createNonExitingRuntime();
    const log = vi.spyOn(runtime, "log").mockImplementation(() => {});

    const result = await zeroGSyncCommand(runtime, {
      loadConfig: () => ({}),
      runZeroGMemorySyncPass: vi.fn(async () => ({
        skipped: true,
        reason: "no-changes",
        uploadedCount: 0,
        failedCount: 0,
        manifestRootHash: null,
      })),
    });

    expect(result.reason).toBe("no-changes");
    expect(log).toHaveBeenCalledWith("No 0G memory changes detected to upload.");
  });

  it("prints the uploaded file count and manifest root on success", async () => {
    const runtime = createNonExitingRuntime();
    const log = vi.spyOn(runtime, "log").mockImplementation(() => {});

    await zeroGSyncCommand(runtime, {
      loadConfig: () => ({}),
      runZeroGMemorySyncPass: vi.fn(async () => ({
        skipped: false,
        uploadedCount: 2,
        failedCount: 0,
        manifestRootHash: "0xmanifest",
      })),
    });

    expect(log).toHaveBeenCalledWith("Uploaded 2 0G memory files.\nManifest root: 0xmanifest");
  });

  it("fails clearly when no wallet is configured", async () => {
    const runtime = createNonExitingRuntime();

    await expect(
      zeroGSyncCommand(runtime, {
        loadConfig: () => ({}),
        runZeroGMemorySyncPass: vi.fn(async () => ({
          skipped: true,
          reason: "missing-wallet",
          uploadedCount: 0,
          failedCount: 0,
          manifestRootHash: null,
        })),
      }),
    ).rejects.toThrow(
      "0G memory sync requires a configured Ethereum wallet secret. Run openclaw setup first.",
    );
  });
});
