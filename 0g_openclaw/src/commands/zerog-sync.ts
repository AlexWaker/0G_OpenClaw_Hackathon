import { loadConfig } from "../config/config.js";
import type { RuntimeEnv } from "../runtime.js";
import {
  runZeroGMemorySyncPass,
  type ZeroGMemorySyncRunResult,
} from "../zerog/storage-sync.service.js";

type ZeroGSyncCommandDeps = {
  loadConfig?: typeof loadConfig;
  runZeroGMemorySyncPass?: typeof runZeroGMemorySyncPass;
};

function formatUploadedSummary(result: ZeroGMemorySyncRunResult): string {
  const uploadedLabel = result.uploadedCount === 1 ? "file" : "files";
  const lines = [`Uploaded ${result.uploadedCount} 0G memory ${uploadedLabel}.`];
  if (result.manifestRootHash) {
    lines.push(`Manifest root: ${result.manifestRootHash}`);
  }
  return lines.join("\n");
}

function formatNoChangesSummary(): string {
  return "No 0G memory changes detected to upload.";
}

export async function zeroGSyncCommand(
  runtime: RuntimeEnv,
  deps: ZeroGSyncCommandDeps = {},
): Promise<ZeroGMemorySyncRunResult> {
  const cfg = (deps.loadConfig ?? loadConfig)();
  const result = await (deps.runZeroGMemorySyncPass ?? runZeroGMemorySyncPass)({
    cfg,
    log: {
      info: (message) => runtime.log(message),
      warn: (message) => runtime.log(message),
    },
  });

  if (result.skipped) {
    if (result.reason === "no-changes") {
      runtime.log(formatNoChangesSummary());
      return result;
    }
    if (result.reason === "missing-wallet") {
      throw new Error(
        "0G memory sync requires a configured Ethereum wallet secret. Run openclaw setup first.",
      );
    }
    if (result.reason === "disabled") {
      throw new Error("0G memory sync is disabled by OPENCLAW_0G_MEMORY_SYNC_DISABLED.");
    }
    runtime.log("0G memory sync skipped.");
    return result;
  }

  if (result.failedCount > 0) {
    throw new Error(
      `0G memory sync uploaded ${result.uploadedCount} file${result.uploadedCount === 1 ? "" : "s"}, but ${result.failedCount} upload${result.failedCount === 1 ? " failed" : "s failed"}.`,
    );
  }

  runtime.log(formatUploadedSummary(result));
  return result;
}
