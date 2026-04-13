import { loadConfig } from "../config/config.js";
import type { RuntimeEnv } from "../runtime.js";
import {
  restoreZeroGMemorySyncIfAvailable,
  type ZeroGMemorySyncRestoreResult,
} from "../zerog/storage-sync.service.js";

type ZeroGRestoreCommandDeps = {
  loadConfig?: typeof loadConfig;
  restoreZeroGMemorySyncIfAvailable?: typeof restoreZeroGMemorySyncIfAvailable;
};

export type ZeroGRestoreCommandOptions = {
  force?: boolean;
};

function formatRestoreSummary(result: ZeroGMemorySyncRestoreResult): string {
  const restoredLabel = result.restoredCount === 1 ? "file" : "files";
  const lines = [`Restored ${result.restoredCount} 0G memory ${restoredLabel}.`];
  if (result.skippedCount > 0) {
    lines.push(
      `Skipped ${result.skippedCount} existing local file${result.skippedCount === 1 ? "" : "s"}.`,
    );
  }
  if (result.manifestRootHash) {
    lines.push(`Manifest root: ${result.manifestRootHash}`);
  }
  return lines.join("\n");
}

function formatAlreadyRestoredSummary(): string {
  return "0G memory is already restored for this wallet.";
}

function formatNoRemoteManifestSummary(): string {
  return "No remote 0G memory manifest found for this wallet.";
}

function formatLocalFilesPresentSummary(): string {
  return "Local 0G memory files are already present. Re-run with --force to overwrite from remote state.";
}

export async function zeroGRestoreCommand(
  runtime: RuntimeEnv,
  deps: ZeroGRestoreCommandDeps = {},
  options: ZeroGRestoreCommandOptions = {},
): Promise<ZeroGMemorySyncRestoreResult> {
  const cfg = (deps.loadConfig ?? loadConfig)();
  const result = await (
    deps.restoreZeroGMemorySyncIfAvailable ?? restoreZeroGMemorySyncIfAvailable
  )({
    cfg,
    force: options.force === true,
    log: {
      info: (message) => runtime.log(message),
      warn: (message) => runtime.log(message),
    },
  });

  if (result.skipped) {
    if (result.reason === "missing-wallet") {
      throw new Error(
        "0G memory restore requires a configured Ethereum wallet secret. Run openclaw setup first.",
      );
    }
    if (result.reason === "disabled") {
      throw new Error("0G memory restore is disabled by OPENCLAW_0G_MEMORY_SYNC_DISABLED.");
    }
    if (result.reason === "kv-read-unavailable") {
      throw new Error(
        "0G memory restore requires OPENCLAW_0G_KV_RPC_URL to point to a KV node RPC endpoint.",
      );
    }
    if (result.reason === "no-remote-manifest") {
      runtime.log(formatNoRemoteManifestSummary());
      return result;
    }
    if (result.reason === "already-restored") {
      runtime.log(formatAlreadyRestoredSummary());
      return result;
    }
    if (result.reason === "local-files-present") {
      runtime.log(formatLocalFilesPresentSummary());
      return result;
    }
    runtime.log("0G memory restore skipped.");
    return result;
  }

  if (result.failedCount > 0) {
    throw new Error(
      `0G memory restore recovered ${result.restoredCount} file${result.restoredCount === 1 ? "" : "s"}, but ${result.failedCount} download${result.failedCount === 1 ? " failed" : "s failed"}.`,
    );
  }

  runtime.log(formatRestoreSummary(result));
  return result;
}
