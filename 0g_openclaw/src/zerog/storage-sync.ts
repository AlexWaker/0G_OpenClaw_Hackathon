import { Buffer } from "node:buffer";
import path from "node:path";
import { keccak256, toUtf8Bytes } from "ethers";
import { normalizeAgentId } from "../routing/session-key.js";

export const ZERO_G_MEMORY_SYNC_VERSION = 1;

const ZERO_G_MEMORY_SYNC_NAMESPACE = "openclaw:0g-memory-sync:v1";
const ZERO_G_MEMORY_SYNC_POINTER_KEY = "latest-manifest";

export type ZeroGMemorySyncFileKind = "session-transcript" | "session-memory";

export type ZeroGMemorySyncTrackedFile = {
  logicalPath: string;
  size: number;
  mtimeMs: number;
  sha256: string;
  rootHash: string;
  txHash: string;
  uploadedAt: string;
};

export type ZeroGMemorySyncRemoteFile = ZeroGMemorySyncTrackedFile & {
  kind: ZeroGMemorySyncFileKind;
  agentId: string;
};

export type ZeroGMemorySyncManifest = {
  version: typeof ZERO_G_MEMORY_SYNC_VERSION;
  walletAddress: string;
  createdAt: string;
  updatedAt: string;
  files: Record<string, ZeroGMemorySyncRemoteFile>;
};

export type ZeroGMemorySyncPointer = {
  version: typeof ZERO_G_MEMORY_SYNC_VERSION;
  walletAddress: string;
  manifestRootHash: string;
  updatedAt: string;
  fileCount: number;
};

export type ZeroGMemorySyncState = {
  version: typeof ZERO_G_MEMORY_SYNC_VERSION;
  walletAddress?: string;
  lastManifestRootHash?: string;
  lastRestoreManifestRootHash?: string;
  files: Record<string, ZeroGMemorySyncTrackedFile>;
};

export type ZeroGMemorySyncCandidate = {
  kind: ZeroGMemorySyncFileKind;
  agentId: string;
  localPath: string;
  fileName: string;
  logicalPath: string;
  size: number;
  mtimeMs: number;
  sha256: string;
};

export type ZeroGMemorySyncLogicalPath = {
  kind: ZeroGMemorySyncFileKind;
  agentId: string;
  fileName: string;
};

function normalizeWalletAddress(walletAddress: string): string {
  return walletAddress.trim().toLowerCase();
}

function normalizeFileName(fileName: string): string {
  const base = path.posix.basename(fileName.trim());
  if (!base || base === "." || base === "..") {
    throw new Error(`Invalid file name: ${fileName}`);
  }
  return base;
}

export function createZeroGMemorySyncStreamId(walletAddress: string): string {
  const normalizedWalletAddress = normalizeWalletAddress(walletAddress);
  if (!normalizedWalletAddress) {
    throw new Error("Wallet address is required to derive 0G memory sync stream id.");
  }
  return keccak256(toUtf8Bytes(`${ZERO_G_MEMORY_SYNC_NAMESPACE}:${normalizedWalletAddress}`));
}

export function getZeroGMemorySyncPointerKeyBytes(): Uint8Array {
  return Uint8Array.from(Buffer.from(ZERO_G_MEMORY_SYNC_POINTER_KEY, "utf8"));
}

export function buildTranscriptLogicalPath(agentId: string, fileName: string): string {
  return path.posix.join(
    "agents",
    normalizeAgentId(agentId),
    "sessions",
    normalizeFileName(fileName),
  );
}

export function buildSessionMemoryLogicalPath(agentId: string, fileName: string): string {
  return path.posix.join(
    "agents",
    normalizeAgentId(agentId),
    "memory",
    normalizeFileName(fileName),
  );
}

export function parseZeroGMemorySyncLogicalPath(
  logicalPath: string,
): ZeroGMemorySyncLogicalPath | null {
  const normalized = logicalPath.trim().replace(/\\/g, "/");
  const parts = normalized.split("/").filter(Boolean);
  if (parts.length !== 4 || parts[0] !== "agents") {
    return null;
  }

  const [, rawAgentId, category, rawFileName] = parts;
  if (!rawAgentId || !rawFileName) {
    return null;
  }

  const agentId = normalizeAgentId(rawAgentId);
  const fileName = path.posix.basename(rawFileName);
  if (!agentId || !fileName || fileName === "." || fileName === "..") {
    return null;
  }

  if (category === "sessions") {
    return { kind: "session-transcript", agentId, fileName };
  }
  if (category === "memory") {
    return { kind: "session-memory", agentId, fileName };
  }
  return null;
}

export function createEmptyZeroGMemorySyncManifest(
  walletAddress: string,
  timestamp: string,
): ZeroGMemorySyncManifest {
  return {
    version: ZERO_G_MEMORY_SYNC_VERSION,
    walletAddress: normalizeWalletAddress(walletAddress),
    createdAt: timestamp,
    updatedAt: timestamp,
    files: {},
  };
}

export function createEmptyZeroGMemorySyncState(walletAddress?: string): ZeroGMemorySyncState {
  return {
    version: ZERO_G_MEMORY_SYNC_VERSION,
    ...(walletAddress ? { walletAddress: normalizeWalletAddress(walletAddress) } : {}),
    files: {},
  };
}

export function upsertZeroGMemorySyncManifestFile(params: {
  manifest: ZeroGMemorySyncManifest;
  file: ZeroGMemorySyncRemoteFile;
  timestamp: string;
}): ZeroGMemorySyncManifest {
  const files = {
    ...params.manifest.files,
    [params.file.logicalPath]: params.file,
  };
  return {
    ...params.manifest,
    updatedAt: params.timestamp,
    files,
  };
}

export function setZeroGMemorySyncStateFile(params: {
  state: ZeroGMemorySyncState;
  file: ZeroGMemorySyncTrackedFile;
  walletAddress?: string;
  lastManifestRootHash?: string;
}): ZeroGMemorySyncState {
  return {
    ...params.state,
    ...(params.walletAddress
      ? { walletAddress: normalizeWalletAddress(params.walletAddress) }
      : {}),
    ...(params.lastManifestRootHash ? { lastManifestRootHash: params.lastManifestRootHash } : {}),
    files: {
      ...params.state.files,
      [params.file.logicalPath]: params.file,
    },
  };
}

export function createZeroGMemorySyncPointer(params: {
  walletAddress: string;
  manifestRootHash: string;
  updatedAt: string;
  fileCount: number;
}): ZeroGMemorySyncPointer {
  return {
    version: ZERO_G_MEMORY_SYNC_VERSION,
    walletAddress: normalizeWalletAddress(params.walletAddress),
    manifestRootHash: params.manifestRootHash,
    updatedAt: params.updatedAt,
    fileCount: params.fileCount,
  };
}

export function shouldUploadZeroGMemorySyncCandidate(params: {
  candidate: ZeroGMemorySyncCandidate;
  state: ZeroGMemorySyncState;
}): boolean {
  const tracked = params.state.files[params.candidate.logicalPath];
  if (!tracked) {
    return true;
  }
  return (
    tracked.sha256 !== params.candidate.sha256 ||
    tracked.size !== params.candidate.size ||
    tracked.mtimeMs !== params.candidate.mtimeMs
  );
}

export function countZeroGMemorySyncManifestFiles(manifest: ZeroGMemorySyncManifest): number {
  return Object.keys(manifest.files).length;
}
