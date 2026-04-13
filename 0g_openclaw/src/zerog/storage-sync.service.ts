import { createHash } from "node:crypto";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import {
  listAgentIds,
  resolveAgentIdByWorkspacePath,
  resolveAgentWorkspaceDir,
  resolveDefaultAgentId,
} from "../agents/agent-scope.js";
import { listAgentWorkspaceDirs } from "../agents/workspace-dirs.js";
import type { OpenClawConfig } from "../config/config.js";
import { resolveStateDir } from "../config/paths.js";
import { resolveSessionTranscriptsDirForAgent } from "../config/sessions/paths.js";
import { isTruthyEnvValue } from "../infra/env.js";
import {
  deriveEthereumWalletAddress,
  readStoredEthereumWalletSecret,
  type StoredEthereumWalletSecret,
} from "../wallet/ethereum-wallet.js";
import {
  type ZeroGMemorySyncCandidate,
  type ZeroGMemorySyncFileKind,
  type ZeroGMemorySyncManifest,
  type ZeroGMemorySyncPointer,
  type ZeroGMemorySyncState,
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
import {
  isZeroGKvReadEndpointError,
  type ZeroGStorageNetworkConfig,
  type ZeroGStorageUploadResult,
  downloadFileFromZeroGStorage,
  readZeroGKvValue,
  resolveZeroGStorageNetworkConfig,
  uploadDataToZeroGStorage,
  uploadFileToZeroGStorage,
  writeZeroGKvValue,
} from "./storage-sync.sdk.js";

const ZERO_G_MEMORY_SYNC_STATE_FILENAME = "memory-sync-state.json";
const ZERO_G_MEMORY_SYNC_INTERVAL_MS_DEFAULT = 60 * 60 * 1000;

export type ZeroGMemorySyncLog = {
  info?: (message: string) => void;
  warn: (message: string) => void;
};

export type ZeroGMemorySyncRunResult = {
  skipped: boolean;
  reason?: string;
  uploadedCount: number;
  failedCount: number;
  manifestRootHash: string | null;
};

export type ZeroGMemorySyncRestoreResult = {
  skipped: boolean;
  reason?: string;
  restoredCount: number;
  skippedCount: number;
  failedCount: number;
  manifestRootHash: string | null;
};

export type ZeroGMemorySyncTransport = {
  resolveNetworkConfig: (env?: NodeJS.ProcessEnv) => ZeroGStorageNetworkConfig;
  uploadFile: (params: {
    localPath: string;
    network: ZeroGStorageNetworkConfig;
    secret: StoredEthereumWalletSecret;
  }) => Promise<ZeroGStorageUploadResult>;
  uploadData: (params: {
    data: Uint8Array;
    network: ZeroGStorageNetworkConfig;
    secret: StoredEthereumWalletSecret;
  }) => Promise<ZeroGStorageUploadResult>;
  downloadFile: (params: {
    rootHash: string;
    outputPath: string;
    network: ZeroGStorageNetworkConfig;
  }) => Promise<void>;
  readKv: (params: {
    streamId: string;
    key: Uint8Array;
    network: ZeroGStorageNetworkConfig;
    env?: NodeJS.ProcessEnv;
  }) => Promise<string | null>;
  writeKv: (params: {
    streamId: string;
    key: Uint8Array;
    value: Uint8Array;
    network: ZeroGStorageNetworkConfig;
    secret: StoredEthereumWalletSecret;
  }) => Promise<ZeroGStorageUploadResult>;
};

type ZeroGMemorySyncDeps = {
  readWalletSecret?: () => StoredEthereumWalletSecret | null;
  now?: () => Date;
  transport?: ZeroGMemorySyncTransport;
};

const defaultTransport: ZeroGMemorySyncTransport = {
  resolveNetworkConfig: (env) => resolveZeroGStorageNetworkConfig(env),
  uploadFile: uploadFileToZeroGStorage,
  uploadData: uploadDataToZeroGStorage,
  downloadFile: downloadFileFromZeroGStorage,
  readKv: readZeroGKvValue,
  writeKv: writeZeroGKvValue,
};

export function isZeroGMemorySyncDisabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return isTruthyEnvValue(env.OPENCLAW_0G_MEMORY_SYNC_DISABLED);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stringifyError(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

function normalizeWalletAddress(walletAddress: string): string {
  return walletAddress.trim().toLowerCase();
}

function normalizeStateForWallet(
  state: ZeroGMemorySyncState,
  walletAddress: string,
): ZeroGMemorySyncState {
  const normalizedWalletAddress = normalizeWalletAddress(walletAddress);
  if (state.walletAddress && state.walletAddress !== normalizedWalletAddress) {
    return createEmptyZeroGMemorySyncState(normalizedWalletAddress);
  }
  return state.walletAddress ? state : { ...state, walletAddress: normalizedWalletAddress };
}

type ZeroGMemorySyncTrackedFile = ZeroGMemorySyncState["files"][string];

function isZeroGMemorySyncTrackedFile(value: unknown): value is ZeroGMemorySyncTrackedFile {
  return (
    isRecord(value) &&
    typeof value.logicalPath === "string" &&
    typeof value.size === "number" &&
    typeof value.mtimeMs === "number" &&
    typeof value.sha256 === "string" &&
    typeof value.rootHash === "string" &&
    typeof value.txHash === "string" &&
    typeof value.uploadedAt === "string"
  );
}

function parseZeroGMemorySyncState(raw: unknown): ZeroGMemorySyncState {
  if (!isRecord(raw) || raw.version !== 1 || !isRecord(raw.files)) {
    return createEmptyZeroGMemorySyncState();
  }

  const files: ZeroGMemorySyncState["files"] = {};
  for (const [logicalPath, value] of Object.entries(raw.files)) {
    if (!isZeroGMemorySyncTrackedFile(value)) {
      continue;
    }
    files[logicalPath] = {
      logicalPath: value.logicalPath,
      size: value.size,
      mtimeMs: value.mtimeMs,
      sha256: value.sha256,
      rootHash: value.rootHash,
      txHash: value.txHash,
      uploadedAt: value.uploadedAt,
    };
  }

  return {
    version: 1,
    ...(typeof raw.walletAddress === "string" ? { walletAddress: raw.walletAddress } : {}),
    ...(typeof raw.lastManifestRootHash === "string"
      ? { lastManifestRootHash: raw.lastManifestRootHash }
      : {}),
    ...(typeof raw.lastRestoreManifestRootHash === "string"
      ? { lastRestoreManifestRootHash: raw.lastRestoreManifestRootHash }
      : {}),
    files,
  };
}

function parseZeroGMemorySyncPointer(raw: unknown): ZeroGMemorySyncPointer | null {
  if (
    !isRecord(raw) ||
    raw.version !== 1 ||
    typeof raw.walletAddress !== "string" ||
    typeof raw.manifestRootHash !== "string" ||
    typeof raw.updatedAt !== "string" ||
    typeof raw.fileCount !== "number"
  ) {
    return null;
  }

  return {
    version: 1,
    walletAddress: normalizeWalletAddress(raw.walletAddress),
    manifestRootHash: raw.manifestRootHash,
    updatedAt: raw.updatedAt,
    fileCount: raw.fileCount,
  };
}

function parseZeroGMemorySyncManifest(raw: unknown): ZeroGMemorySyncManifest | null {
  if (
    !isRecord(raw) ||
    raw.version !== 1 ||
    typeof raw.walletAddress !== "string" ||
    typeof raw.createdAt !== "string" ||
    typeof raw.updatedAt !== "string" ||
    !isRecord(raw.files)
  ) {
    return null;
  }

  const manifest = createEmptyZeroGMemorySyncManifest(raw.walletAddress, raw.createdAt);
  manifest.updatedAt = raw.updatedAt;

  for (const [logicalPath, value] of Object.entries(raw.files)) {
    if (
      !isRecord(value) ||
      (value.kind !== "session-transcript" && value.kind !== "session-memory") ||
      typeof value.agentId !== "string" ||
      typeof value.logicalPath !== "string" ||
      typeof value.size !== "number" ||
      typeof value.mtimeMs !== "number" ||
      typeof value.sha256 !== "string" ||
      typeof value.rootHash !== "string" ||
      typeof value.txHash !== "string" ||
      typeof value.uploadedAt !== "string"
    ) {
      continue;
    }
    manifest.files[logicalPath] = {
      kind: value.kind,
      agentId: value.agentId,
      logicalPath: value.logicalPath,
      size: value.size,
      mtimeMs: value.mtimeMs,
      sha256: value.sha256,
      rootHash: value.rootHash,
      txHash: value.txHash,
      uploadedAt: value.uploadedAt,
    };
  }

  return manifest;
}

function buildManifestFromState(params: {
  state: ZeroGMemorySyncState;
  walletAddress: string;
  timestamp: string;
}): ZeroGMemorySyncManifest {
  let manifest = createEmptyZeroGMemorySyncManifest(params.walletAddress, params.timestamp);
  for (const trackedFile of Object.values(params.state.files).toSorted((left, right) =>
    left.logicalPath.localeCompare(right.logicalPath),
  )) {
    const parsed = parseZeroGMemorySyncLogicalPath(trackedFile.logicalPath);
    if (!parsed) {
      continue;
    }
    manifest = upsertZeroGMemorySyncManifestFile({
      manifest,
      timestamp: params.timestamp,
      file: {
        kind: parsed.kind,
        agentId: parsed.agentId,
        ...trackedFile,
      },
    });
  }
  return manifest;
}

async function readZeroGMemorySyncState(statePath: string): Promise<ZeroGMemorySyncState> {
  try {
    const raw = await fsp.readFile(statePath, "utf8");
    return parseZeroGMemorySyncState(JSON.parse(raw));
  } catch (err) {
    const code = (err as { code?: string }).code;
    if (code === "ENOENT") {
      return createEmptyZeroGMemorySyncState();
    }
    return createEmptyZeroGMemorySyncState();
  }
}

async function writeZeroGMemorySyncState(
  statePath: string,
  state: ZeroGMemorySyncState,
): Promise<void> {
  await fsp.mkdir(path.dirname(statePath), { recursive: true });
  await fsp.writeFile(statePath, `${JSON.stringify(state, null, 2)}\n`, "utf8");
}

async function hashFileSha256(filePath: string): Promise<string> {
  return await new Promise((resolve, reject) => {
    const hash = createHash("sha256");
    const stream = fs.createReadStream(filePath);
    stream.on("data", (chunk) => {
      hash.update(chunk);
    });
    stream.on("error", reject);
    stream.on("end", () => {
      resolve(hash.digest("hex"));
    });
  });
}

async function readDirSafe(dirPath: string): Promise<fs.Dirent[]> {
  try {
    return await fsp.readdir(dirPath, { withFileTypes: true });
  } catch (err) {
    const code = (err as { code?: string }).code;
    if (code === "ENOENT") {
      return [];
    }
    throw err;
  }
}

async function hasMatchingFileInDir(dirPath: string, suffix: string): Promise<boolean> {
  const entries = await readDirSafe(dirPath);
  return entries.some((entry) => entry.isFile() && entry.name.toLowerCase().endsWith(suffix));
}

async function buildCandidate(params: {
  kind: ZeroGMemorySyncFileKind;
  agentId: string;
  localPath: string;
  fileName: string;
  logicalPath: string;
}): Promise<ZeroGMemorySyncCandidate> {
  const stat = await fsp.stat(params.localPath);
  return {
    kind: params.kind,
    agentId: params.agentId,
    localPath: params.localPath,
    fileName: params.fileName,
    logicalPath: params.logicalPath,
    size: stat.size,
    mtimeMs: stat.mtimeMs,
    sha256: await hashFileSha256(params.localPath),
  };
}

export function resolveZeroGMemorySyncStatePath(
  env: NodeJS.ProcessEnv = process.env,
  stateDir: string = resolveStateDir(env),
): string {
  return path.join(stateDir, "zerog", ZERO_G_MEMORY_SYNC_STATE_FILENAME);
}

export async function hasLocalZeroGMemorySyncFiles(params: {
  cfg: OpenClawConfig;
  env?: NodeJS.ProcessEnv;
}): Promise<boolean> {
  const env = params.env ?? process.env;
  for (const agentId of listAgentIds(params.cfg)) {
    if (await hasMatchingFileInDir(resolveSessionTranscriptsDirForAgent(agentId, env), ".jsonl")) {
      return true;
    }
  }

  for (const workspaceDir of listAgentWorkspaceDirs(params.cfg)) {
    if (await hasMatchingFileInDir(path.join(workspaceDir, "memory"), ".md")) {
      return true;
    }
  }

  return false;
}

export async function collectZeroGMemorySyncCandidates(params: {
  cfg: OpenClawConfig;
  env?: NodeJS.ProcessEnv;
}): Promise<ZeroGMemorySyncCandidate[]> {
  const env = params.env ?? process.env;
  const candidates = new Map<string, ZeroGMemorySyncCandidate>();

  for (const agentId of listAgentIds(params.cfg).toSorted((left, right) =>
    left.localeCompare(right),
  )) {
    const sessionsDir = resolveSessionTranscriptsDirForAgent(agentId, env);
    const entries = await readDirSafe(sessionsDir);
    for (const entry of entries) {
      if (!entry.isFile() || !entry.name.toLowerCase().endsWith(".jsonl")) {
        continue;
      }
      const logicalPath = buildTranscriptLogicalPath(agentId, entry.name);
      const localPath = path.join(sessionsDir, entry.name);
      candidates.set(
        logicalPath,
        await buildCandidate({
          kind: "session-transcript",
          agentId,
          localPath,
          fileName: entry.name,
          logicalPath,
        }),
      );
    }
  }

  const defaultAgentId = resolveDefaultAgentId(params.cfg);
  for (const workspaceDir of listAgentWorkspaceDirs(params.cfg).toSorted((left, right) =>
    left.localeCompare(right),
  )) {
    const agentId = resolveAgentIdByWorkspacePath(params.cfg, workspaceDir) ?? defaultAgentId;
    const memoryDir = path.join(workspaceDir, "memory");
    const entries = await readDirSafe(memoryDir);
    for (const entry of entries) {
      if (!entry.isFile() || !entry.name.toLowerCase().endsWith(".md")) {
        continue;
      }
      const logicalPath = buildSessionMemoryLogicalPath(agentId, entry.name);
      const localPath = path.join(memoryDir, entry.name);
      candidates.set(
        logicalPath,
        await buildCandidate({
          kind: "session-memory",
          agentId,
          localPath,
          fileName: entry.name,
          logicalPath,
        }),
      );
    }
  }

  return [...candidates.values()].toSorted((left, right) =>
    left.logicalPath.localeCompare(right.logicalPath),
  );
}

export function resolveZeroGMemorySyncLocalPath(params: {
  cfg: OpenClawConfig;
  logicalPath: string;
  env?: NodeJS.ProcessEnv;
}): string | null {
  const env = params.env ?? process.env;
  const parsed = parseZeroGMemorySyncLogicalPath(params.logicalPath);
  if (!parsed) {
    return null;
  }

  if (parsed.kind === "session-transcript") {
    return path.join(resolveSessionTranscriptsDirForAgent(parsed.agentId, env), parsed.fileName);
  }

  return path.join(resolveAgentWorkspaceDir(params.cfg, parsed.agentId), "memory", parsed.fileName);
}

async function readRemoteManifest(params: {
  walletAddress: string;
  env?: NodeJS.ProcessEnv;
  deps?: ZeroGMemorySyncDeps;
}): Promise<{
  pointer: ZeroGMemorySyncPointer | null;
  manifest: ZeroGMemorySyncManifest | null;
}> {
  const env = params.env ?? process.env;
  const transport = params.deps?.transport ?? defaultTransport;
  const network = transport.resolveNetworkConfig(env);
  const streamId = createZeroGMemorySyncStreamId(params.walletAddress);
  const pointerRaw = await transport.readKv({
    streamId,
    key: getZeroGMemorySyncPointerKeyBytes(),
    network,
    env,
  });
  if (!pointerRaw) {
    return { pointer: null, manifest: null };
  }

  const pointer = parseZeroGMemorySyncPointer(JSON.parse(pointerRaw));
  if (!pointer) {
    return { pointer: null, manifest: null };
  }

  const tempDir = await fsp.mkdtemp(path.join(os.tmpdir(), "openclaw-zerog-manifest-"));
  const manifestPath = path.join(tempDir, "manifest.json");
  try {
    await transport.downloadFile({
      rootHash: pointer.manifestRootHash,
      outputPath: manifestPath,
      network,
    });
    const manifestRaw = JSON.parse(await fsp.readFile(manifestPath, "utf8"));
    return {
      pointer,
      manifest: parseZeroGMemorySyncManifest(manifestRaw),
    };
  } finally {
    await fsp.rm(tempDir, { recursive: true, force: true }).catch(() => {});
  }
}

export async function restoreZeroGMemorySyncIfAvailable(params: {
  cfg: OpenClawConfig;
  env?: NodeJS.ProcessEnv;
  log?: ZeroGMemorySyncLog;
  skipWhenLocalFilesPresent?: boolean;
  force?: boolean;
  deps?: ZeroGMemorySyncDeps;
}): Promise<ZeroGMemorySyncRestoreResult> {
  const env = params.env ?? process.env;
  if (isZeroGMemorySyncDisabled(env)) {
    return {
      skipped: true,
      reason: "disabled",
      restoredCount: 0,
      skippedCount: 0,
      failedCount: 0,
      manifestRootHash: null,
    };
  }

  const log = params.log;
  const readWalletSecret =
    params.deps?.readWalletSecret ?? (() => readStoredEthereumWalletSecret());
  const secret = readWalletSecret();
  if (!secret) {
    return {
      skipped: true,
      reason: "missing-wallet",
      restoredCount: 0,
      skippedCount: 0,
      failedCount: 0,
      manifestRootHash: null,
    };
  }

  if (
    params.skipWhenLocalFilesPresent &&
    (await hasLocalZeroGMemorySyncFiles({ cfg: params.cfg, env }))
  ) {
    return {
      skipped: true,
      reason: "local-files-present",
      restoredCount: 0,
      skippedCount: 0,
      failedCount: 0,
      manifestRootHash: null,
    };
  }

  const walletAddress = normalizeWalletAddress(deriveEthereumWalletAddress(secret));
  const statePath = resolveZeroGMemorySyncStatePath(env);
  const currentState = normalizeStateForWallet(
    await readZeroGMemorySyncState(statePath),
    walletAddress,
  );
  let remote;
  try {
    remote = await readRemoteManifest({ walletAddress, env, deps: params.deps });
  } catch (err) {
    if (isZeroGKvReadEndpointError(err)) {
      log?.warn?.(err.message);
      return {
        skipped: true,
        reason: "kv-read-unavailable",
        restoredCount: 0,
        skippedCount: 0,
        failedCount: 0,
        manifestRootHash: null,
      };
    }
    throw err;
  }
  if (!remote.pointer || !remote.manifest) {
    return {
      skipped: true,
      reason: "no-remote-manifest",
      restoredCount: 0,
      skippedCount: 0,
      failedCount: 0,
      manifestRootHash: null,
    };
  }

  if (
    !params.force &&
    currentState.lastRestoreManifestRootHash === remote.pointer.manifestRootHash
  ) {
    return {
      skipped: true,
      reason: "already-restored",
      restoredCount: 0,
      skippedCount: 0,
      failedCount: 0,
      manifestRootHash: remote.pointer.manifestRootHash,
    };
  }

  const transport = params.deps?.transport ?? defaultTransport;
  const network = transport.resolveNetworkConfig(env);
  let nextState = createEmptyZeroGMemorySyncState(walletAddress);
  nextState.lastManifestRootHash = remote.pointer.manifestRootHash;

  let restoredCount = 0;
  let skippedCount = 0;
  let failedCount = 0;

  for (const file of Object.values(remote.manifest.files).toSorted((left, right) =>
    left.logicalPath.localeCompare(right.logicalPath),
  )) {
    const localPath = resolveZeroGMemorySyncLocalPath({
      cfg: params.cfg,
      logicalPath: file.logicalPath,
      env,
    });
    if (!localPath) {
      failedCount += 1;
      log?.warn?.(`0G memory restore skipped unsupported path: ${file.logicalPath}`);
      continue;
    }

    nextState = setZeroGMemorySyncStateFile({ state: nextState, file });

    if (!params.force) {
      try {
        await fsp.access(localPath);
        skippedCount += 1;
        continue;
      } catch {
        // Missing locally; continue with download.
      }
    }

    const tempPath = `${localPath}.openclaw-zerog-download`;
    try {
      await fsp.mkdir(path.dirname(localPath), { recursive: true });
      await fsp.rm(tempPath, { force: true }).catch(() => {});
      await transport.downloadFile({
        rootHash: file.rootHash,
        outputPath: tempPath,
        network,
      });
      await fsp.rename(tempPath, localPath);
      restoredCount += 1;
    } catch (err) {
      failedCount += 1;
      delete nextState.files[file.logicalPath];
      log?.warn?.(`0G memory restore failed for ${file.logicalPath}: ${stringifyError(err)}`);
      await fsp.rm(tempPath, { force: true }).catch(() => {});
    }
  }

  if (failedCount === 0) {
    nextState.lastRestoreManifestRootHash = remote.pointer.manifestRootHash;
  }
  await writeZeroGMemorySyncState(statePath, nextState);

  return {
    skipped: false,
    restoredCount,
    skippedCount,
    failedCount,
    manifestRootHash: remote.pointer.manifestRootHash,
  };
}

export async function runZeroGMemorySyncPass(params: {
  cfg: OpenClawConfig;
  env?: NodeJS.ProcessEnv;
  log?: ZeroGMemorySyncLog;
  deps?: ZeroGMemorySyncDeps;
}): Promise<ZeroGMemorySyncRunResult> {
  const env = params.env ?? process.env;
  if (isZeroGMemorySyncDisabled(env)) {
    return {
      skipped: true,
      reason: "disabled",
      uploadedCount: 0,
      failedCount: 0,
      manifestRootHash: null,
    };
  }

  const log = params.log;
  const readWalletSecret =
    params.deps?.readWalletSecret ?? (() => readStoredEthereumWalletSecret());
  const secret = readWalletSecret();
  if (!secret) {
    return {
      skipped: true,
      reason: "missing-wallet",
      uploadedCount: 0,
      failedCount: 0,
      manifestRootHash: null,
    };
  }

  const transport = params.deps?.transport ?? defaultTransport;
  const network = transport.resolveNetworkConfig(env);
  const walletAddress = normalizeWalletAddress(deriveEthereumWalletAddress(secret));
  const statePath = resolveZeroGMemorySyncStatePath(env);
  const currentState = normalizeStateForWallet(
    await readZeroGMemorySyncState(statePath),
    walletAddress,
  );
  const candidates = await collectZeroGMemorySyncCandidates({ cfg: params.cfg, env });

  let nextState = currentState;
  let uploadedCount = 0;
  let failedCount = 0;

  for (const candidate of candidates) {
    if (!shouldUploadZeroGMemorySyncCandidate({ candidate, state: nextState })) {
      continue;
    }

    try {
      const result = await transport.uploadFile({
        localPath: candidate.localPath,
        network,
        secret,
      });
      nextState = setZeroGMemorySyncStateFile({
        state: nextState,
        walletAddress,
        file: {
          logicalPath: candidate.logicalPath,
          size: candidate.size,
          mtimeMs: candidate.mtimeMs,
          sha256: candidate.sha256,
          rootHash: result.rootHash,
          txHash: result.txHash,
          uploadedAt: (params.deps?.now ?? (() => new Date()))().toISOString(),
        },
      });
      uploadedCount += 1;
    } catch (err) {
      failedCount += 1;
      log?.warn?.(
        `0G memory sync upload failed for ${candidate.logicalPath}: ${stringifyError(err)}`,
      );
    }
  }

  if (uploadedCount === 0) {
    if (!currentState.walletAddress && nextState.walletAddress) {
      await writeZeroGMemorySyncState(statePath, nextState);
    }
    return {
      skipped: true,
      reason: "no-changes",
      uploadedCount: 0,
      failedCount,
      manifestRootHash: currentState.lastManifestRootHash ?? null,
    };
  }

  const timestamp = (params.deps?.now ?? (() => new Date()))().toISOString();
  const manifest = buildManifestFromState({
    state: nextState,
    walletAddress,
    timestamp,
  });

  const manifestUpload = await transport.uploadData({
    data: Uint8Array.from(Buffer.from(JSON.stringify(manifest), "utf8")),
    network,
    secret,
  });

  const pointer = createZeroGMemorySyncPointer({
    walletAddress,
    manifestRootHash: manifestUpload.rootHash,
    updatedAt: timestamp,
    fileCount: countZeroGMemorySyncManifestFiles(manifest),
  });

  await transport.writeKv({
    streamId: createZeroGMemorySyncStreamId(walletAddress),
    key: getZeroGMemorySyncPointerKeyBytes(),
    value: Uint8Array.from(Buffer.from(JSON.stringify(pointer), "utf8")),
    network,
    secret,
  });

  nextState.lastManifestRootHash = manifestUpload.rootHash;
  await writeZeroGMemorySyncState(statePath, nextState);
  log?.info?.(`0G memory sync uploaded ${uploadedCount} file${uploadedCount === 1 ? "" : "s"}.`);

  return {
    skipped: false,
    uploadedCount,
    failedCount,
    manifestRootHash: manifestUpload.rootHash,
  };
}

function resolveZeroGMemorySyncIntervalMs(env: NodeJS.ProcessEnv = process.env): number {
  const raw = env.OPENCLAW_0G_MEMORY_SYNC_INTERVAL_MS?.trim();
  if (!raw) {
    return ZERO_G_MEMORY_SYNC_INTERVAL_MS_DEFAULT;
  }
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : ZERO_G_MEMORY_SYNC_INTERVAL_MS_DEFAULT;
}

export async function startZeroGMemorySyncService(params: {
  cfg: OpenClawConfig;
  env?: NodeJS.ProcessEnv;
  log: ZeroGMemorySyncLog;
  skipInitialRestore?: boolean;
  deps?: ZeroGMemorySyncDeps;
}): Promise<void> {
  const env = params.env ?? process.env;
  if (isZeroGMemorySyncDisabled(env)) {
    return;
  }

  const intervalMs = resolveZeroGMemorySyncIntervalMs(env);
  let running = false;

  const runCycle = async (restoreFirst: boolean) => {
    if (running) {
      return;
    }
    running = true;
    try {
      if (restoreFirst) {
        await restoreZeroGMemorySyncIfAvailable({
          cfg: params.cfg,
          env,
          log: params.log,
          skipWhenLocalFilesPresent: true,
          deps: params.deps,
        });
      }
      await runZeroGMemorySyncPass({
        cfg: params.cfg,
        env,
        log: params.log,
        deps: params.deps,
      });
    } catch (err) {
      params.log.warn(`0G memory sync cycle failed: ${stringifyError(err)}`);
    } finally {
      running = false;
    }
  };

  void runCycle(params.skipInitialRestore !== true);
  const timer = setInterval(() => {
    void runCycle(false);
  }, intervalMs);
  timer.unref?.();
}
