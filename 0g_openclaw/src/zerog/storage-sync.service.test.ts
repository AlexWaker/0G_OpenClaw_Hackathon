import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { buildStoredEthereumWalletSecret } from "../wallet/ethereum-wallet.js";
import {
  buildSessionMemoryLogicalPath,
  buildTranscriptLogicalPath,
  createZeroGMemorySyncPointer,
} from "./storage-sync.js";
import { ZeroGKvReadEndpointError } from "./storage-sync.sdk.js";
import {
  collectZeroGMemorySyncCandidates,
  resolveZeroGMemorySyncStatePath,
  restoreZeroGMemorySyncIfAvailable,
  runZeroGMemorySyncPass,
} from "./storage-sync.service.js";

const walletSecret = buildStoredEthereumWalletSecret({
  kind: "private-key",
  value: "0x59c6995e998f97a5a0044966f0945382d7f5b8b7d6c590e2a6e2e20f0f3d9fc1",
  source: "user",
  createdAtMs: 1,
});

async function createTempRoot(): Promise<string> {
  return await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-zerog-sync-"));
}

describe("0G memory sync service", () => {
  const tempRoots: string[] = [];

  afterEach(async () => {
    while (tempRoots.length > 0) {
      const root = tempRoots.pop();
      if (!root) {
        continue;
      }
      await fs.rm(root, { recursive: true, force: true }).catch(() => {});
    }
  });

  it("collects transcript and memory candidates across agents", async () => {
    const root = await createTempRoot();
    tempRoots.push(root);
    const mainWorkspace = path.join(root, "workspace-main");
    const opsWorkspace = path.join(root, "workspace-ops");
    const env = { ...process.env, OPENCLAW_STATE_DIR: root };
    const cfg = {
      agents: {
        defaults: { workspace: mainWorkspace },
        list: [
          { id: "main", default: true },
          { id: "ops", workspace: opsWorkspace },
        ],
      },
    };

    await fs.mkdir(path.join(root, "agents", "main", "sessions"), { recursive: true });
    await fs.mkdir(path.join(root, "agents", "ops", "sessions"), { recursive: true });
    await fs.mkdir(path.join(mainWorkspace, "memory"), { recursive: true });
    await fs.mkdir(path.join(opsWorkspace, "memory"), { recursive: true });

    await fs.writeFile(path.join(root, "agents", "main", "sessions", "a.jsonl"), "{}\n");
    await fs.writeFile(path.join(root, "agents", "ops", "sessions", "b.jsonl"), "{}\n");
    await fs.writeFile(path.join(mainWorkspace, "memory", "2026-04-11-main.md"), "main");
    await fs.writeFile(path.join(opsWorkspace, "memory", "2026-04-11-ops.md"), "ops");

    const candidates = await collectZeroGMemorySyncCandidates({ cfg, env });
    expect(candidates.map((candidate) => candidate.logicalPath)).toEqual([
      "agents/main/memory/2026-04-11-main.md",
      "agents/main/sessions/a.jsonl",
      "agents/ops/memory/2026-04-11-ops.md",
      "agents/ops/sessions/b.jsonl",
    ]);
  });

  it("uploads changed files once and persists state for later scans", async () => {
    const root = await createTempRoot();
    tempRoots.push(root);
    const workspace = path.join(root, "workspace-main");
    const env = { ...process.env, OPENCLAW_STATE_DIR: root };
    const cfg = {
      agents: {
        defaults: { workspace },
        list: [{ id: "main", default: true }],
      },
    };

    await fs.mkdir(path.join(root, "agents", "main", "sessions"), { recursive: true });
    await fs.mkdir(path.join(workspace, "memory"), { recursive: true });
    await fs.writeFile(path.join(root, "agents", "main", "sessions", "session.jsonl"), "{}\n");

    const transport = {
      resolveNetworkConfig: vi.fn(() => ({
        network: "mainnet" as const,
        mode: "turbo" as const,
        rpcUrl: "https://evmrpc.0g.ai",
        indexerRpc: "https://indexer-storage-turbo.0g.ai",
      })),
      uploadFile: vi.fn(async () => ({ rootHash: "0xfile", txHash: "0xtx-file" })),
      uploadData: vi.fn(async () => ({ rootHash: "0xmanifest", txHash: "0xtx-manifest" })),
      downloadFile: vi.fn(async () => {}),
      readKv: vi.fn(async () => null),
      writeKv: vi.fn(async () => ({ rootHash: "0xpointer", txHash: "0xtx-pointer" })),
    };

    const first = await runZeroGMemorySyncPass({
      cfg,
      env,
      deps: { readWalletSecret: () => walletSecret, transport },
    });
    expect(first.skipped).toBe(false);
    expect(first.uploadedCount).toBe(1);
    expect(first.failedCount).toBe(0);
    expect(first.manifestRootHash).toBe("0xmanifest");
    expect(transport.uploadFile).toHaveBeenCalledTimes(1);
    expect(transport.uploadData).toHaveBeenCalledTimes(1);
    expect(transport.writeKv).toHaveBeenCalledTimes(1);

    const second = await runZeroGMemorySyncPass({
      cfg,
      env,
      deps: { readWalletSecret: () => walletSecret, transport },
    });
    expect(second.skipped).toBe(true);
    expect(second.reason).toBe("no-changes");
    expect(second.uploadedCount).toBe(0);
    expect(transport.uploadFile).toHaveBeenCalledTimes(1);
    expect(transport.uploadData).toHaveBeenCalledTimes(1);
    expect(transport.writeKv).toHaveBeenCalledTimes(1);

    const statePath = resolveZeroGMemorySyncStatePath(env);
    const savedState = JSON.parse(await fs.readFile(statePath, "utf8")) as {
      lastManifestRootHash?: string;
      files?: Record<string, unknown>;
    };
    expect(savedState.lastManifestRootHash).toBe("0xmanifest");
    expect(Object.keys(savedState.files ?? {})).toEqual([
      buildTranscriptLogicalPath("main", "session.jsonl"),
    ]);
  });

  it("restores remote transcript and memory files into local paths", async () => {
    const root = await createTempRoot();
    tempRoots.push(root);
    const workspace = path.join(root, "workspace-main");
    const env = { ...process.env, OPENCLAW_STATE_DIR: root };
    const cfg = {
      agents: {
        defaults: { workspace },
        list: [{ id: "main", default: true }],
      },
    };

    const manifest = {
      version: 1,
      walletAddress: walletSecret.kind === "private-key" ? "" : "",
      createdAt: "2026-04-11T00:00:00.000Z",
      updatedAt: "2026-04-11T00:00:00.000Z",
      files: {
        [buildTranscriptLogicalPath("main", "remote.jsonl")]: {
          kind: "session-transcript",
          agentId: "main",
          logicalPath: buildTranscriptLogicalPath("main", "remote.jsonl"),
          size: 4,
          mtimeMs: 1,
          sha256: "hash-a",
          rootHash: "0xremote-transcript",
          txHash: "0xtx-a",
          uploadedAt: "2026-04-11T00:00:00.000Z",
        },
        [buildSessionMemoryLogicalPath("main", "remote.md")]: {
          kind: "session-memory",
          agentId: "main",
          logicalPath: buildSessionMemoryLogicalPath("main", "remote.md"),
          size: 6,
          mtimeMs: 2,
          sha256: "hash-b",
          rootHash: "0xremote-memory",
          txHash: "0xtx-b",
          uploadedAt: "2026-04-11T00:00:00.000Z",
        },
      },
    };

    const pointer = createZeroGMemorySyncPointer({
      walletAddress: "0xabc123",
      manifestRootHash: "0xmanifest-root",
      updatedAt: "2026-04-11T00:00:00.000Z",
      fileCount: 2,
    });

    const downloads = new Map<string, string>([
      ["0xmanifest-root", JSON.stringify({ ...manifest, walletAddress: "0xabc123" })],
      ["0xremote-transcript", '{"ok":true}\n'],
      ["0xremote-memory", "# hello\n"],
    ]);

    const transport = {
      resolveNetworkConfig: vi.fn(() => ({
        network: "mainnet" as const,
        mode: "turbo" as const,
        rpcUrl: "https://evmrpc.0g.ai",
        indexerRpc: "https://indexer-storage-turbo.0g.ai",
      })),
      uploadFile: vi.fn(async () => ({ rootHash: "", txHash: "" })),
      uploadData: vi.fn(async () => ({ rootHash: "", txHash: "" })),
      downloadFile: vi.fn(
        async ({ rootHash, outputPath }: { rootHash: string; outputPath: string }) => {
          await fs.mkdir(path.dirname(outputPath), { recursive: true });
          await fs.writeFile(outputPath, downloads.get(rootHash) ?? "", "utf8");
        },
      ),
      readKv: vi.fn(async () => JSON.stringify(pointer)),
      writeKv: vi.fn(async () => ({ rootHash: "", txHash: "" })),
    };

    const result = await restoreZeroGMemorySyncIfAvailable({
      cfg,
      env,
      deps: { readWalletSecret: () => walletSecret, transport },
      force: true,
    });

    expect(result.skipped).toBe(false);
    expect(result.restoredCount).toBe(2);
    expect(result.failedCount).toBe(0);
    expect(result.manifestRootHash).toBe("0xmanifest-root");
    expect(
      await fs.readFile(path.join(root, "agents", "main", "sessions", "remote.jsonl"), "utf8"),
    ).toBe('{"ok":true}\n');
    expect(await fs.readFile(path.join(workspace, "memory", "remote.md"), "utf8")).toBe(
      "# hello\n",
    );
  });

  it("skips restore when no KV read endpoint is configured", async () => {
    const root = await createTempRoot();
    tempRoots.push(root);
    const workspace = path.join(root, "workspace-main");
    const env = { ...process.env, OPENCLAW_STATE_DIR: root };
    const cfg = {
      agents: {
        defaults: { workspace },
        list: [{ id: "main", default: true }],
      },
    };

    const transport = {
      resolveNetworkConfig: vi.fn(() => ({
        network: "mainnet" as const,
        mode: "turbo" as const,
        rpcUrl: "https://evmrpc.0g.ai",
        indexerRpc: "https://indexer-storage-turbo.0g.ai",
      })),
      uploadFile: vi.fn(async () => ({ rootHash: "", txHash: "" })),
      uploadData: vi.fn(async () => ({ rootHash: "", txHash: "" })),
      downloadFile: vi.fn(async () => {}),
      readKv: vi.fn(async () => {
        throw new ZeroGKvReadEndpointError(
          "unsupported-endpoint",
          "Configured 0G KV read endpoint does not support kv_getValue.",
        );
      }),
      writeKv: vi.fn(async () => ({ rootHash: "", txHash: "" })),
    };

    const result = await restoreZeroGMemorySyncIfAvailable({
      cfg,
      env,
      deps: { readWalletSecret: () => walletSecret, transport },
    });

    expect(result.skipped).toBe(true);
    expect(result.reason).toBe("kv-read-unavailable");
    expect(result.restoredCount).toBe(0);
    expect(result.manifestRootHash).toBeNull();
    expect(transport.downloadFile).not.toHaveBeenCalled();
  });
});
