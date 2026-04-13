import { beforeEach, describe, expect, it, vi } from "vitest";
import type { OpenClawConfig } from "../config/config.js";

const restoreZeroGMemorySyncIfAvailable = vi.hoisted(() =>
  vi.fn(async () => ({
    skipped: true,
    reason: "no-remote-manifest",
    restoredCount: 0,
    skippedCount: 0,
    failedCount: 0,
    manifestRootHash: null,
  })),
);
const startZeroGMemorySyncService = vi.hoisted(() => vi.fn(async () => {}));
const startGmailWatcherWithLogs = vi.hoisted(() => vi.fn(async () => {}));
const loadInternalHooks = vi.hoisted(() => vi.fn(async () => 0));
const clearInternalHooks = vi.hoisted(() => vi.fn(() => {}));
const startPluginServices = vi.hoisted(() => vi.fn(async () => null));
const startGatewayMemoryBackend = vi.hoisted(() => vi.fn(async () => {}));
const resolveAgentSessionDirs = vi.hoisted(() => vi.fn(async () => []));
const cleanStaleLockFiles = vi.hoisted(() => vi.fn(async () => {}));

const ensureOpenClawModelsJsonMock = vi.fn<
  (config: unknown, agentDir: unknown) => Promise<{ agentDir: string; wrote: boolean }>
>(async () => ({ agentDir: "/tmp/agent", wrote: false }));
const resolveModelMock = vi.fn<
  (
    provider: unknown,
    modelId: unknown,
    agentDir: unknown,
    cfg: unknown,
    options?: unknown,
  ) => { model: { id: string; provider: string; api: string } }
>(() => ({
  model: {
    id: "gpt-5.4",
    provider: "openai-codex",
    api: "openai-codex-responses",
  },
}));

vi.mock("../agents/agent-paths.js", () => ({
  resolveOpenClawAgentDir: () => "/tmp/agent",
}));

vi.mock("../agents/models-config.js", () => ({
  ensureOpenClawModelsJson: (config: unknown, agentDir: unknown) =>
    ensureOpenClawModelsJsonMock(config, agentDir),
}));

vi.mock("../agents/pi-embedded-runner/model.js", () => ({
  resolveModel: (
    provider: unknown,
    modelId: unknown,
    agentDir: unknown,
    cfg: unknown,
    options?: unknown,
  ) => resolveModelMock(provider, modelId, agentDir, cfg, options),
}));

vi.mock("../zerog/storage-sync.service.js", () => ({
  restoreZeroGMemorySyncIfAvailable,
  startZeroGMemorySyncService,
}));

vi.mock("../hooks/gmail-watcher-lifecycle.js", () => ({
  startGmailWatcherWithLogs,
}));

vi.mock("../hooks/internal-hooks.js", () => ({
  clearInternalHooks,
  createInternalHookEvent: vi.fn(() => ({})),
  triggerInternalHook: vi.fn(async () => {}),
}));

vi.mock("../hooks/loader.js", () => ({
  loadInternalHooks,
}));

vi.mock("../plugins/services.js", () => ({
  startPluginServices,
}));

vi.mock("./server-startup-memory.js", () => ({
  startGatewayMemoryBackend,
}));

vi.mock("../agents/session-dirs.js", () => ({
  resolveAgentSessionDirs,
}));

vi.mock("../agents/session-write-lock.js", () => ({
  cleanStaleLockFiles,
}));

vi.mock("./server-restart-sentinel.js", () => ({
  scheduleRestartSentinelWake: vi.fn(async () => {}),
  shouldWakeFromRestartSentinel: () => false,
}));

vi.mock("../acp/control-plane/manager.js", () => ({
  getAcpSessionManager: () => ({
    reconcilePendingSessionIdentities: vi.fn(async () => ({
      checked: 0,
      resolved: 0,
      failed: 0,
    })),
  }),
}));

describe("gateway startup primary model warmup", () => {
  beforeEach(() => {
    ensureOpenClawModelsJsonMock.mockClear();
    resolveModelMock.mockClear();
    restoreZeroGMemorySyncIfAvailable.mockClear();
    startZeroGMemorySyncService.mockClear();
    startGmailWatcherWithLogs.mockClear();
    loadInternalHooks.mockClear();
    clearInternalHooks.mockClear();
    startPluginServices.mockClear();
    startGatewayMemoryBackend.mockClear();
    resolveAgentSessionDirs.mockClear();
    cleanStaleLockFiles.mockClear();
  });

  it("prewarms an explicit configured primary model", async () => {
    const { __testing } = await import("./server-startup.js");
    const cfg = {
      agents: {
        defaults: {
          model: {
            primary: "openai-codex/gpt-5.4",
          },
        },
      },
    } as OpenClawConfig;

    await __testing.prewarmConfiguredPrimaryModel({
      cfg,
      log: { warn: vi.fn() },
    });

    expect(ensureOpenClawModelsJsonMock).toHaveBeenCalledWith(cfg, "/tmp/agent");
    expect(resolveModelMock).toHaveBeenCalledWith("openai-codex", "gpt-5.4", "/tmp/agent", cfg, {
      skipProviderRuntimeHooks: true,
    });
  });

  it("skips warmup when no explicit primary model is configured", async () => {
    const { __testing } = await import("./server-startup.js");

    await __testing.prewarmConfiguredPrimaryModel({
      cfg: {} as OpenClawConfig,
      log: { warn: vi.fn() },
    });

    expect(ensureOpenClawModelsJsonMock).not.toHaveBeenCalled();
    expect(resolveModelMock).not.toHaveBeenCalled();
  });

  it("restores and starts 0G memory sync during sidecar startup", async () => {
    process.env.OPENCLAW_SKIP_CHANNELS = "1";
    const { startGatewaySidecars } = await import("./server-startup.js");
    const cfg = {} as OpenClawConfig;

    await startGatewaySidecars({
      cfg,
      pluginRegistry: [] as never,
      defaultWorkspaceDir: "/tmp/workspace",
      deps: {} as never,
      startChannels: vi.fn(async () => {}),
      log: { warn: vi.fn() },
      logHooks: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
      logChannels: { info: vi.fn(), error: vi.fn() },
    });

    expect(restoreZeroGMemorySyncIfAvailable).toHaveBeenCalledTimes(1);
    expect(startZeroGMemorySyncService).toHaveBeenCalledTimes(1);
    expect(startZeroGMemorySyncService).toHaveBeenCalledWith(
      expect.objectContaining({
        cfg,
        skipInitialRestore: true,
      }),
    );
    expect(startGatewayMemoryBackend).toHaveBeenCalledTimes(1);
    delete process.env.OPENCLAW_SKIP_CHANNELS;
  });
});
