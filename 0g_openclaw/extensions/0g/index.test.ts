import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
// import { buildZeroGProvider, resetZeroGModelCatalogCache } from "./index.js";
// import { encodeZeroGModelRef, parseZeroGModelRef } from "./model-ref.js";

const createZGComputeNetworkReadOnlyBrokerMock = vi.hoisted(() => vi.fn());
const createZGComputeNetworkBrokerMock = vi.hoisted(() => vi.fn());

vi.mock("@0glabs/0g-serving-broker", () => ({
  createZGComputeNetworkReadOnlyBroker: createZGComputeNetworkReadOnlyBrokerMock,
  createZGComputeNetworkBroker: createZGComputeNetworkBrokerMock,
}));

import { buildZeroGProvider, resetZeroGModelCatalogCache } from "./index.js";
import { encodeZeroGModelRef, parseZeroGModelRef } from "./model-ref.js";

const TEST_PRIVATE_KEY = `0x${"11".repeat(32)}`;

describe("0g provider", () => {
  let stateDir = "";
  let previousStateDir: string | undefined;
  let previousOauthDir: string | undefined;
  let previousRpcUrl: string | undefined;
  let previousNetwork: string | undefined;

  beforeEach(async () => {
    resetZeroGModelCatalogCache();
    createZGComputeNetworkReadOnlyBrokerMock.mockReset();
    createZGComputeNetworkBrokerMock.mockReset();
    previousStateDir = process.env.OPENCLAW_STATE_DIR;
    previousOauthDir = process.env.OPENCLAW_OAUTH_DIR;
    previousRpcUrl = process.env.OPENCLAW_0G_RPC_URL;
    previousNetwork = process.env.OPENCLAW_0G_NETWORK;
    stateDir = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-0g-provider-"));
    process.env.OPENCLAW_STATE_DIR = stateDir;
    delete process.env.OPENCLAW_OAUTH_DIR;
    delete process.env.OPENCLAW_0G_RPC_URL;
    delete process.env.OPENCLAW_0G_NETWORK;
    await fs.mkdir(path.join(stateDir, "credentials"), { recursive: true });
  });

  afterEach(async () => {
    resetZeroGModelCatalogCache();
    if (previousStateDir === undefined) {
      delete process.env.OPENCLAW_STATE_DIR;
    } else {
      process.env.OPENCLAW_STATE_DIR = previousStateDir;
    }
    if (previousOauthDir === undefined) {
      delete process.env.OPENCLAW_OAUTH_DIR;
    } else {
      process.env.OPENCLAW_OAUTH_DIR = previousOauthDir;
    }
    if (previousRpcUrl === undefined) {
      delete process.env.OPENCLAW_0G_RPC_URL;
    } else {
      process.env.OPENCLAW_0G_RPC_URL = previousRpcUrl;
    }
    if (previousNetwork === undefined) {
      delete process.env.OPENCLAW_0G_NETWORK;
    } else {
      process.env.OPENCLAW_0G_NETWORK = previousNetwork;
    }
    if (stateDir) {
      await fs.rm(stateDir, { recursive: true, force: true });
    }
  });

  async function writeWalletSecret() {
    await fs.writeFile(
      path.join(stateDir, "credentials", "ethereum-wallet.json"),
      JSON.stringify({
        version: 1,
        chain: "ethereum",
        kind: "private-key",
        privateKey: TEST_PRIVATE_KEY,
        source: "user",
        createdAtMs: 1,
      }),
      "utf8",
    );
  }

  it("encodes and parses 0g model refs", () => {
    const encoded = encodeZeroGModelRef({
      providerAddress: "0xABCD000000000000000000000000000000001234",
      model: "deepseek-r1",
      serviceType: "chatbot",
    });

    expect(parseZeroGModelRef(encoded)).toEqual({
      providerAddress: "0xabcd000000000000000000000000000000001234",
      model: "deepseek-r1",
      serviceType: "chatbot",
    });
  });

  it("adds only chat-capable 0g services to the catalog", async () => {
    await writeWalletSecret();
    const listServiceWithDetail = vi
      .fn()
      .mockResolvedValueOnce([
        {
          provider: "0x123400000000000000000000000000000000abcd",
          serviceType: "chatbot",
          model: "deepseek-r1",
          modelInfo: {
            name: "DeepSeek R1",
            context_length: 128000,
            max_completion_tokens: 8192,
            supported_parameters: ["reasoning_effort"],
            architecture: {
              input_modalities: ["text"],
              output_modalities: ["text"],
            },
          },
        },
        {
          provider: "0xabcd000000000000000000000000000000001111",
          serviceType: "text-to-image",
          model: "stable-diffusion",
          modelInfo: {
            name: "Stable Diffusion",
            architecture: {
              input_modalities: ["text"],
              output_modalities: ["image"],
            },
          },
        },
      ])
      .mockResolvedValueOnce([]);
    createZGComputeNetworkReadOnlyBrokerMock.mockResolvedValue({
      inference: {
        listServiceWithDetail,
      },
    });

    const provider = buildZeroGProvider();
    const entries = await provider.augmentModelCatalog?.({
      env: process.env,
      entries: [],
    } as never);

    expect(createZGComputeNetworkReadOnlyBrokerMock).toHaveBeenCalledWith("https://evmrpc.0g.ai");
    expect(entries).toHaveLength(1);
    expect(entries?.[0]).toMatchObject({
      provider: "0g",
      name: "DeepSeek R1 (0x1234...abcd)",
      reasoning: true,
      contextWindow: 128000,
    });
    expect(parseZeroGModelRef(entries?.[0]?.id ?? "")).toMatchObject({
      providerAddress: "0x123400000000000000000000000000000000abcd",
      model: "deepseek-r1",
      serviceType: "chatbot",
    });
    expect(listServiceWithDetail).toHaveBeenNthCalledWith(1, 0, 50, false);
  });

  it("allows forcing 0g testnet discovery with OPENCLAW_0G_NETWORK", async () => {
    await writeWalletSecret();
    process.env.OPENCLAW_0G_NETWORK = "testnet";
    createZGComputeNetworkReadOnlyBrokerMock.mockResolvedValue({
      inference: {
        listServiceWithDetail: vi.fn().mockResolvedValue([]),
      },
    });

    const provider = buildZeroGProvider();
    await provider.augmentModelCatalog?.({
      env: process.env,
      entries: [],
    } as never);

    expect(createZGComputeNetworkReadOnlyBrokerMock).toHaveBeenCalledWith(
      "https://evmrpc-testnet.0g.ai",
    );
  });

  it("synthesizes auth when a local ethereum wallet is configured", async () => {
    await writeWalletSecret();
    const provider = buildZeroGProvider();

    expect(
      provider.resolveSyntheticAuth?.({
        config: undefined,
        provider: "0g",
        providerConfig: undefined,
      }),
    ).toEqual({
      apiKey: "__openclaw_0g_wallet__",
      source: "ethereum-wallet.json (synthetic 0G key)",
      mode: "api-key",
    });
  });

  it("exchanges the wallet-backed 0g auth into a runtime api key and base url", async () => {
    await writeWalletSecret();
    createZGComputeNetworkBrokerMock.mockResolvedValue({
      inference: {
        acknowledged: vi.fn(async () => true),
        getServiceMetadata: vi.fn(async () => ({
          endpoint: "https://provider.example/v1/proxy",
          model: "deepseek-r1",
        })),
        getRequestHeaders: vi.fn(async () => ({
          Authorization: "Bearer app-sk-ephemeral-123",
        })),
      },
    });

    const provider = buildZeroGProvider();
    const modelId = encodeZeroGModelRef({
      providerAddress: "0x123400000000000000000000000000000000abcd",
      model: "deepseek-r1",
      serviceType: "chatbot",
    });
    const runtimeModel = provider.resolveDynamicModel?.({
      provider: "0g",
      modelId,
      modelRegistry: {} as never,
    });

    await expect(
      provider.prepareRuntimeAuth?.({
        config: undefined,
        agentDir: stateDir,
        workspaceDir: stateDir,
        env: process.env,
        provider: "0g",
        modelId,
        model: runtimeModel as never,
        apiKey: "__openclaw_0g_wallet__",
        authMode: "api-key",
      }),
    ).resolves.toEqual({
      apiKey: "app-sk-ephemeral-123",
      baseUrl: "https://provider.example/v1/proxy",
      expiresAt: expect.any(Number),
    });
  });

  it("falls back to requestProcessor.createApiKey when request headers are unavailable", async () => {
    await writeWalletSecret();
    createZGComputeNetworkBrokerMock.mockResolvedValue({
      inference: {
        acknowledged: vi.fn(async () => true),
        getServiceMetadata: vi.fn(async () => ({
          endpoint: "https://provider.example/v1/proxy",
          model: "deepseek-r1",
        })),
        requestProcessor: {
          createApiKey: vi.fn(async () => ({
            tokenId: 1,
            createdAt: 1,
            expiresAt: 999999,
            rawToken: "app-sk-123",
          })),
        },
      },
    });

    const provider = buildZeroGProvider();
    const modelId = encodeZeroGModelRef({
      providerAddress: "0x123400000000000000000000000000000000abcd",
      model: "deepseek-r1",
      serviceType: "chatbot",
    });
    const runtimeModel = provider.resolveDynamicModel?.({
      provider: "0g",
      modelId,
      modelRegistry: {} as never,
    });

    await expect(
      provider.prepareRuntimeAuth?.({
        config: undefined,
        agentDir: stateDir,
        workspaceDir: stateDir,
        env: process.env,
        provider: "0g",
        modelId,
        model: runtimeModel as never,
        apiKey: "__openclaw_0g_wallet__",
        authMode: "api-key",
      }),
    ).resolves.toEqual({
      apiKey: "app-sk-123",
      baseUrl: "https://provider.example/v1/proxy",
      expiresAt: 999999,
    });
  });
});
