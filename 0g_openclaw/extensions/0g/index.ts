import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  createZGComputeNetworkBroker,
  createZGComputeNetworkReadOnlyBroker,
} from "@0glabs/0g-serving-broker";
import { HDNodeWallet, JsonRpcProvider, Wallet } from "ethers";
import {
  type ProviderBuildMissingAuthMessageContext,
  type ProviderAugmentModelCatalogContext,
  type ProviderBuildUnknownModelHintContext,
  type ProviderPrepareDynamicModelContext,
  type ProviderPrepareRuntimeAuthContext,
  type ProviderResolveDynamicModelContext,
  type ProviderRuntimeModel,
} from "openclaw/plugin-sdk/core";
import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";
import { DEFAULT_CONTEXT_TOKENS } from "openclaw/plugin-sdk/provider-model-shared";
import {
  encodeZeroGModelRef,
  formatZeroGModelLabel,
  parseZeroGModelRef,
  ZERO_G_PROVIDER_ID,
} from "./model-ref.js";

type ZeroGWalletSecret =
  | {
      chain: "ethereum";
      kind: "mnemonic";
      mnemonic: string;
    }
  | {
      chain: "ethereum";
      kind: "private-key";
      privateKey: string;
    };

type ZeroGServiceWithDetail = {
  provider: string;
  serviceType: string;
  model: string;
  modelInfo?: {
    name?: string;
    context_length?: number;
    max_completion_tokens?: number;
    supported_parameters?: string[];
    architecture?: {
      input_modalities?: string[];
      output_modalities?: string[];
    };
  };
};

type ZeroGCatalogRecord = {
  id: string;
  label: string;
  model: string;
  providerAddress: string;
  contextWindow?: number;
  maxTokens?: number;
  reasoning?: boolean;
  input?: Array<"text" | "image">;
};

type ZeroGApiKeyInfo = {
  rawToken: string;
  expiresAt: number;
};

type ZeroGInferenceApiKeyBroker = {
  createApiKey: (
    providerAddress: string,
    options?: { expiresIn?: number; tokenId?: number },
  ) => Promise<ZeroGApiKeyInfo>;
};

type ZeroGServingRequestHeaders = {
  Authorization?: string;
  authorization?: string;
};

type ZeroGInferenceRuntimeAuthBroker = {
  getRequestHeaders?: (
    providerAddress: string,
    content?: string,
  ) => Promise<ZeroGServingRequestHeaders>;
  requestProcessor?: ZeroGInferenceApiKeyBroker;
};

type ZeroGResolveSyntheticAuthContext = {
  config?: unknown;
  provider: string;
  providerConfig?: unknown;
};

const ZERO_G_RUNTIME_AUTH_MARKER = "__openclaw_0g_wallet__";
const ZERO_G_PLACEHOLDER_BASE_URL = "https://compute-marketplace.0g.ai/v1/proxy";
const ZERO_G_TESTNET_RPC_URL = "https://evmrpc-testnet.0g.ai";
const ZERO_G_MAINNET_RPC_URL = "https://evmrpc.0g.ai";
const ZERO_G_MODEL_CACHE_TTL_MS = 5 * 60 * 1000;
// const ZERO_G_MODEL_PAGE_SIZE = 100;
const ZERO_G_MODEL_PAGE_SIZE = 50;
const ZERO_G_RUNTIME_TOKEN_TTL_MS = 60 * 60 * 1000;

let zeroGModelCacheExpiresAt = 0;
let zeroGModelCachePromise: Promise<ProviderAugmentModelCatalogContext["entries"]> | null = null;
const zeroGModelDetailsById = new Map<string, ZeroGCatalogRecord>();

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function resolveZeroGRpcUrl(env: NodeJS.ProcessEnv = process.env): string {
  const explicit = env.OPENCLAW_0G_RPC_URL?.trim();
  if (explicit) {
    return explicit;
  }
  const network = env.OPENCLAW_0G_NETWORK?.trim().toLowerCase();
  // return network === "mainnet" ? ZERO_G_MAINNET_RPC_URL : ZERO_G_TESTNET_RPC_URL;
  return network === "testnet" ? ZERO_G_TESTNET_RPC_URL : ZERO_G_MAINNET_RPC_URL;
}

function resolveZeroGCredentialsDir(env: NodeJS.ProcessEnv = process.env): string {
  const explicit = env.OPENCLAW_OAUTH_DIR?.trim();
  if (explicit) {
    return explicit;
  }
  const stateDir = env.OPENCLAW_STATE_DIR?.trim() || path.join(os.homedir(), ".openclaw");
  return path.join(stateDir, "credentials");
}

function resolveZeroGWalletPath(env: NodeJS.ProcessEnv = process.env): string {
  return path.join(resolveZeroGCredentialsDir(env), "ethereum-wallet.json");
}

function readZeroGWalletSecret(env: NodeJS.ProcessEnv = process.env): ZeroGWalletSecret | null {
  const walletPath = resolveZeroGWalletPath(env);
  try {
    if (!fs.existsSync(walletPath)) {
      return null;
    }
    const raw = fs.readFileSync(walletPath, "utf8");
    const parsed = JSON.parse(raw) as unknown;
    if (!isRecord(parsed) || parsed.chain !== "ethereum") {
      return null;
    }
    if (parsed.kind === "mnemonic" && typeof parsed.mnemonic === "string") {
      return {
        chain: "ethereum",
        kind: "mnemonic",
        mnemonic: parsed.mnemonic.trim(),
      };
    }
    if (parsed.kind === "private-key" && typeof parsed.privateKey === "string") {
      return {
        chain: "ethereum",
        kind: "private-key",
        privateKey: parsed.privateKey.trim(),
      };
    }
    return null;
  } catch {
    return null;
  }
}

function hasZeroGWalletSecret(env: NodeJS.ProcessEnv = process.env): boolean {
  return Boolean(readZeroGWalletSecret(env));
}

function createZeroGWallet(secret: ZeroGWalletSecret, rpcUrl: string): Wallet {
  const provider = new JsonRpcProvider(rpcUrl);
  if (secret.kind === "private-key") {
    return new Wallet(secret.privateKey, provider);
  }
  return new Wallet(HDNodeWallet.fromPhrase(secret.mnemonic).privateKey, provider);
}

function isZeroGChatService(service: ZeroGServiceWithDetail): boolean {
  const serviceType = service.serviceType.trim().toLowerCase();
  const inputModalities =
    service.modelInfo?.architecture?.input_modalities?.map((value) => value.toLowerCase()) ?? [];
  const outputModalities =
    service.modelInfo?.architecture?.output_modalities?.map((value) => value.toLowerCase()) ?? [];

  if (outputModalities.includes("text")) {
    return true;
  }
  if (
    serviceType.includes("image") ||
    serviceType.includes("speech") ||
    serviceType.includes("audio") ||
    outputModalities.includes("image") ||
    outputModalities.includes("audio") ||
    inputModalities.includes("audio")
  ) {
    return false;
  }
  return (
    serviceType.includes("chat") || serviceType.includes("llm") || serviceType.includes("text")
  );
}

function resolveZeroGReasoning(service: ZeroGServiceWithDetail): boolean {
  return (
    service.modelInfo?.supported_parameters?.some((value) =>
      value.toLowerCase().includes("reason"),
    ) ?? false
  );
}

function resolveZeroGModelInputs(
  service: ZeroGServiceWithDetail,
): Array<"text" | "image"> | undefined {
  const inputModalities =
    service.modelInfo?.architecture?.input_modalities?.map((value) => value.toLowerCase()) ?? [];
  if (inputModalities.includes("image")) {
    return ["text", "image"];
  }
  return ["text"];
}

function toZeroGModelCatalogEntry(service: ZeroGServiceWithDetail) {
  const id = encodeZeroGModelRef({
    providerAddress: service.provider,
    model: service.model,
    serviceType: service.serviceType,
  });
  const label = formatZeroGModelLabel({
    providerAddress: service.provider,
    model: service.model,
    modelName: service.modelInfo?.name,
  });
  const contextWindow =
    typeof service.modelInfo?.context_length === "number" && service.modelInfo.context_length > 0
      ? service.modelInfo.context_length
      : undefined;
  const maxTokens =
    typeof service.modelInfo?.max_completion_tokens === "number" &&
    service.modelInfo.max_completion_tokens > 0
      ? service.modelInfo.max_completion_tokens
      : undefined;
  const input = resolveZeroGModelInputs(service);
  const reasoning = resolveZeroGReasoning(service);

  zeroGModelDetailsById.set(id, {
    id,
    label,
    model: service.model,
    providerAddress: service.provider,
    contextWindow,
    maxTokens,
    reasoning,
    input,
  });

  return {
    id,
    name: label,
    provider: ZERO_G_PROVIDER_ID,
    ...(contextWindow ? { contextWindow } : {}),
    ...(typeof reasoning === "boolean" ? { reasoning } : {}),
    ...(input ? { input } : {}),
  };
}

async function fetchZeroGModelCatalog(
  env: NodeJS.ProcessEnv = process.env,
): Promise<ProviderAugmentModelCatalogContext["entries"]> {
  if (!hasZeroGWalletSecret(env)) {
    zeroGModelDetailsById.clear();
    return [];
  }
  try {
    const broker = await createZGComputeNetworkReadOnlyBroker(resolveZeroGRpcUrl(env));
    const discovered: ZeroGServiceWithDetail[] = [];
    for (let offset = 0; ; offset += ZERO_G_MODEL_PAGE_SIZE) {
      const page = (await broker.inference.listServiceWithDetail(
        offset,
        ZERO_G_MODEL_PAGE_SIZE,
        false,
      )) as ZeroGServiceWithDetail[];
      if (page.length === 0) {
        break;
      }
      discovered.push(...page);
      if (page.length < ZERO_G_MODEL_PAGE_SIZE) {
        break;
      }
    }

    zeroGModelDetailsById.clear();
    return discovered.filter(isZeroGChatService).map(toZeroGModelCatalogEntry);
  } catch {
    zeroGModelDetailsById.clear();
    return [];
  }
}

async function loadZeroGModelCatalog(
  env: NodeJS.ProcessEnv = process.env,
): Promise<ProviderAugmentModelCatalogContext["entries"]> {
  if (zeroGModelCachePromise && zeroGModelCacheExpiresAt > Date.now()) {
    return zeroGModelCachePromise;
  }
  zeroGModelCachePromise = fetchZeroGModelCatalog(env);
  zeroGModelCacheExpiresAt = Date.now() + ZERO_G_MODEL_CACHE_TTL_MS;
  return zeroGModelCachePromise;
}

function buildZeroGRuntimeModel(modelId: string): ProviderRuntimeModel | undefined {
  const parsed = parseZeroGModelRef(modelId);
  if (!parsed) {
    return undefined;
  }
  const details = zeroGModelDetailsById.get(modelId);
  const contextWindow = details?.contextWindow ?? DEFAULT_CONTEXT_TOKENS;
  const maxTokens = details?.maxTokens ?? Math.min(contextWindow, 8192);
  return {
    id: parsed.model,
    name: details?.label ?? parsed.model,
    provider: ZERO_G_PROVIDER_ID,
    api: "openai-completions",
    baseUrl: ZERO_G_PLACEHOLDER_BASE_URL,
    reasoning: details?.reasoning ?? false,
    input: details?.input ?? ["text"],
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    contextWindow,
    maxTokens,
  };
}

function extractBearerToken(headers: ZeroGServingRequestHeaders | null | undefined): string | null {
  const rawAuthorization = headers?.Authorization ?? headers?.authorization;
  if (typeof rawAuthorization !== "string") {
    return null;
  }
  const trimmed = rawAuthorization.trim();
  if (!trimmed) {
    return null;
  }
  const bearerPrefix = /^Bearer\s+/i;
  return bearerPrefix.test(trimmed) ? trimmed.replace(bearerPrefix, "") : trimmed;
}

export function resetZeroGModelCatalogCache(): void {
  zeroGModelCacheExpiresAt = 0;
  zeroGModelCachePromise = null;
  zeroGModelDetailsById.clear();
}

export function buildZeroGProvider() {
  return {
    id: ZERO_G_PROVIDER_ID,
    label: "0G Compute",
    envVars: ["OPENCLAW_0G_RPC_URL", "OPENCLAW_0G_NETWORK"],
    auth: [],
    capabilities: {
      providerFamily: "openai" as const,
    },
    resolveDynamicModel: (ctx: ProviderResolveDynamicModelContext) => {
      return buildZeroGRuntimeModel(ctx.modelId);
    },
    prepareDynamicModel: async (_ctx: ProviderPrepareDynamicModelContext) => {
      await loadZeroGModelCatalog();
    },
    prepareRuntimeAuth: async (ctx: ProviderPrepareRuntimeAuthContext) => {
      if (ctx.apiKey.trim() !== ZERO_G_RUNTIME_AUTH_MARKER) {
        return null;
      }
      const parsed = parseZeroGModelRef(ctx.modelId);
      if (!parsed) {
        throw new Error(`Invalid 0G model ref: ${ctx.modelId}`);
      }
      const walletSecret = readZeroGWalletSecret(ctx.env);
      if (!walletSecret) {
        throw new Error(
          'No 0G wallet configured. Run "openclaw setup" and configure an Ethereum wallet first.',
        );
      }

      const rpcUrl = resolveZeroGRpcUrl(ctx.env);
      const wallet = createZeroGWallet(walletSecret, rpcUrl);
      const broker = await createZGComputeNetworkBroker(
        wallet as unknown as Parameters<typeof createZGComputeNetworkBroker>[0],
      );
      const acknowledged = await broker.inference.acknowledged(parsed.providerAddress);
      if (!acknowledged) {
        throw new Error(
          `0G provider ${parsed.providerAddress} is not acknowledged. Acknowledge it in 0G Compute before using this model.`,
        );
      }

      const metadata = await broker.inference.getServiceMetadata(parsed.providerAddress);
      const runtimeAuthBroker = broker.inference as typeof broker.inference &
        ZeroGInferenceRuntimeAuthBroker &
        Partial<ZeroGInferenceApiKeyBroker>;
      const runtimeHeaders =
        typeof runtimeAuthBroker.getRequestHeaders === "function"
          ? await runtimeAuthBroker.getRequestHeaders(parsed.providerAddress)
          : null;
      const ephemeralApiKey = extractBearerToken(runtimeHeaders);
      if (ephemeralApiKey) {
        return {
          apiKey: ephemeralApiKey,
          ...(metadata.endpoint?.trim() ? { baseUrl: metadata.endpoint.trim() } : {}),
          expiresAt: Date.now() + ZERO_G_RUNTIME_TOKEN_TTL_MS,
        };
      }

      const createApiKey =
        typeof runtimeAuthBroker.createApiKey === "function"
          ? runtimeAuthBroker.createApiKey.bind(runtimeAuthBroker)
          : typeof runtimeAuthBroker.requestProcessor?.createApiKey === "function"
            ? runtimeAuthBroker.requestProcessor.createApiKey.bind(
                runtimeAuthBroker.requestProcessor,
              )
            : null;
      if (!createApiKey) {
        throw new Error(
          "The installed 0G broker does not expose getRequestHeaders or createApiKey for runtime auth.",
        );
      }
      const apiKey = await createApiKey(parsed.providerAddress, {
        expiresIn: ZERO_G_RUNTIME_TOKEN_TTL_MS,
      });
      return {
        apiKey: apiKey.rawToken,
        ...(metadata.endpoint?.trim() ? { baseUrl: metadata.endpoint.trim() } : {}),
        ...(apiKey.expiresAt > 0 ? { expiresAt: apiKey.expiresAt } : {}),
      };
    },
    resolveSyntheticAuth: (ctx: ZeroGResolveSyntheticAuthContext) => {
      if (ctx.provider !== ZERO_G_PROVIDER_ID) {
        return undefined;
      }
      if (!hasZeroGWalletSecret(process.env)) {
        return undefined;
      }
      return {
        apiKey: ZERO_G_RUNTIME_AUTH_MARKER,
        source: "ethereum-wallet.json (synthetic 0G key)",
        mode: "api-key" as const,
      };
    },
    buildMissingAuthMessage: (ctx: ProviderBuildMissingAuthMessageContext) => {
      if (ctx.provider !== ZERO_G_PROVIDER_ID) {
        return undefined;
      }
      return 'No wallet configured for provider "0g". Run "openclaw setup" and configure an Ethereum wallet first.';
    },
    buildUnknownModelHint: (ctx: ProviderBuildUnknownModelHintContext) => {
      if (parseZeroGModelRef(ctx.modelId)) {
        return undefined;
      }
      return "Choose a 0G model from the chat picker so OpenClaw can resolve the provider address.";
    },
    augmentModelCatalog: async (ctx: ProviderAugmentModelCatalogContext) => {
      return await loadZeroGModelCatalog(ctx.env);
    },
  };
}

export default definePluginEntry({
  id: ZERO_G_PROVIDER_ID,
  name: "0G Compute Provider",
  description: "Bundled 0G Compute provider plugin",
  register(api) {
    api.registerProvider(buildZeroGProvider());
  },
});
