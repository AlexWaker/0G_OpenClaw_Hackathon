import { Buffer } from "node:buffer";
import {
  Batcher,
  Indexer,
  KvClient,
  MemData,
  ZgFile,
  getFlowContract,
} from "@0gfoundation/0g-ts-sdk";
import { HDNodeWallet, JsonRpcProvider, Wallet, encodeBase64 } from "ethers";
import type { StoredEthereumWalletSecret } from "../wallet/ethereum-wallet.js";
import { resolveZeroGNetwork, resolveZeroGRpcUrl } from "./account.js";

export type ZeroGStorageMode = "turbo" | "standard";
export type ZeroGStorageNetworkName = "mainnet" | "testnet" | "custom";

export type ZeroGStorageNetworkConfig = {
  network: ZeroGStorageNetworkName;
  mode: ZeroGStorageMode;
  rpcUrl: string;
  indexerRpc: string;
};

export type ZeroGStorageUploadResult = {
  rootHash: string;
  txHash: string;
};

const ZERO_G_KV_RPC_ENV_KEYS = ["OPENCLAW_0G_KV_RPC_URL", "OPENCLAW_0G_KV_RPC_URLS"] as const;

type ZeroGKvReadEndpointErrorCode = "missing-config" | "unsupported-endpoint";
type ZeroGSigner = Parameters<Indexer["upload"]>[2];
type ZeroGKvKey = Parameters<KvClient["getValue"]>[1];

const ZERO_G_STORAGE_INDEXER_URLS = {
  testnet: {
    turbo: "https://indexer-storage-testnet-turbo.0g.ai",
    standard: "https://indexer-storage-testnet-standard.0g.ai",
  },
  mainnet: {
    turbo: "https://indexer-storage-turbo.0g.ai",
    standard: "https://indexer-storage.0g.ai",
  },
} as const;

export class ZeroGKvReadEndpointError extends Error {
  readonly code: ZeroGKvReadEndpointErrorCode;

  constructor(code: ZeroGKvReadEndpointErrorCode, message: string) {
    super(message);
    this.name = "ZeroGKvReadEndpointError";
    this.code = code;
  }
}

export function isZeroGKvReadEndpointError(err: unknown): err is ZeroGKvReadEndpointError {
  return err instanceof ZeroGKvReadEndpointError;
}

function describeZeroGKvError(err: unknown): string {
  if (err instanceof Error) {
    return err.message;
  }
  return String(err);
}

function isUnsupportedZeroGKvEndpointError(err: unknown): boolean {
  const message = describeZeroGKvError(err).toLowerCase();
  return (
    message.includes("method not found") ||
    message.includes("unknown method") ||
    message.includes("not implemented") ||
    message.includes("unsupported")
  );
}

export function resolveZeroGKvRpcUrls(env: NodeJS.ProcessEnv = process.env): string[] {
  const values = ZERO_G_KV_RPC_ENV_KEYS.flatMap((key) => {
    const raw = env[key]?.trim();
    if (!raw) {
      return [];
    }
    return raw
      .split(/[\s,]+/)
      .map((value) => value.trim())
      .filter((value) => value.length > 0);
  });

  return [...new Set(values)];
}

function normalizeUploadResult(
  tx:
    | { rootHash: string; txHash: string }
    | {
        rootHashes: string[];
        txHashes: string[];
      },
): ZeroGStorageUploadResult {
  if ("rootHash" in tx) {
    return tx;
  }
  return {
    rootHash: tx.rootHashes[0] ?? "",
    txHash: tx.txHashes[0] ?? "",
  };
}

function coerceZeroGSigner(signer: Wallet): ZeroGSigner {
  return signer as unknown as ZeroGSigner;
}

function encodeZeroGKvKey(key: Uint8Array): ZeroGKvKey {
  return encodeBase64(key) as unknown as ZeroGKvKey;
}

function createZeroGStorageSigner(secret: StoredEthereumWalletSecret, rpcUrl: string): Wallet {
  const provider = new JsonRpcProvider(rpcUrl);
  if (secret.kind === "private-key") {
    return new Wallet(secret.privateKey, provider);
  }
  return new Wallet(HDNodeWallet.fromPhrase(secret.mnemonic).privateKey, provider);
}

export function resolveZeroGStorageMode(env: NodeJS.ProcessEnv = process.env): ZeroGStorageMode {
  const mode = env.OPENCLAW_0G_STORAGE_MODE?.trim().toLowerCase();
  return mode === "standard" ? "standard" : "turbo";
}

export function resolveZeroGStorageIndexerUrl(env: NodeJS.ProcessEnv = process.env): string {
  const explicit = env.OPENCLAW_0G_STORAGE_INDEXER_URL?.trim();
  if (explicit) {
    return explicit;
  }

  const network = resolveZeroGNetwork(env);
  const mode = resolveZeroGStorageMode(env);
  if (network === "testnet") {
    return ZERO_G_STORAGE_INDEXER_URLS.testnet[mode];
  }
  return ZERO_G_STORAGE_INDEXER_URLS.mainnet[mode];
}

export function resolveZeroGStorageNetworkConfig(
  env: NodeJS.ProcessEnv = process.env,
): ZeroGStorageNetworkConfig {
  return {
    network: resolveZeroGNetwork(env),
    mode: resolveZeroGStorageMode(env),
    rpcUrl: resolveZeroGRpcUrl(env),
    indexerRpc: resolveZeroGStorageIndexerUrl(env),
  };
}

export async function uploadFileToZeroGStorage(params: {
  localPath: string;
  network: ZeroGStorageNetworkConfig;
  secret: StoredEthereumWalletSecret;
}): Promise<ZeroGStorageUploadResult> {
  const signer = createZeroGStorageSigner(params.secret, params.network.rpcUrl);
  const zeroGSigner = coerceZeroGSigner(signer);
  const indexer = new Indexer(params.network.indexerRpc);
  const zgFile = await ZgFile.fromFilePath(params.localPath);

  try {
    const [, treeErr] = await zgFile.merkleTree();
    if (treeErr !== null) {
      throw new Error(`0G merkle tree generation failed: ${String(treeErr)}`);
    }

    const [tx, uploadErr] = await indexer.upload(zgFile, params.network.rpcUrl, zeroGSigner);
    if (uploadErr !== null) {
      throw new Error(`0G upload failed: ${String(uploadErr)}`);
    }

    return normalizeUploadResult(tx);
  } finally {
    await zgFile.close().catch(() => {});
  }
}

export async function uploadDataToZeroGStorage(params: {
  data: Uint8Array;
  network: ZeroGStorageNetworkConfig;
  secret: StoredEthereumWalletSecret;
}): Promise<ZeroGStorageUploadResult> {
  const signer = createZeroGStorageSigner(params.secret, params.network.rpcUrl);
  const zeroGSigner = coerceZeroGSigner(signer);
  const indexer = new Indexer(params.network.indexerRpc);
  const memData = new MemData(params.data);

  const [, treeErr] = await memData.merkleTree();
  if (treeErr !== null) {
    throw new Error(`0G merkle tree generation failed: ${String(treeErr)}`);
  }

  const [tx, uploadErr] = await indexer.upload(memData, params.network.rpcUrl, zeroGSigner);
  if (uploadErr !== null) {
    throw new Error(`0G upload failed: ${String(uploadErr)}`);
  }

  return normalizeUploadResult(tx);
}

export async function downloadFileFromZeroGStorage(params: {
  rootHash: string;
  outputPath: string;
  network: ZeroGStorageNetworkConfig;
}): Promise<void> {
  const indexer = new Indexer(params.network.indexerRpc);
  const err = await indexer.download(params.rootHash, params.outputPath, true);
  if (err !== null) {
    throw new Error(`0G download failed: ${String(err)}`);
  }
}

export async function readZeroGKvValue(params: {
  streamId: string;
  key: Uint8Array;
  network: ZeroGStorageNetworkConfig;
  env?: NodeJS.ProcessEnv;
}): Promise<string | null> {
  const urls = resolveZeroGKvRpcUrls(params.env);
  if (urls.length === 0) {
    throw new ZeroGKvReadEndpointError(
      "missing-config",
      "0G memory restore requires OPENCLAW_0G_KV_RPC_URL to point to a KV node RPC endpoint.",
    );
  }

  let lastError: unknown = null;
  let sawResponse = false;
  let sawUnsupportedEndpoint = false;
  for (const url of urls) {
    const client = new KvClient(url);
    try {
      const value = await client.getValue(params.streamId, encodeZeroGKvKey(params.key));
      sawResponse = true;
      if (!value) {
        continue;
      }
      return Buffer.from(value.data, "base64").toString("utf8");
    } catch (err) {
      lastError = err;
      if (isUnsupportedZeroGKvEndpointError(err)) {
        sawUnsupportedEndpoint = true;
      }
    }
  }

  if (sawUnsupportedEndpoint) {
    throw new ZeroGKvReadEndpointError(
      "unsupported-endpoint",
      "Configured 0G KV read endpoint does not support kv_getValue. Point OPENCLAW_0G_KV_RPC_URL at a KV node RPC endpoint.",
    );
  }

  if (!sawResponse && lastError) {
    throw lastError;
  }
  return null;
}

export async function writeZeroGKvValue(params: {
  streamId: string;
  key: Uint8Array;
  value: Uint8Array;
  network: ZeroGStorageNetworkConfig;
  secret: StoredEthereumWalletSecret;
}): Promise<ZeroGStorageUploadResult> {
  const signer = createZeroGStorageSigner(params.secret, params.network.rpcUrl);
  const zeroGSigner = coerceZeroGSigner(signer);
  const indexer = new Indexer(params.network.indexerRpc);
  const [nodes, nodesErr] = await indexer.selectNodes(1);
  if (nodesErr !== null || nodes.length === 0) {
    throw new Error(`Failed to select 0G KV nodes: ${String(nodesErr ?? "no nodes")}`);
  }

  const status = await nodes[0].getStatus();
  const flow = getFlowContract(status.networkIdentity.flowAddress, zeroGSigner);
  const batcher = new Batcher(1, nodes, flow, params.network.rpcUrl);
  batcher.streamDataBuilder.set(params.streamId, params.key, params.value);

  const [tx, err] = await batcher.exec();
  if (err !== null) {
    throw new Error(`0G KV write failed: ${String(err)}`);
  }
  return tx;
}
