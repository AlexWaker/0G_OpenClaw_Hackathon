import {
  createZGComputeNetworkBroker,
  createZGComputeNetworkReadOnlyBroker,
} from "@0glabs/0g-serving-broker";
import { HDNodeWallet, JsonRpcProvider, Wallet, formatUnits, parseUnits } from "ethers";
import {
  readStoredEthereumWalletSecret,
  readStoredEthereumWalletSummary,
  type EthereumWalletSummary,
  type StoredEthereumWalletSecret,
} from "../wallet/ethereum-wallet.js";

const ZERO_G_PROVIDER_ID = "0g";
const ZERO_G_MODEL_REF_PREFIX = "svc_";
const ZERO_G_TESTNET_RPC_URL = "https://evmrpc-testnet.0g.ai";
const ZERO_G_MAINNET_RPC_URL = "https://evmrpc.0g.ai";
const ZERO_G_DECIMALS = 18;
const ZERO_G_MIN_PROVIDER_TRANSFER_UNITS = 10n ** 18n;
const ZERO_G_SERVICE_PAGE_SIZE = 50;

type ZeroGNetwork = "mainnet" | "testnet" | "custom";

type ParsedZeroGModelRef = {
  providerAddress: string;
  model: string;
  serviceType?: string;
};

type ZeroGQualifiedModelRef = ParsedZeroGModelRef & {
  qualifiedModel: string;
};

type ZeroGLedgerSnapshot = {
  user: string;
  availableBalance: bigint;
  totalBalance: bigint;
};

type ZeroGAccountSnapshot = {
  balance: bigint;
  pendingRefund: bigint;
  acknowledged: boolean;
};

type ZeroGServiceSnapshot = {
  provider: string;
  serviceType: string;
  url: string;
  inputPrice: bigint;
  outputPrice: bigint;
  model: string;
  verifiability: string;
  teeSignerAddress: string;
  teeSignerAcknowledged: boolean;
};

type ZeroGServiceMetadata = {
  endpoint: string;
  model: string;
};

type ZeroGSummaryShape = {
  wallet: EthereumWalletSummary;
  selectedModel: string;
  providerAddress: string;
  serviceType: string | null;
  network: ZeroGNetwork;
  rpcUrl: string;
  ready: boolean;
  issues: string[];
  mainLedger: {
    exists: boolean;
    availableBalance: string;
    totalBalance: string;
  };
  providerAccount: {
    exists: boolean;
    availableBalance: string;
    totalBalance: string;
    pendingRefund: string;
    userAcknowledged: boolean;
  };
  service: {
    model: string;
    serviceType: string | null;
    endpoint: string | null;
    url: string | null;
    verifiability: string | null;
    inputPrice: string | null;
    outputPrice: string | null;
    teeSignerAddress: string | null;
    teeSignerAcknowledged: boolean | null;
  };
};

type ZeroGBrokerLike = {
  ledger: {
    addLedger: (balance: number, gasPrice?: number) => Promise<void>;
    depositFund: (amount: number, gasPrice?: number) => Promise<void>;
    getLedger: () => Promise<ZeroGLedgerSnapshot>;
    transferFund: (
      provider: string,
      serviceType: "inference" | "fine-tuning",
      amount: bigint,
      gasPrice?: number,
    ) => Promise<void>;
  };
  inference: {
    getAccount: (provider: string) => Promise<ZeroGAccountSnapshot>;
    getServiceMetadata: (provider: string) => Promise<ZeroGServiceMetadata>;
    acknowledgeProviderSigner?: (provider: string, gasPrice?: number) => Promise<void>;
    userAcknowledged?: (provider: string) => Promise<boolean>;
    acknowledged?: (provider: string) => Promise<boolean>;
    getService?: (provider: string) => Promise<ZeroGServiceSnapshot>;
  };
};

type ZeroGReadOnlyBrokerLike = {
  inference: {
    listServiceWithDetail?: (
      offset?: number,
      limit?: number,
      includeUnacknowledged?: boolean,
    ) => Promise<ZeroGServiceSnapshot[]>;
  };
};

export type ZeroGAccountSummary = ZeroGSummaryShape;

export type ZeroGAccountDeps = {
  env?: NodeJS.ProcessEnv;
  readWalletSecret?: () => StoredEthereumWalletSecret | null;
  readWalletSummary?: () => EthereumWalletSummary;
  createBroker?: (wallet: Wallet) => Promise<ZeroGBrokerLike>;
  createReadOnlyBroker?: (rpcUrl: string) => Promise<ZeroGReadOnlyBrokerLike>;
};

export class ZeroGOperationError extends Error {
  readonly code: "INVALID_REQUEST" | "UNAVAILABLE";

  constructor(code: "INVALID_REQUEST" | "UNAVAILABLE", message: string) {
    super(message);
    this.name = "ZeroGOperationError";
    this.code = code;
  }
}

function invalidRequest(message: string): never {
  throw new ZeroGOperationError("INVALID_REQUEST", message);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function trimNullableString(value: string | null | undefined): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function formatZeroGAmount(amount: bigint): string {
  const normalized = formatUnits(amount, ZERO_G_DECIMALS).replace(
    /(?:\.0+|(?:(\.\d*?[1-9]))0+)$/,
    "$1",
  );
  return normalized === "" ? "0" : normalized;
}

function unwrapZeroGErrorMessage(err: unknown): string {
  const raw = err instanceof Error ? err.message : String(err);
  return raw.replace(/^(?:Error:\s*)+/i, "").trim() || "Unknown 0G error.";
}

function isZeroGCallFailedMessage(message: string): boolean {
  return /callfailed|execution reverted|require\(false\)/i.test(message);
}

function normalizeProviderAddress(value: string): string {
  return value.trim().toLowerCase();
}

function parseZeroGModelRef(modelId: string): ParsedZeroGModelRef | null {
  const trimmed = modelId.trim();
  if (!trimmed.startsWith(ZERO_G_MODEL_REF_PREFIX)) {
    return null;
  }

  try {
    const decoded = JSON.parse(
      Buffer.from(trimmed.slice(ZERO_G_MODEL_REF_PREFIX.length), "base64url").toString("utf8"),
    ) as unknown;
    if (!isRecord(decoded)) {
      return null;
    }
    const providerAddress =
      typeof decoded.p === "string" ? normalizeProviderAddress(decoded.p) : "";
    const model = typeof decoded.m === "string" ? decoded.m.trim() : "";
    const serviceType = typeof decoded.t === "string" ? decoded.t.trim() : undefined;
    if (!providerAddress || !model) {
      return null;
    }
    return {
      providerAddress,
      model,
      ...(serviceType ? { serviceType } : {}),
    };
  } catch {
    return null;
  }
}

export function parseQualifiedZeroGModel(qualifiedModel: string): ZeroGQualifiedModelRef {
  const trimmed = qualifiedModel.trim();
  const separator = trimmed.indexOf("/");
  if (separator <= 0) {
    invalidRequest("0G account actions require a qualified model id.");
  }

  const providerId = trimmed.slice(0, separator).trim().toLowerCase();
  if (providerId !== ZERO_G_PROVIDER_ID) {
    invalidRequest(`Expected a 0G model, received provider "${providerId || "unknown"}".`);
  }

  const parsed = parseZeroGModelRef(trimmed.slice(separator + 1));
  if (!parsed) {
    invalidRequest(`Invalid 0G model ref: ${qualifiedModel}`);
  }

  return {
    qualifiedModel: trimmed,
    ...parsed,
  };
}

export function resolveZeroGRpcUrl(env: NodeJS.ProcessEnv = process.env): string {
  const explicit = env.OPENCLAW_0G_RPC_URL?.trim();
  if (explicit) {
    return explicit;
  }
  const network = env.OPENCLAW_0G_NETWORK?.trim().toLowerCase();
  return network === "testnet" ? ZERO_G_TESTNET_RPC_URL : ZERO_G_MAINNET_RPC_URL;
}

export function resolveZeroGNetwork(env: NodeJS.ProcessEnv = process.env): ZeroGNetwork {
  const explicit = env.OPENCLAW_0G_RPC_URL?.trim();
  if (explicit) {
    if (explicit === ZERO_G_MAINNET_RPC_URL) {
      return "mainnet";
    }
    if (explicit === ZERO_G_TESTNET_RPC_URL) {
      return "testnet";
    }
    return "custom";
  }
  const network = env.OPENCLAW_0G_NETWORK?.trim().toLowerCase();
  return network === "testnet" ? "testnet" : "mainnet";
}

function createWallet(secret: StoredEthereumWalletSecret, rpcUrl: string): Wallet {
  const provider = new JsonRpcProvider(rpcUrl);
  if (secret.kind === "private-key") {
    return new Wallet(secret.privateKey, provider);
  }
  return new Wallet(HDNodeWallet.fromPhrase(secret.mnemonic).privateKey, provider);
}

function resolveDeps(deps: ZeroGAccountDeps = {}) {
  return {
    env: deps.env ?? process.env,
    readWalletSecret: deps.readWalletSecret ?? (() => readStoredEthereumWalletSecret()),
    readWalletSummary: deps.readWalletSummary ?? (() => readStoredEthereumWalletSummary()),
    createBroker:
      deps.createBroker ??
      (async (wallet: Wallet) => {
        return (await createZGComputeNetworkBroker(
          wallet as unknown as Parameters<typeof createZGComputeNetworkBroker>[0],
        )) as ZeroGBrokerLike;
      }),
    createReadOnlyBroker:
      deps.createReadOnlyBroker ??
      (async (rpcUrl: string) => {
        return (await createZGComputeNetworkReadOnlyBroker(rpcUrl)) as ZeroGReadOnlyBrokerLike;
      }),
  };
}

async function createContext(model: string, deps: ZeroGAccountDeps = {}) {
  const resolved = resolveDeps(deps);
  const parsed = parseQualifiedZeroGModel(model);
  const walletSecret = resolved.readWalletSecret();
  if (!walletSecret) {
    invalidRequest(
      'No 0G wallet configured. Run "openclaw setup" and configure an Ethereum wallet first.',
    );
  }

  const rpcUrl = resolveZeroGRpcUrl(resolved.env);
  const wallet = createWallet(walletSecret, rpcUrl);
  const broker = await resolved.createBroker(wallet);
  const readOnlyBroker = await resolved.createReadOnlyBroker(rpcUrl);

  return {
    broker,
    readOnlyBroker,
    parsed,
    rpcUrl,
    network: resolveZeroGNetwork(resolved.env),
    walletSummary: resolved.readWalletSummary(),
    deps: resolved,
  };
}

async function loadLedgerSafe(broker: ZeroGBrokerLike) {
  try {
    return {
      exists: true,
      data: await broker.ledger.getLedger(),
    };
  } catch {
    return {
      exists: false,
      data: null,
    };
  }
}

async function loadProviderAccountSafe(broker: ZeroGBrokerLike, providerAddress: string) {
  try {
    return {
      exists: true,
      data: await broker.inference.getAccount(providerAddress),
    };
  } catch {
    return {
      exists: false,
      data: null,
    };
  }
}

async function readUserAcknowledged(
  broker: ZeroGBrokerLike,
  providerAddress: string,
  fallback: boolean,
): Promise<boolean> {
  try {
    if (typeof broker.inference.userAcknowledged === "function") {
      return await broker.inference.userAcknowledged(providerAddress);
    }
    if (typeof broker.inference.acknowledged === "function") {
      return await broker.inference.acknowledged(providerAddress);
    }
    return fallback;
  } catch {
    return fallback;
  }
}

async function loadServiceSnapshot(
  broker: ZeroGBrokerLike,
  readOnlyBroker: ZeroGReadOnlyBrokerLike,
  parsed: ZeroGQualifiedModelRef,
): Promise<ZeroGServiceSnapshot> {
  if (typeof broker.inference.getService === "function") {
    return await broker.inference.getService(parsed.providerAddress);
  }

  const listServiceWithDetail = readOnlyBroker.inference.listServiceWithDetail;
  if (typeof listServiceWithDetail !== "function") {
    throw new ZeroGOperationError(
      "UNAVAILABLE",
      "The installed 0G read-only broker does not support service detail lookups.",
    );
  }

  const targetModel = parsed.model.trim();
  const targetServiceType = trimNullableString(parsed.serviceType)?.toLowerCase();
  let fallback: { score: number; service: ZeroGServiceSnapshot } | null = null;

  for (let offset = 0; ; offset += ZERO_G_SERVICE_PAGE_SIZE) {
    const page = await listServiceWithDetail.call(
      readOnlyBroker.inference,
      offset,
      ZERO_G_SERVICE_PAGE_SIZE,
      true,
    );
    for (const service of page) {
      if (normalizeProviderAddress(service.provider) !== parsed.providerAddress) {
        continue;
      }

      const modelMatches = service.model.trim() === targetModel;
      const serviceTypeMatches =
        targetServiceType == null || service.serviceType.trim().toLowerCase() === targetServiceType;
      const score = modelMatches ? (serviceTypeMatches ? 3 : 2) : 1;

      if (score === 3) {
        return service;
      }
      if (!fallback || score > fallback.score) {
        fallback = { score, service };
      }
    }

    if (page.length < ZERO_G_SERVICE_PAGE_SIZE) {
      break;
    }
  }

  if (fallback) {
    return fallback.service;
  }

  throw new ZeroGOperationError(
    "UNAVAILABLE",
    `Unable to load 0G service details for provider ${parsed.providerAddress}.`,
  );
}

function buildIssues(params: {
  ledgerExists: boolean;
  ledgerAvailableBalance: bigint;
  providerAvailableBalance: bigint;
  providerUserAcknowledged: boolean;
  teeSignerAcknowledged: boolean;
}): string[] {
  const issues: string[] = [];

  if (!params.teeSignerAcknowledged) {
    issues.push("This provider is not acknowledged by 0G yet.");
  }
  if (!params.providerUserAcknowledged) {
    issues.push("Acknowledge this provider before starting chat.");
  }
  if (params.providerAvailableBalance <= 0n) {
    if (params.ledgerAvailableBalance > 0n) {
      issues.push("Transfer funds into the provider sub-account to start chatting.");
    } else if (!params.ledgerExists) {
      issues.push("Create and fund the main 0G account, then transfer funds to this provider.");
    } else {
      issues.push("Fund the main 0G account, then transfer funds to this provider.");
    }
  }

  return issues;
}

export async function getZeroGAccountSummary(
  params: { model: string },
  deps: ZeroGAccountDeps = {},
): Promise<ZeroGAccountSummary> {
  const { broker, readOnlyBroker, parsed, rpcUrl, network, walletSummary } = await createContext(
    params.model,
    deps,
  );
  const [ledgerResult, providerAccountResult, service, metadata] = await Promise.all([
    loadLedgerSafe(broker),
    loadProviderAccountSafe(broker, parsed.providerAddress),
    loadServiceSnapshot(broker, readOnlyBroker, parsed),
    broker.inference.getServiceMetadata(parsed.providerAddress),
  ]);

  const ledgerAvailableBalance = ledgerResult.data?.availableBalance ?? 0n;
  const ledgerTotalBalance = ledgerResult.data?.totalBalance ?? 0n;
  const providerTotalBalance = providerAccountResult.data?.balance ?? 0n;
  const providerPendingRefund = providerAccountResult.data?.pendingRefund ?? 0n;
  const providerAvailableBalance =
    providerTotalBalance > providerPendingRefund
      ? providerTotalBalance - providerPendingRefund
      : 0n;
  const providerUserAcknowledged = await readUserAcknowledged(
    broker,
    parsed.providerAddress,
    providerAccountResult.data?.acknowledged ?? false,
  );
  const teeSignerAcknowledged = Boolean(service.teeSignerAcknowledged);
  const issues = buildIssues({
    ledgerExists: ledgerResult.exists,
    ledgerAvailableBalance,
    providerAvailableBalance,
    providerUserAcknowledged,
    teeSignerAcknowledged,
  });

  return {
    wallet: walletSummary,
    selectedModel: parsed.qualifiedModel,
    providerAddress: parsed.providerAddress,
    serviceType: trimNullableString(parsed.serviceType ?? service.serviceType),
    network,
    rpcUrl,
    ready: issues.length === 0,
    issues,
    mainLedger: {
      exists: ledgerResult.exists,
      availableBalance: formatZeroGAmount(ledgerAvailableBalance),
      totalBalance: formatZeroGAmount(ledgerTotalBalance),
    },
    providerAccount: {
      exists: providerAccountResult.exists,
      availableBalance: formatZeroGAmount(providerAvailableBalance),
      totalBalance: formatZeroGAmount(providerTotalBalance),
      pendingRefund: formatZeroGAmount(providerPendingRefund),
      userAcknowledged: providerUserAcknowledged,
    },
    service: {
      model: trimNullableString(metadata.model) ?? service.model,
      serviceType: trimNullableString(service.serviceType),
      endpoint: trimNullableString(metadata.endpoint),
      url: trimNullableString(service.url),
      verifiability: trimNullableString(service.verifiability),
      inputPrice: formatZeroGAmount(service.inputPrice),
      outputPrice: formatZeroGAmount(service.outputPrice),
      teeSignerAddress: trimNullableString(service.teeSignerAddress),
      teeSignerAcknowledged,
    },
  };
}

function parseFundingAmount(amount: string): {
  amountString: string;
  amountNumber: number;
  amountUnits: bigint;
} {
  const trimmed = amount.trim();
  if (!/^\d+(?:\.\d+)?$/.test(trimmed)) {
    invalidRequest("Amount must be a positive 0G value.");
  }

  let amountUnits: bigint;
  try {
    amountUnits = parseUnits(trimmed, ZERO_G_DECIMALS);
  } catch {
    invalidRequest("Amount must use at most 18 decimal places.");
  }
  if (amountUnits <= 0n) {
    invalidRequest("Amount must be greater than 0.");
  }

  const amountNumber = Number(trimmed);
  if (!Number.isFinite(amountNumber) || amountNumber <= 0) {
    invalidRequest("Amount must be a positive 0G value.");
  }

  return {
    amountString: trimmed,
    amountNumber,
    amountUnits,
  };
}

export async function fundZeroGMainAccount(
  params: { model: string; amount: string },
  deps: ZeroGAccountDeps = {},
): Promise<ZeroGAccountSummary> {
  const { broker } = await createContext(params.model, deps);
  const { amountNumber } = parseFundingAmount(params.amount);
  const ledger = await loadLedgerSafe(broker);

  if (!ledger.exists) {
    await broker.ledger.addLedger(amountNumber);
  } else {
    await broker.ledger.depositFund(amountNumber);
  }

  return await getZeroGAccountSummary({ model: params.model }, deps);
}

export async function fundZeroGProviderAccount(
  params: { model: string; amount: string },
  deps: ZeroGAccountDeps = {},
): Promise<ZeroGAccountSummary> {
  const { broker, parsed } = await createContext(params.model, deps);
  const { amountUnits } = parseFundingAmount(params.amount);
  const ledger = await loadLedgerSafe(broker);
  if (!ledger.exists) {
    invalidRequest("Fund the main 0G account before transferring to a provider.");
  }

  const providerAccount = await loadProviderAccountSafe(broker, parsed.providerAddress);
  const providerUserAcknowledged = await readUserAcknowledged(
    broker,
    parsed.providerAddress,
    providerAccount.data?.acknowledged ?? false,
  );

  let remainingAmountUnits = amountUnits;
  try {
    if (!providerUserAcknowledged) {
      if (typeof broker.inference.acknowledgeProviderSigner !== "function") {
        throw new ZeroGOperationError(
          "UNAVAILABLE",
          "The installed 0G broker does not support provider acknowledgement.",
        );
      }

      if (!providerAccount.exists) {
        if (amountUnits < ZERO_G_MIN_PROVIDER_TRANSFER_UNITS) {
          invalidRequest(
            "The first transfer to a 0G provider must be at least 1 0G so the provider sub-account can be initialized.",
          );
        }
        await broker.inference.acknowledgeProviderSigner(parsed.providerAddress);
        remainingAmountUnits -= ZERO_G_MIN_PROVIDER_TRANSFER_UNITS;
      } else {
        await broker.inference.acknowledgeProviderSigner(parsed.providerAddress);
      }
    }

    if (remainingAmountUnits > 0n) {
      await broker.ledger.transferFund(parsed.providerAddress, "inference", remainingAmountUnits);
    }
  } catch (err) {
    const message = unwrapZeroGErrorMessage(err);
    if (!providerUserAcknowledged && isZeroGCallFailedMessage(message)) {
      throw new ZeroGOperationError(
        "UNAVAILABLE",
        "0G rejected the first provider-funding transaction while initializing or acknowledging the provider account. Try Acknowledge Provider first, then retry the transfer.",
      );
    }
    throw err;
  }

  return await getZeroGAccountSummary({ model: params.model }, deps);
}

export async function acknowledgeZeroGProvider(
  params: { model: string },
  deps: ZeroGAccountDeps = {},
): Promise<ZeroGAccountSummary> {
  const { broker, parsed } = await createContext(params.model, deps);
  if (typeof broker.inference.acknowledgeProviderSigner !== "function") {
    throw new ZeroGOperationError(
      "UNAVAILABLE",
      "The installed 0G broker does not support provider acknowledgement.",
    );
  }

  try {
    await broker.inference.acknowledgeProviderSigner(parsed.providerAddress);
  } catch (err) {
    const message = unwrapZeroGErrorMessage(err);
    if (isZeroGCallFailedMessage(message)) {
      throw new ZeroGOperationError(
        "UNAVAILABLE",
        "0G rejected the provider acknowledgement transaction. Make sure the main 0G account has at least 1 0G available, then retry acknowledgement.",
      );
    }
    throw err;
  }

  return await getZeroGAccountSummary({ model: params.model }, deps);
}
