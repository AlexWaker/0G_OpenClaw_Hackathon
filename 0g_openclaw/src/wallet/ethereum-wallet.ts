import fs from "node:fs";
import { generateMnemonic, validateMnemonic } from "@scure/bip39";
import { wordlist } from "@scure/bip39/wordlists/english.js";
import { Wallet } from "ethers";
import { resolveEthereumWalletPath } from "../config/paths.js";
import { readTextFileIfExists, writeJsonFileSecure } from "../secrets/shared.js";

export type EthereumWalletSource = "user" | "generated";

export type StoredEthereumWalletSecret =
  | {
      version: 1;
      chain: "ethereum";
      kind: "mnemonic";
      mnemonic: string;
      source: EthereumWalletSource;
      createdAtMs: number;
    }
  | {
      version: 1;
      chain: "ethereum";
      kind: "private-key";
      privateKey: string;
      source: EthereumWalletSource;
      createdAtMs: number;
    };

export type EthereumWalletSecretKind = StoredEthereumWalletSecret["kind"];

export type EthereumWalletSummary = {
  address: string | null;
  kind: EthereumWalletSecretKind | null;
  source: EthereumWalletSource | null;
};

export type EthereumWalletValidationResult =
  | { ok: true; value: string }
  | { ok: false; error: string };

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function normalizeEthereumMnemonic(input: string): string {
  return input
    .trim()
    .toLowerCase()
    .split(/\s+/)
    .filter((word) => word.length > 0)
    .join(" ");
}

export function validateEthereumMnemonicInput(input: string): EthereumWalletValidationResult {
  const mnemonic = normalizeEthereumMnemonic(input);
  if (!mnemonic) {
    return { ok: false, error: "Mnemonic is required." };
  }

  const words = mnemonic.split(" ");
  if (![12, 15, 18, 21, 24].includes(words.length)) {
    return {
      ok: false,
      error: "Mnemonic must contain 12, 15, 18, 21, or 24 words.",
    };
  }

  if (!validateMnemonic(mnemonic, wordlist)) {
    return { ok: false, error: "Mnemonic is not a valid BIP39 phrase." };
  }

  return { ok: true, value: mnemonic };
}

export function normalizeEthereumPrivateKey(input: string): string {
  const trimmed = input.trim();
  if (!trimmed) {
    return "";
  }
  const withoutPrefix =
    trimmed.startsWith("0x") || trimmed.startsWith("0X") ? trimmed.slice(2) : trimmed;
  return `0x${withoutPrefix.toLowerCase()}`;
}

export function validateEthereumPrivateKeyInput(input: string): EthereumWalletValidationResult {
  const privateKey = normalizeEthereumPrivateKey(input);
  if (!privateKey) {
    return { ok: false, error: "Ethereum private key is required." };
  }
  if (!/^0x[0-9a-f]{64}$/i.test(privateKey)) {
    return {
      ok: false,
      error: "Ethereum private key must be 32 bytes of hex (64 hex chars, optional 0x prefix).",
    };
  }
  return { ok: true, value: privateKey };
}

export function createEthereumMnemonic(): string {
  return generateMnemonic(wordlist);
}

export function buildStoredEthereumWalletSecret(params: {
  kind: "mnemonic" | "private-key";
  value: string;
  source: EthereumWalletSource;
  createdAtMs?: number;
}): StoredEthereumWalletSecret {
  const createdAtMs = params.createdAtMs ?? Date.now();
  if (params.kind === "mnemonic") {
    return {
      version: 1,
      chain: "ethereum",
      kind: "mnemonic",
      mnemonic: params.value,
      source: params.source,
      createdAtMs,
    };
  }
  return {
    version: 1,
    chain: "ethereum",
    kind: "private-key",
    privateKey: params.value,
    source: params.source,
    createdAtMs,
  };
}

export function describeStoredEthereumWalletSecret(secret: StoredEthereumWalletSecret): string {
  return secret.kind === "mnemonic" ? "mnemonic" : "Ethereum private key";
}

function isStoredEthereumWalletSecret(value: unknown): value is StoredEthereumWalletSecret {
  if (!isRecord(value) || value.version !== 1 || value.chain !== "ethereum") {
    return false;
  }
  if (value.kind === "mnemonic") {
    return (
      typeof value.mnemonic === "string" &&
      value.mnemonic.trim().length > 0 &&
      (value.source === "user" || value.source === "generated") &&
      typeof value.createdAtMs === "number" &&
      Number.isFinite(value.createdAtMs)
    );
  }
  if (value.kind === "private-key") {
    return (
      typeof value.privateKey === "string" &&
      value.privateKey.trim().length > 0 &&
      (value.source === "user" || value.source === "generated") &&
      typeof value.createdAtMs === "number" &&
      Number.isFinite(value.createdAtMs)
    );
  }
  return false;
}

export function readStoredEthereumWalletSecret(
  pathname: string = resolveEthereumWalletPath(),
): StoredEthereumWalletSecret | null {
  const raw = readTextFileIfExists(pathname);
  if (!raw) {
    return null;
  }
  try {
    const parsed = JSON.parse(raw) as unknown;
    return isStoredEthereumWalletSecret(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

export function deriveEthereumWalletAddress(secret: StoredEthereumWalletSecret): string {
  return secret.kind === "mnemonic"
    ? Wallet.fromPhrase(secret.mnemonic).address
    : new Wallet(secret.privateKey).address;
}

export function readStoredEthereumWalletSummary(
  pathname: string = resolveEthereumWalletPath(),
): EthereumWalletSummary {
  const secret = readStoredEthereumWalletSecret(pathname);
  if (!secret) {
    return {
      address: null,
      kind: null,
      source: null,
    };
  }

  try {
    return {
      address: deriveEthereumWalletAddress(secret),
      kind: secret.kind,
      source: secret.source,
    };
  } catch {
    return {
      address: null,
      kind: null,
      source: null,
    };
  }
}

export function writeStoredEthereumWalletSecret(
  secret: StoredEthereumWalletSecret,
  pathname: string = resolveEthereumWalletPath(),
): void {
  writeJsonFileSecure(pathname, secret);
}

export function deleteStoredEthereumWalletSecret(
  pathname: string = resolveEthereumWalletPath(),
): void {
  try {
    fs.rmSync(pathname, { force: true });
  } catch {
    // best-effort cleanup
  }
}
