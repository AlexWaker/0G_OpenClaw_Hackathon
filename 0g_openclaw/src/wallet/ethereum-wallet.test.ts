import fs from "node:fs/promises";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { withTempDir } from "../test-utils/temp-dir.js";
import {
  buildStoredEthereumWalletSecret,
  createEthereumMnemonic,
  deleteStoredEthereumWalletSecret,
  deriveEthereumWalletAddress,
  readStoredEthereumWalletSecret,
  readStoredEthereumWalletSummary,
  validateEthereumMnemonicInput,
  validateEthereumPrivateKeyInput,
  writeStoredEthereumWalletSecret,
} from "./ethereum-wallet.js";

describe("ethereum wallet secret helpers", () => {
  it("validates and normalizes BIP39 mnemonics", () => {
    const result = validateEthereumMnemonicInput(
      "  TEST test test test test test test test test test test junk  ",
    );
    expect(result).toEqual({
      ok: true,
      value: "test test test test test test test test test test test junk",
    });
  });

  it("rejects malformed mnemonics", () => {
    const result = validateEthereumMnemonicInput("not a real mnemonic phrase");
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error).toContain("Mnemonic");
    }
  });

  it("validates and normalizes Ethereum private keys", () => {
    const result = validateEthereumPrivateKeyInput(
      "AABBCCDDEEFF00112233445566778899AABBCCDDEEFF00112233445566778899",
    );
    expect(result).toEqual({
      ok: true,
      value: "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
    });
  });

  it("rejects private keys with the wrong length", () => {
    const result = validateEthereumPrivateKeyInput("0x1234");
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error).toContain("32 bytes");
    }
  });

  it("generates valid mnemonics", () => {
    const mnemonic = createEthereumMnemonic();
    const result = validateEthereumMnemonicInput(mnemonic);
    expect(result).toEqual({ ok: true, value: mnemonic });
  });

  it("derives the expected wallet address from supported secret kinds", () => {
    expect(
      deriveEthereumWalletAddress(
        buildStoredEthereumWalletSecret({
          kind: "mnemonic",
          value: "test test test test test test test test test test test junk",
          source: "generated",
        }),
      ),
    ).toBe("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266");

    expect(
      deriveEthereumWalletAddress(
        buildStoredEthereumWalletSecret({
          kind: "private-key",
          value: "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
          source: "user",
        }),
      ),
    ).toBe("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
  });

  it("writes and reads wallet secrets with secure permissions", async () => {
    await withTempDir("openclaw-eth-wallet-", async (dir) => {
      const pathname = path.join(dir, "ethereum-wallet.json");
      const secret = buildStoredEthereumWalletSecret({
        kind: "mnemonic",
        value: "test test test test test test test test test test test junk",
        source: "generated",
        createdAtMs: 123,
      });

      writeStoredEthereumWalletSecret(secret, pathname);

      expect(readStoredEthereumWalletSecret(pathname)).toEqual(secret);

      const stat = await fs.stat(pathname);
      expect(stat.mode & 0o777).toBe(0o600);
    });
  });

  it("reads a wallet summary without exposing secret material", async () => {
    await withTempDir("openclaw-eth-wallet-summary-", async (dir) => {
      const pathname = path.join(dir, "ethereum-wallet.json");
      writeStoredEthereumWalletSecret(
        buildStoredEthereumWalletSecret({
          kind: "mnemonic",
          value: "test test test test test test test test test test test junk",
          source: "generated",
        }),
        pathname,
      );

      expect(readStoredEthereumWalletSummary(pathname)).toEqual({
        address: "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
        kind: "mnemonic",
        source: "generated",
      });
    });
  });

  it("deletes wallet secrets", async () => {
    await withTempDir("openclaw-eth-wallet-delete-", async (dir) => {
      const pathname = path.join(dir, "ethereum-wallet.json");
      writeStoredEthereumWalletSecret(
        buildStoredEthereumWalletSecret({
          kind: "private-key",
          value: "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
          source: "user",
        }),
        pathname,
      );

      deleteStoredEthereumWalletSecret(pathname);

      expect(readStoredEthereumWalletSecret(pathname)).toBeNull();
    });
  });
});
