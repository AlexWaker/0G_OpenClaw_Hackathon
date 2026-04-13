import { describe, expect, it, vi } from "vitest";
import { createWizardPrompter } from "../../test/helpers/wizard-prompter.js";
import { withTempDir } from "../test-utils/temp-dir.js";
import {
  buildStoredEthereumWalletSecret,
  readStoredEthereumWalletSecret,
  writeStoredEthereumWalletSecret,
} from "../wallet/ethereum-wallet.js";
import { applySetupEthereumWalletSelection, promptSetupEthereumWallet } from "./setup.wallet.js";

describe("setup Ethereum wallet step", () => {
  it("can collect a private key before provider setup", async () => {
    await withTempDir("openclaw-wallet-step-", async (dir) => {
      const walletPath = `${dir}/ethereum-wallet.json`;
      const text = vi.fn(async (params: { validate?: (value: string) => string | undefined }) => {
        const value = "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899";
        const error = params.validate?.(value);
        if (error) {
          throw new Error(error);
        }
        return value;
      });
      const prompter = createWizardPrompter({
        select: vi.fn().mockResolvedValueOnce("private-key") as unknown as ReturnType<
          typeof createWizardPrompter
        >["select"],
        text,
      });

      const result = await promptSetupEthereumWallet({ prompter, walletPath });

      expect(result).toMatchObject({
        status: "set",
        secret: {
          kind: "private-key",
          privateKey: "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
        },
      });
    });
  });

  it("can generate a new mnemonic and persist it", async () => {
    await withTempDir("openclaw-wallet-step-generate-", async (dir) => {
      const walletPath = `${dir}/ethereum-wallet.json`;
      const prompter = createWizardPrompter({
        select: vi.fn().mockResolvedValueOnce("generate") as unknown as ReturnType<
          typeof createWizardPrompter
        >["select"],
        confirm: vi.fn(async () => true),
      });

      const result = await promptSetupEthereumWallet({ prompter, walletPath });
      expect(result.status).toBe("set");
      applySetupEthereumWalletSelection(result);

      const stored = readStoredEthereumWalletSecret(walletPath);
      expect(stored?.kind).toBe("mnemonic");
      expect(stored?.source).toBe("generated");
      expect(stored?.kind === "mnemonic" ? stored.mnemonic.split(" ").length : 0).toBe(12);
    });
  });

  it("keeps an existing wallet when requested", async () => {
    await withTempDir("openclaw-wallet-step-keep-", async (dir) => {
      const walletPath = `${dir}/ethereum-wallet.json`;
      writeStoredEthereumWalletSecret(
        buildStoredEthereumWalletSecret({
          kind: "mnemonic",
          value: "test test test test test test test test test test test junk",
          source: "user",
          createdAtMs: 1,
        }),
        walletPath,
      );

      const prompter = createWizardPrompter({
        select: vi.fn().mockResolvedValueOnce("keep") as unknown as ReturnType<
          typeof createWizardPrompter
        >["select"],
      });

      const result = await promptSetupEthereumWallet({ prompter, walletPath });
      expect(result).toEqual({ status: "unchanged", path: walletPath });
    });
  });

  it("can remove an existing wallet", async () => {
    await withTempDir("openclaw-wallet-step-remove-", async (dir) => {
      const walletPath = `${dir}/ethereum-wallet.json`;
      writeStoredEthereumWalletSecret(
        buildStoredEthereumWalletSecret({
          kind: "private-key",
          value: "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
          source: "user",
        }),
        walletPath,
      );

      const prompter = createWizardPrompter({
        select: vi.fn().mockResolvedValueOnce("remove") as unknown as ReturnType<
          typeof createWizardPrompter
        >["select"],
      });

      const result = await promptSetupEthereumWallet({ prompter, walletPath });
      expect(result).toEqual({ status: "remove", path: walletPath });

      applySetupEthereumWalletSelection(result);
      expect(readStoredEthereumWalletSecret(walletPath)).toBeNull();
    });
  });
});
