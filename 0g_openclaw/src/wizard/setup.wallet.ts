import { resolveEthereumWalletPath } from "../config/paths.js";
import {
  buildStoredEthereumWalletSecret,
  createEthereumMnemonic,
  deleteStoredEthereumWalletSecret,
  describeStoredEthereumWalletSecret,
  readStoredEthereumWalletSecret,
  type StoredEthereumWalletSecret,
  validateEthereumMnemonicInput,
  validateEthereumPrivateKeyInput,
  writeStoredEthereumWalletSecret,
} from "../wallet/ethereum-wallet.js";
import type { WizardPrompter } from "./prompts.js";

export type SetupEthereumWalletSelection =
  | { status: "unchanged" | "skipped"; path: string }
  | { status: "remove"; path: string }
  | { status: "set"; path: string; secret: StoredEthereumWalletSecret };

async function promptWalletCreationAction(
  prompter: WizardPrompter,
): Promise<"skip" | "mnemonic" | "private-key" | "generate"> {
  return await prompter.select({
    message: "Ethereum wallet setup",
    options: [
      {
        value: "skip",
        label: "Skip for now",
        hint: "Continue without storing wallet material",
      },
      {
        value: "mnemonic",
        label: "Enter mnemonic",
        hint: "Validate and store a BIP39 seed phrase",
      },
      {
        value: "private-key",
        label: "Enter Ethereum private key",
        hint: "Validate and store a 32-byte hex private key",
      },
      {
        value: "generate",
        label: "Create new mnemonic",
        hint: "Generate and store a new 12-word BIP39 phrase",
      },
    ],
    initialValue: "skip",
  });
}

export async function promptSetupEthereumWallet(params: {
  prompter: WizardPrompter;
  walletPath?: string;
}): Promise<SetupEthereumWalletSelection> {
  const walletPath = params.walletPath ?? resolveEthereumWalletPath();
  const existing = readStoredEthereumWalletSecret(walletPath);

  await params.prompter.note(
    [
      "Optional Ethereum wallet setup.",
      "This stores a mnemonic or Ethereum private key in a separate credentials file.",
      `Path: ${walletPath}`,
    ].join("\n"),
    "Wallet",
  );

  if (existing) {
    const existingAction = await params.prompter.select({
      message: "Ethereum wallet already configured",
      options: [
        {
          value: "keep",
          label: "Keep existing",
          hint: `Current value type: ${describeStoredEthereumWalletSecret(existing)}`,
        },
        {
          value: "replace",
          label: "Replace",
          hint: "Enter a new mnemonic/private key or generate a new mnemonic",
        },
        {
          value: "remove",
          label: "Remove",
          hint: "Delete the existing wallet file and continue",
        },
      ],
      initialValue: "keep",
    });

    if (existingAction === "keep") {
      return { status: "unchanged", path: walletPath };
    }
    if (existingAction === "remove") {
      return { status: "remove", path: walletPath };
    }
  }

  while (true) {
    const action = await promptWalletCreationAction(params.prompter);
    if (action === "skip") {
      return { status: "skipped", path: walletPath };
    }

    if (action === "mnemonic") {
      const mnemonicInput = await params.prompter.text({
        message: "BIP39 mnemonic",
        placeholder: "test test test ... junk",
        validate: (value) => {
          const result = validateEthereumMnemonicInput(value);
          return result.ok ? undefined : result.error;
        },
      });
      const validated = validateEthereumMnemonicInput(mnemonicInput);
      if (!validated.ok) {
        throw new Error(validated.error);
      }
      return {
        status: "set",
        path: walletPath,
        secret: buildStoredEthereumWalletSecret({
          kind: "mnemonic",
          value: validated.value,
          source: "user",
        }),
      };
    }

    if (action === "private-key") {
      const privateKeyInput = await params.prompter.text({
        message: "Ethereum private key",
        placeholder: "0x...",
        validate: (value) => {
          const result = validateEthereumPrivateKeyInput(value);
          return result.ok ? undefined : result.error;
        },
      });
      const validated = validateEthereumPrivateKeyInput(privateKeyInput);
      if (!validated.ok) {
        throw new Error(validated.error);
      }
      return {
        status: "set",
        path: walletPath,
        secret: buildStoredEthereumWalletSecret({
          kind: "private-key",
          value: validated.value,
          source: "user",
        }),
      };
    }

    const mnemonic = createEthereumMnemonic();
    await params.prompter.note(
      [
        "Write this mnemonic down now.",
        "",
        mnemonic,
        "",
        "It will also be stored in the local credentials directory.",
        `Path: ${walletPath}`,
      ].join("\n"),
      "New Ethereum mnemonic",
    );
    const confirmed = await params.prompter.confirm({
      message: "I have saved this mnemonic. Continue?",
      initialValue: false,
    });
    if (!confirmed) {
      await params.prompter.note(
        "Mnemonic was not saved. Choose how you want to continue.",
        "Wallet",
      );
      continue;
    }
    return {
      status: "set",
      path: walletPath,
      secret: buildStoredEthereumWalletSecret({
        kind: "mnemonic",
        value: mnemonic,
        source: "generated",
      }),
    };
  }
}

export function applySetupEthereumWalletSelection(selection: SetupEthereumWalletSelection): void {
  if (selection.status === "set") {
    writeStoredEthereumWalletSecret(selection.secret, selection.path);
    return;
  }
  if (selection.status === "remove") {
    deleteStoredEthereumWalletSecret(selection.path);
  }
}
