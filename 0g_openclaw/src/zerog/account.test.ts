import { describe, expect, it, vi } from "vitest";
import { buildStoredEthereumWalletSecret } from "../wallet/ethereum-wallet.js";
import {
  acknowledgeZeroGProvider,
  fundZeroGMainAccount,
  fundZeroGProviderAccount,
  getZeroGAccountSummary,
  parseQualifiedZeroGModel,
  type ZeroGAccountDeps,
} from "./account.js";

const QUALIFIED_MODEL =
  "0g/svc_eyJwIjoiMHhhYmNkZWYxMjM0NTY3ODkwYWJjZGVmMTIzNDU2Nzg5MGFiY2RlZjEyIiwibSI6IkxhbWEtMy4zLTcwQi1JbnN0cnVjdCIsInQiOiJpbmZlcmVuY2UifQ";
const PROVIDER = "0xabcdef1234567890abcdef1234567890abcdef12";

type ZeroGServiceDetailStub = {
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

function createDeps(
  overrides: {
    broker?: ReturnType<typeof createBroker>;
    readOnlyBroker?: ReturnType<typeof createReadOnlyBroker>;
  } = {},
): ZeroGAccountDeps {
  return {
    env: {},
    readWalletSecret: () =>
      buildStoredEthereumWalletSecret({
        kind: "mnemonic",
        value: "test test test test test test test test test test test junk",
        source: "generated",
      }),
    readWalletSummary: () => ({
      address: "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
      kind: "mnemonic" as const,
      source: "generated" as const,
    }),
    createBroker: async () => overrides.broker ?? createBroker(),
    createReadOnlyBroker: async () => overrides.readOnlyBroker ?? createReadOnlyBroker(),
  };
}

function createReadOnlyBroker() {
  return {
    inference: {
      services: [
        {
          provider: PROVIDER,
          serviceType: "inference",
          url: "https://provider.0g.ai/v1",
          inputPrice: 1000000000000n,
          outputPrice: 2000000000000n,
          model: "Llama-3.3-70B-Instruct",
          verifiability: "tee",
          teeSignerAddress: "0x1111111111111111111111111111111111111111",
          teeSignerAcknowledged: true,
        },
      ] as ZeroGServiceDetailStub[],
      listServiceWithDetail: vi.fn(async function (this: { services: ZeroGServiceDetailStub[] }) {
        return this.services;
      }),
    },
  };
}

function createBroker() {
  return {
    ledger: {
      addLedger: vi.fn(async () => undefined),
      depositFund: vi.fn(async () => undefined),
      getLedger: vi.fn(async () => ({
        user: "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
        availableBalance: 3_000000000000000000n,
        totalBalance: 4_000000000000000000n,
      })),
      transferFund: vi.fn(async () => undefined),
    },
    inference: {
      getAccount: vi.fn(async () => ({
        balance: 2_000000000000000000n,
        pendingRefund: 250000000000000000n,
        acknowledged: true,
      })),
      getService: undefined,
      getServiceMetadata: vi.fn(async () => ({
        endpoint: "https://provider.0g.ai/v1",
        model: "Llama-3.3-70B-Instruct",
      })),
      acknowledgeProviderSigner: vi.fn(async () => undefined),
      userAcknowledged: vi.fn(async () => true),
    },
  };
}

describe("parseQualifiedZeroGModel", () => {
  it("extracts provider and model details from a qualified 0G model", () => {
    expect(parseQualifiedZeroGModel(QUALIFIED_MODEL)).toEqual({
      qualifiedModel: QUALIFIED_MODEL,
      providerAddress: PROVIDER,
      model: "Lama-3.3-70B-Instruct",
      serviceType: "inference",
    });
  });
});

describe("0G account helper", () => {
  it("builds a ready summary from ledger and provider state", async () => {
    const broker = createBroker();
    const readOnlyBroker = createReadOnlyBroker();

    const summary = await getZeroGAccountSummary(
      { model: QUALIFIED_MODEL },
      createDeps({ broker, readOnlyBroker }),
    );

    expect(summary.ready).toBe(true);
    expect(summary.mainLedger).toEqual({
      exists: true,
      availableBalance: "3",
      totalBalance: "4",
    });
    expect(summary.providerAccount).toEqual({
      exists: true,
      availableBalance: "1.75",
      totalBalance: "2",
      pendingRefund: "0.25",
      userAcknowledged: true,
    });
    expect(summary.service.endpoint).toBe("https://provider.0g.ai/v1");
    expect(summary.issues).toEqual([]);
    expect(readOnlyBroker.inference.listServiceWithDetail).toHaveBeenCalledWith(0, 50, true);
  });

  it("falls back to the read-only broker when runtime inference lacks getService", async () => {
    const broker = createBroker();
    delete broker.inference.getService;
    const readOnlyBroker = createReadOnlyBroker();

    const summary = await getZeroGAccountSummary(
      { model: QUALIFIED_MODEL },
      createDeps({ broker, readOnlyBroker }),
    );

    expect(summary.service.model).toBe("Llama-3.3-70B-Instruct");
    expect(readOnlyBroker.inference.listServiceWithDetail).toHaveBeenCalledWith(0, 50, true);
  });

  it("returns a not-ready summary when the provider sub-account is missing", async () => {
    const broker = createBroker();
    broker.inference.getAccount.mockRejectedValueOnce(new Error("Sub-account not found"));
    broker.inference.userAcknowledged.mockRejectedValueOnce(new Error("Sub-account not found"));
    const readOnlyBroker = createReadOnlyBroker();

    const summary = await getZeroGAccountSummary(
      { model: QUALIFIED_MODEL },
      createDeps({ broker, readOnlyBroker }),
    );

    expect(summary.providerAccount).toEqual({
      exists: false,
      availableBalance: "0",
      totalBalance: "0",
      pendingRefund: "0",
      userAcknowledged: false,
    });
    expect(summary.ready).toBe(false);
    expect(summary.issues).toContain("Acknowledge this provider before starting chat.");
  });

  it("creates the main ledger on first funding", async () => {
    const broker = createBroker();
    broker.ledger.getLedger
      .mockRejectedValueOnce(new Error("missing ledger"))
      .mockResolvedValueOnce({
        user: "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
        availableBalance: 1_500000000000000000n,
        totalBalance: 1_500000000000000000n,
      });

    const summary = await fundZeroGMainAccount(
      { model: QUALIFIED_MODEL, amount: "1.5" },
      createDeps({ broker }),
    );

    expect(broker.ledger.addLedger).toHaveBeenCalledWith(1.5);
    expect(broker.ledger.depositFund).not.toHaveBeenCalled();
    expect(summary.mainLedger.availableBalance).toBe("1.5");
  });

  it("transfers provider funding in 18-decimal units", async () => {
    const broker = createBroker();

    await fundZeroGProviderAccount(
      { model: QUALIFIED_MODEL, amount: "2.25" },
      createDeps({ broker }),
    );

    expect(broker.ledger.transferFund).toHaveBeenCalledWith(
      PROVIDER,
      "inference",
      2250000000000000000n,
    );
  });

  it("initializes and acknowledges a missing provider account before funding the requested amount", async () => {
    const broker = createBroker();
    broker.inference.getAccount.mockRejectedValueOnce(new Error("Sub-account not found"));
    broker.inference.userAcknowledged.mockRejectedValueOnce(new Error("Sub-account not found"));

    await fundZeroGProviderAccount(
      { model: QUALIFIED_MODEL, amount: "2.25" },
      createDeps({ broker }),
    );

    expect(broker.inference.acknowledgeProviderSigner).toHaveBeenCalledWith(PROVIDER);
    expect(broker.ledger.transferFund).toHaveBeenCalledWith(
      PROVIDER,
      "inference",
      1250000000000000000n,
    );
  });

  it("requires at least 1 0G for the first provider transfer", async () => {
    const broker = createBroker();
    broker.inference.getAccount.mockRejectedValueOnce(new Error("Sub-account not found"));
    broker.inference.userAcknowledged.mockRejectedValueOnce(new Error("Sub-account not found"));

    await expect(
      fundZeroGProviderAccount({ model: QUALIFIED_MODEL, amount: "0.5" }, createDeps({ broker })),
    ).rejects.toMatchObject({
      code: "INVALID_REQUEST",
      message:
        "The first transfer to a 0G provider must be at least 1 0G so the provider sub-account can be initialized.",
    });

    expect(broker.inference.acknowledgeProviderSigner).not.toHaveBeenCalled();
    expect(broker.ledger.transferFund).not.toHaveBeenCalled();
  });

  it("acknowledges a provider through the broker", async () => {
    const broker = createBroker();

    await acknowledgeZeroGProvider({ model: QUALIFIED_MODEL }, createDeps({ broker }));

    expect(broker.inference.acknowledgeProviderSigner).toHaveBeenCalledWith(PROVIDER);
  });

  it("wraps generic acknowledgement reverts with an actionable message", async () => {
    const broker = createBroker();
    broker.inference.acknowledgeProviderSigner.mockRejectedValueOnce(
      new Error("Error: CallFailed"),
    );

    await expect(
      acknowledgeZeroGProvider({ model: QUALIFIED_MODEL }, createDeps({ broker })),
    ).rejects.toMatchObject({
      code: "UNAVAILABLE",
      message:
        "0G rejected the provider acknowledgement transaction. Make sure the main 0G account has at least 1 0G available, then retry acknowledgement.",
    });
  });
});
