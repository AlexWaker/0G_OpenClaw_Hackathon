import { describe, expect, it, vi } from "vitest";
import {
  acknowledgeZeroGProvider,
  closeZeroGFundingDialog,
  fundZeroGMainAccount,
  fundZeroGProviderAccount,
  loadZeroGAccountState,
  openZeroGFundingDialog,
  resolveSelectedZeroGModelValue,
  type ZeroGFundingState,
} from "./zero-g.ts";

const QUALIFIED_MODEL =
  "0g/svc_eyJwIjoiMHhhYmNkZWYxMjM0NTY3ODkwYWJjZGVmMTIzNDU2Nzg5MGFiY2RlZjEyIiwibSI6IkxhbWEtMy4zLTcwQi1JbnN0cnVjdCIsInQiOiJpbmZlcmVuY2UifQ";

const SUMMARY = {
  wallet: {
    address: "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
    kind: "mnemonic" as const,
    source: "generated" as const,
  },
  selectedModel: QUALIFIED_MODEL,
  providerAddress: "0xabcdef1234567890abcdef1234567890abcdef12",
  serviceType: "inference",
  network: "mainnet" as const,
  rpcUrl: "https://evmrpc.0g.ai",
  ready: true,
  issues: [],
  mainLedger: {
    exists: true,
    availableBalance: "3",
    totalBalance: "4",
  },
  providerAccount: {
    exists: true,
    availableBalance: "2",
    totalBalance: "2",
    pendingRefund: "0",
    userAcknowledged: true,
  },
  service: {
    model: "Llama-3.3-70B-Instruct",
    serviceType: "inference",
    endpoint: "https://provider.0g.ai/v1",
    url: "https://provider.0g.ai/v1",
    verifiability: "tee",
    inputPrice: "0.000001",
    outputPrice: "0.000002",
    teeSignerAddress: "0x1111111111111111111111111111111111111111",
    teeSignerAcknowledged: true,
  },
};

function createState(model: string | null = QUALIFIED_MODEL): {
  state: ZeroGFundingState;
  request: ReturnType<typeof vi.fn>;
} {
  const request = vi.fn();
  const state: ZeroGFundingState = {
    client: { request } as unknown as ZeroGFundingState["client"],
    connected: true,
    sessionKey: "main",
    chatModelOverrides: {
      main: model ? { kind: "qualified", value: model } : null,
    },
    chatModelCatalog: [],
    sessionsResult: null,
    zeroGAccountLoading: false,
    zeroGAccountSummary: null,
    zeroGAccountError: null,
    zeroGFundingDialogOpen: false,
    zeroGFundingBusy: false,
    zeroGFundingAction: null,
    zeroGFundingMainAmount: "",
    zeroGFundingProviderAmount: "",
    zeroGFundingError: null,
    zeroGFundingSuccess: null,
  };
  return { state, request };
}

describe("zero-g controller", () => {
  it("resolves the currently selected 0G model", () => {
    const { state } = createState();
    expect(resolveSelectedZeroGModelValue(state)).toBe(QUALIFIED_MODEL);
  });

  it("loads the selected 0G account summary", async () => {
    const { state, request } = createState();
    request.mockResolvedValue(SUMMARY);

    await loadZeroGAccountState(state);

    expect(request).toHaveBeenCalledWith("zerog.account.get", { model: QUALIFIED_MODEL });
    expect(state.zeroGAccountSummary).toEqual(SUMMARY);
    expect(state.zeroGAccountError).toBeNull();
  });

  it("clears state when no 0G model is selected", async () => {
    const { state, request } = createState(null);
    state.zeroGAccountSummary = SUMMARY;

    await loadZeroGAccountState(state);

    expect(request).not.toHaveBeenCalled();
    expect(state.zeroGAccountSummary).toBeNull();
  });

  it("opens and closes the funding dialog", () => {
    const { state } = createState();

    openZeroGFundingDialog(state);
    expect(state.zeroGFundingDialogOpen).toBe(true);
    expect(state.zeroGFundingMainAmount).toBe("1");
    expect(state.zeroGFundingProviderAmount).toBe("1");

    closeZeroGFundingDialog(state);
    expect(state.zeroGFundingDialogOpen).toBe(false);
  });

  it("funds the main 0G account and stores the refreshed summary", async () => {
    const { state, request } = createState();
    state.zeroGFundingMainAmount = "1.5";
    request.mockResolvedValue(SUMMARY);

    await fundZeroGMainAccount(state);

    expect(request).toHaveBeenCalledWith("zerog.account.fundMain", {
      model: QUALIFIED_MODEL,
      amount: "1.5",
    });
    expect(state.zeroGFundingSuccess).toBe("Main 0G account funded.");
    expect(state.zeroGFundingMainAmount).toBe("");
  });

  it("acknowledges the provider for the selected wallet", async () => {
    const { state, request } = createState();
    request.mockResolvedValue(SUMMARY);

    await acknowledgeZeroGProvider(state);

    expect(request).toHaveBeenCalledWith("zerog.account.acknowledgeProvider", {
      model: QUALIFIED_MODEL,
    });
    expect(state.zeroGFundingSuccess).toBe("Provider acknowledged for this wallet.");
  });

  it("surfaces cleaned provider-funding errors without GatewayRequestError prefixes", async () => {
    const { state, request } = createState();
    state.zeroGFundingProviderAmount = "1";
    request.mockRejectedValue(new Error("GatewayRequestError: Error: Error: CallFailed"));

    await fundZeroGProviderAccount(state);

    expect(state.zeroGFundingError).toBe("CallFailed");
  });
});
