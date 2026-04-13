import { beforeEach, describe, expect, it, vi } from "vitest";
import type { GatewayRequestHandlerOptions } from "./types.js";
import { zeroGHandlers } from "./zerog.js";

const {
  acknowledgeZeroGProviderMock,
  fundZeroGMainAccountMock,
  fundZeroGProviderAccountMock,
  getZeroGAccountSummaryMock,
} = vi.hoisted(() => ({
  acknowledgeZeroGProviderMock: vi.fn(),
  fundZeroGMainAccountMock: vi.fn(),
  fundZeroGProviderAccountMock: vi.fn(),
  getZeroGAccountSummaryMock: vi.fn(),
}));

vi.mock("../../zerog/account.js", () => ({
  acknowledgeZeroGProvider: acknowledgeZeroGProviderMock,
  fundZeroGMainAccount: fundZeroGMainAccountMock,
  fundZeroGProviderAccount: fundZeroGProviderAccountMock,
  getZeroGAccountSummary: getZeroGAccountSummaryMock,
  ZeroGOperationError: class ZeroGOperationError extends Error {
    code: "INVALID_REQUEST" | "UNAVAILABLE";
    constructor(code: "INVALID_REQUEST" | "UNAVAILABLE", message: string) {
      super(message);
      this.code = code;
    }
  },
}));

const SUMMARY = {
  wallet: {
    address: "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
    kind: "mnemonic",
    source: "generated",
  },
  selectedModel: "0g/svc_test",
  providerAddress: "0xabcdef1234567890abcdef1234567890abcdef12",
  serviceType: "inference",
  network: "mainnet",
  rpcUrl: "https://evmrpc.0g.ai",
  ready: true,
  issues: [],
  mainLedger: { exists: true, availableBalance: "1", totalBalance: "1" },
  providerAccount: {
    exists: true,
    availableBalance: "1",
    totalBalance: "1",
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

function createOptions(
  method: string,
  params: Record<string, unknown>,
): GatewayRequestHandlerOptions {
  return {
    req: { type: "req", id: "req-1", method, params },
    params,
    client: null,
    isWebchatConnect: () => false,
    respond: vi.fn(),
    context: {},
  } as unknown as GatewayRequestHandlerOptions;
}

describe("zeroGHandlers", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("returns account summaries for zerog.account.get", async () => {
    getZeroGAccountSummaryMock.mockResolvedValue(SUMMARY);
    const opts = createOptions("zerog.account.get", { model: "0g/svc_test" });

    await zeroGHandlers["zerog.account.get"](opts);

    expect(getZeroGAccountSummaryMock).toHaveBeenCalledWith({ model: "0g/svc_test" });
    expect(opts.respond).toHaveBeenCalledWith(true, SUMMARY, undefined);
  });

  it("rejects invalid zerog.account.fundMain params", async () => {
    const opts = createOptions("zerog.account.fundMain", { amount: "1" });

    await zeroGHandlers["zerog.account.fundMain"](opts);

    expect(fundZeroGMainAccountMock).not.toHaveBeenCalled();
    expect(opts.respond).toHaveBeenCalledWith(
      false,
      undefined,
      expect.objectContaining({
        message: expect.stringContaining("invalid zerog.account.fundMain params"),
      }),
    );
  });

  it("maps acknowledge requests to the helper", async () => {
    acknowledgeZeroGProviderMock.mockResolvedValue(SUMMARY);
    const opts = createOptions("zerog.account.acknowledgeProvider", {
      model: "0g/svc_test",
    });

    await zeroGHandlers["zerog.account.acknowledgeProvider"](opts);

    expect(acknowledgeZeroGProviderMock).toHaveBeenCalledWith({ model: "0g/svc_test" });
    expect(opts.respond).toHaveBeenCalledWith(true, SUMMARY, undefined);
  });

  it("maps provider funding requests to the helper", async () => {
    fundZeroGProviderAccountMock.mockResolvedValue(SUMMARY);
    const opts = createOptions("zerog.account.fundProvider", {
      model: "0g/svc_test",
      amount: "2",
    });

    await zeroGHandlers["zerog.account.fundProvider"](opts);

    expect(fundZeroGProviderAccountMock).toHaveBeenCalledWith({
      model: "0g/svc_test",
      amount: "2",
    });
    expect(opts.respond).toHaveBeenCalledWith(true, SUMMARY, undefined);
  });
});
