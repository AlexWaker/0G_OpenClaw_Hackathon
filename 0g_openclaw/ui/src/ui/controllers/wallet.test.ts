import { describe, expect, it, vi } from "vitest";
import { GatewayRequestError } from "../gateway.ts";
import { loadEthereumWalletState, type EthereumWalletState } from "./wallet.ts";

function createState(): { state: EthereumWalletState; request: ReturnType<typeof vi.fn> } {
  const request = vi.fn();
  const state: EthereumWalletState = {
    client: {
      request,
    } as unknown as EthereumWalletState["client"],
    connected: true,
    ethereumWalletLoading: false,
    ethereumWalletSummary: null,
    ethereumWalletError: null,
  };
  return { state, request };
}

describe("loadEthereumWalletState", () => {
  it("loads the configured wallet summary", async () => {
    const { state, request } = createState();
    request.mockResolvedValue({
      address: "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
      kind: "mnemonic",
      source: "generated",
    });

    await loadEthereumWalletState(state);

    expect(request).toHaveBeenCalledWith("wallet.ethereum.get", {});
    expect(state.ethereumWalletSummary).toEqual({
      address: "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
      kind: "mnemonic",
      source: "generated",
    });
    expect(state.ethereumWalletError).toBeNull();
    expect(state.ethereumWalletLoading).toBe(false);
  });

  it("stores a readable error when loading fails", async () => {
    const { state, request } = createState();
    request.mockRejectedValue(new Error("gateway unavailable"));

    await loadEthereumWalletState(state);

    expect(state.ethereumWalletSummary).toBeNull();
    expect(state.ethereumWalletError).toContain("gateway unavailable");
    expect(state.ethereumWalletLoading).toBe(false);
  });

  it("formats missing operator.read failures consistently", async () => {
    const { state, request } = createState();
    request.mockRejectedValue(
      new GatewayRequestError({
        code: "INVALID_REQUEST",
        message: "missing scope: operator.read",
      }),
    );

    await loadEthereumWalletState(state);

    expect(state.ethereumWalletSummary).toBeNull();
    expect(state.ethereumWalletError).toContain("operator.read");
  });
});
