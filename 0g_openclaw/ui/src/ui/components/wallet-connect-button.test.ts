import { describe, expect, it } from "vitest";
import {
  walletConnectChain,
  walletConnectChains,
  walletConnectInitialChain,
  walletConnectTransports,
} from "./wallet-connect-button.js";

describe("wallet connect button", () => {
  it("targets 0G mainnet", () => {
    expect(walletConnectChain.id).toBe(16661);
    expect(walletConnectChain.name).toBe("0G Mainnet");
    expect(walletConnectChain.rpcUrls.default.http).toContain("https://evmrpc.0g.ai");
    expect(walletConnectInitialChain).toBe(walletConnectChain);
    expect(walletConnectChains).toEqual([walletConnectChain]);
    expect(Object.keys(walletConnectTransports)).toEqual([String(walletConnectChain.id)]);
  });
});
