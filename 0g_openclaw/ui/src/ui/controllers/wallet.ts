import type { GatewayBrowserClient } from "../gateway.ts";
import type { EthereumWalletSummary } from "../types.ts";
import {
  formatMissingOperatorReadScopeMessage,
  isMissingOperatorReadScopeError,
} from "./scope-errors.ts";

const EMPTY_ETHEREUM_WALLET_SUMMARY: EthereumWalletSummary = {
  address: null,
  kind: null,
  source: null,
};

export type EthereumWalletState = {
  client: GatewayBrowserClient | null;
  connected: boolean;
  ethereumWalletLoading: boolean;
  ethereumWalletSummary: EthereumWalletSummary | null;
  ethereumWalletError: string | null;
};

export async function loadEthereumWalletState(state: EthereumWalletState): Promise<void> {
  if (!state.client || !state.connected) {
    return;
  }
  if (state.ethereumWalletLoading) {
    return;
  }

  state.ethereumWalletLoading = true;
  state.ethereumWalletError = null;
  try {
    const result = await state.client.request<EthereumWalletSummary>("wallet.ethereum.get", {});
    state.ethereumWalletSummary = result ?? EMPTY_ETHEREUM_WALLET_SUMMARY;
  } catch (err) {
    state.ethereumWalletSummary = null;
    state.ethereumWalletError = isMissingOperatorReadScopeError(err)
      ? formatMissingOperatorReadScopeMessage("wallet details")
      : String(err);
  } finally {
    state.ethereumWalletLoading = false;
  }
}
