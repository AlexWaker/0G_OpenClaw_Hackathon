import type { Chain, HttpTransport } from "viem";

export const walletConnectChain: Chain;
export const walletConnectInitialChain: Chain;
export const walletConnectChains: readonly Chain[];
export const walletConnectTransports: Record<number, HttpTransport>;
