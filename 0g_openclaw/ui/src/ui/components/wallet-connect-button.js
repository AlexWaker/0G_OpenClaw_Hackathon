import { ConnectButton, RainbowKitProvider, darkTheme, lightTheme } from "@rainbow-me/rainbowkit";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import React from "react";
import { createRoot } from "react-dom/client";
// import { mainnet } from "wagmi/chains";
import { zeroGMainnet } from "viem/chains";
import { WagmiProvider, createConfig, http, injected } from "wagmi";

const queryClient = new QueryClient();
export const walletConnectChain = zeroGMainnet;
export const walletConnectInitialChain = walletConnectChain;
export const walletConnectChains = [walletConnectChain];
export const walletConnectTransports = {
  [walletConnectChain.id]: http(),
};

const wagmiConfig = createConfig({
  // chains: [mainnet],
  chains: walletConnectChains,
  connectors: [injected()],
  ssr: false,
  transports: {
    // [mainnet.id]: http(),
    ...walletConnectTransports,
  },
});

function resolveRainbowTheme() {
  const mode = document.documentElement.dataset.themeMode;
  const accentColor = "#2563eb";
  const accentColorForeground = "#ffffff";
  return mode === "light"
    ? lightTheme({ accentColor, accentColorForeground, borderRadius: "medium" })
    : darkTheme({ accentColor, accentColorForeground, borderRadius: "medium" });
}

function WalletConnectApp() {
  const [theme, setTheme] = React.useState(() => resolveRainbowTheme());

  React.useEffect(() => {
    const observer = new MutationObserver(() => {
      setTheme(resolveRainbowTheme());
    });
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["data-theme-mode"],
    });
    return () => observer.disconnect();
  }, []);

  return React.createElement(
    WagmiProvider,
    { config: wagmiConfig },
    React.createElement(
      QueryClientProvider,
      { client: queryClient },
      React.createElement(
        RainbowKitProvider,
        // { theme, initialChain: mainnet },
        { theme, initialChain: walletConnectInitialChain },
        React.createElement(ConnectButton, {
          showBalance: false,
          chainStatus: "icon",
        }),
      ),
    ),
  );
}

class WalletConnectButtonElement extends HTMLElement {
  connectedCallback() {
    if (this._reactRoot) {
      return;
    }

    const mount = document.createElement("div");
    mount.className = "wallet-connect-slot__mount";
    this.appendChild(mount);

    this._reactRoot = createRoot(mount);
    this._reactRoot.render(React.createElement(WalletConnectApp));
  }

  disconnectedCallback() {
    this._reactRoot?.unmount();
    this._reactRoot = null;
    this.textContent = "";
  }
}

if (!customElements.get("wallet-connect-button")) {
  customElements.define("wallet-connect-button", WalletConnectButtonElement);
}
