import { resolveChatModelOverrideValue } from "../chat-model-select-state.ts";
import type { GatewayBrowserClient } from "../gateway.ts";
import type {
  ChatModelOverride,
  ModelCatalogEntry,
  SessionsListResult,
  ZeroGAccountSummary,
} from "../types.ts";
import {
  formatMissingOperatorReadScopeMessage,
  isMissingOperatorReadScopeError,
} from "./scope-errors.ts";

export type ZeroGFundingAction = "main" | "provider" | "acknowledge";

export type ZeroGAccountLoadState = {
  client: GatewayBrowserClient | null;
  connected: boolean;
  sessionKey: string;
  chatModelOverrides: Record<string, ChatModelOverride | null>;
  chatModelCatalog: ModelCatalogEntry[];
  sessionsResult: SessionsListResult | null;
  zeroGAccountLoading: boolean;
  zeroGAccountSummary: ZeroGAccountSummary | null;
  zeroGAccountError: string | null;
};

export type ZeroGFundingState = ZeroGAccountLoadState & {
  zeroGFundingDialogOpen: boolean;
  zeroGFundingBusy: boolean;
  zeroGFundingAction: ZeroGFundingAction | null;
  zeroGFundingMainAmount: string;
  zeroGFundingProviderAmount: string;
  zeroGFundingError: string | null;
  zeroGFundingSuccess: string | null;
};

function isZeroGQualifiedModel(value: string | null | undefined): value is string {
  return typeof value === "string" && value.trim().toLowerCase().startsWith("0g/");
}

export function resolveSelectedZeroGModelValue(
  state: Pick<
    ZeroGAccountLoadState,
    "sessionKey" | "chatModelOverrides" | "chatModelCatalog" | "sessionsResult"
  >,
): string | null {
  const resolved = resolveChatModelOverrideValue(state).trim();
  return isZeroGQualifiedModel(resolved) ? resolved : null;
}

function formatZeroGReadError(err: unknown): string {
  return isMissingOperatorReadScopeError(err)
    ? formatMissingOperatorReadScopeMessage("0G account details")
    : String(err);
}

function formatZeroGMutationError(err: unknown): string {
  if (isMissingOperatorReadScopeError(err)) {
    return formatMissingOperatorReadScopeMessage("0G account changes");
  }
  const raw = err instanceof Error ? err.message : String(err);
  return raw.replace(/^(?:GatewayRequestError:\s*)?(?:Error:\s*)+/i, "").trim() || raw;
}

export async function loadZeroGAccountState(
  state: ZeroGAccountLoadState,
  explicitModel?: string | null,
): Promise<void> {
  if (!state.client || !state.connected) {
    return;
  }

  const model = explicitModel?.trim() || resolveSelectedZeroGModelValue(state);
  if (!isZeroGQualifiedModel(model)) {
    state.zeroGAccountLoading = false;
    state.zeroGAccountSummary = null;
    state.zeroGAccountError = null;
    return;
  }
  if (state.zeroGAccountLoading) {
    return;
  }

  state.zeroGAccountLoading = true;
  state.zeroGAccountError = null;
  try {
    const result = await state.client.request<ZeroGAccountSummary>("zerog.account.get", {
      model,
    });
    if (resolveSelectedZeroGModelValue(state) !== model) {
      return;
    }
    state.zeroGAccountSummary = result;
  } catch (err) {
    if (resolveSelectedZeroGModelValue(state) !== model) {
      return;
    }
    state.zeroGAccountSummary = null;
    state.zeroGAccountError = formatZeroGReadError(err);
  } finally {
    state.zeroGAccountLoading = false;
  }
}

export function openZeroGFundingDialog(state: ZeroGFundingState): void {
  state.zeroGFundingDialogOpen = true;
  state.zeroGFundingError = null;
  state.zeroGFundingSuccess = null;
  if (!state.zeroGFundingMainAmount) {
    state.zeroGFundingMainAmount = "1";
  }
  if (!state.zeroGFundingProviderAmount) {
    state.zeroGFundingProviderAmount = "1";
  }
}

export function closeZeroGFundingDialog(state: ZeroGFundingState): void {
  if (state.zeroGFundingBusy) {
    return;
  }
  state.zeroGFundingDialogOpen = false;
  state.zeroGFundingError = null;
  state.zeroGFundingSuccess = null;
}

async function runZeroGMutation(
  state: ZeroGFundingState,
  params: {
    action: ZeroGFundingAction;
    method:
      | "zerog.account.fundMain"
      | "zerog.account.fundProvider"
      | "zerog.account.acknowledgeProvider";
    amount?: string;
    successMessage: string;
  },
): Promise<void> {
  if (!state.client || !state.connected || state.zeroGFundingBusy) {
    return;
  }

  const model = resolveSelectedZeroGModelValue(state);
  if (!model) {
    state.zeroGFundingError = "Select a 0G model first.";
    return;
  }

  if (params.amount != null && !params.amount.trim()) {
    state.zeroGFundingError = "Enter a positive 0G amount.";
    return;
  }

  state.zeroGFundingBusy = true;
  state.zeroGFundingAction = params.action;
  state.zeroGFundingError = null;
  state.zeroGFundingSuccess = null;
  try {
    const nextSummary = await state.client.request<ZeroGAccountSummary>(params.method, {
      model,
      ...(params.amount != null ? { amount: params.amount.trim() } : {}),
    });
    state.zeroGAccountSummary = nextSummary;
    state.zeroGAccountError = null;
    state.zeroGFundingSuccess = params.successMessage;
    if (params.action === "main") {
      state.zeroGFundingMainAmount = "";
    }
    if (params.action === "provider") {
      state.zeroGFundingProviderAmount = "";
    }
  } catch (err) {
    state.zeroGFundingError = formatZeroGMutationError(err);
  } finally {
    state.zeroGFundingBusy = false;
    state.zeroGFundingAction = null;
  }
}

export async function fundZeroGMainAccount(state: ZeroGFundingState): Promise<void> {
  await runZeroGMutation(state, {
    action: "main",
    method: "zerog.account.fundMain",
    amount: state.zeroGFundingMainAmount,
    successMessage: "Main 0G account funded.",
  });
}

export async function fundZeroGProviderAccount(state: ZeroGFundingState): Promise<void> {
  await runZeroGMutation(state, {
    action: "provider",
    method: "zerog.account.fundProvider",
    amount: state.zeroGFundingProviderAmount,
    successMessage: "Provider sub-account funded.",
  });
}

export async function acknowledgeZeroGProvider(state: ZeroGFundingState): Promise<void> {
  await runZeroGMutation(state, {
    action: "acknowledge",
    method: "zerog.account.acknowledgeProvider",
    successMessage: "Provider acknowledged for this wallet.",
  });
}
