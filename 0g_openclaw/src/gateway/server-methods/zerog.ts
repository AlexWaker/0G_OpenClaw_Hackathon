import {
  ZeroGOperationError,
  acknowledgeZeroGProvider,
  fundZeroGMainAccount,
  fundZeroGProviderAccount,
  getZeroGAccountSummary,
} from "../../zerog/account.js";
import { ErrorCodes, errorShape } from "../protocol/index.js";
import {
  validateZeroGAcknowledgeProviderParams,
  validateZeroGAccountGetParams,
  validateZeroGFundMainParams,
  validateZeroGFundProviderParams,
} from "../protocol/index.js";
import type { GatewayRequestHandlers } from "./types.js";
import { assertValidParams } from "./validation.js";

function respondZeroGError(
  respond: Parameters<GatewayRequestHandlers[string]>[0]["respond"],
  err: unknown,
) {
  const message =
    err instanceof Error ? err.message.replace(/^(?:Error:\s*)+/i, "").trim() : String(err);

  if (err instanceof ZeroGOperationError) {
    respond(
      false,
      undefined,
      errorShape(
        err.code === "INVALID_REQUEST" ? ErrorCodes.INVALID_REQUEST : ErrorCodes.UNAVAILABLE,
        err.message,
      ),
    );
    return;
  }

  respond(false, undefined, errorShape(ErrorCodes.UNAVAILABLE, message || "0G request failed."));
}

export const zeroGHandlers: GatewayRequestHandlers = {
  "zerog.account.get": async ({ params, respond }) => {
    if (!assertValidParams(params, validateZeroGAccountGetParams, "zerog.account.get", respond)) {
      return;
    }

    try {
      respond(true, await getZeroGAccountSummary(params), undefined);
    } catch (err) {
      respondZeroGError(respond, err);
    }
  },
  "zerog.account.fundMain": async ({ params, respond }) => {
    if (
      !assertValidParams(params, validateZeroGFundMainParams, "zerog.account.fundMain", respond)
    ) {
      return;
    }

    try {
      respond(true, await fundZeroGMainAccount(params), undefined);
    } catch (err) {
      respondZeroGError(respond, err);
    }
  },
  "zerog.account.fundProvider": async ({ params, respond }) => {
    if (
      !assertValidParams(
        params,
        validateZeroGFundProviderParams,
        "zerog.account.fundProvider",
        respond,
      )
    ) {
      return;
    }

    try {
      respond(true, await fundZeroGProviderAccount(params), undefined);
    } catch (err) {
      respondZeroGError(respond, err);
    }
  },
  "zerog.account.acknowledgeProvider": async ({ params, respond }) => {
    if (
      !assertValidParams(
        params,
        validateZeroGAcknowledgeProviderParams,
        "zerog.account.acknowledgeProvider",
        respond,
      )
    ) {
      return;
    }

    try {
      respond(true, await acknowledgeZeroGProvider(params), undefined);
    } catch (err) {
      respondZeroGError(respond, err);
    }
  },
};
