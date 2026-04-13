export const ZERO_G_PROVIDER_ID = "0g";
export const ZERO_G_MODEL_REF_PREFIX = "svc_";

export type ZeroGModelRef = {
  providerAddress: string;
  model: string;
  serviceType?: string;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeProviderAddress(value: string): string {
  return value.trim().toLowerCase();
}

export function encodeZeroGModelRef(ref: ZeroGModelRef): string {
  const providerAddress = normalizeProviderAddress(ref.providerAddress);
  const model = ref.model.trim();
  const serviceType = ref.serviceType?.trim();
  if (!providerAddress || !model) {
    throw new Error("0G model refs require both providerAddress and model.");
  }
  return `${ZERO_G_MODEL_REF_PREFIX}${Buffer.from(
    JSON.stringify({
      p: providerAddress,
      m: model,
      ...(serviceType ? { t: serviceType } : {}),
    }),
  ).toString("base64url")}`;
}

export function parseZeroGModelRef(modelId: string): ZeroGModelRef | null {
  const trimmed = modelId.trim();
  if (!trimmed.startsWith(ZERO_G_MODEL_REF_PREFIX)) {
    return null;
  }

  try {
    const decoded = JSON.parse(
      Buffer.from(trimmed.slice(ZERO_G_MODEL_REF_PREFIX.length), "base64url").toString("utf8"),
    ) as unknown;
    if (!isRecord(decoded)) {
      return null;
    }
    const providerAddress =
      typeof decoded.p === "string" ? normalizeProviderAddress(decoded.p) : "";
    const model = typeof decoded.m === "string" ? decoded.m.trim() : "";
    const serviceType = typeof decoded.t === "string" ? decoded.t.trim() : undefined;
    if (!providerAddress || !model) {
      return null;
    }
    return {
      providerAddress,
      model,
      ...(serviceType ? { serviceType } : {}),
    };
  } catch {
    return null;
  }
}

export function formatZeroGModelLabel(params: {
  providerAddress: string;
  model: string;
  modelName?: string;
}): string {
  const providerAddress = normalizeProviderAddress(params.providerAddress);
  const model = params.model.trim();
  const modelName = params.modelName?.trim() || model;
  if (providerAddress.length <= 12) {
    return `${modelName} (${providerAddress})`;
  }
  return `${modelName} (${providerAddress.slice(0, 6)}...${providerAddress.slice(-4)})`;
}
