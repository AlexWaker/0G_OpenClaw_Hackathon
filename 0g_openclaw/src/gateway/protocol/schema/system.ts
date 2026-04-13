import { Type } from "@sinclair/typebox";

const EthereumAddressSchema = Type.String({ pattern: "^0x[a-fA-F0-9]{40}$" });
const EthereumWalletKindSchema = Type.Union([
  Type.Literal("mnemonic"),
  Type.Literal("private-key"),
]);
const EthereumWalletSourceSchema = Type.Union([Type.Literal("user"), Type.Literal("generated")]);
const DecimalAmountSchema = Type.String({ pattern: "^\\d+(?:\\.\\d+)?$" });
const NullableStringSchema = Type.Union([Type.String(), Type.Null()]);
const ZeroGNetworkSchema = Type.Union([
  Type.Literal("mainnet"),
  Type.Literal("testnet"),
  Type.Literal("custom"),
]);

export const EthereumWalletSummarySchema = Type.Object(
  {
    address: Type.Union([EthereumAddressSchema, Type.Null()]),
    kind: Type.Union([EthereumWalletKindSchema, Type.Null()]),
    source: Type.Union([EthereumWalletSourceSchema, Type.Null()]),
  },
  { additionalProperties: false },
);

export const ZeroGAccountGetParamsSchema = Type.Object(
  {
    model: Type.String({ minLength: 1 }),
  },
  { additionalProperties: false },
);

export const ZeroGFundMainParamsSchema = Type.Object(
  {
    model: Type.String({ minLength: 1 }),
    amount: Type.String({ minLength: 1 }),
  },
  { additionalProperties: false },
);

export const ZeroGFundProviderParamsSchema = Type.Object(
  {
    model: Type.String({ minLength: 1 }),
    amount: Type.String({ minLength: 1 }),
  },
  { additionalProperties: false },
);

export const ZeroGAcknowledgeProviderParamsSchema = Type.Object(
  {
    model: Type.String({ minLength: 1 }),
  },
  { additionalProperties: false },
);

export const ZeroGLedgerSummarySchema = Type.Object(
  {
    exists: Type.Boolean(),
    availableBalance: DecimalAmountSchema,
    totalBalance: DecimalAmountSchema,
  },
  { additionalProperties: false },
);

export const ZeroGProviderAccountSummarySchema = Type.Object(
  {
    exists: Type.Boolean(),
    availableBalance: DecimalAmountSchema,
    totalBalance: DecimalAmountSchema,
    pendingRefund: DecimalAmountSchema,
    userAcknowledged: Type.Boolean(),
  },
  { additionalProperties: false },
);

export const ZeroGServiceSummarySchema = Type.Object(
  {
    model: Type.String(),
    serviceType: NullableStringSchema,
    endpoint: NullableStringSchema,
    url: NullableStringSchema,
    verifiability: NullableStringSchema,
    inputPrice: NullableStringSchema,
    outputPrice: NullableStringSchema,
    teeSignerAddress: NullableStringSchema,
    teeSignerAcknowledged: Type.Union([Type.Boolean(), Type.Null()]),
  },
  { additionalProperties: false },
);

export const ZeroGAccountSummarySchema = Type.Object(
  {
    wallet: EthereumWalletSummarySchema,
    selectedModel: Type.String(),
    providerAddress: EthereumAddressSchema,
    serviceType: NullableStringSchema,
    network: ZeroGNetworkSchema,
    rpcUrl: Type.String(),
    ready: Type.Boolean(),
    issues: Type.Array(Type.String()),
    mainLedger: ZeroGLedgerSummarySchema,
    providerAccount: ZeroGProviderAccountSummarySchema,
    service: ZeroGServiceSummarySchema,
  },
  { additionalProperties: false },
);
