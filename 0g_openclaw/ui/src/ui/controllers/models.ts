import type { GatewayBrowserClient } from "../gateway.ts";
import type { ModelCatalogEntry } from "../types.ts";

/**
 * Fetch the model catalog from the gateway.
 *
 * Accepts a {@link GatewayBrowserClient} (matching the existing ui/ controller
 * convention).  Returns an array of {@link ModelCatalogEntry}; on failure the
 * caller receives an empty array rather than throwing.
 */
export async function loadModels(client: GatewayBrowserClient): Promise<ModelCatalogEntry[]> {
  try {
    // const result = await client.request<{ models: ModelCatalogEntry[] }>("models.list", {});
    const result = await client.request<{ models: ModelCatalogEntry[] }>("models.list", {
      includeUnallowlisted: true,
    });
    // return result?.models ?? [];
    return (result?.models ?? []).filter((entry) => entry.provider?.trim().toLowerCase() === "0g");
  } catch {
    return [];
  }
}
