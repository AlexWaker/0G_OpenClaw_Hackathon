import { describe, expect, it, vi } from "vitest";
import { loadModels } from "./models.ts";

describe("loadModels", () => {
  it("returns only 0g models for the chat picker", async () => {
    const client = {
      request: vi.fn(async () => ({
        models: [
          { id: "gpt-5-mini", name: "GPT-5 Mini", provider: "openai" },
          { id: "svc_demo", name: "DeepSeek R1 (0x1234...abcd)", provider: "0g" },
        ],
      })),
    };

    await expect(loadModels(client as never)).resolves.toEqual([
      { id: "svc_demo", name: "DeepSeek R1 (0x1234...abcd)", provider: "0g" },
    ]);
    expect(client.request).toHaveBeenCalledWith("models.list", {
      includeUnallowlisted: true,
    });
  });

  it("returns an empty list when the gateway request fails", async () => {
    const client = {
      request: vi.fn(async () => {
        throw new Error("boom");
      }),
    };

    await expect(loadModels(client as never)).resolves.toEqual([]);
  });
});
