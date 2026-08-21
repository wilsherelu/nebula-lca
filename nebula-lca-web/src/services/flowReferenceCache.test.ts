import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  clearFlowReferenceCacheForTests,
  fetchFlowReferenceCached,
  primeFlowReferenceCache,
} from "./flowReferenceCache";

describe("fetchFlowReferenceCached", () => {
  beforeEach(() => clearFlowReferenceCacheForTests());

  it("deduplicates concurrent and later requests for the same Flow", async () => {
    const fetcher = vi.fn(async () => new Response(JSON.stringify({
      flow_uuid: "flow-1",
      flow_name: "交流电",
    }), { status: 200 })) as unknown as typeof fetch;

    const [first, second] = await Promise.all([
      fetchFlowReferenceCached("/api", "flow-1", fetcher),
      fetchFlowReferenceCached("/api", "flow-1", fetcher),
    ]);
    const third = await fetchFlowReferenceCached("/api", "flow-1", fetcher);

    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(first).toEqual(second);
    expect(third?.flow_name).toBe("交流电");
  });

  it("allows a retry after a network failure", async () => {
    const fetcher = vi.fn()
      .mockRejectedValueOnce(new Error("offline"))
      .mockResolvedValueOnce(new Response(JSON.stringify({ flow_uuid: "flow-1" }), { status: 200 })) as unknown as typeof fetch;

    await expect(fetchFlowReferenceCached("/api", "flow-1", fetcher)).rejects.toThrow("offline");
    await expect(fetchFlowReferenceCached("/api", "flow-1", fetcher)).resolves.toMatchObject({ flow_uuid: "flow-1" });
    expect(fetcher).toHaveBeenCalledTimes(2);
  });

  it("replaces the cached row after an explicit Flow refresh", async () => {
    const fetcher = vi.fn(async () => new Response(JSON.stringify({
      flow_uuid: "flow-1",
      flow_name: "旧名称",
    }), { status: 200 })) as unknown as typeof fetch;
    await fetchFlowReferenceCached("/api", "flow-1", fetcher);

    primeFlowReferenceCache("/api", "flow-1", { flow_uuid: "flow-1", flow_name: "新名称" });

    await expect(fetchFlowReferenceCached("/api", "flow-1", fetcher)).resolves.toMatchObject({ flow_name: "新名称" });
    expect(fetcher).toHaveBeenCalledTimes(1);
  });
});
