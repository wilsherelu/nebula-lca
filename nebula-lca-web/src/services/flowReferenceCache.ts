export type FlowReferenceRecord = {
  flow_uuid?: string;
  flow_name?: string | null;
  flow_name_en?: string | null;
  flow_type?: string | null;
  unit_group?: string | null;
  default_unit?: string | null;
  [key: string]: unknown;
};

const flowReferenceRequests = new Map<string, Promise<FlowReferenceRecord | null>>();

const flowReferenceUrl = (apiBase: string, flowUuid: string): string =>
  `${apiBase}/reference/flows/${encodeURIComponent(flowUuid)}`;

export function fetchFlowReferenceCached(
  apiBase: string,
  flowUuid: string,
  fetcher: typeof fetch = fetch,
): Promise<FlowReferenceRecord | null> {
  const normalizedUuid = String(flowUuid ?? "").trim();
  if (!normalizedUuid) {
    return Promise.resolve(null);
  }
  const url = flowReferenceUrl(apiBase, normalizedUuid);
  const cached = flowReferenceRequests.get(url);
  if (cached) {
    return cached;
  }
  const request = fetcher(url)
    .then(async (response) => {
      if (!response.ok) {
        return null;
      }
      const row = (await response.json()) as FlowReferenceRecord;
      return String(row.flow_uuid ?? "").trim() ? row : null;
    })
    .then((row) => {
      if (!row) {
        flowReferenceRequests.delete(url);
      }
      return row;
    })
    .catch((error) => {
      flowReferenceRequests.delete(url);
      throw error;
    });
  flowReferenceRequests.set(url, request);
  return request;
}

export function primeFlowReferenceCache(apiBase: string, flowUuid: string, row: FlowReferenceRecord): void {
  const normalizedUuid = String(flowUuid ?? "").trim();
  if (!normalizedUuid) {
    return;
  }
  flowReferenceRequests.set(flowReferenceUrl(apiBase, normalizedUuid), Promise.resolve(row));
}

export function clearFlowReferenceCacheForTests(): void {
  flowReferenceRequests.clear();
}
