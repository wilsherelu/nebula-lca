import { getApiBase } from "../apiBase";
import type { FlowPort, IntermediateFlowLink } from "../model/node";

const API_BASE = getApiBase();

export type RawResolution = {
  source_flow_uuid: string;
  target_flow_uuid: string;
  amount_factor: number;
  source_unit: string;
  target_unit: string;
  mapping_level: "L1" | "L2" | "L3";
  mapping_reason: string;
  rule_id: string;
  rule_origin: "builtin" | "user";
  package_id?: string;
  package_version?: string;
  package_hash?: string;
  application_mode?: "strict_identity" | "auto_compatible";
  warnings?: string[];
  status?: "auto" | "user_confirmed";
  target_flow_name?: string;
  target_flow_name_en?: string;
};

export type LinkCandidate = {
  target_flow_uuid: string;
  target_flow_name: string;
  score: number;
  applicable: false;
};

export type ResolveItem = {
  port_id?: string;
  status: "explicit" | "L1" | "L2" | "L3" | "unmatched" | "blocked" | "skipped";
  reason?: string;
  resolution?: RawResolution | IntermediateFlowLink | null;
  l2_candidates?: LinkCandidate[];
};

export type ProviderCandidate = {
  process_uuid: string;
  process_name: string;
  process_name_en?: string;
  source?: string;
  location?: string;
  reference_product_flow_uuid?: string;
  reference_product_name?: string;
  reference_product_unit?: string;
  has_lci_vector: boolean;
  vector_nnz: number;
};

export type EcoIntermediateFlow = {
  flow_uuid: string;
  flow_name: string;
  flow_name_en?: string;
  default_unit: string;
  unit_group: string;
};

export const toIntermediateFlowLink = (raw: RawResolution): IntermediateFlowLink => ({
  sourceFlowUuid: raw.source_flow_uuid,
  targetFlowUuid: raw.target_flow_uuid,
  amountFactor: raw.amount_factor,
  sourceUnit: raw.source_unit,
  targetUnit: raw.target_unit,
  mappingLevel: raw.mapping_level,
  mappingReason: raw.mapping_reason,
  ruleId: raw.rule_id,
  ruleOrigin: raw.rule_origin,
  status: raw.status ?? (raw.mapping_level === "L1" ? "auto" : "user_confirmed"),
  packageId: raw.package_id,
  packageVersion: raw.package_version,
  packageHash: raw.package_hash,
  applicationMode: raw.application_mode,
  warnings: raw.warnings ?? [],
});

export async function confirmL2IntermediateFlowLink(
  sourceFlowUuid: string,
  ruleId: string,
): Promise<IntermediateFlowLink> {
  const response = await fetch(`${API_BASE}/intermediate-flow-links/confirm-l2`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ source_flow_uuid: sourceFlowUuid, rule_id: ruleId }),
  });
  if (!response.ok) throw new Error(`L2 confirmation failed (${response.status})`);
  return toIntermediateFlowLink(await response.json() as RawResolution);
}

export async function resolveIntermediateFlowPorts(ports: FlowPort[]): Promise<{
  counts: Record<string, number>;
  items: ResolveItem[];
}> {
  const response = await fetch(`${API_BASE}/intermediate-flow-links/resolve-batch`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      items: ports.map((port) => ({
        port_id: port.id,
        flow_uuid: port.flowUuid,
        direction: port.direction,
        exchange_type: port.type === "biosphere" ? "biosphere" : "technosphere",
        unit: port.unit,
        intermediate_flow_link: port.intermediateFlowLink,
      })),
    }),
  });
  if (!response.ok) {
    throw new Error(`Intermediate flow resolution failed (${response.status})`);
  }
  return response.json();
}

export async function fetchIntermediateFlowProviders(
  targetFlowUuid: string,
): Promise<ProviderCandidate[]> {
  const params = new URLSearchParams({ target_flow_uuid: targetFlowUuid });
  const response = await fetch(
    `${API_BASE}/intermediate-flow-links/providers?${params.toString()}`,
  );
  if (!response.ok) {
    throw new Error(`Provider lookup failed (${response.status})`);
  }
  const payload = (await response.json()) as { providers?: ProviderCandidate[] };
  return payload.providers ?? [];
}

export async function searchEcoIntermediateFlows(query: string): Promise<EcoIntermediateFlow[]> {
  const params = new URLSearchParams({
    search: query,
    page: "1",
    page_size: "20",
    type: "intermediate_flow",
    source_space: "ecoinvent",
  });
  const response = await fetch(`${API_BASE}/flows?${params.toString()}`);
  if (!response.ok) throw new Error(`Flow search failed (${response.status})`);
  const payload = (await response.json()) as { items?: EcoIntermediateFlow[] };
  return payload.items ?? [];
}

export async function createUserProxyRule(
  sourceFlowUuid: string,
  targetFlowUuid: string,
  mappingReason: string,
): Promise<IntermediateFlowLink> {
  const response = await fetch(`${API_BASE}/intermediate-flow-links/user-rules`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      source_flow_uuid: sourceFlowUuid,
      target_flow_uuid: targetFlowUuid,
      mapping_reason: mappingReason,
    }),
  });
  if (!response.ok) throw new Error(`User proxy creation failed (${response.status})`);
  const raw = (await response.json()) as RawResolution & { id: string };
  return toIntermediateFlowLink({
    ...raw,
    rule_id: raw.id,
    rule_origin: "user",
    mapping_level: "L3",
  });
}
