import type { Edge, Node } from "@xyflow/react";
import type { LcaEdgeData } from "../model/exchange";
import type { FlowPort, LcaNodeData } from "../model/node";

type NodeId = string;
type EdgeId = string;

export type GraphRelations = {
  nodeById: ReadonlyMap<NodeId, Node<LcaNodeData>>;
  edgeById: ReadonlyMap<EdgeId, Edge<LcaEdgeData>>;
  incomingEdgesByNodeId: ReadonlyMap<NodeId, ReadonlyArray<Edge<LcaEdgeData>>>;
  outgoingEdgesByNodeId: ReadonlyMap<NodeId, ReadonlyArray<Edge<LcaEdgeData>>>;
  adjacentNodeIdsByNodeId: ReadonlyMap<NodeId, ReadonlyArray<NodeId>>;
  sourcePortByEdgeId: ReadonlyMap<EdgeId, FlowPort | undefined>;
  targetPortByEdgeId: ReadonlyMap<EdgeId, FlowPort | undefined>;
  marketInputDisplayByNodeId: ReadonlyMap<NodeId, ReadonlyMap<string, string>>;
};

const EMPTY_EDGE_LIST: ReadonlyArray<Edge<LcaEdgeData>> = Object.freeze([]);
const EMPTY_NODE_ID_LIST: ReadonlyArray<NodeId> = Object.freeze([]);
const EMPTY_LABEL_MAP: ReadonlyMap<string, string> = new Map();

const parseHandlePortId = (
  handleId: string | null | undefined,
  prefixes: readonly string[],
): string | undefined => {
  const raw = String(handleId ?? "").trim();
  if (!raw) {
    return undefined;
  }
  const matched = prefixes.find((prefix) => raw.startsWith(prefix));
  return matched ? raw.slice(matched.length) : undefined;
};

const parseInputPortId = (handleId: string | null | undefined): string | undefined =>
  parseHandlePortId(handleId, ["in:", "inl:", "inr:"]);

const parseOutputPortId = (handleId: string | null | undefined): string | undefined =>
  parseHandlePortId(handleId, ["out:", "outl:", "outr:"]);

const stripPortSuffix = (value: string): string => {
  const trimmed = String(value ?? "").trim();
  const at = trimmed.indexOf("@");
  return at > 0 ? trimmed.slice(0, at).trim() : trimmed;
};

const extractPortSuffix = (value: string): string => {
  const trimmed = String(value ?? "").trim();
  const at = trimmed.indexOf("@");
  return at >= 0 ? trimmed.slice(at + 1).trim() : "";
};

const isMarketDisplayNode = (node: Node<LcaNodeData>): boolean =>
  node.data.nodeKind === "market_process" ||
  (node.data.nodeKind === "unit_process" && String(node.data.processUuid ?? "").startsWith("market_"));

const areReadonlyArraysEqualByRef = <T>(left: ReadonlyArray<T> | undefined, right: ReadonlyArray<T>): boolean => {
  if (!left || left.length !== right.length) {
    return false;
  }
  for (let index = 0; index < right.length; index += 1) {
    if (left[index] !== right[index]) {
      return false;
    }
  }
  return true;
};

const reuseReadonlyArray = <T>(
  previous: ReadonlyArray<T> | undefined,
  next: ReadonlyArray<T>,
  emptyValue: ReadonlyArray<T>,
): ReadonlyArray<T> => {
  if (next.length === 0) {
    return emptyValue;
  }
  return areReadonlyArraysEqualByRef(previous, next) ? previous! : next;
};

const areReadonlyMapsEqual = <K, V>(
  left: ReadonlyMap<K, V> | undefined,
  right: ReadonlyMap<K, V>,
): boolean => {
  if (!left || left.size !== right.size) {
    return false;
  }
  for (const [key, value] of right.entries()) {
    if (!Object.is(left.get(key), value)) {
      return false;
    }
  }
  return true;
};

const reuseReadonlyMap = <K, V>(
  previous: ReadonlyMap<K, V> | undefined,
  next: ReadonlyMap<K, V>,
): ReadonlyMap<K, V> => {
  if (next.size === 0) {
    return next;
  }
  return areReadonlyMapsEqual(previous, next) ? previous! : next;
};

const resolveSourcePortForEdge = (
  edge: Edge<LcaEdgeData>,
  nodeById: ReadonlyMap<NodeId, Node<LcaNodeData>>,
): FlowPort | undefined => {
  const sourceNode = nodeById.get(edge.source);
  if (!sourceNode) {
    return undefined;
  }
  const flowUuid = String(edge.data?.flowUuid ?? "").trim();
  if (!flowUuid) {
    return undefined;
  }
  const explicitPortId = parseOutputPortId(edge.sourceHandle ?? undefined);
  return (
    (explicitPortId
      ? sourceNode.data.outputs.find(
          (port) =>
            (port.id === explicitPortId || String(port.legacyPortId ?? "").trim() === explicitPortId) &&
            port.flowUuid === flowUuid,
        )
      : undefined) ?? sourceNode.data.outputs.find((port) => port.flowUuid === flowUuid)
  );
};

const resolveTargetPortForEdge = (
  edge: Edge<LcaEdgeData>,
  nodeById: ReadonlyMap<NodeId, Node<LcaNodeData>>,
): FlowPort | undefined => {
  const targetNode = nodeById.get(edge.target);
  if (!targetNode) {
    return undefined;
  }
  const flowUuid = String(edge.data?.flowUuid ?? "").trim();
  if (!flowUuid) {
    return undefined;
  }
  const explicitPortId = parseInputPortId(edge.targetHandle ?? undefined);
  return (
    (explicitPortId
      ? targetNode.data.inputs.find(
          (port) =>
            (port.id === explicitPortId || String(port.legacyPortId ?? "").trim() === explicitPortId) &&
            port.flowUuid === flowUuid,
        )
      : undefined) ?? targetNode.data.inputs.find((port) => port.flowUuid === flowUuid)
  );
};

const buildMarketInputDisplayMap = (params: {
  node: Node<LcaNodeData>;
  incomingEdges: ReadonlyArray<Edge<LcaEdgeData>>;
  nodeById: ReadonlyMap<NodeId, Node<LcaNodeData>>;
  sourcePortByEdgeId: ReadonlyMap<EdgeId, FlowPort | undefined>;
  previous?: ReadonlyMap<string, string>;
}): ReadonlyMap<string, string> => {
  if (!isMarketDisplayNode(params.node) || params.incomingEdges.length === 0) {
    return EMPTY_LABEL_MAP;
  }
  const next = new Map<string, string>();
  for (const edge of params.incomingEdges) {
    const targetPortId = parseInputPortId(edge.targetHandle ?? undefined);
    if (!targetPortId) {
      continue;
    }
    const sourceNode = params.nodeById.get(edge.source);
    const sourcePort = params.sourcePortByEdgeId.get(edge.id);
    if (!sourceNode || !sourcePort) {
      continue;
    }
    const flowLabel = stripPortSuffix(sourcePort.name) || String(edge.data?.flowName ?? "").trim();
    if (!flowLabel) {
      continue;
    }
    if (sourceNode.data.nodeKind === "pts_module") {
      const ptsLabel = String(sourceNode.data.name ?? "").trim();
      const nestedLabel =
        String(sourcePort.sourceProcessName ?? "").trim() || extractPortSuffix(sourcePort.name);
      const suffixParts = [ptsLabel, nestedLabel].filter(
        (part, index, items) => part && items.indexOf(part) === index,
      );
      next.set(targetPortId, suffixParts.length > 0 ? `${flowLabel}@${suffixParts.join("@")}` : flowLabel);
      continue;
    }
    if (sourceNode.data.nodeKind === "lci_dataset" || sourceNode.data.nodeKind === "unit_process") {
      const sourceLabel = String(sourceNode.data.name ?? "").trim();
      next.set(targetPortId, sourceLabel ? `${flowLabel}@${sourceLabel}` : flowLabel);
      continue;
    }
    next.set(targetPortId, flowLabel);
  }
  if (next.size === 0) {
    return EMPTY_LABEL_MAP;
  }
  return reuseReadonlyMap(params.previous, next);
};

const buildNodeEdgeMap = (): Map<NodeId, Edge<LcaEdgeData>[]> => new Map<NodeId, Edge<LcaEdgeData>[]>();

const pushNodeEdge = (map: Map<NodeId, Edge<LcaEdgeData>[]>, nodeId: NodeId, edge: Edge<LcaEdgeData>) => {
  const current = map.get(nodeId);
  if (current) {
    current.push(edge);
    return;
  }
  map.set(nodeId, [edge]);
};

export const createEmptyGraphRelations = (): GraphRelations => ({
  nodeById: new Map(),
  edgeById: new Map(),
  incomingEdgesByNodeId: new Map(),
  outgoingEdgesByNodeId: new Map(),
  adjacentNodeIdsByNodeId: new Map(),
  sourcePortByEdgeId: new Map(),
  targetPortByEdgeId: new Map(),
  marketInputDisplayByNodeId: new Map(),
});

export const buildGraphRelations = (
  nodes: ReadonlyArray<Node<LcaNodeData>>,
  edges: ReadonlyArray<Edge<LcaEdgeData>>,
  previous?: GraphRelations,
): GraphRelations => {
  const nextNodeById = new Map<NodeId, Node<LcaNodeData>>(nodes.map((node) => [node.id, node]));
  const rawIncomingEdgesByNodeId = buildNodeEdgeMap();
  const rawOutgoingEdgesByNodeId = buildNodeEdgeMap();
  const nextEdgeById = new Map<EdgeId, Edge<LcaEdgeData>>();
  const rawSourcePortByEdgeId = new Map<EdgeId, FlowPort | undefined>();
  const rawTargetPortByEdgeId = new Map<EdgeId, FlowPort | undefined>();

  for (const edge of edges) {
    nextEdgeById.set(edge.id, edge);
    pushNodeEdge(rawOutgoingEdgesByNodeId, edge.source, edge);
    pushNodeEdge(rawIncomingEdgesByNodeId, edge.target, edge);
    rawSourcePortByEdgeId.set(edge.id, resolveSourcePortForEdge(edge, nextNodeById));
    rawTargetPortByEdgeId.set(edge.id, resolveTargetPortForEdge(edge, nextNodeById));
  }

  const nextIncomingEdgesByNodeId = new Map<NodeId, ReadonlyArray<Edge<LcaEdgeData>>>();
  const nextOutgoingEdgesByNodeId = new Map<NodeId, ReadonlyArray<Edge<LcaEdgeData>>>();
  const nextAdjacentNodeIdsByNodeId = new Map<NodeId, ReadonlyArray<NodeId>>();
  const nextMarketInputDisplayByNodeId = new Map<NodeId, ReadonlyMap<string, string>>();

  for (const node of nodes) {
    const incomingEdges = reuseReadonlyArray(
      previous?.incomingEdgesByNodeId.get(node.id),
      rawIncomingEdgesByNodeId.get(node.id) ?? EMPTY_EDGE_LIST,
      EMPTY_EDGE_LIST,
    );
    const outgoingEdges = reuseReadonlyArray(
      previous?.outgoingEdgesByNodeId.get(node.id),
      rawOutgoingEdgesByNodeId.get(node.id) ?? EMPTY_EDGE_LIST,
      EMPTY_EDGE_LIST,
    );

    if (incomingEdges.length > 0) {
      nextIncomingEdgesByNodeId.set(node.id, incomingEdges);
    }
    if (outgoingEdges.length > 0) {
      nextOutgoingEdgesByNodeId.set(node.id, outgoingEdges);
    }

    const adjacencyNext: NodeId[] = [];
    const seenAdjacent = new Set<NodeId>();
    for (const edge of outgoingEdges) {
      if (!seenAdjacent.has(edge.target)) {
        seenAdjacent.add(edge.target);
        adjacencyNext.push(edge.target);
      }
    }
    for (const edge of incomingEdges) {
      if (!seenAdjacent.has(edge.source)) {
        seenAdjacent.add(edge.source);
        adjacencyNext.push(edge.source);
      }
    }
    const adjacentNodeIds = reuseReadonlyArray(
      previous?.adjacentNodeIdsByNodeId.get(node.id),
      adjacencyNext.length > 0 ? adjacencyNext : EMPTY_NODE_ID_LIST,
      EMPTY_NODE_ID_LIST,
    );
    if (adjacentNodeIds.length > 0) {
      nextAdjacentNodeIdsByNodeId.set(node.id, adjacentNodeIds);
    }

    const marketInputDisplay = buildMarketInputDisplayMap({
      node,
      incomingEdges,
      nodeById: nextNodeById,
      sourcePortByEdgeId: rawSourcePortByEdgeId,
      previous: previous?.marketInputDisplayByNodeId.get(node.id),
    });
    if (marketInputDisplay.size > 0) {
      nextMarketInputDisplayByNodeId.set(node.id, marketInputDisplay);
    }
  }

  const nodeById = reuseReadonlyMap(previous?.nodeById, nextNodeById);
  const edgeById = reuseReadonlyMap(previous?.edgeById, nextEdgeById);
  const sourcePortByEdgeId = reuseReadonlyMap(previous?.sourcePortByEdgeId, rawSourcePortByEdgeId);
  const targetPortByEdgeId = reuseReadonlyMap(previous?.targetPortByEdgeId, rawTargetPortByEdgeId);

  return {
    nodeById,
    edgeById,
    incomingEdgesByNodeId: nextIncomingEdgesByNodeId,
    outgoingEdgesByNodeId: nextOutgoingEdgesByNodeId,
    adjacentNodeIdsByNodeId: nextAdjacentNodeIdsByNodeId,
    sourcePortByEdgeId,
    targetPortByEdgeId,
    marketInputDisplayByNodeId: nextMarketInputDisplayByNodeId,
  };
};

export const getAffectedEdgeIdsForNodeIds = (
  relations: GraphRelations,
  nodeIds: ReadonlySet<string>,
): ReadonlySet<string> => {
  const affected = new Set<string>();
  nodeIds.forEach((nodeId) => {
    const incoming = relations.incomingEdgesByNodeId.get(nodeId) ?? EMPTY_EDGE_LIST;
    const outgoing = relations.outgoingEdgesByNodeId.get(nodeId) ?? EMPTY_EDGE_LIST;
    incoming.forEach((edge) => affected.add(edge.id));
    outgoing.forEach((edge) => affected.add(edge.id));
  });
  return affected;
};
