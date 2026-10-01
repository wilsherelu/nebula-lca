import type { FlowPort } from "./node";

type PinnedIdentity = Pick<FlowPort,
  "flowSourceNamespace" | "flowVersion" | "flowPropertyUuid" |
  "flowPropertyVersion" | "unitGroupUuid" | "unitGroupVersion">;

export function uniquePinnedFlowIdentity(flowUuid: string, ports: FlowPort[]): PinnedIdentity | null {
  const identities = ports
    .filter(port => port.flowUuid === flowUuid && port.flowSourceNamespace && port.flowVersion &&
      port.flowPropertyUuid && port.flowPropertyVersion && port.unitGroupUuid && port.unitGroupVersion)
    .map(port => ({
      flowSourceNamespace: port.flowSourceNamespace,
      flowVersion: port.flowVersion,
      flowPropertyUuid: port.flowPropertyUuid,
      flowPropertyVersion: port.flowPropertyVersion,
      unitGroupUuid: port.unitGroupUuid,
      unitGroupVersion: port.unitGroupVersion,
    }));
  const distinct = new Map(identities.map(identity => [JSON.stringify(identity), identity]));
  return distinct.size === 1 ? [...distinct.values()][0] : null;
}
