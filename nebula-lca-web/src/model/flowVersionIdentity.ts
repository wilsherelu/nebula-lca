import type { FlowPort } from "./node";

const LEGACY_NAMESPACE = "tiangong_open_source";
const LEGACY_VERSION = "TG-1.0";

const normalizedPart = (value: string | undefined): string => String(value ?? "").trim();

export const flowPortVersionIdentity = (port: FlowPort): string => {
  const namespace = normalizedPart(port.flowSourceNamespace);
  const version = normalizedPart(port.flowVersion);
  if (!namespace && !version) {
    return `${LEGACY_NAMESPACE}:${LEGACY_VERSION}`;
  }
  return `${namespace || "tiangong_open_data"}:${version}`;
};

export const flowPortIdentityKey = (port: FlowPort): string =>
  `${normalizedPart(port.flowUuid)}:${flowPortVersionIdentity(port)}`;

export const flowPortsAreVersionCompatible = (source: FlowPort, target: FlowPort): boolean =>
  normalizedPart(source.flowUuid) === normalizedPart(target.flowUuid)
  && flowPortVersionIdentity(source) === flowPortVersionIdentity(target);
