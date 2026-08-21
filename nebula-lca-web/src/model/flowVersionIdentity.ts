import type { FlowPort } from "./node";

const LEGACY_NAMESPACE = "tiangong_open_source";
const LEGACY_VERSION = "TG-1.0";

const normalizedPart = (value: string | undefined): string => String(value ?? "").trim();

const normalizedSemanticPart = (value: string | undefined): string =>
  normalizedPart(value).replace(/\s+/g, " ").toLocaleLowerCase();

const optionalIdentityMatches = (source: string | undefined, target: string | undefined): boolean => {
  const sourceValue = normalizedPart(source);
  const targetValue = normalizedPart(target);
  return !sourceValue && !targetValue ? true : Boolean(sourceValue && sourceValue === targetValue);
};

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

/**
 * A manual foreground connection may preserve two source-system Flow UUIDs when
 * both ports describe the same immutable-version exchange semantics. This is
 * deliberately stricter than display-name matching and never crosses versions.
 */
export const flowPortsAreForegroundAliases = (source: FlowPort, target: FlowPort): boolean => {
  if (flowPortsAreVersionCompatible(source, target)) {
    return true;
  }
  if (
    !normalizedPart(source.flowUuid)
    || !normalizedPart(target.flowUuid)
    || flowPortVersionIdentity(source) !== flowPortVersionIdentity(target)
    || source.type === "biosphere"
    || target.type === "biosphere"
    || source.type !== target.type
    || normalizedSemanticPart(source.name) !== normalizedSemanticPart(target.name)
    || normalizedSemanticPart(source.unit) !== normalizedSemanticPart(target.unit)
    || !normalizedSemanticPart(source.unitGroup)
    || normalizedSemanticPart(source.unitGroup) !== normalizedSemanticPart(target.unitGroup)
    || !optionalIdentityMatches(source.flowPropertyUuid, target.flowPropertyUuid)
    || !optionalIdentityMatches(source.flowPropertyVersion, target.flowPropertyVersion)
    || !optionalIdentityMatches(source.unitGroupUuid, target.unitGroupUuid)
    || !optionalIdentityMatches(source.unitGroupVersion, target.unitGroupVersion)
  ) {
    return false;
  }
  const sourceNameEn = normalizedSemanticPart(source.flowNameEn);
  const targetNameEn = normalizedSemanticPart(target.flowNameEn);
  return !sourceNameEn && !targetNameEn ? true : Boolean(sourceNameEn && sourceNameEn === targetNameEn);
};
