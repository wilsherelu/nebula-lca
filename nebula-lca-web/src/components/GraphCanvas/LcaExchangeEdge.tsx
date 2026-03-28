import { useMemo } from "react";
import { BaseEdge, type EdgeProps } from "@xyflow/react";
import type { LcaEdgeData } from "../../model/exchange";
import { useLcaGraphStore } from "../../store/lcaGraphStore";

type Point = { x: number; y: number };

type HorizontalSide = "left" | "right";

const HORIZONTAL_OFFSET = 20;
const CORNER_RADIUS = 10;
const HEAVY_EDGE_ANIMATION_THRESHOLD = 160;
const LIGHT_EDGE_SWAY = 18;

const hashEdgeId = (value: string): number => {
  let hash = 0;
  for (let index = 0; index < value.length; index += 1) {
    hash = (hash * 31 + value.charCodeAt(index)) | 0;
  }
  return Math.abs(hash);
};

const getStableEdgeSway = (edgeId: string): number => {
  const bucket = hashEdgeId(edgeId) % 5;
  return (bucket - 2) * LIGHT_EDGE_SWAY;
};

const resolveSourceHorizontalSide = (
  sourceHandle: string | undefined,
  fallbackPosition: EdgeProps["sourcePosition"],
): HorizontalSide => {
  if (sourceHandle?.startsWith("outl:")) {
    return "left";
  }
  if (sourceHandle?.startsWith("out:") || sourceHandle?.startsWith("outr:")) {
    return "right";
  }
  return fallbackPosition === "left" ? "left" : "right";
};

const resolveTargetHorizontalSide = (
  targetHandle: string | undefined,
  fallbackPosition: EdgeProps["targetPosition"],
): HorizontalSide => {
  if (targetHandle?.startsWith("inr:")) {
    return "right";
  }
  if (targetHandle?.startsWith("in:") || targetHandle?.startsWith("inl:")) {
    return "left";
  }
  return fallbackPosition === "right" ? "right" : "left";
};

const buildRoundedOrthogonalPath = (points: Point[], radius = CORNER_RADIUS): string => {
  if (points.length === 0) {
    return "";
  }
  if (points.length === 1) {
    return `M ${points[0].x} ${points[0].y}`;
  }
  const parts: string[] = [`M ${points[0].x} ${points[0].y}`];
  for (let i = 1; i < points.length; i += 1) {
    const prev = points[i - 1];
    const current = points[i];
    const next = points[i + 1];
    if (!next) {
      parts.push(`L ${current.x} ${current.y}`);
      continue;
    }
    const dx1 = current.x - prev.x;
    const dy1 = current.y - prev.y;
    const dx2 = next.x - current.x;
    const dy2 = next.y - current.y;
    const len1 = Math.abs(dx1) + Math.abs(dy1);
    const len2 = Math.abs(dx2) + Math.abs(dy2);
    const turnRadius = Math.min(radius, len1 / 2, len2 / 2);
    if (turnRadius < 0.5) {
      parts.push(`L ${current.x} ${current.y}`);
      continue;
    }
    const before: Point = {
      x: current.x - Math.sign(dx1 || 0) * turnRadius,
      y: current.y - Math.sign(dy1 || 0) * turnRadius,
    };
    const after: Point = {
      x: current.x + Math.sign(dx2 || 0) * turnRadius,
      y: current.y + Math.sign(dy2 || 0) * turnRadius,
    };
    parts.push(`L ${before.x} ${before.y}`);
    parts.push(`Q ${current.x} ${current.y} ${after.x} ${after.y}`);
  }
  return parts.join(" ");
};

const buildClassicCurvePath = (
  sourceX: number,
  sourceY: number,
  targetX: number,
  targetY: number,
  sourceSide: HorizontalSide,
  targetSide: HorizontalSide,
  horizontalOffset = HORIZONTAL_OFFSET,
): string => {
  const sourceStubX = sourceX + (sourceSide === "right" ? horizontalOffset : -horizontalOffset);
  const targetStubX = targetX + (targetSide === "left" ? -horizontalOffset : horizontalOffset);
  const span = Math.abs(targetStubX - sourceStubX);
  const baseHandle = Math.max(24, Math.min(84, span * 0.45));
  const sourceDirection = sourceSide === "right" ? 1 : -1;
  const targetDirection = targetSide === "right" ? 1 : -1;
  const c1x = sourceStubX + sourceDirection * baseHandle;
  const c2x = targetStubX + targetDirection * baseHandle;
  return [
    `M ${sourceX} ${sourceY}`,
    `L ${sourceStubX} ${sourceY}`,
    `C ${c1x} ${sourceY} ${c2x} ${targetY} ${targetStubX} ${targetY}`,
    `L ${targetX} ${targetY}`,
  ].join(" ");
};

export function LcaExchangeEdge({
  id,
  data,
  selected,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
}: EdgeProps) {
  const flowAnimationEnabled = useLcaGraphStore((state) => state.flowAnimationEnabled);
  const flowAnimationEpoch = useLcaGraphStore((state) => state.flowAnimationEpoch);
  const edgeRoutingStyle = useLcaGraphStore((state) => state.edgeRoutingStyle);
  const totalEdgeCount = useLcaGraphStore((state) => state.graphRelations.edgeById.size);
  const currentEdge = useLcaGraphStore((state) => state.graphRelations.edgeById.get(id));
  const sourceSide = resolveSourceHorizontalSide(currentEdge?.sourceHandle ?? undefined, sourcePosition);
  const targetSide = resolveTargetHorizontalSide(currentEdge?.targetHandle ?? undefined, targetPosition);
  const edgeData = (data ?? {}) as Partial<LcaEdgeData>;
  const quantityMode = edgeData.quantityMode ?? "single";
  const strokeColor = quantityMode === "single" ? "#2ea44f" : "#8a94a6";
  const pulseColor = quantityMode === "single" ? "#22c55e" : "#94a3b8";
  const stableEdgeSway = useMemo(() => getStableEdgeSway(id), [id]);
  const orthogonalPath = useMemo(() => {
    const sourceStubX = sourceX + (sourceSide === "right" ? HORIZONTAL_OFFSET : -HORIZONTAL_OFFSET);
    const targetStubX = targetX + (targetSide === "left" ? -HORIZONTAL_OFFSET : HORIZONTAL_OFFSET);
    const baseMidX = (sourceStubX + targetStubX) / 2;
    const span = Math.abs(targetStubX - sourceStubX);
    const swayLimit = Math.max(12, Math.min(36, span * 0.16));
    const sway = Math.max(-swayLimit, Math.min(swayLimit, stableEdgeSway));
    const midX = baseMidX + sway;
    return buildRoundedOrthogonalPath(
      [
        { x: sourceX, y: sourceY },
        { x: sourceStubX, y: sourceY },
        { x: midX, y: sourceY },
        { x: midX, y: targetY },
        { x: targetStubX, y: targetY },
        { x: targetX, y: targetY },
      ],
      CORNER_RADIUS,
    );
  }, [sourceSide, sourceX, sourceY, stableEdgeSway, targetSide, targetX, targetY]);
  const classicCurvePath = buildClassicCurvePath(
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourceSide,
    targetSide,
    HORIZONTAL_OFFSET,
  );
  const path = edgeRoutingStyle === "classic_curve" ? classicCurvePath : orthogonalPath;

  const lineStyle =
    quantityMode === "single"
      ? { stroke: selected ? "#1f8f40" : strokeColor, strokeWidth: selected ? 3.2 : 1.8 }
      : {
          stroke: selected ? "#596277" : strokeColor,
          strokeWidth: selected ? 3.2 : 1.8,
          strokeDasharray: "7 5",
        };

  const motionPathId = useMemo(() => `flow-motion-${id.replace(/[^a-zA-Z0-9_-]/g, "_")}`, [id]);
  const animationDurationSeconds = 13;
  const beginTime = useMemo(() => {
    const phaseSeconds = ((Date.now() - flowAnimationEpoch) % (animationDurationSeconds * 1000)) / 1000;
    return `${-Math.max(0, phaseSeconds)}s`;
  }, [flowAnimationEpoch, id]);
  const heavyAnimationMode = totalEdgeCount >= HEAVY_EDGE_ANIMATION_THRESHOLD;
  const effectiveFlowAnimationEnabled = flowAnimationEnabled && !heavyAnimationMode;
  return (
    <>
      <BaseEdge id={id} path={path} style={lineStyle} />
      {effectiveFlowAnimationEnabled && (
        <g className="edge-flow-animation" style={{ pointerEvents: "none" }}>
          <path id={motionPathId} d={path} fill="none" stroke="none" />
          <circle r={selected ? 4.2 : 3.6} fill={pulseColor}>
            <animate
              attributeName="opacity"
              values="0;1;1;0;0"
              keyTimes="0;0.02;0.23;0.25;1"
              dur="13s"
              begin={beginTime}
              repeatCount="indefinite"
            />
            <animateMotion
              dur="13s"
              begin={beginTime}
              repeatCount="indefinite"
              keyTimes="0;0.230769;1"
              keyPoints="0;1;1"
              calcMode="linear"
            >
              <mpath href={`#${motionPathId}`} />
            </animateMotion>
          </circle>
        </g>
      )}
    </>
  );
}
