import { useMemo } from "react";
import { BaseEdge, type EdgeProps } from "@xyflow/react";
import type { LcaEdgeData } from "../../model/exchange";
import { useLcaGraphStore } from "../../store/lcaGraphStore";

type Point = { x: number; y: number };

type Rect = {
  left: number;
  right: number;
  top: number;
  bottom: number;
};

type HorizontalSide = "left" | "right";

const DEFAULT_NODE_WIDTH = 220;
const DEFAULT_NODE_HEIGHT = 160;
const HORIZONTAL_OFFSET = 20;
const CORNER_RADIUS = 10;
const OBSTACLE_MARGIN = 20;

const normalizeRect = (left: number, right: number, top: number, bottom: number): Rect => ({
  left: Math.min(left, right),
  right: Math.max(left, right),
  top: Math.min(top, bottom),
  bottom: Math.max(top, bottom),
});

const buildObstacleRect = (
  x: number,
  y: number,
  width: number,
  height: number,
  margin = OBSTACLE_MARGIN,
): Rect =>
  normalizeRect(x - margin, x + width + margin, y - margin, y + height + margin);

const intersectsVerticalSegment = (x: number, y1: number, y2: number, rect: Rect): boolean => {
  const top = Math.min(y1, y2);
  const bottom = Math.max(y1, y2);
  return x >= rect.left && x <= rect.right && bottom >= rect.top && top <= rect.bottom;
};

const chooseRoutedMidX = (baseMidX: number, y1: number, y2: number, obstacles: Rect[]): number => {
  if (obstacles.length === 0) {
    return baseMidX;
  }
  const candidates = new Set<number>([baseMidX]);
  obstacles.forEach((rect) => {
    candidates.add(rect.left - OBSTACLE_MARGIN);
    candidates.add(rect.right + OBSTACLE_MARGIN);
  });
  const ordered = Array.from(candidates).sort((a, b) => Math.abs(a - baseMidX) - Math.abs(b - baseMidX));
  return (
    ordered.find((candidate) => obstacles.every((rect) => !intersectsVerticalSegment(candidate, y1, y2, rect))) ?? baseMidX
  );
};

const getNodeRect = (node: { position: { x: number; y: number }; width?: number; height?: number } | undefined): Rect | undefined => {
  if (!node) {
    return undefined;
  }
  return normalizeRect(
    node.position.x,
    node.position.x + (typeof node.width === "number" && Number.isFinite(node.width) ? node.width : DEFAULT_NODE_WIDTH),
    node.position.y,
    node.position.y + (typeof node.height === "number" && Number.isFinite(node.height) ? node.height : DEFAULT_NODE_HEIGHT),
  );
};

const isSameColumnLayout = (
  sourceRect: Rect | undefined,
  targetRect: Rect | undefined,
): boolean => {
  if (!sourceRect || !targetRect) {
    return false;
  }
  const xOverlap = Math.min(sourceRect.right, targetRect.right) >= Math.max(sourceRect.left, targetRect.left) - 12;
  const centerGap = Math.abs((sourceRect.left + sourceRect.right) / 2 - (targetRect.left + targetRect.right) / 2);
  return xOverlap || centerGap <= 90;
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
  sourceRect?: Rect,
  targetRect?: Rect,
  horizontalOffset = HORIZONTAL_OFFSET,
): string => {
  const sourceStubX = sourceX + (sourceSide === "right" ? horizontalOffset : -horizontalOffset);
  const targetStubX = targetX + (targetSide === "left" ? -horizontalOffset : horizontalOffset);
  const sameColumn = isSameColumnLayout(sourceRect, targetRect);
  const span = Math.abs(targetStubX - sourceStubX);
  const baseHandle = Math.max(24, Math.min(84, span * 0.45));
  const columnBoost = sameColumn ? Math.max(18, Math.min(64, Math.abs(targetY - sourceY) * 0.22)) : 0;
  const sourceDirection = sourceSide === "right" ? 1 : -1;
  const targetDirection = targetSide === "right" ? 1 : -1;
  const c1x = sourceStubX + sourceDirection * (baseHandle + columnBoost);
  const c2x = targetStubX + targetDirection * (baseHandle + columnBoost);
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
  source,
  target,
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
  const nodes = useLcaGraphStore((state) => state.nodes);
  const edges = useLcaGraphStore((state) => state.edges);
  const currentEdge = useMemo(() => edges.find((edge) => edge.id === id), [edges, id]);
  const sourceNodeId = typeof source === "string" ? source : "";
  const targetNodeId = typeof target === "string" ? target : "";
  const sourceNode = nodes.find((node) => node.id === sourceNodeId);
  const targetNode = nodes.find((node) => node.id === targetNodeId);
  const sourceRect = getNodeRect(sourceNode);
  const targetRect = getNodeRect(targetNode);
  const sourceSide = resolveSourceHorizontalSide(currentEdge?.sourceHandle ?? undefined, sourcePosition);
  const targetSide = resolveTargetHorizontalSide(currentEdge?.targetHandle ?? undefined, targetPosition);
  const edgeData = (data ?? {}) as Partial<LcaEdgeData>;
  const quantityMode = edgeData.quantityMode ?? "single";
  const strokeColor = quantityMode === "single" ? "#2ea44f" : "#8a94a6";
  const pulseColor = quantityMode === "single" ? "#22c55e" : "#94a3b8";
  const orthogonalPath = useMemo(() => {
    const sourceStubX = sourceX + (sourceSide === "right" ? HORIZONTAL_OFFSET : -HORIZONTAL_OFFSET);
    const targetStubX = targetX + (targetSide === "left" ? -HORIZONTAL_OFFSET : HORIZONTAL_OFFSET);
    const isLeftToRight = sourceStubX <= targetStubX;
    const currentSourceHandle = currentEdge?.sourceHandle;
    const siblingEdges = edges
      .filter((edge) => edge.source === sourceNodeId && edge.sourceHandle === currentSourceHandle && edge.id !== id)
      .sort((a, b) => {
        const aTarget = nodes.find((node) => node.id === a.target);
        const bTarget = nodes.find((node) => node.id === b.target);
        const ay = aTarget?.position.y ?? 0;
        const by = bTarget?.position.y ?? 0;
        if (Math.abs(ay - by) > 0.1) {
          return ay - by;
        }
        return a.id.localeCompare(b.id);
      });
    const currentTargetY = nodes.find((node) => node.id === targetNodeId)?.position.y ?? targetY;
    const laneOrder = [...siblingEdges, { id, target: targetNodeId, source: sourceNodeId, sourceHandle: currentSourceHandle }].sort((a, b) => {
      const aTarget = nodes.find((node) => node.id === a.target);
      const bTarget = nodes.find((node) => node.id === b.target);
      const ay = a.id === id ? currentTargetY : (aTarget?.position.y ?? 0);
      const by = b.id === id ? currentTargetY : (bTarget?.position.y ?? 0);
      if (Math.abs(ay - by) > 0.1) {
        return ay - by;
      }
      return a.id.localeCompare(b.id);
    });
    const laneIndex = Math.max(
      0,
      laneOrder.findIndex((edge) => edge.id === id),
    );
    const obstacles = nodes
      .filter((node) => node.id !== sourceNodeId && node.id !== targetNodeId)
      .map((node) =>
        buildObstacleRect(
          node.position.x,
          node.position.y,
          typeof node.width === "number" && Number.isFinite(node.width) ? node.width : DEFAULT_NODE_WIDTH,
          typeof node.height === "number" && Number.isFinite(node.height) ? node.height : DEFAULT_NODE_HEIGHT,
        ),
      );
    const laneSpacing = 22;
    const preferredMidX = isLeftToRight
      ? sourceStubX + 40 + laneIndex * laneSpacing
      : sourceStubX - 40 - laneIndex * laneSpacing;
    const xRangesOverlap =
      sourceRect && targetRect
        ? Math.min(sourceRect.right, targetRect.right) >= Math.max(sourceRect.left, targetRect.left) - 8
        : false;
    const outerRight =
      sourceRect && targetRect ? Math.max(sourceRect.right, targetRect.right) + 36 + laneIndex * laneSpacing : undefined;
    const outerLeft =
      sourceRect && targetRect ? Math.min(sourceRect.left, targetRect.left) - 36 - laneIndex * laneSpacing : undefined;
    const baseMidX = xRangesOverlap
      ? isLeftToRight
        ? (outerRight ?? preferredMidX)
        : (outerLeft ?? preferredMidX)
      : isLeftToRight
        ? Math.min(preferredMidX, targetStubX - 24)
        : Math.max(preferredMidX, targetStubX + 24);
    const midX = chooseRoutedMidX(baseMidX, sourceY, targetY, obstacles);
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
  }, [currentEdge?.sourceHandle, edges, id, nodes, sourceNodeId, sourceRect, sourceSide, sourceX, sourceY, targetNodeId, targetRect, targetSide, targetX, targetY]);
  const classicCurvePath = buildClassicCurvePath(
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourceSide,
    targetSide,
    sourceRect,
    targetRect,
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
  const phaseSeconds = ((Date.now() - flowAnimationEpoch) % (animationDurationSeconds * 1000)) / 1000;
  const beginTime = `${-Math.max(0, phaseSeconds)}s`;
  return (
    <>
      <BaseEdge id={id} path={path} style={lineStyle} />
      {flowAnimationEnabled && (
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
