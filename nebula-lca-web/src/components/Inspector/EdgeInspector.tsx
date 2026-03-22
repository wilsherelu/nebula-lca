import type { Edge } from "@xyflow/react";
import { useEffect, useMemo, useState } from "react";
import type { LcaEdgeData } from "../../model/exchange";
import { useLcaGraphStore } from "../../store/lcaGraphStore";

type UnitDefinition = {
  unit_group: string;
  unit_name: string;
  factor_to_reference: number;
  is_reference: boolean;
};
type CatalogFlow = {
  flow_uuid: string;
  unit_group: string;
};

type Props = {
  edge: Edge<LcaEdgeData>;
  onClose?: () => void;
};

const RAW_API_BASE = ((import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "/api").replace(/\/$/, "");
const API_BASE = RAW_API_BASE.endsWith("/api") ? RAW_API_BASE : `${RAW_API_BASE}/api`;

export function EdgeInspector({ edge, onClose }: Props) {
  const setFlowBalanceTotal = useLcaGraphStore((state) => state.setFlowBalanceTotal);
  const setFlowBalanceEdgeAmount = useLcaGraphStore((state) => state.setFlowBalanceEdgeAmount);
  const setFlowBalanceUnit = useLcaGraphStore((state) => state.setFlowBalanceUnit);
  const closeInspector = useLcaGraphStore((state) => state.closeInspector);
  const unitAutoScaleEnabled = useLcaGraphStore((state) => state.unitAutoScaleEnabled);
  const uiLanguage = useLcaGraphStore((state) => state.uiLanguage);
  const nodes = useLcaGraphStore((state) => state.nodes);
  const edges = useLcaGraphStore((state) => state.edges);
  const data = edge.data;
  if (!data) {
    return null;
  }
  const t = (zh: string, en: string) => (uiLanguage === "zh" ? zh : en);
  const handleClose = onClose ?? (() => closeInspector());

  const sourceNode = nodes.find((n) => n.id === edge.source);
  const sourcePortId = edge.sourceHandle ? edge.sourceHandle.slice(edge.sourceHandle.indexOf(":") + 1) : undefined;
  const sourcePort =
    sourceNode?.data.outputs.find((p) => p.id === sourcePortId) ??
    sourceNode?.data.outputs.find((p) => p.flowUuid === data.flowUuid);
  const sourceActualTotal = sourcePort?.amount ?? 0;
  const sourceActualExternalSale = sourcePort?.externalSaleAmount ?? 0;
  const sourceActualUnit = sourcePort?.unit ?? data.unit ?? "kg";
  const sourceMode = sourceNode?.data.nodeKind === "unit_process" ? sourceNode.data.mode : "normalized";

  const fanoutEdges = useMemo(() => {
    if (!sourcePort) {
      return [];
    }
    return edges.filter((item) => {
      if (item.source !== edge.source || item.data?.flowUuid !== data.flowUuid) {
        return false;
      }
      const target = nodes.find((n) => n.id === item.target);
      return Boolean(target?.data.inputs.some((p) => p.flowUuid === data.flowUuid));
    });
  }, [data.flowUuid, edge.source, edges, nodes, sourcePort]);

  const relationRows = useMemo(
    () =>
      fanoutEdges
        .map((item) => {
          const target = nodes.find((n) => n.id === item.target);
          const targetMode = target?.data.nodeKind === "unit_process" ? target.data.mode : "normalized";
          const targetPortId = item.targetHandle ? item.targetHandle.slice(item.targetHandle.indexOf(":") + 1) : undefined;
          const targetPort =
            target?.data.inputs.find((p) => p.id === targetPortId) ??
            target?.data.inputs.find((p) => p.flowUuid === data.flowUuid);
          return {
            edgeId: item.id,
            targetName: target?.data.name ?? item.target,
            targetMode,
            amount: targetPort?.amount ?? item.data?.amount ?? 0,
          };
        })
        .sort((a, b) => a.targetName.localeCompare(b.targetName, "zh-CN")),
    [data.flowUuid, fanoutEdges, nodes],
  );

  const [draftSourceTotal, setDraftSourceTotal] = useState(0);
  const [draftExternalSale, setDraftExternalSale] = useState(0);
  const [draftRows, setDraftRows] = useState<Record<string, number>>({});
  const [baseSourceTotal, setBaseSourceTotal] = useState(0);
  const [baseExternalSale, setBaseExternalSale] = useState(0);
  const [baseRows, setBaseRows] = useState<Record<string, number>>({});
  const [draftUnit, setDraftUnit] = useState(sourceActualUnit);
  const [autoBalanceWarning, setAutoBalanceWarning] = useState("");
  const [unitDefinitions, setUnitDefinitions] = useState<UnitDefinition[]>([]);
  const [flowUnitGroupByUuid, setFlowUnitGroupByUuid] = useState<Record<string, string>>({});
  const sourceUnitGroupRaw = sourcePort?.unitGroup || flowUnitGroupByUuid[data.flowUuid];

  const relationRowsRevision = useMemo(
    () =>
      relationRows
        .map((row) => `${row.edgeId}:${row.amount}`)
        .sort()
        .join("|"),
    [relationRows],
  );

  useEffect(() => {
    const rows = Object.fromEntries(relationRows.map((row) => [row.edgeId, row.amount]));
    setDraftSourceTotal(sourceActualTotal);
    setDraftExternalSale(sourceActualExternalSale);
    setDraftRows(rows);
    setBaseSourceTotal(sourceActualTotal);
    setBaseExternalSale(sourceActualExternalSale);
    setBaseRows(rows);
    setDraftUnit(sourceActualUnit);
    setAutoBalanceWarning("");
  }, [edge.id, sourceActualTotal, sourceActualExternalSale, sourceActualUnit, relationRowsRevision]);

  useEffect(() => {
    let canceled = false;
    fetch(`${API_BASE}/reference/units`)
      .then((resp) => {
        if (!resp.ok) {
          throw new Error(`HTTP ${resp.status}`);
        }
        return resp.json() as Promise<UnitDefinition[]>;
      })
      .then((rows) => {
        if (!canceled) {
          setUnitDefinitions(rows);
        }
      })
      .catch(() => {
        if (!canceled) {
          setUnitDefinitions([]);
        }
      });
    return () => {
      canceled = true;
    };
  }, []);

  const normalizedUnitGroupLookup = useMemo(() => {
    const normalize = (value: string) => value.trim().toLowerCase();
    const map = new Map<string, string>();
    for (const row of unitDefinitions) {
      if (!row.unit_group) {
        continue;
      }
      const norm = normalize(row.unit_group);
      if (!map.has(norm)) {
        map.set(norm, row.unit_group);
      }
    }
    return map;
  }, [unitDefinitions]);

  const sourceUnitGroup = useMemo(() => {
    if (!sourceUnitGroupRaw) {
      return undefined;
    }
    if (unitDefinitions.some((row) => row.unit_group === sourceUnitGroupRaw)) {
      return sourceUnitGroupRaw;
    }
    return normalizedUnitGroupLookup.get(sourceUnitGroupRaw.trim().toLowerCase());
  }, [normalizedUnitGroupLookup, sourceUnitGroupRaw, unitDefinitions]);

  useEffect(() => {
    if (sourcePort?.unitGroup || !data.flowUuid || flowUnitGroupByUuid[data.flowUuid]) {
      return;
    }
    let canceled = false;
    fetch(`${API_BASE}/reference/flows/${encodeURIComponent(data.flowUuid)}`)
      .then(async (resp) => {
        if (!resp.ok) {
          return null;
        }
        const row = (await resp.json()) as CatalogFlow;
        if (!row.flow_uuid || !row.unit_group) {
          return null;
        }
        return { flowUuid: row.flow_uuid, unitGroup: row.unit_group };
      })
      .then((payload) => {
        if (canceled || !payload) {
          return;
        }
        setFlowUnitGroupByUuid((prev) => ({
          ...prev,
          [payload.flowUuid]: payload.unitGroup,
        }));
      })
      .catch(() => {
        // ignore
      });
    return () => {
      canceled = true;
    };
  }, [data.flowUuid, flowUnitGroupByUuid, sourcePort?.unitGroup]);

  const draftInputTotal = relationRows.reduce((sum, row) => sum + (draftRows[row.edgeId] ?? row.amount), 0);
  const internalRequiredTotal = draftSourceTotal - draftExternalSale;
  const allBalanced = sourceMode === "balanced" && relationRows.every((row) => row.targetMode === "balanced");
  const balanceGap = internalRequiredTotal - draftInputTotal;
  const balancedOk = allBalanced && Math.abs(balanceGap) < 1e-9;
  const canConfirm =
    Number.isFinite(draftSourceTotal) &&
    Number.isFinite(draftExternalSale) &&
    draftExternalSale >= 0 &&
    relationRows.every((row) => Number.isFinite(draftRows[row.edgeId] ?? row.amount));
  const title = t("线配平", "Flow Balance");
  const unitOptions = useMemo(() => {
    const groupUnits = sourceUnitGroup
      ? unitDefinitions.filter((item) => item.unit_group === sourceUnitGroup).map((item) => item.unit_name)
      : [];
    return Array.from(
      new Set(
        [sourceActualUnit, ...fanoutEdges.map((item) => item.data?.unit ?? ""), ...groupUnits]
          .map((u) => u.trim())
          .filter((u) => u.length > 0),
      ),
    );
  }, [fanoutEdges, sourceActualUnit, sourceUnitGroup, unitDefinitions]);

  const convertValue = async (value: number, fromUnit: string, toUnit: string): Promise<number> => {
    if (!Number.isFinite(value) || fromUnit === toUnit) {
      return value;
    }
    try {
      const resp = await fetch(`${API_BASE}/units/convert`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          value,
          from_unit: fromUnit,
          to_unit: toUnit,
          unit_group: sourceUnitGroup,
        }),
      });
      if (!resp.ok) {
        return value;
      }
      const payload = (await resp.json()) as { converted_value?: number };
      return Number.isFinite(payload.converted_value) ? Number(payload.converted_value) : value;
    } catch {
      return value;
    }
  };

  const onChangeDraftUnit = async (nextUnit: string) => {
    if (nextUnit === draftUnit) {
      return;
    }
    if (!unitAutoScaleEnabled) {
      setDraftUnit(nextUnit);
      return;
    }
    const oldUnit = draftUnit;
    const nextSource = await convertValue(draftSourceTotal, oldUnit, nextUnit);
    const nextSale = await convertValue(draftExternalSale, oldUnit, nextUnit);
    const nextRows: Record<string, number> = {};
    for (const row of relationRows) {
      const current = draftRows[row.edgeId] ?? row.amount;
      nextRows[row.edgeId] = await convertValue(current, oldUnit, nextUnit);
    }
    setDraftSourceTotal(nextSource);
    setDraftExternalSale(nextSale);
    setDraftRows(nextRows);
    setDraftUnit(nextUnit);
  };

  const runAutoBalance = () => {
    setAutoBalanceWarning("");
    if (relationRows.length === 0) {
      return;
    }
    const EPS = 1e-9;
    const targetInternalTotal = draftSourceTotal - draftExternalSale;
    if (allBalanced && Math.abs(targetInternalTotal - draftInputTotal) < EPS) {
      setAutoBalanceWarning(t("已配平，无需自动配平。", "Already balanced. Auto-balance is not needed."));
      return;
    }
    const rowValues = relationRows.map((row) => ({
      edgeId: row.edgeId,
      value: draftRows[row.edgeId] ?? row.amount,
      base: baseRows[row.edgeId] ?? row.amount,
    }));
    const zeroRows = rowValues.filter((row) => Math.abs(row.value) < EPS);
    const anyNegative = rowValues.some((row) => row.value < -EPS) || draftSourceTotal < -EPS || draftExternalSale < -EPS;
    if (anyNegative) {
      setAutoBalanceWarning(t("检测到负值，无法自动配平。请先修正为非负数。", "Negative values detected. Please make all values non-negative before auto-balance."));
      return;
    }

    if (zeroRows.length > 0) {
      const nonZeroSum = rowValues
        .filter((row) => Math.abs(row.value) >= EPS)
        .reduce((sum, row) => sum + row.value, 0);
      const residual = targetInternalTotal - nonZeroSum;
      const fill = residual / zeroRows.length;
      if (fill < -EPS) {
        setAutoBalanceWarning(
          t(
            "补齐0值会产生负值，无法自动配平，请调整输出或非零输入项。",
            "Filling zero rows would create negative values. Please adjust output or non-zero inputs first.",
          ),
        );
        return;
      }
      const nextRows: Record<string, number> = {};
      rowValues.forEach((row) => {
        nextRows[row.edgeId] = Math.abs(row.value) < EPS ? fill : row.value;
      });
      setDraftRows(nextRows);
      return;
    }

    const sourceChanged = Math.abs(draftSourceTotal - baseSourceTotal) > EPS;
    const externalChanged = Math.abs(draftExternalSale - baseExternalSale) > EPS;
    const inputChanged = rowValues.some((row) => Math.abs(row.value - row.base) > EPS);
    const outputSideChanged = sourceChanged || externalChanged;
    const changedCount = [outputSideChanged, inputChanged].filter(Boolean).length;

    if (changedCount === 0) {
      return;
    }

    if (changedCount > 1) {
      setAutoBalanceWarning(
        t(
          "检测到多侧同时改动，无法自动判定唯一配平路径，请手动调整。",
          "Multiple sides changed at once. Auto-balance cannot determine a unique path; please adjust manually.",
        ),
      );
      return;
    }

    if (externalChanged && !sourceChanged && !inputChanged) {
      // 仅改外售：保持输入不变，总量自动调整
      setDraftSourceTotal(draftInputTotal + draftExternalSale);
      return;
    }

    if (inputChanged && !sourceChanged && !externalChanged) {
      // 仅改输入：保持外售不变，总量自动调整
      setDraftSourceTotal(draftInputTotal + draftExternalSale);
      return;
    }

    if (outputSideChanged && !inputChanged) {
      // 仅改输出侧：按当前输出总量 - 外售量 推导内部需求，并按基线输入比例缩放输入项
      const targetInternalTotal = draftSourceTotal - draftExternalSale;
      if (targetInternalTotal < -EPS) {
        setAutoBalanceWarning(t("目标内部总量为负，无法自动配平。", "Target internal total is negative. Auto-balance aborted."));
        return;
      }
      const baseInternalTotal = rowValues.reduce((sum, row) => sum + row.base, 0);
      if (Math.abs(baseInternalTotal) < EPS) {
        const each = rowValues.length > 0 ? targetInternalTotal / rowValues.length : 0;
        if (each < -EPS) {
          setAutoBalanceWarning(t("目标总量导致负值，无法自动配平。", "Target total would create negative values. Auto-balance aborted."));
          return;
        }
        const nextRows: Record<string, number> = {};
        rowValues.forEach((row) => {
          nextRows[row.edgeId] = each;
        });
        setDraftRows(nextRows);
        return;
      }
      const ratio = targetInternalTotal / baseInternalTotal;
      if (ratio < -EPS) {
        setAutoBalanceWarning(t("目标总量为负，无法自动配平。", "Target total is negative. Auto-balance aborted."));
        return;
      }
      const nextRows: Record<string, number> = {};
      rowValues.forEach((row) => {
        nextRows[row.edgeId] = row.base * ratio;
      });
      setDraftRows(nextRows);
      return;
    }

  };

  const commitEditingThen = (fn: () => void) => {
    const active = document.activeElement;
    if (active instanceof HTMLInputElement || active instanceof HTMLSelectElement || active instanceof HTMLTextAreaElement) {
      active.blur();
    }
    window.setTimeout(fn, 0);
  };

  const onAutoBalance = () => {
    commitEditingThen(runAutoBalance);
  };

  const onConfirm = () => {
    commitEditingThen(() => {
      if (!canConfirm) {
        return;
      }
      relationRows.forEach((row) => {
        const nextValue = draftRows[row.edgeId] ?? row.amount;
        setFlowBalanceEdgeAmount(row.edgeId, nextValue);
      });
      setFlowBalanceTotal(edge.source, data.flowUuid, draftSourceTotal, draftExternalSale, true);
      setFlowBalanceUnit(edge.source, data.flowUuid, draftUnit);
      handleClose();
    });
  };

  return (
    <div className="inspector-block edge-balance-layout">
      <h3>{title}</h3>
      <div className={balancedOk ? "mode-hint balanced-ok" : allBalanced ? "mode-hint balanced" : "mode-hint normalized"}>
        {balancedOk
          ? t("已配平", "Balanced")
          : allBalanced
            ? t("输出需满足输入总量", "Outputs must match total inputs")
            : t("不需要满足守恒", "Conservation not required")}
      </div>
      {autoBalanceWarning && <div className="mode-hint balanced">{autoBalanceWarning}</div>}
      <div className="balance-summary">
        <span>{`${t("清单输出", "Inventory output")}: ${draftSourceTotal}`}</span>
        <span>{`${t("清单输入合计", "Inventory input total")}: ${draftInputTotal}`}</span>
        <span>{`${t("外售量", "External sale")}: ${draftExternalSale}`}</span>
        {allBalanced && <span className={Math.abs(balanceGap) < 1e-9 ? "ok" : "warn"}>{`${t("差值", "Gap")}: ${balanceGap}`}</span>}
      </div>
      <div className="balance-columns">
        <section className="balance-col source">
          <h4>{t("输出过程", "Output Process")}</h4>
          <div className="balance-card">
            <div className="balance-name">{sourceNode?.data.name ?? edge.source}</div>
            <div className="amount-unit-block">
              <div className="amount-unit-label-row amount-unit-label-row-3">
                <span>{t("产量", "Output Amount")}</span>
                <span>{t("外售量", "External Sale")}</span>
                <span>{t("单位", "Unit")}</span>
              </div>
              <div className="amount-unit-row amount-unit-row-3">
                <input
                  type="number"
                  value={Number.isFinite(draftSourceTotal) ? draftSourceTotal : 0}
                  onChange={(event) => {
                    const value = Number(event.target.value) || 0;
                    setDraftSourceTotal(value);
                  }}
                />
                <input
                  type="number"
                  min={0}
                  value={Number.isFinite(draftExternalSale) ? draftExternalSale : 0}
                  onChange={(event) => {
                    const value = Number(event.target.value);
                    setDraftExternalSale(Number.isFinite(value) ? Math.max(0, value) : 0);
                  }}
                />
                <select
                  value={draftUnit}
                  onChange={(event) => {
                    void onChangeDraftUnit(event.target.value);
                  }}
                >
                  {unitOptions.map((unit) => (
                    <option key={unit} value={unit}>
                      {unit}
                    </option>
                  ))}
                </select>
              </div>
            </div>
          </div>
        </section>

        <section className="balance-col target">
          <h4>{t("输入过程", "Input Process")}</h4>
          <div className="balance-list">
            {relationRows.map((row) => (
              <div className="balance-card" key={row.edgeId}>
                <div className="balance-name">{row.targetName}</div>
                <div className="amount-unit-block">
                  <div className="amount-unit-label-row amount-unit-label-row-2">
                    <span>{t("输入量", "Input Amount")}</span>
                    <span>{t("单位", "Unit")}</span>
                  </div>
                  <div className="amount-unit-row amount-unit-row-2">
                    <input
                      type="number"
                      value={Number.isFinite(draftRows[row.edgeId] ?? row.amount) ? (draftRows[row.edgeId] ?? row.amount) : 0}
                      onChange={(event) => {
                        const value = Number(event.target.value) || 0;
                        setDraftRows((prev) => ({
                          ...prev,
                          [row.edgeId]: value,
                        }));
                      }}
                    />
                    <select
                      value={draftUnit}
                      onChange={(event) => {
                        void onChangeDraftUnit(event.target.value);
                      }}
                    >
                      {unitOptions.map((unit) => (
                        <option key={unit} value={unit}>
                          {unit}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>
              </div>
            ))}
            {relationRows.length === 0 && <div className="table-empty">{t("暂无下游输入关系", "No downstream input relations")}</div>}
          </div>
        </section>
      </div>

      <div className="balance-actions bottom">
        <button type="button" onClick={onAutoBalance}>
          {t("自动配平", "Auto Balance")}
        </button>
        <button type="button" className="primary" disabled={!canConfirm} onClick={onConfirm}>
          {t("确认并保存", "Confirm and Save")}
        </button>
        <button type="button" onClick={handleClose}>
          {t("取消", "Cancel")}
        </button>
      </div>
    </div>
  );
}
