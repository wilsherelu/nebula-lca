import { getApiBase } from "../apiBase";
import { useEffect, useMemo, useState } from "react";

export type FlowAllocationProperty = {
  propertyType: string;
  value: number;
  basisUnit?: string | null;
  targetUnitGroup: string;
  targetUnit?: string | null;
  source?: string | null;
  note?: string | null;
};

type Props = {
  open: boolean;
  uiLanguage: "zh" | "en";
  flowUuid: string | null;
  flowName?: string;
  sourceUnit?: string;
  sourceUnitGroup?: string;
  sourcePolicy?: string;
  tidasAllowedUnitGroups?: Set<string>;
  onClose: () => void;
  onSaved?: (properties: FlowAllocationProperty[]) => void;
  onResetToDefault?: () => void;
  onStatus?: (text: string) => void;
};

const API_BASE = getApiBase();

function normalizeUnitGroup(value: string | null | undefined): string {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/\*/g, "_")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function canonicalUnitGroupKey(value: string | null | undefined): string {
  return normalizeUnitGroup(value)
    .replace(/^units?_of_/, "")
    .replace(/^unit_of_/, "");
}

const TIDAS_UNIT_GROUP_ZH: Record<string, string> = {
  unit_of_currency: "货币",
  unit_of_kg_km: "质量*距离",
  units_of_mass_time: "质量*时间",
  units_of_items: "数量",
  units_of_length: "长度",
  units_of_energy: "能量",
  units_of_volume: "体积",
  units_of_radioactivity: "放射性",
  units_of_area: "面积",
  units_of_area_time: "面积*时间",
  units_of_volume_time: "体积*时间",
  units_of_mass: "质量",
  units_of_mole: "物质的量",
  sej: "太阳能值",
};

const TIDAS_UNIT_GROUP_OPTIONS = [
  { unitGroup: "Unit of currency", referenceUnit: "EUR" },
  { unitGroup: "Unit of kg*km", referenceUnit: "kg*km" },
  { unitGroup: "Units of mass*time", referenceUnit: "kg*a" },
  { unitGroup: "Units of items", referenceUnit: "Item(s)" },
  { unitGroup: "Units of length", referenceUnit: "m" },
  { unitGroup: "Units of energy", referenceUnit: "MJ" },
  { unitGroup: "Units of volume", referenceUnit: "m3" },
  { unitGroup: "Units of radioactivity", referenceUnit: "kBq" },
  { unitGroup: "Units of area", referenceUnit: "m2" },
  { unitGroup: "Units of area*time", referenceUnit: "m2*a" },
  { unitGroup: "Units of volume*time", referenceUnit: "m3*a" },
  { unitGroup: "Units of mass", referenceUnit: "kg" },
  { unitGroup: "Units of mole", referenceUnit: "mol" },
  { unitGroup: "sej", referenceUnit: "sej" },
];

type UnitDefinition = {
  unit_group: string;
  unit_name: string;
  factor_to_reference: number;
  is_reference: boolean;
};

const defaultProperty = (sourceUnit?: string): FlowAllocationProperty => ({
  propertyType: "custom_conversion",
  value: 1,
  basisUnit: sourceUnit ?? "",
  targetUnitGroup: "",
  targetUnit: "",
  source: "user_declared",
  note: "",
});

function normalizeProperty(raw: Partial<FlowAllocationProperty>, sourceUnit?: string): FlowAllocationProperty {
  return {
    propertyType: String(raw.propertyType ?? "custom_conversion"),
    value: Number(raw.value) > 0 ? Number(raw.value) : 1,
    basisUnit: raw.basisUnit ?? sourceUnit ?? "",
    targetUnitGroup: String(raw.targetUnitGroup ?? ""),
    targetUnit: raw.targetUnit ?? "",
    source: raw.source ?? "user_declared",
    note: raw.note ?? "",
  };
}

export function allocationMethodForProperty(propertyType: string): "density" | "heating_value" | "custom_conversion" {
  if (propertyType === "density") {
    return "density";
  }
  if (propertyType === "heating_value_lhv" || propertyType === "heating_value_hhv") {
    return "heating_value";
  }
  return "custom_conversion";
}

export function FlowAllocationPropertiesModal({
  open,
  uiLanguage,
  flowUuid,
  flowName,
  sourceUnit,
  sourceUnitGroup,
  sourcePolicy,
  tidasAllowedUnitGroups,
  onClose,
  onSaved,
  onResetToDefault,
  onStatus,
}: Props) {
  const zh = uiLanguage === "zh";
  const [properties, setProperties] = useState<FlowAllocationProperty[]>([]);
  const [unitRows, setUnitRows] = useState<UnitDefinition[]>([]);
  const [busy, setBusy] = useState(false);
  const [errorText, setErrorText] = useState("");

  const unitGroups = useMemo(() => {
    const map = new Map<string, string>();
    for (const row of unitRows) {
      if (!row.unit_group) {
        continue;
      }
      if (row.is_reference || !map.has(row.unit_group)) {
        map.set(row.unit_group, row.unit_name);
      }
    }
    return Array.from(map.entries())
      .map(([unitGroup, referenceUnit]) => ({ unitGroup, referenceUnit }))
      .sort((a, b) => a.unitGroup.localeCompare(b.unitGroup));
  }, [unitRows]);

  const selectableUnitGroups = useMemo(() => {
    const sourceGroupKey = canonicalUnitGroupKey(sourceUnitGroup);
    const strictTidas = sourcePolicy === "tidas_compliant" || Boolean(tidasAllowedUnitGroups && tidasAllowedUnitGroups.size > 0);
    const baseRows = strictTidas ? TIDAS_UNIT_GROUP_OPTIONS : unitGroups;
    return baseRows.filter((row) => {
      const key = canonicalUnitGroupKey(row.unitGroup);
      if (key === sourceGroupKey) {
        return false;
      }
      if (sourcePolicy === "tidas_compliant" && tidasAllowedUnitGroups && tidasAllowedUnitGroups.size > 0) {
        return tidasAllowedUnitGroups.has(normalizeUnitGroup(row.unitGroup));
      }
      return true;
    });
  }, [sourcePolicy, sourceUnitGroup, tidasAllowedUnitGroups, unitGroups]);

  const referenceUnitByGroup = useMemo(
    () => new Map([...unitGroups, ...TIDAS_UNIT_GROUP_OPTIONS].map((row) => [row.unitGroup, row.referenceUnit])),
    [unitGroups],
  );
  const sourceReferenceUnit = sourceUnitGroup ? referenceUnitByGroup.get(sourceUnitGroup) ?? sourceUnit : sourceUnit;

  const unitGroupLabel = (unitGroup: string, referenceUnit: string): string => {
    const zhName = TIDAS_UNIT_GROUP_ZH[normalizeUnitGroup(unitGroup)];
    if (zh && zhName) {
      return `${zhName} / ${referenceUnit}`;
    }
    return `${unitGroup} / ${referenceUnit}`;
  };
  const sourceUnitGroupDisplay = sourceUnitGroup
    ? unitGroupLabel(sourceUnitGroup, sourceReferenceUnit || sourceUnit || "")
    : "-";

  useEffect(() => {
    if (!open || !flowUuid) return;
    let cancelled = false;
    setBusy(true);
    setErrorText("");
    fetch(`${API_BASE}/flows/${encodeURIComponent(flowUuid)}/allocation-properties`)
      .then((resp) => {
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        return resp.json() as Promise<{ properties?: FlowAllocationProperty[] }>;
      })
      .then((payload) => {
        if (!cancelled) {
          const rows = Array.isArray(payload.properties) ? payload.properties : [];
          setProperties(rows.length > 0 ? [normalizeProperty(rows[0], sourceUnit)] : [defaultProperty(sourceUnit)]);
        }
      })
      .catch((error) => {
        if (!cancelled) {
          setProperties([defaultProperty(sourceUnit)]);
          setErrorText(error instanceof Error ? error.message : zh ? "读取失败" : "Load failed");
        }
      })
      .finally(() => {
        if (!cancelled) setBusy(false);
      });
    return () => {
      cancelled = true;
    };
  }, [flowUuid, open, sourceUnit, zh]);

  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    fetch(`${API_BASE}/reference/units`)
      .then((resp) => {
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        return resp.json() as Promise<UnitDefinition[]>;
      })
      .then((rows) => {
        if (!cancelled) {
          setUnitRows(Array.isArray(rows) ? rows : []);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setUnitRows([]);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [open]);

  const validProperties = useMemo(
    () => {
      const sourceGroupKey = canonicalUnitGroupKey(sourceUnitGroup);
      return properties.filter((item) => (
        item.propertyType &&
        Number(item.value) > 0 &&
        item.targetUnitGroup.trim() &&
        canonicalUnitGroupKey(item.targetUnitGroup) !== sourceGroupKey
      ));
    },
    [properties, sourceUnitGroup],
  );

  if (!open || !flowUuid) {
    return null;
  }

  const updateProperty = (index: number, patch: Partial<FlowAllocationProperty>) => {
    setProperties((prev) => prev.map((item, i) => (i === index ? normalizeProperty({ ...item, ...patch }, sourceUnit) : item)));
  };

  const save = async () => {
    setBusy(true);
    setErrorText("");
    try {
      const payloadProperties = validProperties.slice(0, 1).map((item) => ({
        ...item,
        propertyType: "custom_conversion",
        basisUnit: sourceReferenceUnit || item.basisUnit,
        targetUnit: referenceUnitByGroup.get(item.targetUnitGroup) ?? item.targetUnit,
      }));
      const resp = await fetch(`${API_BASE}/flows/${encodeURIComponent(flowUuid)}/allocation-properties`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ properties: payloadProperties }),
      });
      if (!resp.ok) {
        const payload = await resp.json().catch(() => ({}));
        throw new Error(payload?.detail?.message ?? payload?.message ?? `HTTP ${resp.status}`);
      }
      const payload = await resp.json() as { properties?: FlowAllocationProperty[] };
      const rows = (Array.isArray(payload.properties) ? payload.properties : []).map((row) => normalizeProperty(row, sourceUnit));
      setProperties(rows.length > 0 ? [rows[0]] : [defaultProperty(sourceUnit)]);
      onSaved?.(payloadProperties.map((row) => normalizeProperty(row, sourceUnit)));
      onStatus?.(zh ? "Flow 单位组切换配置已保存。" : "Flow unit-group switch saved.");
      onClose();
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : zh ? "保存失败" : "Save failed");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="overlay-modal" onClick={onClose}>
      <section className="pm-modal flow-allocation-modal" onClick={(event) => event.stopPropagation()}>
        <div className="pm-modal-head">
          <strong>{zh ? "Flow 单位组切换配置" : "Flow Unit-group Switch"}</strong>
          <button type="button" className="drawer-close-btn" onClick={onClose}>{zh ? "关闭" : "Close"}</button>
        </div>
        <div className="flow-unit-switch-flow" title={flowName || flowUuid}>
          {flowName || flowUuid}
        </div>
        <div className="flow-allocation-property-list">
          {properties.slice(0, 1).map((property, index) => (
            <div className="flow-unit-switch-card" key="unit-switch-rule">
              <div className="flow-unit-switch-side">
                <div className="flow-unit-switch-side-title">{zh ? "切换前" : "Before"}</div>
                <div className="flow-unit-switch-fields">
                  <label>
                    {zh ? "单位组" : "Unit group"}
                    <input value={sourceUnitGroupDisplay} disabled />
                  </label>
                  <label>
                    {zh ? "基准单位" : "Reference unit"}
                    <input value={sourceReferenceUnit || "-"} disabled />
                  </label>
                </div>
              </div>
              <div className="flow-unit-switch-side">
                <div className="flow-unit-switch-side-title">{zh ? "切换后" : "After"}</div>
                <div className="flow-unit-switch-fields flow-unit-switch-fields-after">
                  <label>
                    {zh ? "目标单位组" : "Target unit group"}
                    <select
                      value={property.targetUnitGroup}
                      onChange={(event) => {
                        const targetUnitGroup = event.target.value;
                        updateProperty(index, {
                          propertyType: "custom_conversion",
                          basisUnit: sourceReferenceUnit,
                          targetUnitGroup,
                          targetUnit: referenceUnitByGroup.get(targetUnitGroup) ?? "",
                        });
                      }}
                    >
                      <option value="">{zh ? "选择目标单位组" : "Select target unit group"}</option>
                      {selectableUnitGroups
                        .map((row) => (
                          <option key={row.unitGroup} value={row.unitGroup}>
                            {unitGroupLabel(row.unitGroup, row.referenceUnit)}
                          </option>
                        ))}
                    </select>
                  </label>
                  <label>
                    {zh ? "目标基准单位" : "Target reference unit"}
                    <input value={property.targetUnitGroup ? referenceUnitByGroup.get(property.targetUnitGroup) ?? property.targetUnit ?? "" : ""} disabled />
                  </label>
                  <label>
                    {zh ? `1 ${sourceReferenceUnit || ""} =` : `1 ${sourceReferenceUnit || ""} =`}
                    <input type="number" min={0} step="any" inputMode="decimal" value={property.value} onChange={(event) => updateProperty(index, { propertyType: "custom_conversion", value: Number(event.target.value) })} />
                  </label>
                  <label>
                    {zh ? "备注" : "Note"}
                    <input value={property.note ?? ""} placeholder={zh ? "来源或说明" : "Source or note"} onChange={(event) => updateProperty(index, { note: event.target.value })} />
                  </label>
                </div>
              </div>
            </div>
          ))}
        </div>
        {errorText && <div className="pm-form-error">{errorText}</div>}
        <div className="pm-modal-actions">
          <button type="button" className="ghost-btn" onClick={onResetToDefault} disabled={busy || !onResetToDefault}>
            {onResetToDefault
              ? (zh ? "恢复默认单位组" : "Use default unit group")
              : (zh ? "已是默认单位组" : "Already default")}
          </button>
          <button type="button" className="ghost-btn" onClick={onClose}>{zh ? "取消" : "Cancel"}</button>
          <button type="button" onClick={() => void save()} disabled={busy || validProperties.length === 0}>{zh ? "保存" : "Save"}</button>
        </div>
      </section>
    </div>
  );
}
