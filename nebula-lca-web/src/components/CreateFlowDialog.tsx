import { getApiBase } from "../apiBase";
import { useEffect, useMemo, useState } from "react";

type UnitDefinition = {
  unit_group: string;
  unit_name: string;
  factor_to_reference: number;
  is_reference: boolean;
};

type CustomFlowType = "product_flow" | "intermediate_flow" | "waste_flow";

type CreateFlowPayload = {
  flow_name: string;
  flow_name_en?: string;
  flow_type: CustomFlowType;
  unitGroupUuid: string;
  default_unit: string;
  category?: string;
  confirmCreate?: boolean;
  sourcePolicy?: string;
  tidasCompatible?: boolean;
  tidasUnitGroup?: string;
  tidasFlowPropertyUuid?: string;
  tidasReferenceSource?: string;
};

type CreateFlowResponse = {
  flow: {
    flow_uuid: string;
    flow_name: string;
    flow_name_en?: string | null;
    flow_type: string;
    default_unit: string;
    unit_group: string;
    category?: string | null;
    compartment?: string | null;
    tidas_compatible?: boolean;
    tidas_unit_group?: string | null;
    tidas_flow_property_uuid?: string | null;
    tidas_reference_source?: string | null;
  };
  warnings?: string[];
  reuseCandidates?: Array<{
    flow_uuid: string;
    flow_name: string;
    flow_name_en?: string | null;
    flow_type: string;
    default_unit: string;
    unit_group: string;
    category?: string | null;
    compartment?: string | null;
    tidas_compatible?: boolean;
    tidas_unit_group?: string | null;
    tidas_flow_property_uuid?: string | null;
    tidas_reference_source?: string | null;
  }>;
};

type ReuseCandidate = NonNullable<CreateFlowResponse["reuseCandidates"]>[number];

type CreateFlowDialogProps = {
  open: boolean;
  uiLanguage: "zh" | "en";
  defaultFlowType?: CustomFlowType;
  defaultCategory?: string;
  sourcePolicy?: string;
  onSuccess: (flow: CreateFlowResponse["flow"]) => void;
  onReuse?: (flow: CreateFlowResponse["flow"]) => void;
  onClose: () => void;
  onStatus?: (text: string) => void;
};

const API_BASE = getApiBase();

const normalizeUnitGroupKey = (value: string): string => value.trim().toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");

// Unit group Chinese mapping – static for now, switch to backend i18n later
const UNIT_GROUP_ZH: Record<string, string> = {
  Mass: "质量",
  Energy: "能量",
  Volume: "体积",
  Area: "面积",
  Length: "长度",
  Time: "时间",
  "Number of items": "数量",

  // LCIA impact categories
  "Unit of GWP": "全球变暖潜势单位",
  "Unit of ODP": "臭氧消耗潜势单位",
  "Unit of POCP": "光化学臭氧生成潜势单位",
  "Unit of AP": "酸化潜势单位",
  "Unit of EP": "富营养化潜势单位",
  "Unit of ADP": "非生物资源消耗潜势单位",
  "Unit of Human toxicity": "人体毒性单位",
  "Unit of Aquatic ecotoxicity": "水生生态毒性单位",
  "Unit of Terrestrial ecotoxicity": "陆地生态毒性单位",
  "Unit of Abiotic resource consumption": "非生物资源消耗单位",
  "Unit of Abiotic resource depletion": "非生物资源枯竭单位",
  "Unit of Abiotic resource consum": "非生物资源消耗单位",
  "Unit of Ionising radiation": "电离辐射单位",
  "Unit of Land use": "土地利用单位",
  "Unit of Water use": "水资源使用单位",
  "Unit of Particulate matter": "颗粒物单位",
  "Unit of Respiratory inorganics": "无机呼吸影响单位",
  "Unit of Respiratory organics": "有机呼吸影响单位",
  "Unit of Photochemical ozone formation": "光化学臭氧形成单位",
  "Unit of Climate change": "气候变化单位",
  "Unit of Eutrophication": "富营养化单位",
  "Unit of Acidification": "酸化单位",
  "Unit of Resource use, fossils": "化石资源使用单位",
  "Unit of Resource use, minerals and metals": "矿物和金属资源使用单位",
  "Unit of Freshwater ecotoxicity": "淡水生态毒性单位",
  "Unit of Marine ecotoxicity": "海洋生态毒性单位",
  "Unit of Terrestrial acidification": "陆地酸化单位",
  "Unit of Marine eutrophication": "海洋富营养化单位",
  "Unit of Freshwater eutrophication": "淡水富营养化单位",
  "Unit of Human toxicity, cancer": "人体毒性（癌症）单位",
  "Unit of Human toxicity, non-cancer": "人体毒性（非癌症）单位",
  "Unit of Ionising radiation, human health": "电离辐射（人体健康）单位",
  "Unit of Ecotoxicity, freshwater": "生态毒性（淡水）单位",
  "Unit of Ozone depletion": "臭氧消耗单位",
  "Unit of Particulate matter formation": "颗粒物形成单位",
  "Unit of Photochemical oxidant formation": "光化学氧化剂形成单位",

  // Land use
  "Land Occupation": "土地占用",
  "Land Transformation": "土地转化",

  // Ecoinvent-specific units
  "Aretime-unit": "面积时间单位",
  "Ecoinvent unit cubic meter-year": "ecoinvent 立方米·年单位",
  "Biotic Production Unit (Occ.)": "生物生产单位（占用）",
  "Biotic Production Unit (Transf.)": "生物生产单位（转化）",
  "Erosion Resistance Unit (Occ.)": "抗侵蚀单位（占用）",
  "Erosion Resistance Unit (Transf.)": "抗侵蚀单位（转化）",
  "Groundwater Replenishment Unit": "地下水补给单位",
  "Mechanical Filtration Unit (Occ.)": "机械过滤单位（占用）",
  "Mechanical Filtration Unit (Transf.)": "机械过滤单位（转化）",
  "Mechanical Filtration Unit (Occ)": "机械过滤单位（占用）",
  "Mechanical Filtration Unit (Tra)": "机械过滤单位（转化）",
  "Physicochemical Filtration Unit": "物理化学过滤单位",
};

const unitGroupLabelFallback = (group: string): string => {
  // "Unit(s) of XXX" -> "XXX 单位"
  const m1 = group.match(/^Units?\s+of\s+(.+)$/i);
  if (m1) return `${translateFallbackKey(m1[1])} 单位`;
  // "XXX Unit(s) (Occ.)" / "XXX Unit(s) (Transf.)" -> "XXX 单位（占用）"/"XXX 单位（转化）"
  const m2 = group.match(/^(.+)\s+Units?\s+\((Occ\.?|Transf\.?)\)$/i);
  if (m2) {
    const suffix = m2[2].toLowerCase().startsWith("occ") ? "占用" : "转化";
    return `${translateFallbackKey(m2[1])} 单位（${suffix}）`;
  }
  // "XXX Unit(s)" -> "XXX 单位"
  const m3 = group.match(/^(.+)\s+Units?$/i);
  if (m3) return `${translateFallbackKey(m3[1])} 单位`;
  // "XXX unit(s) YYY" -> "XXX YYY 单位"
  const m4 = group.match(/^(.+?)\s+units?\s+(.+)$/i);
  if (m4) return `${translateFallbackKey(m4[1])} ${m4[2]} 单位`;
  // "XXX-unit(s)" -> "XXX 单位"
  const m5 = group.match(/^(.+)-units?$/i);
  if (m5) return `${translateFallbackKey(m5[1])} 单位`;
  // General: append "单位" as fallback
  return `${translateFallbackKey(group)} 单位`;
};

// Translate common English words in fallback unit group names
const KEYWORD_ZH: Record<string, string> = {
  "Biotic Production": "生物生产",
  "Erosion Resistance": "抗侵蚀",
  "Groundwater Replenishment": "地下水补给",
  "Mechanical Filtration": "机械过滤",
  "Physicochemical Filtration": "物理化学过滤",
  "Land Occupation": "土地占用",
  "Land Transformation": "土地转化",
  "Global Warming": "全球变暖",
  "Ozone Depletion": "臭氧消耗",
  "Photochemical Ozone Formation": "光化学臭氧形成",
  "Photochemical Oxidant Formation": "光化学氧化剂形成",
  "Acidification": "酸化",
  "Eutrophication": "富营养化",
  "Nitrification Potential": "硝化潜势",
  "Nitrification potential": "硝化潜势",
  "Ecotoxicity": "生态毒性",
  "Human Toxicity": "人体毒性",
  "Ionising Radiation": "电离辐射",
  "Particulate Matter": "颗粒物",
  "Respiratory Inorganics": "无机呼吸",
  "Respiratory Organics": "有机呼吸",
  "Climate Change": "气候变化",
  "Resource Use": "资源使用",
  "Fossils": "化石",
  "Minerals And Metals": "矿物和金属",
  "Minerals and Metals": "矿物和金属",
  "Freshwater": "淡水",
  "Marine": "海洋",
  "Terrestrial": "陆地",
  "Aquatic": "水生",
  "Cancer": "癌症",
  "Non-cancer": "非癌症",
  "Human Health": "人体健康",
  "Filtration": "过滤",
  "Replenishment": "补给",
  "Resistance": "抗力",
  "Erosion": "侵蚀",
  "Biotic": "生物",
  "Production": "生产",
  "Occupation": "占用",
  "Transformation": "转化",
  "Ecoinvent": "ecoinvent",
  "Cubic Meter-year": "立方米·年",
  "Cubic meter-year": "立方米·年",
  "Standard Volume": "标准体积",
  "standard volume": "标准体积",
  "Currency 2000": "2000 年货币",
  "currency 2000": "2000 年货币",
  // Single words
  "Area": "面积",
  "Energy": "能量",
  "Volume": "体积",
  "Mass": "质量",
  "Length": "长度",
  "Time": "时间",
  "Items": "物品",
  "items": "物品",
  "Currency": "货币",
  "currency": "货币",
  "Area_time": "面积·时间",
  "area_time": "面积·时间",
  "Energy_area_time": "能量·面积·时间",
  "energy_area_time": "能量·面积·时间",
  "Energy_mass_time": "能量·质量·时间",
  "energy_mass_time": "能量·质量·时间",
  "Items_length": "物品·长度",
  "items_length": "物品·长度",
  "Items_time": "物品·时间",
  "items_time": "物品·时间",
  "Length_time": "长度·时间",
  "length_time": "长度·时间",
  "Standard": "标准",
  "standard": "标准",
  "Mole": "摩尔",
  "mole": "摩尔",
  "Person Transport": "人员运输",
  "person transport": "人员运输",
  "Power": "功率",
  "power": "功率",
  "Radioactivity": "放射性",
  "radioactivity": "放射性",
  "Vehicle Transport": "车辆运输",
  "vehicle transport": "车辆运输",
  "Areatime": "面积时间",
  "Aretime": "面积时间",
  "Area-time": "面积·时间",
  "area-time": "面积·时间",
  "area time": "面积 时间",
  "Transport": "运输",
  "transport": "运输",
  "Person": "人员",
  "person": "人员",
  "Vehicle": "车辆",
  "vehicle": "车辆",
};

const translateFallbackKey = (text: string): string => {
  // Direct keyword match
  if (KEYWORD_ZH[text]) return KEYWORD_ZH[text];
  // Split by whitespace or underscore
  const words = text.split(/[\s_]+/);
  const translated = words.map((w) => {
    if (KEYWORD_ZH[w]) return KEYWORD_ZH[w];
    // Case-insensitive match
    for (const [key, val] of Object.entries(KEYWORD_ZH)) {
      if (key.toLowerCase() === w.toLowerCase()) return val;
    }
    // Try connecting consecutive translated words with · for underscore-joined names
    return w;
  });
  // Join with · if original had underscores, else space
  const separator = text.includes("_") ? "·" : " ";
  return translated.join(separator);
};

type FlowCategoriesResponse = {
  items?: Array<{ category?: string; count?: number }>;
  total?: number;
};

export function CreateFlowDialog({
  open,
  uiLanguage,
  defaultFlowType = "product_flow",
  defaultCategory = "",
  sourcePolicy = "open_mixed",
  onSuccess,
  onReuse,
  onClose,
  onStatus,
}: CreateFlowDialogProps) {
  const zh = uiLanguage === "zh";
  const [flowName, setFlowName] = useState("");
  const [flowNameEn, setFlowNameEn] = useState("");
  const [flowType, setFlowType] = useState<CustomFlowType>(defaultFlowType);
  const [unitGroupUuid, setUnitGroupUuid] = useState("");
  const [defaultUnit, setDefaultUnit] = useState("");
  const [category, setCategory] = useState(defaultCategory);
  const [unitDefinitions, setUnitDefinitions] = useState<UnitDefinition[]>([]);
  const [tidasAllowedUnitGroups, setTidasAllowedUnitGroups] = useState<Set<string>>(new Set());
  const [tidasCompatible, setTidasCompatible] = useState(sourcePolicy === "tidas_compliant");
  const [categories, setCategories] = useState<string[]>([]);
  const [loadingCategories, setLoadingCategories] = useState(false);
  const [loading, setLoading] = useState(false);
  const [errorText, setErrorText] = useState("");
  const [candidates, setCandidates] = useState<ReuseCandidate[]>([]);

  useEffect(() => {
    if (!open) {
      setFlowName("");
      setFlowNameEn("");
      setFlowType(defaultFlowType);
      setUnitGroupUuid("");
      setDefaultUnit("");
      setCategory(defaultCategory);
      setTidasCompatible(sourcePolicy === "tidas_compliant");
      setErrorText("");
      setCandidates([]);
    }
  }, [open, defaultFlowType, defaultCategory, sourcePolicy]);

  // Load unit definitions
  useEffect(() => {
    if (!open) return;
    let canceled = false;
    fetch(`${API_BASE}/reference/units`)
      .then((resp) => {
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        return resp.json() as Promise<UnitDefinition[]>;
      })
      .then((rows) => {
        if (!canceled) setUnitDefinitions(rows);
      })
      .catch(() => {
        if (!canceled) setUnitDefinitions([]);
      });
    return () => {
      canceled = true;
    };
  }, [open]);

  useEffect(() => {
    if (!open) return;
    let canceled = false;
    fetch(`${API_BASE}/reference/tidas-policy`)
      .then((resp) => (resp.ok ? resp.json() : null))
      .then((payload) => {
        if (canceled || !payload) return;
        const allowed = Array.isArray(payload.allowed_unit_groups) ? payload.allowed_unit_groups : [];
        setTidasAllowedUnitGroups(new Set(allowed.map((item: unknown) => normalizeUnitGroupKey(String(item)))));
      })
      .catch(() => {
        if (!canceled) setTidasAllowedUnitGroups(new Set());
      });
    return () => {
      canceled = true;
    };
  }, [open]);

  // Load categories when flowType or open changes
  useEffect(() => {
    if (!open) return;
    let canceled = false;
    setCategories([]);
    setCategory("");
    setLoadingCategories(true);
    fetch(`${API_BASE}/flows/categories?type=${flowType}&level=1`)
      .then((resp) => {
        if (!resp.ok) {
          if (!canceled) setCategories([]);
          return;
        }
        return resp.json() as Promise<FlowCategoriesResponse>;
      })
      .then((payload) => {
        if (canceled) return;
        if (!payload) { setCategories([]); return; }
        const cats = (payload.items ?? [])
          .map((x) => x.category?.trim())
          .filter((c): c is string => Boolean(c));
        setCategories(cats);
      })
      .catch(() => {
        if (!canceled) setCategories([]);
      })
      .finally(() => {
        if (!canceled) setLoadingCategories(false);
      });
    return () => {
      canceled = true;
    };
  }, [open, flowType]);

  const availableUnitGroups = useMemo(() => {
    const groups = Array.from(new Set(unitDefinitions.map((u) => u.unit_group).filter(Boolean)));
    if (sourcePolicy !== "tidas_compliant" && !tidasCompatible) {
      return groups;
    }
    if (tidasAllowedUnitGroups.size === 0) {
      return groups;
    }
    return groups.filter((group) => tidasAllowedUnitGroups.has(normalizeUnitGroupKey(group)));
  }, [sourcePolicy, tidasAllowedUnitGroups, tidasCompatible, unitDefinitions]);

  const unitsForGroup = (group: string) =>
    unitDefinitions.filter((u) => u.unit_group === group);

  useEffect(() => {
    if (unitGroupUuid) {
      const units = unitsForGroup(unitGroupUuid);
      if (units.length > 0) {
        // Prefer reference unit, fallback to first
        const ref = units.find((u) => u.is_reference);
        setDefaultUnit((ref ?? units[0]).unit_name);
      } else {
        setDefaultUnit("");
      }
    }
  }, [unitGroupUuid]);

  const clearCandidatesOnError = () => {
    setCandidates([]);
    setErrorText("");
  };

  const tFlowType = (type: string): string => {
    const zhMap: Record<string, string> = {
      intermediate_flow: "中间流",
      product_flow: "产品流",
      waste_flow: "废物流",
    };
    const enMap: Record<string, string> = {
      intermediate_flow: "Intermediate Flow",
      product_flow: "Product Flow",
      waste_flow: "Waste Flow",
    };
    return uiLanguage === "zh" ? zhMap[type] || type : enMap[type] || type;
  };

  const unitGroupLabel = (group: string): string => {
    if (uiLanguage === "en") return group;
    return UNIT_GROUP_ZH[group] ?? unitGroupLabelFallback(group);
  };

  const handleSubmit = async (payloadOverride?: Partial<CreateFlowPayload>) => {
    if (!flowName.trim()) {
      setErrorText(zh ? "流名称不能为空。" : "Flow name is required.");
      return;
    }
    if (!unitGroupUuid) {
      setErrorText(zh ? "请选择单位组。" : "Please select a unit group.");
      return;
    }
    if (!defaultUnit) {
      setErrorText(zh ? "请选择默认单位。" : "Please select a default unit.");
      return;
    }
    if ((sourcePolicy === "tidas_compliant" || tidasCompatible) && tidasAllowedUnitGroups.size > 0 && !tidasAllowedUnitGroups.has(normalizeUnitGroupKey(unitGroupUuid))) {
      setErrorText(zh ? "该单位组不在 TIDAS 允许范围内。" : "This unit group is not allowed by TIDAS policy.");
      return;
    }

    setLoading(true);
    setErrorText("");

    const payload: CreateFlowPayload = {
      flow_name: flowName.trim(),
      flow_name_en: flowNameEn.trim() || undefined,
      flow_type: flowType,
      unitGroupUuid,
      default_unit: defaultUnit,
      category: category?.trim() || undefined,
      confirmCreate: payloadOverride?.confirmCreate ?? false,
      sourcePolicy,
      tidasCompatible: tidasCompatible || sourcePolicy === "tidas_compliant",
      tidasUnitGroup: unitGroupUuid,
      tidasReferenceSource: tidasCompatible || sourcePolicy === "tidas_compliant" ? "user_declared" : undefined,
    };

    try {
      const resp = await fetch(`${API_BASE}/flows`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (resp.status === 409) {
        const data = (await resp.json()) as {
          code?: string;
          message?: string;
          detail?: { code?: string; message?: string; candidates?: ReuseCandidate[] };
        };
        const candidateList = data.detail?.candidates ?? [];
        if (candidateList.length > 0 && !payload.confirmCreate) {
          setCandidates(candidateList);
          const hint = data.detail?.message ?? (zh
            ? "检测到名称重复的已有 Flow，建议优先复用。您可以选择引用已有 Flow，或在名称中增加区分信息后仍然新建。"
            : "Duplicate flow names detected. We recommend reusing an existing flow.");
          setErrorText(hint);
          setLoading(false);
          return;
        }
      }

      if (!resp.ok) {
        const err = (await resp.json().catch(() => ({}))) as { message?: string };
        throw new Error(err.message ?? `HTTP ${resp.status}`);
      }

      const result = (await resp.json()) as CreateFlowResponse;
      if (result.flow) {
        onStatus?.(
          zh
            ? `Flow 创建成功：${result.flow.flow_name}`
            : `Flow created successfully: ${result.flow.flow_name}`,
        );
        onSuccess(result.flow);
        onClose();
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : zh ? "创建失败" : "Creation failed";
      setErrorText(message);
      onStatus?.(zh ? `创建失败：${message}` : `Creation failed: ${message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleUseCandidate = (candidate: ReuseCandidate) => {
    const flowData = {
      flow_uuid: candidate.flow_uuid,
      flow_name: candidate.flow_name,
      flow_name_en: candidate.flow_name_en,
      flow_type: candidate.flow_type,
      default_unit: candidate.default_unit,
      unit_group: candidate.unit_group,
      category: candidate.category ?? candidate.compartment,
      compartment: candidate.compartment ?? candidate.category,
      tidas_compatible: candidate.tidas_compatible,
      tidas_unit_group: candidate.tidas_unit_group,
      tidas_flow_property_uuid: candidate.tidas_flow_property_uuid,
      tidas_reference_source: candidate.tidas_reference_source,
    };
    if (onReuse) {
      onReuse(flowData);
    } else {
      onSuccess(flowData);
    }
    onClose();
  };

  const handleConfirmCreate = () => {
    void handleSubmit({ confirmCreate: true });
  };

  if (!open) return null;

  return (
    <div className="pm-modal-mask" onClick={onClose}>
      <div className="pm-modal" onClick={(event) => event.stopPropagation()}>
        <div className="pm-modal-head">
          <strong>{zh ? "新建自定义 Flow" : "Create Custom Flow"}</strong>
          <button type="button" className="pm-link-btn" onClick={onClose}>
            {zh ? "关闭" : "Close"}
          </button>
        </div>

        <div className="pm-help-text" style={{ marginBottom: "12px" }}>
          {zh
            ? "自定义 Flow 仅用于产品流和废物流。基本流请通过 ecoinvent、HIQLCD、TIDAS 等导入来源维护。"
            : "Custom flows are only for product and waste flows. For elementary flows, please import from ecoinvent, HIQLCD, TIDAS, etc."}
        </div>

        <div className="pm-modal-grid" style={{ display: "grid", gap: "12px" }}>
          <label>
            <span>{zh ? "流名称（中文）" : "Flow Name (Chinese)"}</span>
            <input
              value={flowName}
              onChange={(e) => { setFlowName(e.target.value); clearCandidatesOnError(); }}
              placeholder={zh ? "请输入中文名称" : "Enter Chinese name"}
            />
          </label>

          <label>
            <span>{zh ? "流名称（英文）" : "Flow Name (English)"}</span>
            <input
              value={flowNameEn}
              onChange={(e) => { setFlowNameEn(e.target.value); clearCandidatesOnError(); }}
              placeholder={zh ? "请输入英文名称（可选）" : "Enter English name (optional)"}
            />
          </label>

          <label>
            <span>{zh ? "流类型" : "Flow Type"}</span>
            <select value={flowType} onChange={(e) => { setFlowType(e.target.value as CustomFlowType); clearCandidatesOnError(); }}>
              <option value="intermediate_flow">{uiLanguage === "zh" ? "中间流" : "Intermediate Flow"}</option>
              <option value="product_flow">{tFlowType("product_flow")}</option>
              <option value="waste_flow">{tFlowType("waste_flow")}</option>
            </select>
          </label>

          <label>
            <span>{zh ? "TIDAS 兼容" : "TIDAS Compatible"}</span>
            <select
              value={tidasCompatible || sourcePolicy === "tidas_compliant" ? "yes" : "no"}
              disabled={sourcePolicy === "tidas_compliant"}
              onChange={(e) => setTidasCompatible(e.target.value === "yes")}
            >
              <option value="no">{zh ? "否" : "No"}</option>
              <option value="yes">{zh ? "是" : "Yes"}</option>
            </select>
          </label>

          <label>
            <span>{zh ? "单位组" : "Unit Group"}</span>
            <select
              value={unitGroupUuid}
              onChange={(e) => { setUnitGroupUuid(e.target.value); clearCandidatesOnError(); }}
            >
              <option value="">{zh ? "请选择单位组" : "Select unit group"}</option>
              {availableUnitGroups.map((group) => (
                <option key={group} value={group}>
                  {unitGroupLabel(group)}
                </option>
              ))}
            </select>
          </label>

          <label>
            <span>{zh ? "默认单位" : "Default Unit"}</span>
            <select
              value={defaultUnit}
              onChange={(e) => { setDefaultUnit(e.target.value); clearCandidatesOnError(); }}
              disabled={!unitGroupUuid}
            >
              <option value="">{zh ? "请选择单位" : "Select unit"}</option>
              {unitGroupUuid &&
                unitsForGroup(unitGroupUuid).map((u) => (
                  <option key={u.unit_name} value={u.unit_name}>
                    {u.unit_name}{u.is_reference ? " (ref)" : ""}
                  </option>
                ))}
            </select>
          </label>

          <label>
            <span>{zh ? "分类" : "Category"}</span>
            {loadingCategories ? (
              <select disabled>
                <option value="">{zh ? "加载中..." : "Loading..."}</option>
              </select>
            ) : categories.length > 0 ? (
              <select
                value={category}
                onChange={(e) => { setCategory(e.target.value); clearCandidatesOnError(); }}
              >
                <option value="">{zh ? "不选择分类（可选）" : "No category (optional)"}</option>
                {categories.map((cat) => (
                  <option key={cat} value={cat}>
                    {cat}
                  </option>
                ))}
              </select>
            ) : (
              <select disabled>
                <option value="">{zh ? "暂无可用分类" : "No categories available"}</option>
              </select>
            )}
          </label>
        </div>

        {candidates.length > 0 && (
          <div style={{ marginTop: "16px" }}>
            <div className="pm-warning" style={{ marginBottom: "12px" }}>
              {zh
                ? "检测到名称重复的已有 Flow，建议优先复用："
                : "Duplicate flow names detected. We recommend reusing an existing flow:"}
            </div>
            <div style={{ maxHeight: "200px", overflowY: "auto", border: "1px solid #ddd", borderRadius: "4px" }}>
              {candidates.map((c, idx) => (
                <div
                  key={c.flow_uuid}
                  style={{
                    padding: "8px 12px",
                    borderBottom: idx < candidates.length - 1 ? "1px solid #eee" : "none",
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                  }}
                >
                  <div>
                    <div style={{ fontWeight: 500 }}>
                      {uiLanguage === "en" && c.flow_name_en ? c.flow_name_en : c.flow_name}
                    </div>
                    <div style={{ fontSize: "12px", color: "#666" }}>
                      {tFlowType(c.flow_type)} | {c.unit_group}/{c.default_unit} | {c.category || c.compartment || "-"}
                    </div>
                  </div>
                  <button
                    type="button"
                    className="pm-link-btn primary"
                    onClick={() => handleUseCandidate(c)}
                  >
                    {zh ? "引用该 Flow" : "Use this Flow"}
                  </button>
                </div>
              ))}
            </div>
            <div style={{ marginTop: "12px" }}>
              <button type="button" className="pm-primary-btn" onClick={handleConfirmCreate} disabled={loading}>
                {zh ? "仍然新建" : "Create Anyway"}
              </button>
            </div>
          </div>
        )}

        {errorText && <div className="pm-error" style={{ marginTop: "12px" }}>{errorText}</div>}

        <div className="pm-modal-actions" style={{ marginTop: "16px" }}>
          <button type="button" className="pm-ghost-btn" onClick={onClose} disabled={loading}>
            {zh ? "取消" : "Cancel"}
          </button>
          <button type="button" onClick={() => void handleSubmit()} disabled={loading || candidates.length > 0}>
            {loading ? (zh ? "创建中..." : "Creating...") : (zh ? "创建" : "Create")}
          </button>
        </div>
      </div>
    </div>
  );
}
