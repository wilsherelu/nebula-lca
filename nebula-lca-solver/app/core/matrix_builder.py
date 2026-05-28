import csv
import json
import os
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from app.core.ilcd_parser import build_inventory_rows


def build_exchange_rows_from_ilcd(
    lcm_data: dict,
    process_items: Sequence[Tuple[Optional[str], dict]],
    prefer_lang: str = "zh",
) -> List[dict]:
    return build_inventory_rows(lcm_data, process_items, prefer_lang=prefer_lang)


def load_snapshot(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def build_matrices_from_snapshot(
    snapshot: dict,
    characterization: Optional[Dict[str, float]] = None,
    elementary_flow_types: Optional[Iterable[str]] = None,
) -> dict:
    processes = snapshot.get("processes", []) or []
    flows = snapshot.get("flows", []) or []
    exchanges = snapshot.get("exchanges", []) or []
    links = snapshot.get("links", []) or []

    process_ids = sorted(
        p.get("process_uuid", "") for p in processes if p.get("process_uuid")
    )
    flow_by_uuid = {f.get("flow_uuid", ""): f for f in flows if f.get("flow_uuid")}
    process_by_uuid = {p.get("process_uuid", ""): p for p in processes if p.get("process_uuid")}

    exchanges_by_process: Dict[str, List[dict]] = {}
    product_conversion_factor_by_exchange_id: Dict[str, float] = {}
    allocation_scale_by_exchange_id: Dict[str, float] = {}
    for ex in exchanges:
        proc_uuid = ex.get("process_uuid", "")
        if not proc_uuid:
            continue
        exchanges_by_process.setdefault(proc_uuid, []).append(ex)
        exchange_id = str(ex.get("exchange_id", "")).strip()
        conversion_factor = _to_float(ex.get("product_conversion_factor"))
        if exchange_id and conversion_factor is not None:
            product_conversion_factor_by_exchange_id[exchange_id] = conversion_factor
        scale = _to_float(ex.get("allocation_scale"))
        if exchange_id and scale is not None:
            allocation_scale_by_exchange_id[exchange_id] = scale

    issues: List[str] = []
    ref_exchange_by_process: Dict[str, dict] = {}
    ref_amount_by_process: Dict[str, float] = {}
    allocation_total_by_process: Dict[str, float] = {}
    allocation_active_by_process: Dict[str, bool] = {}

    for proc_uuid in process_ids:
        ref_exchange_id = str(process_by_uuid[proc_uuid].get("reference_product_flow_uuid", "")).strip()
        if not ref_exchange_id:
            issues.append(f"process {proc_uuid} missing reference_product_flow_uuid")
            ref_amount_by_process[proc_uuid] = 1.0
            continue
        ref_exchange = None
        for ex in exchanges_by_process.get(proc_uuid, []):
            if str(ex.get("exchange_id", "")).strip() == ref_exchange_id:
                ref_exchange = ex
                break
        if ref_exchange is None:
            issues.append(
                f"process {proc_uuid} missing exchange for reference_product_flow_uuid {ref_exchange_id}"
            )
            ref_amount_by_process[proc_uuid] = 1.0
            continue
        ref_exchange_by_process[proc_uuid] = ref_exchange
        amount = _to_float(ref_exchange.get("amount"))
        if amount <= 0:
            issues.append(f"process {proc_uuid} has non-positive reference amount")
            amount = 1.0
        ref_amount_by_process[proc_uuid] = amount

        allocation_active = False
        allocation_total = 0.0
        allocation_fraction_total = 0.0
        allocation_fraction_count = 0
        allocation_output_count = 0
        allocation_unit_groups = set()
        allocation_weight_active = False
        for ex in exchanges_by_process.get(proc_uuid, []):
            if str(ex.get("direction", "")).lower() != "output":
                continue
            if ex.get("allocation_fraction") is None:
                continue
            allocation_active = True
            allocation_output_count += 1
            allocation_fraction = _to_float(ex.get("allocation_fraction"))
            if allocation_fraction is not None:
                allocation_fraction_count += 1
                allocation_fraction_total += allocation_fraction
            flow_uuid = ex.get("flow_uuid", "")
            flow = flow_by_uuid.get(flow_uuid)
            if not flow:
                continue
            flow_type = flow.get("flow_type")
            if flow_type == "Elementary flow":
                issues.append(
                    f"process {proc_uuid} allocation output {flow_uuid} is elementary flow"
                )
            allocation_weight = _to_float(ex.get("allocation_weight"))
            if allocation_weight is not None:
                allocation_weight_active = True
            unit_group_uuid = ex.get("allocation_weight_unit_group") if allocation_weight is not None else flow.get("unit_group_uuid")
            if unit_group_uuid:
                unit_group_uuid = str(unit_group_uuid)
                if unit_group_uuid.startswith("unit_group::"):
                    unit_group_uuid = unit_group_uuid.removeprefix("unit_group::")
                allocation_unit_groups.add(unit_group_uuid)
            else:
                issues.append(
                    f"process {proc_uuid} allocation output {flow_uuid} missing unit group"
                )
            amount = _to_float(ex.get("amount"))
            if amount is None:
                amount = allocation_weight
            if amount is not None:
                allocation_total += amount

        # Fallback: no allocation outputs defined -> use reference exchange as product
        if not allocation_active:
            ref_amount = _to_float(ref_exchange.get("amount"))
            if ref_amount is None or ref_amount <= 0:
                issues.append(f"process {proc_uuid} reference amount is missing or non-positive")
                ref_amount = 1.0
            allocation_total = ref_amount
            allocation_active = True
        allocation_active_by_process[proc_uuid] = allocation_active
        if allocation_active:
            if allocation_total <= 0:
                issues.append(f"process {proc_uuid} allocation total is zero")
                allocation_total = 1.0
            if len(allocation_unit_groups) > 1 and not allocation_weight_active:
                complete_manual_fractions = (
                    allocation_output_count > 1
                    and allocation_fraction_count == allocation_output_count
                    and abs(allocation_fraction_total - 1.0) <= 1e-6
                )
                if not complete_manual_fractions:
                    issues.append(
                        f"process {proc_uuid} allocation outputs have inconsistent unit groups"
                    )
        allocation_total_by_process[proc_uuid] = allocation_total

    a_entries: Dict[Tuple[str, str], float] = {}
    for proc_uuid in process_ids:
        a_entries[(proc_uuid, proc_uuid)] = 1.0

    for link in links:
        consumer = link.get("consumer_process_uuid", "")
        provider = link.get("provider_process_uuid", "")
        flow_uuid = link.get("flow_uuid", "")
        if consumer not in process_by_uuid or provider not in process_by_uuid:
            issues.append(f"link references unknown process: {consumer} -> {provider}")
            continue
        quantity_mode = str(link.get("quantity_mode", "")).lower()
        if quantity_mode == "dual":
            input_amount = _to_float(link.get("consumer_amount"))
        else:
            input_amount = _to_float(link.get("amount"))
        if input_amount is None or input_amount <= 0:
            input_amount = _sum_exchange_amount(
                exchanges_by_process.get(consumer, []),
                flow_uuid=flow_uuid,
                direction="input",
            )
        if input_amount is None:
            issues.append(
                f"missing input exchange for link consumer {consumer} flow {flow_uuid}"
            )
            continue
        provider_exchange_id = str(link.get("provider_product_exchange_id") or "").strip()
        provider_conversion_factor = _to_float(link.get("provider_product_conversion_factor"))
        if provider_conversion_factor is None and provider_exchange_id:
            provider_conversion_factor = product_conversion_factor_by_exchange_id.get(provider_exchange_id)
        provider_scale = None
        if provider_conversion_factor is not None:
            if provider_conversion_factor <= 0:
                issues.append(
                    f"link {provider} -> {consumer} has non-positive provider product conversion factor"
                )
                continue
            provider_scale = 1.0 / provider_conversion_factor
        if provider_scale is None:
            provider_scale = _to_float(link.get("provider_allocation_scale"))
        if provider_scale is None and provider_exchange_id:
            provider_scale = allocation_scale_by_exchange_id.get(provider_exchange_id)
        if provider_scale is None:
            provider_scale = 1.0
        if provider_scale < 0:
            issues.append(
                f"link {provider} -> {consumer} has negative provider allocation scale"
            )
            continue
        denom = allocation_total_by_process.get(consumer, 0.0)
        if denom == 0:
            issues.append(f"process {consumer} allocation total is zero")
            continue
        coeff = -(input_amount * provider_scale) / denom
        a_entries[(provider, consumer)] = a_entries.get((provider, consumer), 0.0) + coeff

    b_matrix = build_b_matrix_from_snapshot(
        snapshot,
        allocation_total_by_process,
        elementary_flow_types=elementary_flow_types,
        scope="observed",
        issues=issues,
    )
    env_flow_ids = b_matrix["rows"]

    c_entries: Dict[Tuple[str, str], float] = {}
    indicator_ids: List[str] = []
    if characterization:
        indicator_ids = ["indicator_1"]
        for flow_uuid in env_flow_ids:
            factor = _to_float(characterization.get(flow_uuid))
            if factor is None or factor == 0:
                continue
            c_entries[(indicator_ids[0], flow_uuid)] = factor

    return {
        "A": _build_matrix(process_ids, process_ids, a_entries),
        "B": b_matrix,
        "C": _build_matrix(indicator_ids, env_flow_ids, c_entries),
        "allocation_active": allocation_active_by_process,
        "allocation_total": allocation_total_by_process,
        "process_lookup": _build_process_lookup(processes),
        "flow_lookup": _build_flow_lookup(flows),
        "reference_products": {
            proc_uuid: _reference_summary(ref_exchange_by_process.get(proc_uuid))
            for proc_uuid in process_ids
        },
        "issues": issues,
    }


def build_matrices_from_mmr(mmr) -> dict:
    a_matrix = _matrix_from_mmr_sparse(mmr.A, list(mmr.process_index), list(mmr.process_index))
    b_matrix = _matrix_from_mmr_sparse(
        mmr.B,
        list(mmr.elementary_flow_index),
        list(mmr.process_index),
    )
    return {
        "A": a_matrix,
        "B": b_matrix,
        "C": _build_matrix([], [], {}),
        "allocation_active": {},
        "allocation_total": {},
        "process_lookup": _build_process_lookup_from_index(mmr.process_index),
        "flow_lookup": _build_flow_lookup_from_index(mmr.elementary_flow_index),
        "reference_products": {},
        "issues": [],
    }


def _sum_exchange_amount(
    exchanges: Sequence[dict],
    flow_uuid: str,
    direction: str,
) -> Optional[float]:
    total = 0.0
    found = False
    for ex in exchanges:
        if ex.get("flow_uuid") != flow_uuid:
            continue
        if str(ex.get("direction", "")).lower() != direction:
            continue
        amount = _to_float(ex.get("amount"))
        if amount is None:
            continue
        total += amount
        found = True
    return total if found else None


def build_b_matrix_from_snapshot(
    snapshot: dict,
    allocation_total_by_process: Dict[str, float],
    elementary_flow_types: Optional[Iterable[str]] = None,
    scope: str = "observed",
    issues: Optional[List[str]] = None,
) -> dict:
    flows = snapshot.get("flows", []) or []
    exchanges = snapshot.get("exchanges", []) or []
    processes = snapshot.get("processes", []) or []

    if issues is None:
        issues = []

    flow_by_uuid = {f.get("flow_uuid", ""): f for f in flows if f.get("flow_uuid")}
    process_by_uuid = {p.get("process_uuid", ""): p for p in processes if p.get("process_uuid")}

    env_types = set(elementary_flow_types or ("Elementary flow",))
    env_flow_set = set()

    if scope == "all":
        for flow in flows:
            if flow.get("flow_type") in env_types and flow.get("flow_uuid"):
                env_flow_set.add(flow["flow_uuid"])
    else:
        for ex in exchanges:
            flow_uuid = ex.get("flow_uuid", "")
            flow = flow_by_uuid.get(flow_uuid)
            if flow is None:
                if flow_uuid:
                    issues.append(f"exchange references unknown flow {flow_uuid}")
                continue
            if flow.get("flow_type") in env_types:
                env_flow_set.add(flow_uuid)

    env_flow_ids = sorted(
        f.get("flow_uuid") for f in flows if f.get("flow_uuid") in env_flow_set
    )

    b_entries: Dict[Tuple[str, str], float] = {}
    for ex in exchanges:
        flow_uuid = ex.get("flow_uuid", "")
        process_uuid = ex.get("process_uuid", "")
        if flow_uuid not in env_flow_set or process_uuid not in process_by_uuid:
            continue
        amount = _to_float(ex.get("amount"))
        if amount is None:
            continue
        denom = allocation_total_by_process.get(process_uuid, 0.0)
        if denom == 0:
            issues.append(f"process {process_uuid} allocation total is zero")
            continue
        coeff = amount / denom
        key = (flow_uuid, process_uuid)
        b_entries[key] = b_entries.get(key, 0.0) + coeff

    process_ids = sorted(
        p.get("process_uuid", "") for p in processes if p.get("process_uuid")
    )
    return _build_matrix(env_flow_ids, process_ids, b_entries)


def build_c_matrix_from_ef31(
    ef_dir: str,
    b_matrix: dict,
    lcia_methods: Optional[Sequence[str]] = None,
    issues: Optional[List[str]] = None,
    report_missing: bool = True,
) -> dict:
    if issues is None:
        issues = []

    flow_index_path = os.path.join(ef_dir, "flow_index.csv")
    indicator_index_path = os.path.join(ef_dir, "indicator_index.csv")
    factors_path = os.path.join(ef_dir, "lcia_factors.csv")

    flow_uuid_to_index: Dict[str, int] = {}
    with open(flow_index_path, "r", encoding="utf-8") as handle:
        reader = csv.reader(handle, delimiter=";")
        header = next(reader, [])
        header = _strip_bom_header(header)
        col_map = {name: idx for idx, name in enumerate(header)}
        flow_uuid_idx = col_map.get("FlowUUID")
        flow_index_idx = col_map.get("flow_index")
        if flow_uuid_idx is None or flow_index_idx is None:
            issues.append("EF3.1 flow_index.csv missing required headers")
        else:
            for row in reader:
                flow_uuid = row[flow_uuid_idx].strip() if flow_uuid_idx < len(row) else ""
                idx = _to_int(row[flow_index_idx] if flow_index_idx < len(row) else None)
                if flow_uuid and idx is not None:
                    flow_uuid_to_index[flow_uuid] = idx

    indicator_ids: List[int] = []
    indicator_lookup: Dict[int, dict] = {}
    with open(indicator_index_path, "r", encoding="utf-8") as handle:
        reader = csv.reader(handle, delimiter=";")
        header = next(reader, [])
        header = _strip_bom_header(header)
        col_map = {name: idx for idx, name in enumerate(header)}
        indicator_idx_col = col_map.get("indicator_index")
        if indicator_idx_col is None:
            issues.append("EF3.1 indicator_index.csv missing indicator_index header")
        else:
            for row in reader:
                idx = _to_int(row[indicator_idx_col] if indicator_idx_col < len(row) else None)
                if idx is None:
                    continue
                indicator_ids.append(idx)
                indicator_lookup[idx] = {
                    "method_en": _get_csv_value(row, col_map, "method_en"),
                    "method_zh": _get_csv_value(row, col_map, "method_zh"),
                    "indicator_en": _get_csv_value(row, col_map, "indicator_en"),
                    "indicator_zh": _get_csv_value(row, col_map, "indicator_zh"),
                    "ecoinvent_category": _get_csv_value(row, col_map, "ecoinvent_category"),
                }

    selected_methods = {
        str(method).strip()
        for method in (lcia_methods or [])
        if str(method).strip()
    }
    if selected_methods:
        matched_indicator_ids = [
            idx
            for idx, info in indicator_lookup.items()
            if str(info.get("method_en", "")).strip() in selected_methods
            or str(info.get("method_zh", "")).strip() in selected_methods
        ]
        # Legacy Tiangong EF3.1 runtime stores LCIA categories such as
        # "Climate change" in method_en instead of the family name "EF v3.1".
        # Treat EF v3.1 as the whole built-in method family when no explicit
        # method row matches, while still filtering generated ecoinvent runtime
        # CSVs where method_en is "EF v3.1" / "EF v3.1 no LT".
        if matched_indicator_ids:
            matched_set = set(matched_indicator_ids)
            indicator_lookup = {
                idx: info
                for idx, info in indicator_lookup.items()
                if idx in matched_set
            }
            indicator_ids = [idx for idx in indicator_ids if idx in indicator_lookup]
        elif "EF v3.1" not in selected_methods:
            indicator_lookup = {}
            indicator_ids = []

    indicator_ids.sort()
    indicator_id_set = set(indicator_ids)

    b_flow_ids = b_matrix.get("rows", [])
    flow_index_to_uuid: Dict[int, str] = {}
    for flow_uuid in b_flow_ids:
        idx = flow_uuid_to_index.get(flow_uuid)
        if idx is None:
            continue
        flow_index_to_uuid[idx] = flow_uuid

    target_flow_indices = set(flow_index_to_uuid.keys())

    c_entries: Dict[Tuple[int, str], float] = {}
    with open(factors_path, "r", encoding="utf-8") as handle:
        reader = csv.reader(handle, delimiter=";")
        header = next(reader, [])
        header = _strip_bom_header(header)
        col_map = {name: idx for idx, name in enumerate(header)}
        row_idx_col = col_map.get("row")
        col_idx_col = col_map.get("column")
        coeff_col = col_map.get("coefficient")
        if row_idx_col is None or col_idx_col is None or coeff_col is None:
            issues.append("EF3.1 lcia_factors.csv missing required headers")
        else:
            for row in reader:
                indicator_idx = _to_int(row[row_idx_col] if row_idx_col < len(row) else None)
                flow_idx = _to_int(row[col_idx_col] if col_idx_col < len(row) else None)
                if indicator_idx is None or flow_idx is None:
                    continue
                if indicator_idx not in indicator_id_set:
                    continue
                if flow_idx not in target_flow_indices:
                    continue
                coeff = _to_float(row[coeff_col] if coeff_col < len(row) else None)
                if coeff is None or coeff == 0:
                    continue
                flow_uuid = flow_index_to_uuid[flow_idx]
                key = (indicator_idx, flow_uuid)
                c_entries[key] = c_entries.get(key, 0.0) + coeff

    missing = [flow_uuid for flow_uuid in b_flow_ids if flow_uuid not in flow_uuid_to_index]
    if missing and report_missing:
        issues.append(f"EF3.1 missing {len(missing)} flow_uuids from B matrix")

    return {
        "C": _build_matrix(indicator_ids, b_flow_ids, c_entries),
        "indicator_lookup": indicator_lookup,
        "matched_flow_uuids": set(flow_index_to_uuid.values()),
        "runtime_flow_uuids": set(flow_uuid_to_index.keys()),
    }


def build_c_matrix_from_ef31_sources(
    ef_dirs: Sequence[str],
    b_matrix: dict,
    lcia_methods: Optional[Sequence[str]] = None,
    issues: Optional[List[str]] = None,
) -> dict:
    """Build one canonical EF3.1 C matrix from multiple runtime CSV sources.

    This lets Tiangong/TIDAS EF3.1 elementary flows and ecoinvent elementary
    flows contribute to the same 25 EF3.1 indicator rows when their flow UUID
    libraries differ.  Indicator rows are canonicalized by ecoinvent_category.
    """
    if issues is None:
        issues = []

    existing_dirs: List[str] = []
    seen_dirs: set[str] = set()
    for ef_dir in ef_dirs:
        path = os.path.abspath(str(ef_dir))
        if path in seen_dirs:
            continue
        if not os.path.exists(path):
            continue
        seen_dirs.add(path)
        existing_dirs.append(path)

    canonical_order: List[str] = []
    canonical_lookup: Dict[str, dict] = {}
    canonical_entries: Dict[Tuple[str, str], float] = {}
    matched_flow_uuids: set[str] = set()
    runtime_flow_uuids: set[str] = set()

    for ef_dir in existing_dirs:
        pack = build_c_matrix_from_ef31(
            ef_dir,
            b_matrix,
            lcia_methods=lcia_methods,
            issues=issues,
            report_missing=False,
        )
        runtime_flow_uuids.update(pack.get("runtime_flow_uuids", set()))
        matched_flow_uuids.update(pack.get("matched_flow_uuids", set()))
        lookup = pack.get("indicator_lookup", {})
        c_matrix = pack.get("C", {})

        for row_id in c_matrix.get("rows", []) or []:
            info = lookup.get(row_id, {})
            canonical_key = _canonical_ef31_indicator_key(info)
            if not canonical_key:
                continue
            if canonical_key not in canonical_lookup:
                canonical_order.append(canonical_key)
                canonical_lookup[canonical_key] = _canonical_ef31_indicator_info(info)

        for entry in c_matrix.get("data", []) or []:
            row_id = entry.get("row")
            flow_uuid = str(entry.get("col") or "")
            info = lookup.get(row_id, {})
            canonical_key = _canonical_ef31_indicator_key(info)
            if not canonical_key or not flow_uuid:
                continue
            key = (canonical_key, flow_uuid)
            if key in canonical_entries:
                continue
            value = _to_float(entry.get("value"))
            if value is None or value == 0:
                continue
            canonical_entries[key] = value

    b_flow_ids = b_matrix.get("rows", []) or []
    missing = [flow_uuid for flow_uuid in b_flow_ids if flow_uuid not in runtime_flow_uuids]
    if missing:
        issues.append(f"EF3.1 missing {len(missing)} flow_uuids from B matrix")

    canonical_index = {key: idx for idx, key in enumerate(canonical_order)}
    row_ids = list(range(len(canonical_order)))
    c_entries_by_index: Dict[Tuple[int, str], float] = {}
    for (canonical_key, flow_uuid), value in canonical_entries.items():
        row_idx = canonical_index.get(canonical_key)
        if row_idx is None:
            continue
        c_entries_by_index[(row_idx, flow_uuid)] = value

    indicator_lookup = {
        idx: {
            **canonical_lookup[canonical_key],
            "indicator_index": idx,
            "canonical_indicator_key": canonical_key,
        }
        for canonical_key, idx in canonical_index.items()
    }

    return {
        "C": _build_matrix(row_ids, b_flow_ids, c_entries_by_index),
        "indicator_lookup": indicator_lookup,
        "matched_flow_uuids": matched_flow_uuids,
        "runtime_flow_uuids": runtime_flow_uuids,
    }


def _canonical_ef31_indicator_key(info: dict) -> str:
    category = str(info.get("ecoinvent_category") or "").strip().lower()
    if category:
        return category
    method = str(info.get("method_en") or info.get("method_zh") or "").strip().lower()
    indicator = str(info.get("indicator_en") or info.get("indicator_zh") or "").strip().lower()
    return f"{method}|{indicator}" if method or indicator else ""


def _canonical_ef31_indicator_info(info: dict) -> dict:
    category = str(info.get("ecoinvent_category") or "").strip()
    method_en = str(info.get("method_en") or "").strip()
    indicator_en = str(info.get("indicator_en") or "").strip()
    method_zh = str(info.get("method_zh") or "").strip()
    indicator_zh = str(info.get("indicator_zh") or "").strip()
    return {
        "method_en": "EF v3.1",
        "method_zh": "EF v3.1",
        "indicator_en": indicator_en or method_en or category,
        "indicator_zh": indicator_zh or method_zh or indicator_en or method_en or category,
        "ecoinvent_category": category,
    }


def _to_float(value: Optional[object]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Optional[object]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _strip_bom_header(header: List[str]) -> List[str]:
    if not header:
        return header
    header[0] = header[0].lstrip("\ufeff")
    return header


def _get_csv_value(row: List[str], col_map: Dict[str, int], key: str) -> str:
    idx = col_map.get(key)
    if idx is None or idx >= len(row):
        return ""
    return row[idx]


def _reference_summary(exchange: Optional[dict]) -> dict:
    if not exchange:
        return {"exchange_id": "", "flow_uuid": "", "amount": None}
    return {
        "exchange_id": str(exchange.get("exchange_id", "")),
        "flow_uuid": exchange.get("flow_uuid", ""),
        "amount": exchange.get("amount"),
    }


def _build_matrix(
    row_ids: Sequence[str],
    col_ids: Sequence[str],
    entries: Dict[Tuple[str, str], float],
) -> dict:
    row_index = {row_id: idx for idx, row_id in enumerate(row_ids)}
    col_index = {col_id: idx for idx, col_id in enumerate(col_ids)}
    data = []
    for (row_id, col_id), value in entries.items():
        if value == 0:
            continue
        data.append(
            {
                "row": row_id,
                "col": col_id,
                "row_index": row_index.get(row_id, -1),
                "col_index": col_index.get(col_id, -1),
                "value": value,
            }
        )
    data.sort(key=lambda item: (item["row_index"], item["col_index"]))
    return {
        "rows": list(row_ids),
        "cols": list(col_ids),
        "row_index": row_index,
        "col_index": col_index,
        "shape": [len(row_ids), len(col_ids)],
        "data": data,
    }


def _matrix_from_mmr_sparse(
    sparse,
    row_ids: Sequence[str],
    col_ids: Sequence[str],
) -> dict:
    row_index = {row_id: idx for idx, row_id in enumerate(row_ids)}
    col_index = {col_id: idx for idx, col_id in enumerate(col_ids)}
    data = []
    for entry in getattr(sparse, "entries", []):
        if entry.value == 0:
            continue
        row_idx = int(entry.row)
        col_idx = int(entry.col)
        row_id = row_ids[row_idx] if row_idx < len(row_ids) else ""
        col_id = col_ids[col_idx] if col_idx < len(col_ids) else ""
        data.append(
            {
                "row": row_id,
                "col": col_id,
                "row_index": row_idx,
                "col_index": col_idx,
                "value": float(entry.value),
            }
        )
    data.sort(key=lambda item: (item["row_index"], item["col_index"]))
    return {
        "rows": list(row_ids),
        "cols": list(col_ids),
        "row_index": row_index,
        "col_index": col_index,
        "shape": [len(row_ids), len(col_ids)],
        "data": data,
    }


def _build_process_lookup_from_index(process_index: Sequence[str]) -> Dict[str, dict]:
    return {proc_uuid: {"process_name": ""} for proc_uuid in process_index if proc_uuid}


def _build_flow_lookup_from_index(flow_index: Sequence[str]) -> Dict[str, dict]:
    return {flow_uuid: {"flow_name": "", "flow_type": "", "unit_group_uuid": ""} for flow_uuid in flow_index if flow_uuid}


def export_matrices_to_csv(
    snapshot: dict,
    output_dir: str,
    filename_prefix: str = "",
    ef31_dir: Optional[str] = None,
) -> dict:
    os.makedirs(output_dir, exist_ok=True)
    result = build_matrices_from_snapshot(snapshot)

    process_lookup = result.get("process_lookup", {})
    flow_lookup = result.get("flow_lookup", {})

    a_matrix = result["A"]
    b_matrix = result["B"]

    _write_process_index_csv(
        os.path.join(output_dir, f"{filename_prefix}A_index.csv"),
        a_matrix["rows"],
        process_lookup,
        allocation_outputs=_build_allocation_outputs(snapshot),
    )
    _write_matrix_data_csv(
        os.path.join(output_dir, f"{filename_prefix}A_data.csv"),
        a_matrix,
    )

    _write_flow_index_csv(
        os.path.join(output_dir, f"{filename_prefix}B_rows.csv"),
        b_matrix["rows"],
        flow_lookup,
    )
    _write_matrix_data_csv(
        os.path.join(output_dir, f"{filename_prefix}B_data.csv"),
        b_matrix,
    )

    if ef31_dir:
        c_pack = build_c_matrix_from_ef31(ef31_dir, b_matrix, issues=result.get("issues"))
        c_matrix = c_pack["C"]
        _write_indicator_index_csv(
            os.path.join(output_dir, f"{filename_prefix}C_rows.csv"),
            c_matrix["rows"],
            c_pack["indicator_lookup"],
        )
        _write_flow_index_csv(
            os.path.join(output_dir, f"{filename_prefix}C_cols.csv"),
            c_matrix["cols"],
            flow_lookup,
        )
        _write_matrix_data_csv(
            os.path.join(output_dir, f"{filename_prefix}C_data.csv"),
            c_matrix,
        )

    _write_issues_csv(
        os.path.join(output_dir, f"{filename_prefix}issues.csv"),
        result.get("issues", []),
    )

    return result


def _build_process_lookup(processes: Sequence[dict]) -> Dict[str, dict]:
    lookup = {}
    for proc in processes:
        proc_uuid = proc.get("process_uuid", "")
        if not proc_uuid:
            continue
        lookup[proc_uuid] = {
            "process_name": proc.get("process_name", ""),
        }
    return lookup


def _build_flow_lookup(flows: Sequence[dict]) -> Dict[str, dict]:
    lookup = {}
    for flow in flows:
        flow_uuid = flow.get("flow_uuid", "")
        if not flow_uuid:
            continue
        lookup[flow_uuid] = {
            "flow_name": flow.get("flow_name", ""),
            "flow_type": flow.get("flow_type", ""),
            "unit_group_uuid": flow.get("unit_group_uuid", ""),
        }
    return lookup


def _write_process_index_csv(
    path: str,
    ids: Sequence[str],
    lookup: Dict[str, dict],
    allocation_outputs: Optional[Dict[str, List[str]]] = None,
) -> None:
    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["index", "process_uuid", "process_name", "allocation_output_flow_uuids"])
        for idx, proc_uuid in enumerate(ids):
            info = lookup.get(proc_uuid, {})
            outputs = (allocation_outputs or {}).get(proc_uuid, [])
            writer.writerow(
                [
                    idx,
                    proc_uuid,
                    info.get("process_name", ""),
                    ";".join(outputs),
                ]
            )


def _write_flow_index_csv(path: str, ids: Sequence[str], lookup: Dict[str, dict]) -> None:
    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["index", "flow_uuid", "flow_name", "flow_type", "unit_group_uuid"])
        for idx, flow_uuid in enumerate(ids):
            info = lookup.get(flow_uuid, {})
            writer.writerow(
                [
                    idx,
                    flow_uuid,
                    info.get("flow_name", ""),
                    info.get("flow_type", ""),
                    info.get("unit_group_uuid", ""),
                ]
            )


def _write_matrix_data_csv(path: str, matrix: dict) -> None:
    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["row_index", "col_index", "row_id", "col_id", "value"])
        for entry in matrix.get("data", []):
            writer.writerow(
                [
                    entry.get("row_index", -1),
                    entry.get("col_index", -1),
                    entry.get("row", ""),
                    entry.get("col", ""),
                    entry.get("value", 0.0),
                ]
            )


def _write_indicator_index_csv(
    path: str,
    ids: Sequence[int],
    lookup: Dict[int, dict],
) -> None:
    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "index",
                "indicator_index",
                "method_en",
                "method_zh",
                "indicator_en",
                "indicator_zh",
                "ecoinvent_category",
            ]
        )
        for idx in ids:
            info = lookup.get(idx, {})
            writer.writerow(
                [
                    idx,
                    idx,
                    info.get("method_en", ""),
                    info.get("method_zh", ""),
                    info.get("indicator_en", ""),
                    info.get("indicator_zh", ""),
                    info.get("ecoinvent_category", ""),
                ]
            )


def _write_issues_csv(path: str, issues: Sequence[str]) -> None:
    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["issue"])
        for issue in issues:
            writer.writerow([issue])


def _build_allocation_outputs(snapshot: dict) -> Dict[str, List[str]]:
    processes = snapshot.get("processes", []) or []
    exchanges = snapshot.get("exchanges", []) or []
    outputs_by_process: Dict[str, List[str]] = {}

    for ex in exchanges:
        if str(ex.get("direction", "")).lower() != "output":
            continue
        if ex.get("allocation_fraction") is None:
            continue
        proc_uuid = ex.get("process_uuid", "")
        flow_uuid = ex.get("flow_uuid", "")
        if not proc_uuid or not flow_uuid:
            continue
        outputs_by_process.setdefault(proc_uuid, []).append(flow_uuid)

    # Fallback: if no allocation outputs, use reference exchange flow_uuid
    exchanges_by_process: Dict[str, List[dict]] = {}
    for ex in exchanges:
        proc_uuid = ex.get("process_uuid", "")
        if not proc_uuid:
            continue
        exchanges_by_process.setdefault(proc_uuid, []).append(ex)

    for proc in processes:
        proc_uuid = proc.get("process_uuid", "")
        if not proc_uuid or outputs_by_process.get(proc_uuid):
            continue
        ref_exchange_id = str(proc.get("reference_product_flow_uuid", "")).strip()
        if not ref_exchange_id:
            continue
        for ex in exchanges_by_process.get(proc_uuid, []):
            if str(ex.get("exchange_id", "")).strip() == ref_exchange_id:
                flow_uuid = ex.get("flow_uuid", "")
                if flow_uuid:
                    outputs_by_process.setdefault(proc_uuid, []).append(flow_uuid)
                break

    for proc_uuid, items in outputs_by_process.items():
        outputs_by_process[proc_uuid] = sorted(set(items))
    return outputs_by_process
