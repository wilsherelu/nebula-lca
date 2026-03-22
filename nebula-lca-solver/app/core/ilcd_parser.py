import json
import os
import re
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}"
)


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def first_dict(value) -> dict:
    if isinstance(value, list):
        return value[0] if value else {}
    return value if isinstance(value, dict) else {}


def as_list(value) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def safe_text(value, prefer_lang: str = "zh") -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        if not value:
            return ""
        if prefer_lang:
            for item in value:
                if isinstance(item, dict) and item.get("@xml:lang") == prefer_lang:
                    return str(item.get("#text", ""))
        return safe_text(value[0], prefer_lang=prefer_lang)
    if isinstance(value, dict):
        return str(value.get("#text", ""))
    return str(value)


def guess_uuid_from_filename(name: str) -> str:
    match = UUID_RE.search(name)
    return match.group(0) if match else ""


def load_lifecycle_model_links(lcm_data: dict, prefer_lang: str = "zh"):
    data = first_dict(lcm_data)
    lcm = data.get("lifeCycleModelDataSet", {}) or {}

    inst_path_1 = (
        lcm.get("lifeCycleModelInformation", {})
        .get("technology", {})
        .get("processes", {})
        .get("processInstance", None)
    )
    inst_path_2 = lcm.get("processes", {}).get("processInstance", None)
    instances = as_list(inst_path_1 if inst_path_1 is not None else inst_path_2)

    internal_id_to_uuid: Dict[str, str] = {}
    uuid_to_name: Dict[str, str] = {}

    for inst in instances:
        if not isinstance(inst, dict):
            continue
        internal_id = str(inst.get("@dataSetInternalID", "")).strip()
        ref = inst.get("referenceToProcess", {}) or {}
        proc_uuid = str(ref.get("@refObjectId", "")).strip()
        short_desc = ref.get("common:shortDescription", "")
        proc_name = safe_text(short_desc, prefer_lang=prefer_lang).strip()
        if internal_id and proc_uuid:
            internal_id_to_uuid[internal_id] = proc_uuid
            if proc_name:
                uuid_to_name[proc_uuid] = proc_name

    out_links: Dict[Tuple[str, str], set] = {}
    in_links: Dict[Tuple[str, str], set] = {}

    for inst in instances:
        if not isinstance(inst, dict):
            continue
        from_internal = str(inst.get("@dataSetInternalID", "")).strip()
        from_uuid = internal_id_to_uuid.get(from_internal, "")
        if not from_uuid:
            continue
        connections = inst.get("connections", {}) or {}
        output_exchanges = as_list(connections.get("outputExchange"))

        for oe in output_exchanges:
            if not isinstance(oe, dict):
                continue
            flow_uuid = (
                oe.get("@flowUUID", "")
                or oe.get("@flowUuid", "")
                or oe.get("flowUUID", "")
                or oe.get("flowUuid", "")
            )
            flow_uuid = str(flow_uuid).strip()
            if not flow_uuid:
                continue
            downstream_list = as_list(oe.get("downstreamProcess"))
            for dp in downstream_list:
                if not isinstance(dp, dict):
                    continue
                to_internal = str(dp.get("@id", "")).strip()
                to_uuid = internal_id_to_uuid.get(to_internal, "")
                if not to_uuid:
                    continue
                out_links.setdefault((from_uuid, flow_uuid), set()).add(to_uuid)
                in_links.setdefault((to_uuid, flow_uuid), set()).add(from_uuid)

    return internal_id_to_uuid, uuid_to_name, out_links, in_links


def load_process_dataset(process_data: dict, prefer_lang: str = "zh"):
    data = first_dict(process_data)
    pds = data.get("processDataSet", {}) or {}

    ref_flow_internal_id = (
        pds.get("processInformation", {})
        .get("quantitativeReference", {})
        .get("referenceToReferenceFlow", "")
    )
    ref_flow_internal_id = str(ref_flow_internal_id).strip()

    process_uuid = (
        pds.get("processInformation", {})
        .get("dataSetInformation", {})
        .get("common:UUID", "")
    )
    process_uuid = str(process_uuid).strip() if process_uuid else ""

    base_name_block = (
        pds.get("processInformation", {})
        .get("dataSetInformation", {})
        .get("name", {})
        .get("baseName", "")
    )
    process_name = safe_text(base_name_block, prefer_lang=prefer_lang).strip()

    exchanges = as_list(pds.get("exchanges", {}).get("exchange", []))

    rows = []
    for ex in exchanges:
        if not isinstance(ex, dict):
            continue
        ex_internal_id = str(ex.get("@dataSetInternalID", "")).strip()
        flow_ref = ex.get("referenceToFlowDataSet", {}) or {}
        flow_uuid = str(flow_ref.get("@refObjectId", "")).strip()
        flow_name_block = flow_ref.get("common:shortDescription", "")
        flow_name = safe_text(flow_name_block, prefer_lang=prefer_lang).strip()
        direction = str(ex.get("exchangeDirection", "")).strip().lower()
        amount = ex.get("meanAmount", "")
        is_reference = ex_internal_id == ref_flow_internal_id

        rows.append(
            {
                "process_uuid": process_uuid,
                "process_name": process_name,
                "exchange_internal_id": ex_internal_id,
                "flow_uuid": flow_uuid,
                "flow_name": flow_name,
                "direction": direction,
                "amount": amount,
                "unit": "",
                "is_reference_product": is_reference,
            }
        )

    return process_uuid, process_name, rows


def build_inventory_rows(
    lcm_data: dict,
    process_items: Sequence[Tuple[Optional[str], dict]],
    prefer_lang: str = "zh",
):
    internal_id_to_uuid, uuid_to_name, out_links, in_links = load_lifecycle_model_links(
        lcm_data, prefer_lang=prefer_lang
    )
    model_process_uuids = set(internal_id_to_uuid.values())
    results: List[dict] = []

    for source_name, pdata in process_items:
        process_uuid, process_name, rows = load_process_dataset(
            pdata, prefer_lang=prefer_lang
        )
        if not rows:
            continue
        if not process_uuid:
            if source_name:
                process_uuid = guess_uuid_from_filename(source_name)
            for row in rows:
                row["process_uuid"] = process_uuid
        if not process_uuid or process_uuid not in model_process_uuids:
            continue
        if not process_name:
            process_name = uuid_to_name.get(process_uuid, "")
            for row in rows:
                row["process_name"] = process_name

        for row in rows:
            if row.get("direction") == "input":
                links = in_links.get((process_uuid, row.get("flow_uuid", "")), set())
                row["upstream_process_uuid"] = ";".join(sorted(links)) if links else ""
            else:
                row["upstream_process_uuid"] = ""
            if row.get("direction") == "output":
                links = out_links.get((process_uuid, row.get("flow_uuid", "")), set())
                row["downstream_process_uuid"] = ";".join(sorted(links)) if links else ""
            else:
                row["downstream_process_uuid"] = ""
            results.append(row)

    return results


def load_process_items_from_dir(directory: str) -> List[Tuple[Optional[str], dict]]:
    items: List[Tuple[Optional[str], dict]] = []
    for name in os.listdir(directory):
        if not name.lower().endswith(".json"):
            continue
        path = os.path.join(directory, name)
        items.append((name, load_json(path)))
    return items
