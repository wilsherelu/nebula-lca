from __future__ import annotations

from pathlib import Path
from typing import Iterable
import csv
import json
import zipfile
import pandas as pd
from sqlalchemy.orm import Session

from .import_schemas import FLOW_IMPORT_SPECS
from .models import FlowRecord, ReferenceProcess, UnitDefinition, UnitGroup


def _normalize_column_name(name: str) -> str:
    return str(name).strip().lower().replace("_", " ").replace("-", " ")


def _resolve_columns(
    columns: Iterable[str],
    mapping: dict[str, str] | None = None,
) -> dict[str, str]:
    mapping = mapping or {}
    normalized_input = {_normalize_column_name(c): c for c in columns}
    resolved: dict[str, str] = {}

    for spec in FLOW_IMPORT_SPECS:
        # explicit mapping has top priority
        if spec.target in mapping:
            source_col = mapping[spec.target]
            if source_col not in columns:
                raise ValueError(f"Mapped source column '{source_col}' for '{spec.target}' not found.")
            resolved[spec.target] = source_col
            continue

        found = None
        for alias in spec.aliases:
            alias_norm = _normalize_column_name(alias)
            if alias_norm in normalized_input:
                found = normalized_input[alias_norm]
                break

        if spec.required and not found:
            raise ValueError(
                f"Cannot resolve required column '{spec.target}'. "
                f"Accepted aliases: {', '.join(spec.aliases)}"
            )
        if found:
            resolved[spec.target] = found

    return resolved


def _read_table(path: Path, sheet_name: str | int | None = None) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".xlsx", ".xlsm", ".xls"}:
        return pd.read_excel(path, sheet_name=sheet_name or 0)
    raise ValueError(f"Unsupported file extension: {suffix}")


def _resolve_ef31_flow_index_path(path_like: str) -> Path:
    path = Path(path_like)
    if path.is_dir():
        candidate = path / "flow_index.csv"
        if candidate.exists():
            return candidate
        raise FileNotFoundError(f"EF3.1 flow_index.csv not found in directory: {path_like}")
    if path.is_file():
        return path
    raise FileNotFoundError(f"EF3.1 path not found: {path_like}")


def _resolve_default_ef31_flow_index_path() -> Path | None:
    repo_root = Path(__file__).resolve().parent.parent
    candidates = [
        repo_root / "data" / "EF3.1" / "flow_index.csv",
        repo_root / "data" / "flow_index.csv",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def load_ef31_flow_uuid_set(path_like: str) -> set[str]:
    path = _resolve_ef31_flow_index_path(path_like)
    uuids: set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.reader(handle, delimiter=";")
        header = next(reader, [])
        if header:
            header[0] = header[0].lstrip("\ufeff")
        col_map = {name: idx for idx, name in enumerate(header)}
        flow_uuid_idx = col_map.get("FlowUUID")
        if flow_uuid_idx is None:
            raise ValueError(f"FlowUUID column not found in EF3.1 flow index: {path}")
        for row in reader:
            if flow_uuid_idx >= len(row):
                continue
            flow_uuid = _clean_str(row[flow_uuid_idx])
            if flow_uuid:
                uuids.add(flow_uuid)
    return uuids


def _clean_str(value: object) -> str:
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null", ""}:
        return ""
    return text


def _is_output_direction(value: object) -> bool:
    return str(value or "").strip().lower() == "output"


def _is_zero_amount(value: object) -> bool:
    try:
        return abs(float(value)) <= 1e-15
    except Exception:  # noqa: BLE001
        return False


def _iter_process_objects_from_file(path: Path) -> tuple[list[dict], str | None]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        return [], f"invalid json: {exc}"

    if isinstance(raw, dict):
        if isinstance(raw.get("process_uuid"), (str, int)):
            return [raw], None
        if isinstance(raw.get("processes"), list):
            return [item for item in raw["processes"] if isinstance(item, dict)], None
        return [], "json object must be a process object or contain 'processes' list"

    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)], None

    return [], "json must be object or array"


def _iter_process_objects_from_zip(zip_path: Path) -> tuple[list[tuple[str, dict]], list[dict]]:
    process_items: list[tuple[str, dict]] = []
    errors: list[dict] = []

    try:
        with zipfile.ZipFile(zip_path, "r") as archive:
            json_names = sorted(
                name
                for name in archive.namelist()
                if not name.endswith("/") and name.lower().endswith(".json")
            )
            for member_name in json_names:
                try:
                    raw = json.loads(archive.read(member_name).decode("utf-8"))
                except Exception as exc:  # noqa: BLE001
                    errors.append(
                        {
                            "file": f"{zip_path}!{member_name}",
                            "process_uuid": None,
                            "reason": f"invalid json: {exc}",
                        }
                    )
                    continue

                if isinstance(raw, dict):
                    if isinstance(raw.get("process_uuid"), (str, int)):
                        process_items.append((f"{zip_path}!{member_name}", raw))
                        continue
                    if isinstance(raw.get("processes"), list):
                        process_items.extend(
                            (f"{zip_path}!{member_name}", item)
                            for item in raw["processes"]
                            if isinstance(item, dict)
                        )
                        continue
                    continue

                if isinstance(raw, list):
                    process_items.extend(
                        (f"{zip_path}!{member_name}", item) for item in raw if isinstance(item, dict)
                    )
    except Exception as exc:  # noqa: BLE001
        errors.append({"file": str(zip_path), "process_uuid": None, "reason": f"invalid zip: {exc}"})

    return process_items, errors


def _resolve_reference_exchange(process_json: dict) -> dict | None:
    exchanges = process_json.get("exchanges")
    if not isinstance(exchanges, list):
        return None

    for ex in exchanges:
        if not isinstance(ex, dict):
            continue
        if bool(ex.get("is_reference_flow")) and _is_output_direction(ex.get("direction")):
            return ex

    ref_internal_id = _clean_str(process_json.get("reference_flow_internal_id"))
    if ref_internal_id:
        for ex in exchanges:
            if not isinstance(ex, dict):
                continue
            if _clean_str(ex.get("exchange_internal_id")) == ref_internal_id and _is_output_direction(ex.get("direction")):
                return ex
    return None


def import_processes_from_json(
    db: Session,
    *,
    path_like: str,
    replace_existing: bool = True,
    strict_reference_flow: bool = False,
) -> dict:
    path = Path(path_like)
    if not path.exists():
        raise FileNotFoundError(f"Input path not found: {path_like}")

    files: list[Path]
    zip_process_items: list[tuple[str, dict]] = []
    zip_errors: list[dict] = []
    if path.is_dir():
        files = sorted([p for p in path.rglob("*.json") if p.is_file()])
    elif path.is_file():
        if path.suffix.lower() == ".zip":
            zip_process_items, zip_errors = _iter_process_objects_from_zip(path)
            files = []
        elif path.suffix.lower() == ".json":
            files = [path]
        else:
            raise ValueError("Input file must be .json or .zip")
    else:
        raise ValueError(f"Unsupported path: {path_like}")

    total_files = len(files) or (1 if path.is_file() and path.suffix.lower() == ".zip" else 0)
    total_processes = 0
    inserted = 0
    updated = 0
    skipped = 0
    failed = len(zip_errors)
    errors: list[dict] = list(zip_errors)
    warnings: list[dict] = []

    process_sources: list[tuple[str, dict]] = list(zip_process_items)
    for file_path in files:
        process_items, parse_error = _iter_process_objects_from_file(file_path)
        if parse_error:
            failed += 1
            errors.append({"file": str(file_path), "process_uuid": None, "reason": parse_error})
            continue
        process_sources.extend((str(file_path), process_json) for process_json in process_items)

    for source_file, process_json in process_sources:
            total_processes += 1
            process_uuid = _clean_str(process_json.get("process_uuid"))
            if not process_uuid:
                failed += 1
                errors.append({"file": source_file, "process_uuid": None, "reason": "missing process_uuid"})
                continue

            exchanges = process_json.get("exchanges")
            if not isinstance(exchanges, list) or len(exchanges) == 0:
                failed += 1
                errors.append({"file": source_file, "process_uuid": process_uuid, "reason": "exchanges is empty"})
                continue

            ref_exchange = _resolve_reference_exchange(process_json)
            reference_flow_uuid: str | None = None
            if isinstance(ref_exchange, dict):
                reference_flow_uuid = _clean_str(ref_exchange.get("flow_uuid")) or None

            if strict_reference_flow:
                if not isinstance(ref_exchange, dict):
                    failed += 1
                    errors.append(
                        {
                            "file": source_file,
                            "process_uuid": process_uuid,
                            "reason": "reference flow cannot be identified",
                        }
                    )
                    continue
                if not reference_flow_uuid:
                    failed += 1
                    errors.append(
                        {
                            "file": source_file,
                            "process_uuid": process_uuid,
                            "reason": "reference flow uuid is missing",
                        }
                    )
                    continue
                if db.get(FlowRecord, reference_flow_uuid) is None:
                    failed += 1
                    errors.append(
                        {
                            "file": source_file,
                            "process_uuid": process_uuid,
                            "reason": f"reference flow not found in flow catalog: {reference_flow_uuid}",
                        }
                    )
                    continue
            else:
                if not isinstance(ref_exchange, dict):
                    warnings.append(
                        {
                            "file": source_file,
                            "process_uuid": process_uuid,
                            "reason": "reference flow cannot be identified; imported with reference_flow_uuid=null",
                        }
                    )
                elif not reference_flow_uuid:
                    warnings.append(
                        {
                            "file": source_file,
                            "process_uuid": process_uuid,
                            "reason": "reference flow uuid is missing; imported with reference_flow_uuid=null",
                        }
                    )
                elif db.get(FlowRecord, reference_flow_uuid) is None:
                    warnings.append(
                        {
                            "file": source_file,
                            "process_uuid": process_uuid,
                            "reason": f"reference flow not found in flow catalog: {reference_flow_uuid}; imported anyway",
                        }
                    )

            name_zh = _clean_str(process_json.get("process_name_zh"))
            name_en = _clean_str(process_json.get("process_name_en"))
            process_name = name_zh or name_en or process_uuid
            process_type = _clean_str(process_json.get("process_type")) or "unit_process"

            if not name_zh and not name_en:
                warnings.append({"file": source_file, "process_uuid": process_uuid, "reason": "both zh/en process name missing"})
            if any(ex.get("allocation_factor") is None for ex in exchanges if isinstance(ex, dict)):
                warnings.append({"file": source_file, "process_uuid": process_uuid, "reason": "allocation_factor contains null"})
            if any(_is_zero_amount(ex.get("amount")) for ex in exchanges if isinstance(ex, dict)):
                warnings.append({"file": source_file, "process_uuid": process_uuid, "reason": "amount contains zero"})

            existing = db.get(ReferenceProcess, process_uuid)
            if existing is not None and not replace_existing:
                skipped += 1
                continue

            if existing is None:
                db.add(
                    ReferenceProcess(
                        process_uuid=process_uuid,
                        process_name=process_name,
                        process_name_zh=name_zh or None,
                        process_name_en=name_en or None,
                        process_type=process_type,
                        reference_flow_uuid=reference_flow_uuid,
                        process_json=process_json,
                        source_file=source_file,
                        import_mode="tiangong",
                    )
                )
                inserted += 1
            else:
                existing.process_name = process_name
                existing.process_name_zh = name_zh or None
                existing.process_name_en = name_en or None
                existing.process_type = process_type
                existing.reference_flow_uuid = reference_flow_uuid
                existing.process_json = process_json
                existing.source_file = source_file
                existing.import_mode = "tiangong"
                updated += 1

    db.commit()
    return {
        "total_files": total_files,
        "total_processes": total_processes,
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
        "failed": failed,
        "errors": errors,
        "warnings": warnings,
    }


def import_flows_from_file(
    db: Session,
    file_path: str,
    *,
    sheet_name: str | int | None = None,
    mapping: dict[str, str] | None = None,
    replace_existing: bool = False,
    default_flow_type: str | None = None,
    ef31_flow_index_path: str | None = None,
) -> dict:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    df = _read_table(path, sheet_name=sheet_name)
    if df.empty:
        return {"inserted": 0, "updated": 0, "skipped": 0, "errors": ["Input file is empty."]}

    resolved = _resolve_columns(df.columns, mapping=mapping)

    inferred_flow_type = default_flow_type
    lower_name = path.name.lower()
    if not inferred_flow_type:
        if "elementary" in lower_name:
            inferred_flow_type = "Elementary flow"
        elif "waste" in lower_name:
            inferred_flow_type = "Waste flow"
        else:
            inferred_flow_type = "Product flow"

    inserted = 0
    updated = 0
    skipped = 0
    filtered_out = 0
    errors: list[str] = []
    ef31_allow_uuids: set[str] | None = None
    effective_ef31_path: str | None = ef31_flow_index_path

    normalized_default_type = str(inferred_flow_type or "").strip().lower()
    is_elementary_import = "elementary" in normalized_default_type
    if effective_ef31_path is None and is_elementary_import:
        auto_path = _resolve_default_ef31_flow_index_path()
        if auto_path is None:
            raise ValueError(
                "Elementary flow import requires EF3.1 flow_index.csv. "
                "Place it under ./data/EF3.1/flow_index.csv or pass --ef31-flow-index-path."
            )
        effective_ef31_path = str(auto_path)

    if effective_ef31_path:
        ef31_allow_uuids = load_ef31_flow_uuid_set(effective_ef31_path)
    existing_uuids = {item[0] for item in db.query(FlowRecord.flow_uuid).all()}
    seen_in_file: set[str] = set()

    for row_idx, row in df.iterrows():
        try:
            flow_uuid = _clean_str(row[resolved["flow_uuid"]])
            flow_name = _clean_str(row[resolved["flow_name"]])
            flow_name_en = (
                _clean_str(row[resolved["flow_name_en"]])
                if "flow_name_en" in resolved
                else ""
            )
            flow_type = (
                _clean_str(row[resolved["flow_type"]])
                if "flow_type" in resolved
                else inferred_flow_type
            )
            default_unit = _clean_str(row[resolved["default_unit"]])
            unit_group = _clean_str(row[resolved["unit_group"]])
            compartment = (
                _clean_str(row[resolved["compartment"]])
                if "compartment" in resolved
                else ""
            )
            source_updated_at = (
                _clean_str(row[resolved["updated_at"]])
                if "updated_at" in resolved
                else ""
            )

            if not flow_uuid or flow_uuid.lower() in {"nan", "none"}:
                skipped += 1
                continue
            if not flow_name or flow_name.lower() in {"nan", "none"}:
                skipped += 1
                continue
            if ef31_allow_uuids is not None and flow_uuid not in ef31_allow_uuids:
                filtered_out += 1
                continue

            if flow_uuid in seen_in_file and not replace_existing:
                skipped += 1
                continue
            seen_in_file.add(flow_uuid)

            if flow_uuid in existing_uuids:
                if not replace_existing:
                    skipped += 1
                    continue
                item = db.get(FlowRecord, flow_uuid)
                if item is None:
                    skipped += 1
                    continue
                item.flow_name = flow_name
                item.flow_name_en = flow_name_en or item.flow_name_en
                item.flow_type = flow_type or item.flow_type
                item.default_unit = default_unit or item.default_unit
                item.unit_group = unit_group or item.unit_group
                item.compartment = compartment or item.compartment
                item.source_updated_at = source_updated_at or item.source_updated_at
                updated += 1
            else:
                db.add(
                    FlowRecord(
                        flow_uuid=flow_uuid,
                        flow_name=flow_name,
                        flow_name_en=flow_name_en or None,
                        flow_type=flow_type or "Product flow",
                        default_unit=default_unit or "kg",
                        unit_group=unit_group or "mass",
                        compartment=compartment or None,
                        source_updated_at=source_updated_at or None,
                    )
                )
                existing_uuids.add(flow_uuid)
                inserted += 1
        except Exception as exc:  # noqa: BLE001
            errors.append(f"row={row_idx + 2}: {exc}")

    db.commit()
    return {
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
        "filtered_out": filtered_out,
        "errors": errors,
        "resolved_columns": resolved,
        "ef31_allow_uuid_count": len(ef31_allow_uuids or []),
        "ef31_flow_index_path": effective_ef31_path,
    }


def _resolve_unit_group_columns(columns: Iterable[str]) -> dict[str, str]:
    normalized_input = {_normalize_column_name(c): c for c in columns}
    aliases = {
        "unit_name": [
            "Unit Name",
            "unit name",
            "unit",
            "name",
        ],
        "factor_to_reference": [
            "Conversion Factor to Reference Unit",
            "conversion factor to reference unit",
            "factor_to_reference",
            "factor",
        ],
    }
    resolved: dict[str, str] = {}
    for key, key_aliases in aliases.items():
        for alias in key_aliases:
            alias_norm = _normalize_column_name(alias)
            if alias_norm in normalized_input:
                resolved[key] = normalized_input[alias_norm]
                break
    if "unit_name" not in resolved or "factor_to_reference" not in resolved:
        raise ValueError(
            "Unit group sheet must include columns: 'Unit Name' and 'Conversion Factor to Reference Unit'"
        )
    return resolved


def import_unit_groups_from_excel(
    db: Session,
    file_path: str,
    *,
    replace_existing: bool = False,
) -> dict:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")
    if path.suffix.lower() not in {".xlsx", ".xlsm", ".xls"}:
        raise ValueError("Unit groups import only supports Excel files (.xlsx/.xlsm/.xls)")

    workbook = pd.ExcelFile(path)
    errors: list[str] = []
    groups_inserted = 0
    groups_updated = 0
    units_inserted = 0
    units_updated = 0

    if replace_existing:
        db.query(UnitDefinition).delete(synchronize_session=False)
        db.query(UnitGroup).delete(synchronize_session=False)
        db.flush()

    for sheet_name in workbook.sheet_names:
        try:
            df = pd.read_excel(path, sheet_name=sheet_name)
            if df.empty:
                continue
            resolved = _resolve_unit_group_columns(df.columns)
            group_name = str(sheet_name).strip()
            if not group_name:
                continue

            group = db.get(UnitGroup, group_name)
            if group is None:
                group = UnitGroup(name=group_name, reference_unit=None)
                db.add(group)
                groups_inserted += 1
            else:
                groups_updated += 1

            if not replace_existing:
                db.query(UnitDefinition).filter(UnitDefinition.unit_group == group_name).delete(
                    synchronize_session=False
                )

            reference_unit: str | None = None
            for idx, row in df.iterrows():
                unit_name = _clean_str(row[resolved["unit_name"]])
                factor_raw = row[resolved["factor_to_reference"]]
                if not unit_name:
                    continue
                try:
                    factor = float(factor_raw)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"sheet={group_name} row={idx + 2}: invalid factor '{factor_raw}' ({exc})")
                    continue

                is_reference = abs(factor - 1.0) < 1e-12
                if is_reference and reference_unit is None:
                    reference_unit = unit_name

                db.add(
                    UnitDefinition(
                        unit_group=group_name,
                        unit_name=unit_name,
                        factor_to_reference=factor,
                        is_reference=is_reference,
                    )
                )
                units_inserted += 1

            if reference_unit:
                group.reference_unit = reference_unit
        except Exception as exc:  # noqa: BLE001
            errors.append(f"sheet={sheet_name}: {exc}")

    db.commit()
    return {
        "groups_inserted": groups_inserted,
        "groups_updated": groups_updated,
        "units_inserted": units_inserted,
        "units_updated": units_updated,
        "errors": errors,
    }


def _resolve_unit_by_name(
    db: Session,
    *,
    unit_name: str,
    unit_group: str | None = None,
) -> UnitDefinition:
    query = db.query(UnitDefinition).filter(UnitDefinition.unit_name == unit_name)
    if unit_group:
        query = query.filter(UnitDefinition.unit_group == unit_group)
    matches = query.all()
    if not matches:
        raise ValueError(f"Unit not found: {unit_name}" + (f" in group {unit_group}" if unit_group else ""))
    if len(matches) > 1:
        groups = ", ".join(sorted({m.unit_group for m in matches}))
        raise ValueError(f"Unit '{unit_name}' is ambiguous across groups: {groups}. Please provide unit_group.")
    return matches[0]


def convert_unit_value(
    db: Session,
    *,
    value: float,
    from_unit: str,
    to_unit: str,
    unit_group: str | None = None,
) -> dict:
    src = _resolve_unit_by_name(db, unit_name=from_unit, unit_group=unit_group)
    dst = _resolve_unit_by_name(db, unit_name=to_unit, unit_group=unit_group)
    if src.unit_group != dst.unit_group:
        raise ValueError(
            f"Units are not in the same group: {from_unit}({src.unit_group}) -> {to_unit}({dst.unit_group})"
        )
    converted = value * src.factor_to_reference / dst.factor_to_reference
    return {
        "value": value,
        "from_unit": from_unit,
        "to_unit": to_unit,
        "unit_group": src.unit_group,
        "converted_value": converted,
        "from_factor_to_reference": src.factor_to_reference,
        "to_factor_to_reference": dst.factor_to_reference,
    }
