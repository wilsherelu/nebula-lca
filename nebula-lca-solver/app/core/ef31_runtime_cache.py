from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Dict, List, Optional, Sequence, Tuple

from app.core.matrix_builder import (
    _build_matrix,
    _canonical_ef31_indicator_info,
    _canonical_ef31_indicator_key,
    _get_csv_value,
    _strip_bom_header,
    _to_float,
    _to_int,
)


@dataclass(frozen=True)
class _RuntimeFingerprint:
    flow_index: Tuple[int, int]
    indicator_index: Tuple[int, int]
    factors: Tuple[int, int]


@dataclass
class _RuntimeSource:
    path: str
    fingerprint: _RuntimeFingerprint
    flow_uuid_to_index: Dict[str, int]
    indicator_ids: List[int]
    indicator_lookup: Dict[int, dict]
    factors_by_flow_index: Dict[int, List[Tuple[int, float]]]


class Ef31RuntimeCache:
    """In-process sparse EF3.1 runtime index.

    The old path scans lcia_factors.csv on every request.  This cache keeps the
    CSV-derived index in memory and builds per-request C matrices from the B
    matrix flow UUIDs only.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._sources: Dict[str, _RuntimeSource] = {}

    def clear(self) -> None:
        with self._lock:
            self._sources.clear()

    def build_c_matrix_from_dir(
        self,
        ef_dir: str,
        b_matrix: dict,
        lcia_methods: Optional[Sequence[str]] = None,
        issues: Optional[List[str]] = None,
        report_missing: bool = True,
    ) -> dict:
        if issues is None:
            issues = []
        source, cache_hit = self._get_source(ef_dir)
        pack = _build_source_c_matrix(
            source,
            b_matrix,
            lcia_methods=lcia_methods,
            issues=issues,
            report_missing=report_missing,
        )
        pack["cache_hit"] = cache_hit
        pack["runtime_source_count"] = 1
        return pack

    def build_c_matrix_from_sources(
        self,
        ef_dirs: Sequence[str],
        b_matrix: dict,
        lcia_methods: Optional[Sequence[str]] = None,
        issues: Optional[List[str]] = None,
    ) -> dict:
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
        cache_hits: List[bool] = []

        for ef_dir in existing_dirs:
            source, cache_hit = self._get_source(ef_dir)
            cache_hits.append(cache_hit)
            pack = _build_source_c_matrix(
                source,
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
            "cache_hit": bool(cache_hits) and all(cache_hits),
            "runtime_source_count": len(existing_dirs),
        }

    def _get_source(self, ef_dir: str) -> tuple[_RuntimeSource, bool]:
        path = os.path.abspath(str(ef_dir))
        fingerprint = _fingerprint_runtime(path)
        with self._lock:
            cached = self._sources.get(path)
            if cached and cached.fingerprint == fingerprint:
                return cached, True
            source = _load_runtime_source(path, fingerprint)
            self._sources[path] = source
            return source, False


def _fingerprint_runtime(ef_dir: str) -> _RuntimeFingerprint:
    root = Path(ef_dir)
    return _RuntimeFingerprint(
        flow_index=_file_fingerprint(root / "flow_index.csv"),
        indicator_index=_file_fingerprint(root / "indicator_index.csv"),
        factors=_file_fingerprint(root / "lcia_factors.csv"),
    )


def _file_fingerprint(path: Path) -> Tuple[int, int]:
    stat = path.stat()
    return (int(stat.st_mtime_ns), int(stat.st_size))


def _load_runtime_source(ef_dir: str, fingerprint: _RuntimeFingerprint) -> _RuntimeSource:
    root = Path(ef_dir)
    flow_uuid_to_index: Dict[str, int] = {}
    with (root / "flow_index.csv").open("r", encoding="utf-8") as handle:
        reader = csv.reader(handle, delimiter=";")
        header = _strip_bom_header(next(reader, []))
        col_map = {name: idx for idx, name in enumerate(header)}
        flow_uuid_idx = col_map.get("FlowUUID")
        flow_index_idx = col_map.get("flow_index")
        if flow_uuid_idx is not None and flow_index_idx is not None:
            for row in reader:
                flow_uuid = row[flow_uuid_idx].strip() if flow_uuid_idx < len(row) else ""
                idx = _to_int(row[flow_index_idx] if flow_index_idx < len(row) else None)
                if flow_uuid and idx is not None:
                    flow_uuid_to_index[flow_uuid] = idx

    indicator_ids: List[int] = []
    indicator_lookup: Dict[int, dict] = {}
    with (root / "indicator_index.csv").open("r", encoding="utf-8") as handle:
        reader = csv.reader(handle, delimiter=";")
        header = _strip_bom_header(next(reader, []))
        col_map = {name: idx for idx, name in enumerate(header)}
        indicator_idx_col = col_map.get("indicator_index")
        if indicator_idx_col is not None:
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
    indicator_ids.sort()

    index_to_flow_uuid = {idx: flow_uuid for flow_uuid, idx in flow_uuid_to_index.items()}
    factors_by_flow_index: Dict[int, List[Tuple[int, float]]] = {}
    with (root / "lcia_factors.csv").open("r", encoding="utf-8") as handle:
        reader = csv.reader(handle, delimiter=";")
        header = _strip_bom_header(next(reader, []))
        col_map = {name: idx for idx, name in enumerate(header)}
        row_idx_col = col_map.get("row")
        col_idx_col = col_map.get("column")
        coeff_col = col_map.get("coefficient")
        if row_idx_col is not None and col_idx_col is not None and coeff_col is not None:
            for row in reader:
                indicator_idx = _to_int(row[row_idx_col] if row_idx_col < len(row) else None)
                flow_idx = _to_int(row[col_idx_col] if col_idx_col < len(row) else None)
                if indicator_idx is None or flow_idx is None:
                    continue
                if indicator_idx not in indicator_lookup or flow_idx not in index_to_flow_uuid:
                    continue
                coeff = _to_float(row[coeff_col] if coeff_col < len(row) else None)
                if coeff is None or coeff == 0:
                    continue
                factors_by_flow_index.setdefault(flow_idx, []).append((indicator_idx, coeff))

    return _RuntimeSource(
        path=ef_dir,
        fingerprint=fingerprint,
        flow_uuid_to_index=flow_uuid_to_index,
        indicator_ids=indicator_ids,
        indicator_lookup=indicator_lookup,
        factors_by_flow_index=factors_by_flow_index,
    )


def _build_source_c_matrix(
    source: _RuntimeSource,
    b_matrix: dict,
    lcia_methods: Optional[Sequence[str]] = None,
    issues: Optional[List[str]] = None,
    report_missing: bool = True,
) -> dict:
    if issues is None:
        issues = []
    indicator_ids, indicator_lookup = _filter_indicators(
        source.indicator_ids,
        source.indicator_lookup,
        lcia_methods=lcia_methods,
    )
    indicator_id_set = set(indicator_ids)

    b_flow_ids = b_matrix.get("rows", []) or []
    flow_index_to_uuid: Dict[int, str] = {}
    for flow_uuid in b_flow_ids:
        idx = source.flow_uuid_to_index.get(flow_uuid)
        if idx is None:
            continue
        flow_index_to_uuid[idx] = flow_uuid

    c_entries: Dict[Tuple[int, str], float] = {}
    for flow_idx, flow_uuid in flow_index_to_uuid.items():
        for indicator_idx, coeff in source.factors_by_flow_index.get(flow_idx, []):
            if indicator_idx not in indicator_id_set:
                continue
            key = (indicator_idx, flow_uuid)
            c_entries[key] = c_entries.get(key, 0.0) + coeff

    missing = [flow_uuid for flow_uuid in b_flow_ids if flow_uuid not in source.flow_uuid_to_index]
    if missing and report_missing:
        issues.append(f"EF3.1 missing {len(missing)} flow_uuids from B matrix")

    return {
        "C": _build_matrix(indicator_ids, b_flow_ids, c_entries),
        "indicator_lookup": indicator_lookup,
        "matched_flow_uuids": set(flow_index_to_uuid.values()),
        "runtime_flow_uuids": set(source.flow_uuid_to_index.keys()),
    }


def _filter_indicators(
    indicator_ids: Sequence[int],
    indicator_lookup: Dict[int, dict],
    lcia_methods: Optional[Sequence[str]] = None,
) -> tuple[List[int], Dict[int, dict]]:
    selected_methods = {
        str(method).strip()
        for method in (lcia_methods or [])
        if str(method).strip()
    }
    filtered_ids = list(indicator_ids)
    filtered_lookup = dict(indicator_lookup)
    if selected_methods:
        matched_indicator_ids = [
            idx
            for idx, info in filtered_lookup.items()
            if str(info.get("method_en", "")).strip() in selected_methods
            or str(info.get("method_zh", "")).strip() in selected_methods
        ]
        if matched_indicator_ids:
            matched_set = set(matched_indicator_ids)
            filtered_lookup = {
                idx: info
                for idx, info in filtered_lookup.items()
                if idx in matched_set
            }
            filtered_ids = [idx for idx in filtered_ids if idx in filtered_lookup]
        elif "EF v3.1" not in selected_methods:
            filtered_lookup = {}
            filtered_ids = []
    filtered_ids.sort()
    return filtered_ids, filtered_lookup


GLOBAL_EF31_RUNTIME_CACHE = Ef31RuntimeCache()
