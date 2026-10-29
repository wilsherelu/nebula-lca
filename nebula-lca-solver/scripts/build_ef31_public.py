"""Rebuild the license-clean public EF 3.1 characterization assets.

Source: JRC ILCD LCIA method data set ``6209b35f-9447-40b5-b68c-a1099e3674a0``
(Climate change GWP100, European Commission - JRC, IPCC AR5).  Redistribution is
permitted with attribution; see ``provenance.json`` next to the outputs.

Nothing here reads the ecoinvent-derived ``data/EF3.1`` flow index or factor tables.
Flow identity comes from the ILCD ``refObjectId`` UUIDs verbatim, names from the
``common:shortDescription`` elements verbatim, and coefficients from ``meanValue``.

    python scripts/build_ef31_public.py --source-xml <path>/6209b35f-....xml \
        --out data/EF3.1-public --verify
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import xml.etree.ElementTree as ET
from pathlib import Path

CLIMATE_INDICATOR_INDEX = 1
EXTRA_CATALOG_UUIDS = (
    "f79d0f8f-2b0e-49cb-bed0-b1ea0fbd8625",
    "fe0acd60-3ddc-11dd-ac48-0050c2490048",
)
EXPECTED_SHA256 = {
    "flow_index.csv": "12e3bcb3a402aa283ece1c48589e95ceae1a8dcb299358c20befe07ec8e0b736",
    "indicator_index.csv": "3571bf99117714546d9e240ac8013af4fdaffd79bca40c931659c4b0e8fb9bcd",
    "lcia_factors.csv": "07fe84e0adbc3a7ecd2ead2f09dbfa22c76ed80f98707e368b0f3a1dbb6145a7",
}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _child(element, name: str):
    for item in element:
        if _local(item.tag) == name:
            return item
    return None


def parse_factors(source_xml: Path) -> list[tuple[str, str, float]]:
    """Return (flow_uuid, flow_name, coefficient) in source document order."""
    root = ET.parse(source_xml).getroot()
    factors: list[tuple[str, str, float]] = []
    for factor in (item for item in root.iter() if _local(item.tag) == "factor"):
        reference = _child(factor, "referenceToFlowDataSet")
        mean = _child(factor, "meanValue")
        if reference is None or mean is None:
            continue
        uuid = str(reference.get("refObjectId") or "").strip()
        name = ""
        for description in reference:
            if _local(description.tag) != "shortDescription":
                continue
            if description.get("{http://www.w3.org/XML/1998/namespace}lang") == "en":
                name = (description.text or "").strip()
                break
        coefficient = float((mean.text or "").strip())
        if not uuid:
            continue
        # Zero coefficients are kept so the output stays a faithful row-for-row copy of the
        # JRC source; the engine skips zero entries when it builds the C matrix.
        factors.append((uuid, name, coefficient))
    return factors


def parse_extra_flows(catalog_csv: Path) -> dict[str, str]:
    """Elementary flows the Tiangong template needs but the GWP100 set does not cover.

    The engine admits an elementary flow only when its UUID appears in flow_index.csv,
    so these two are indexed without a characterization factor rather than dropped.
    """
    names: dict[str, str] = {}
    with catalog_csv.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            uuid = (row.get("flow_uuid") or "").strip()
            if uuid not in EXTRA_CATALOG_UUIDS:
                continue
            compartment = (row.get("compartment") or "").replace(";", ", ")
            names[uuid] = (
                f"{(row.get('flow_name_en') or '').strip()} "
                f"(Mass, {(row.get('default_unit') or '').strip()}, {compartment})"
            )
    missing = [uuid for uuid in EXTRA_CATALOG_UUIDS if uuid not in names]
    if missing:
        raise ValueError(f"steel elementary catalog lacks required UUIDs: {missing}")
    return names


def write_csv(path: Path, header: list[str], rows: list[list[object]], *, bom: bool) -> None:
    lines = [";".join(header)] + [";".join(str(cell) for cell in row) for row in rows]
    text = "\r\n".join(lines) + "\r\n"
    path.write_bytes(text.encode("utf-8-sig" if bom else "utf-8"))


def build(source_xml: Path, catalog_csv: Path, indicator_source: Path, out_dir: Path) -> dict[str, str]:
    factors = parse_factors(source_xml)
    extra = parse_extra_flows(catalog_csv)

    names: dict[str, str] = {}
    for uuid, name, _ in factors:
        names.setdefault(uuid, name)
    duplicates = len(factors) - len(names)
    if duplicates:
        raise ValueError(f"source XML repeats {duplicates} flow UUIDs; identity would be ambiguous")
    names.update(extra)

    ordered = sorted(names)
    column_of = {uuid: index for index, uuid in enumerate(ordered)}
    out_dir.mkdir(parents=True, exist_ok=True)

    write_csv(
        out_dir / "flow_index.csv",
        ["flow_index", "FlowUUID", "FlowName"],
        [[index, uuid, names[uuid]] for index, uuid in enumerate(ordered)],
        bom=False,
    )
    write_csv(
        out_dir / "lcia_factors.csv",
        ["row", "column", "coefficient"],
        [[CLIMATE_INDICATOR_INDEX, column_of[uuid], str(coefficient)] for uuid, _, coefficient in factors],
        bom=False,
    )
    (out_dir / "indicator_index.csv").write_bytes(indicator_source.read_bytes())

    return {name: sha256(out_dir / name) for name in EXPECTED_SHA256}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-xml", type=Path, required=True,
                        help="JRC ILCD LCIA method data set 6209b35f-9447-40b5-b68c-a1099e3674a0")
    parser.add_argument("--catalog-csv", type=Path, required=True,
                        help="steel_elementary_catalog.v1.csv supplying the two uncovered flows")
    parser.add_argument("--indicator-source", type=Path, required=True,
                        help="EF 3.1 method structure table (25 rows) to ship alongside")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--verify", action="store_true",
                        help="fail unless every output matches the recorded sha256")
    args = parser.parse_args()

    digests = build(args.source_xml, args.catalog_csv, args.indicator_source, args.out)
    for name, digest in digests.items():
        print(f"{name}: {digest}")
    if not args.verify:
        return
    wrong = {name: digest for name, digest in digests.items() if EXPECTED_SHA256[name] != digest}
    if wrong:
        raise SystemExit(f"output does not match the recorded conversion: {wrong}")
    print("verified: byte-identical to the recorded public EF 3.1 conversion")


if __name__ == "__main__":
    main()
