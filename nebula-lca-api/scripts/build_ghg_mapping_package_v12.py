"""Build the reviewed GHG v1.2 mapping package from the local review queue."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


CANONICAL_CANDIDATES = {
    "ghg-review-02": "Methyl chloroform",
    "ghg-review-09": "HCFC-124",
    "ghg-review-11": "Methyl bromide",
    "ghg-review-12": "1-propylbromide",
    "ghg-review-23": "PFC-116",
    "ghg-review-29": "PFC-14",
}
ALIAS_CANDIDATES = {
    "ghg-review-02": {"HCFC-140"},
    "ghg-review-11": {"Halon-1001"},
    "ghg-review-23": {"HFC-116"},
    "ghg-review-29": {"FC-14"},
}
SPECIAL_GROUPS = {"ghg-review-15": "carbon_storage"}


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.casefold()).strip("-")


def _identity_evidence(group_id: str) -> list[str]:
    evidence = ["pro-ghg-engineering-review-2026-07-14", "codex-ghg-adjudication-v1.2"]
    if group_id in {"ghg-review-02", "ghg-review-11"}:
        evidence.append("epa-halocarbon-identity-tables")
    if group_id in {"ghg-review-23", "ghg-review-29"}:
        evidence.append("unep-fgas-designation-list")
    return evidence


def build(package_path: Path, queue_path: Path, review_sha256: str) -> dict[str, object]:
    package = json.loads(package_path.read_text(encoding="utf-8"))
    queue = json.loads(queue_path.read_text(encoding="utf-8"))
    mappings = [
        row for row in package["mappings"] if "review_group_id" not in row
    ]
    evidence = list(package["evidence"])
    evidence_ids = {row["evidence_id"] for row in evidence}
    context_ids = {
        (row["ecoinvent_context"]["compartment"], row["ecoinvent_context"]["subcompartment"]):
        row["ecoinvent_context"]["canonical_context_id"]
        for row in mappings
    }
    common_evidence = [
        {
            "evidence_id": "pro-ghg-engineering-review-2026-07-14",
            "kind": "engineering_identity_review",
            "artifact": "pasted-text.txt",
            "sha256": review_sha256.casefold(),
            "review_scope": "32 GHG substance groups",
        },
        {
            "evidence_id": "codex-ghg-adjudication-v1.2",
            "kind": "mapping_decision",
            "decision_rule": "matching unit, bijective context and EF 3.1 four-dimensional climate CF; reject known identity conflicts",
            "review_status": "approved",
        },
        {
            "evidence_id": "epa-halocarbon-identity-tables",
            "kind": "external_identity_reference",
            "url": "https://www.epa.gov/ozone-layer-protection/ozone-depleting-substances",
        },
        {
            "evidence_id": "unep-fgas-designation-list",
            "kind": "external_identity_reference",
            "url": "https://ozone.unep.org/lists-substances-and-blends",
        },
    ]
    for row in common_evidence:
        if row["evidence_id"] not in evidence_ids:
            evidence.append(row)
            evidence_ids.add(row["evidence_id"])

    one_way: list[dict[str, object]] = []
    excluded: list[dict[str, object]] = []
    special: list[dict[str, object]] = []
    masterdata_hash = next(
        row["sha256"] for row in evidence if row.get("kind") == "masterdata_flow"
    )

    for group in queue["groups"]:
        group_id = group["group_id"]
        if group_id in SPECIAL_GROUPS:
            special.extend(
                {
                    "group_id": group_id,
                    "ecoinvent_flow_uuid": record["ecoinvent_flow_uuid"],
                    "ecoinvent_flow_name": group["ecoinvent_flow_name"],
                    "context": record["context"],
                    "rule": SPECIAL_GROUPS[group_id],
                    "review_status": "excluded_from_standard_cbc",
                }
                for record in group["records"]
            )
            continue

        canonical_name = CANONICAL_CANDIDATES.get(group_id)
        for record in group["records"]:
            candidates = record["ef_candidates"]
            if canonical_name is None:
                if len(candidates) != 1:
                    raise ValueError(f"group does not have a unique candidate: {group_id}")
                canonical = candidates[0]
            else:
                matches = [row for row in candidates if row["ef_flow_name"] == canonical_name]
                if len(matches) != 1:
                    raise ValueError(f"canonical candidate is not unique: {group_id}")
                canonical = matches[0]

            context = record["context"]
            context_key = (context["compartment"], context["subcompartment"])
            canonical_context_id = context_ids[context_key]
            semantic = {
                "substance_id": _slug(group["ecoinvent_flow_name"]),
                "flow_property": "mass",
                "unit_dimension": "mass",
                "qualifiers": {},
            }
            masterdata_id = f"ecoinvent-3.11-masterdata:{record['ecoinvent_flow_uuid']}"
            if masterdata_id not in evidence_ids:
                evidence.append(
                    {
                        "evidence_id": masterdata_id,
                        "kind": "masterdata_flow",
                        "source_version": "3.11",
                        "record_uuid": record["ecoinvent_flow_uuid"],
                        "artifact": "ElementaryExchanges.xml",
                        "sha256": masterdata_hash,
                        "context": {
                            **context,
                            "canonical_context_id": canonical_context_id,
                        },
                    }
                )
                evidence_ids.add(masterdata_id)
            row_evidence = [
                masterdata_id,
                "ecoinvent-3.11-ef31-climate-runtime",
                "ef-3.1-climate-runtime",
                *_identity_evidence(group_id),
            ]
            vector = {
                "climate_total": float(record["cf_vector"]["total"]),
                "climate_biogenic": float(record["cf_vector"]["biogenic"]),
                "climate_fossil": float(record["cf_vector"]["fossil"]),
                "climate_land_use_change": float(record["cf_vector"]["land_use_change"]),
            }
            base = {
                "ecoinvent_version": "3.11",
                "ecoinvent_flow_uuid": record["ecoinvent_flow_uuid"],
                "ecoinvent_flow_name": group["ecoinvent_flow_name"],
                "ecoinvent_context": {**context, "canonical_context_id": canonical_context_id},
                "ef_version": "3.1",
                "ef_flow_uuid": canonical["ef_flow_uuid"],
                "ef_flow_name": canonical["ef_flow_name"],
                "ef_context": {
                    "compartment": context["compartment"],
                    "subcompartment": context["subcompartment"],
                    "canonical_context_id": canonical_context_id,
                    "catalog_compartment": canonical["catalog_compartment"],
                    "catalog_subcompartment": None,
                },
                "semantic": semantic,
                "direction": "output",
                "unit": record["unit"],
                "eco_to_ef_factor": 1.0,
                "ef_to_eco_factor": 1.0,
                "cf_vector": vector,
                "semantic_grade": "S1",
                "review_status": "approved",
                "evidence_ids": row_evidence,
                "review_group_id": group_id,
            }
            mappings.append(base)

            for candidate in candidates:
                if candidate["ef_flow_uuid"] == canonical["ef_flow_uuid"]:
                    continue
                if candidate["ef_flow_name"] not in ALIAS_CANDIDATES.get(group_id, set()):
                    excluded.append(
                        {
                            "group_id": group_id,
                            "ef_flow_uuid": candidate["ef_flow_uuid"],
                            "ef_flow_name": candidate["ef_flow_name"],
                            "ecoinvent_flow_uuid": record["ecoinvent_flow_uuid"],
                            "reason": "chemical_identity_mismatch",
                            "review_status": "rejected",
                        }
                    )
                    continue
                alias = dict(base)
                alias.update(
                    {
                        "ef_flow_uuid": candidate["ef_flow_uuid"],
                        "ef_flow_name": candidate["ef_flow_name"],
                        "ef_context": {
                            **base["ef_context"],
                            "catalog_compartment": candidate["catalog_compartment"],
                        },
                        "source_namespace": "EF",
                        "amount_factor": 1.0,
                        "alias_of_ef_flow_uuid": canonical["ef_flow_uuid"],
                    }
                )
                alias.pop("eco_to_ef_factor")
                alias.pop("ef_to_eco_factor")
                one_way.append(alias)

    if len(mappings) != 180 or len(one_way) != 16 or len(excluded) != 6 or len(special) != 4:
        raise ValueError("unexpected reviewed mapping counts")
    package.update(
        {
            "package_version": "1.2.0",
            "mapping_count": len(mappings),
            "one_way_mapping_count": len(one_way),
            "evidence": evidence,
            "mappings": sorted(mappings, key=lambda row: row["ecoinvent_flow_uuid"]),
            "one_way_mappings": sorted(one_way, key=lambda row: row["ef_flow_uuid"]),
            "excluded_candidates": sorted(excluded, key=lambda row: row["ef_flow_uuid"]),
            "special_flows": sorted(special, key=lambda row: row["ecoinvent_flow_uuid"]),
        }
    )
    return package


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--queue", type=Path, required=True)
    parser.add_argument("--review-sha256", required=True)
    args = parser.parse_args()
    package = build(args.package, args.queue, args.review_sha256)
    args.package.write_text(
        json.dumps(package, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
