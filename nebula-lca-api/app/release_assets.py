"""Fail-closed selection of data assets allowed in desktop releases."""

from __future__ import annotations

import json
import re
from pathlib import Path


_STABLE_VERSION = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")
_PUBLIC_MANIFEST_KEYS = {
    "dataset_version", "elementary", "intermediate", "license",
    "mapping_direction", "release_date", "schema_version", "source_compatibility",
}
_PUBLIC_SCOPE_KEYS = {"file", "mapping_count", "mapping_levels", "sha256"}
_PUBLIC_ACCEPTANCE_KEYS = {
    "dataset_version", "elementary_mapping_count", "intermediate_mapping_count",
    "l1_bilateral_uniqueness", "status", "tiangong_uuid_uniqueness_per_scope",
    "total_mapping_count",
}


def stable_release_version(value: object) -> tuple[int, int, int] | None:
    match = _STABLE_VERSION.fullmatch(str(value or "").strip())
    if match is None:
        return None
    return tuple(int(part) for part in match.groups())


def public_mapping_contract_is_accepted(manifest: object, acceptance: object) -> bool:
    """Validate the frozen public-package contract without private audit fields."""
    if not isinstance(manifest, dict) or not isinstance(acceptance, dict):
        return False
    if set(manifest) != _PUBLIC_MANIFEST_KEYS or set(acceptance) != _PUBLIC_ACCEPTANCE_KEYS:
        return False
    version = stable_release_version(manifest.get("dataset_version"))
    if version is None or acceptance.get("dataset_version") != manifest.get("dataset_version"):
        return False
    if manifest.get("schema_version") != "nebula-flow-mapping-release.v1":
        return False
    if manifest.get("mapping_direction") != "TIANGONG_TO_ECOINVENT":
        return False
    if manifest.get("license") != "CC-BY-4.0":
        return False
    if manifest.get("source_compatibility") != {"ecoinvent_release": "3.11"}:
        return False
    if acceptance.get("status") != "passed":
        return False
    if acceptance.get("l1_bilateral_uniqueness") is not True:
        return False
    if acceptance.get("tiangong_uuid_uniqueness_per_scope") is not True:
        return False

    counts: dict[str, int] = {}
    for scope in ("intermediate", "elementary"):
        contract = manifest.get(scope)
        if not isinstance(contract, dict) or set(contract) != _PUBLIC_SCOPE_KEYS:
            return False
        try:
            count = int(contract.get("mapping_count", -1))
        except (TypeError, ValueError):
            return False
        levels = contract.get("mapping_levels")
        if count < 0 or not isinstance(levels, dict) or set(levels) != {"L1", "L2"}:
            return False
        if any(not isinstance(levels[level], int) or levels[level] < 0 for level in ("L1", "L2")):
            return False
        if levels["L1"] + levels["L2"] != count:
            return False
        counts[scope] = count

    return (
        acceptance.get("intermediate_mapping_count") == counts["intermediate"]
        and acceptance.get("elementary_mapping_count") == counts["elementary"]
        and acceptance.get("total_mapping_count") == sum(counts.values())
    )


def select_latest_public_mapping_root(parent: Path) -> Path:
    """Return the newest accepted public mapping release, excluding demos/smokes."""
    candidates: list[tuple[tuple[int, int, int], Path]] = []
    for root in parent.glob("nebula-flow-mapping-*"):
        if not root.is_dir():
            continue
        try:
            manifest = json.loads((root / "MANIFEST.json").read_text(encoding="utf-8"))
            acceptance = json.loads((root / "ACCEPTANCE.json").read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            continue
        if not public_mapping_contract_is_accepted(manifest, acceptance):
            continue
        version = stable_release_version(manifest.get("dataset_version"))
        assert version is not None
        candidates.append((version, root))
    if not candidates:
        raise FileNotFoundError(f"no accepted public flow-mapping release under {parent}")
    candidates.sort(key=lambda item: (item[0], item[1].name))
    return candidates[-1][1]


def select_official_ef31_runtime(runtime_root: Path) -> tuple[dict[str, object], Path]:
    """Resolve only the explicitly active official EF3.1 artifact for release."""
    ef31_root = (runtime_root / "ef31").resolve()
    manifest_path = ef31_root / "active_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    job_id = str(manifest.get("job_id") or "").strip()
    if not job_id.startswith("lcia-official-"):
        raise ValueError("desktop release requires an active lcia-official-* EF3.1 runtime")
    artifact_dir = (ef31_root / job_id).resolve()
    try:
        artifact_dir.relative_to(ef31_root)
    except ValueError as exc:
        raise ValueError("active EF3.1 runtime escapes the runtime root") from exc
    for name in ("flow_index.csv", "indicator_index.csv", "lcia_factors.csv"):
        if not (artifact_dir / name).is_file():
            raise FileNotFoundError(f"official EF3.1 runtime is missing {name}")
    return manifest, artifact_dir
