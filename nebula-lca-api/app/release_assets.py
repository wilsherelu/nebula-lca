"""Fail-closed selection of data assets allowed in desktop releases."""

from __future__ import annotations

import json
import re
from pathlib import Path


_STABLE_VERSION = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")


def stable_release_version(value: object) -> tuple[int, int, int] | None:
    match = _STABLE_VERSION.fullmatch(str(value or "").strip())
    if match is None:
        return None
    return tuple(int(part) for part in match.groups())


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
        version = stable_release_version(manifest.get("dataset_version"))
        if version is None or acceptance.get("dataset_version") != manifest.get("dataset_version"):
            continue
        if manifest.get("schema_version") != "nebula-flow-mapping-release.v1":
            continue
        if manifest.get("mapping_direction") != "TIANGONG_TO_ECOINVENT":
            continue
        if manifest.get("forbidden_content_included") is not False or manifest.get("l3_included") is not False:
            continue
        if acceptance.get("status") != "passed" or acceptance.get("l3_included") is not False:
            continue
        if int(acceptance.get("forbidden_public_fields", -1)) != 0:
            continue
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
