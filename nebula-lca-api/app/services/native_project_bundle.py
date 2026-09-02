"""Portable, integrity-checked Nebula project bundles.

The native bundle is a lossless application backup format.  TIDAS remains the
standards-interchange format; licensed background databases are referenced but
never redistributed by this module.
"""

from __future__ import annotations

from datetime import datetime
from hashlib import sha256
from io import BytesIO
import json
from typing import Any
import uuid
from zipfile import ZIP_DEFLATED, BadZipFile, ZipFile

from sqlalchemy import inspect
from sqlalchemy.orm import Session

from ..models import (
    FlowRecord,
    FlowVersionRecord,
    Model,
    ModelVersion,
    PtsCompileArtifact,
    PtsDefinition,
    PtsExternalArtifact,
    PtsResource,
    RunJob,
    UnitDefinition,
    UnitGroup,
)
from .graph_storage import compute_graph_hash_from_slim_graph


BUNDLE_SCHEMA = "nebula.project.bundle.v1"
MAX_BUNDLE_BYTES = 64 * 1024 * 1024
MAX_FILES = 32
LICENSED_NAMESPACES = ("ecoinvent",)


class NativeBundleError(ValueError):
    pass


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Unsupported JSON value: {type(value).__name__}")


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    ).encode("utf-8")


def _row(record: Any) -> dict[str, Any]:
    return {column.key: getattr(record, column.key) for column in inspect(record).mapper.column_attrs}


def _parse_datetime(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            return value
    return value


def _model_kwargs(model_cls: type, payload: dict[str, Any], *, drop: set[str] | None = None) -> dict[str, Any]:
    drop = drop or set()
    result: dict[str, Any] = {}
    for column in inspect(model_cls).columns:
        if column.key in drop or column.key not in payload:
            continue
        value = payload[column.key]
        if isinstance(column.type.python_type, type) and column.type.python_type is datetime:
            value = _parse_datetime(value)
        result[column.key] = value
    return result


def _walk(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def _flow_identities(graphs: list[dict[str, Any]]) -> set[tuple[str, str, str]]:
    identities: set[tuple[str, str, str]] = set()
    for graph in graphs:
        for obj in _walk(graph):
            flow_uuid = str(obj.get("flowUuid") or obj.get("flow_uuid") or "").strip()
            if not flow_uuid:
                continue
            namespace = str(obj.get("flowSourceNamespace") or obj.get("flow_source_namespace") or "").strip()
            version = str(obj.get("flowVersion") or obj.get("flow_version") or "").strip()
            identities.add((flow_uuid, namespace, version))
    return identities


def _is_licensed(namespace: str | None, source: str | None) -> bool:
    value = f"{namespace or ''} {source or ''}".lower()
    return any(token in value for token in LICENSED_NAMESPACES)


def _replace_exact(value: Any, replacements: dict[str, str]) -> Any:
    if isinstance(value, str):
        return replacements.get(value, value)
    if isinstance(value, list):
        return [_replace_exact(item, replacements) for item in value]
    if isinstance(value, dict):
        return {key: _replace_exact(item, replacements) for key, item in value.items()}
    return value


def export_project_bundle(db: Session, project_id: str) -> tuple[bytes, dict[str, Any]]:
    project = db.query(Model).filter(Model.id == project_id).first()
    if project is None:
        raise NativeBundleError("Project not found")

    versions = db.query(ModelVersion).filter(ModelVersion.model_id == project_id).order_by(ModelVersion.version).all()
    version_ids = [item.id for item in versions]
    graphs = [item.hybrid_graph_json for item in versions]
    runs = db.query(RunJob).filter(RunJob.model_version_id.in_(version_ids)).order_by(RunJob.created_at).all() if version_ids else []
    pts_definitions = db.query(PtsDefinition).filter(PtsDefinition.project_id == project_id).all()
    pts_resources = db.query(PtsResource).filter(PtsResource.project_id == project_id).all()
    pts_artifacts = db.query(PtsCompileArtifact).filter(PtsCompileArtifact.project_id == project_id).all()
    pts_external = db.query(PtsExternalArtifact).filter(PtsExternalArtifact.project_id == project_id).all()

    identity_documents = list(graphs)
    identity_documents.extend(item.definition_json for item in pts_definitions)
    identity_documents.extend(item.pts_graph_json for item in pts_resources)
    identity_documents.extend(item.shell_node_json for item in pts_resources)
    identity_documents.extend(item.artifact_json for item in pts_artifacts)
    identity_documents.extend(item.artifact_json for item in pts_external)
    identities = _flow_identities(identity_documents)
    flow_uuids = {item[0] for item in identities}
    flow_rows = db.query(FlowRecord).filter(FlowRecord.flow_uuid.in_(flow_uuids)).all() if flow_uuids else []
    bundled_flows = [item for item in flow_rows if not _is_licensed(item.source_namespace, item.source)]
    dependency_flows = [item for item in flow_rows if _is_licensed(item.source_namespace, item.source)]
    version_rows = db.query(FlowVersionRecord).filter(FlowVersionRecord.flow_uuid.in_(flow_uuids)).all() if flow_uuids else []
    bundled_versions = [item for item in version_rows if not _is_licensed(item.source_namespace, None)]

    unit_group_names = {item.unit_group for item in flow_rows} | {item.unit_group for item in version_rows}
    unit_groups = db.query(UnitGroup).filter(UnitGroup.name.in_(unit_group_names)).all() if unit_group_names else []
    unit_definitions = db.query(UnitDefinition).filter(UnitDefinition.unit_group.in_(unit_group_names)).all() if unit_group_names else []

    dependencies = []
    known = {item.flow_uuid for item in flow_rows}
    for flow_uuid, namespace, version in sorted(identities):
        row = next((item for item in dependency_flows if item.flow_uuid == flow_uuid), None)
        if row is not None or flow_uuid not in known:
            dependencies.append({
                "kind": "flow",
                "flow_uuid": flow_uuid,
                "source_namespace": namespace or (row.source_namespace if row else None),
                "source_version": version or (row.source_version if row else None),
                "reason": "licensed_external" if row is not None else "catalog_record_not_present",
            })

    files: dict[str, Any] = {
        "project.json": _row(project),
        "versions.json": [_row(item) for item in versions],
        "runs.json": [_row(item) for item in runs],
        "catalog/flows.json": [_row(item) for item in bundled_flows],
        "catalog/flow_versions.json": [_row(item) for item in bundled_versions],
        "catalog/unit_groups.json": [_row(item) for item in unit_groups],
        "catalog/unit_definitions.json": [_row(item) for item in unit_definitions],
        "pts/definitions.json": [_row(item) for item in pts_definitions],
        "pts/resources.json": [_row(item) for item in pts_resources],
        "pts/compile_artifacts.json": [_row(item) for item in pts_artifacts],
        "pts/external_artifacts.json": [_row(item) for item in pts_external],
        "dependencies.json": dependencies,
    }
    encoded = {name: _json_bytes(value) for name, value in files.items()}
    manifest = {
        "schema_version": BUNDLE_SCHEMA,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "project_id": project.id,
        "project_name": project.name,
        "content": {name: {"sha256": sha256(data).hexdigest(), "bytes": len(data)} for name, data in sorted(encoded.items())},
        "counts": {
            "versions": len(versions), "runs": len(runs), "flows": len(bundled_flows),
            "flow_versions": len(bundled_versions), "pts_definitions": len(pts_definitions),
            "pts_resources": len(pts_resources), "pts_compile_artifacts": len(pts_artifacts),
            "pts_external_artifacts": len(pts_external), "external_dependencies": len(dependencies),
        },
    }
    output = BytesIO()
    with ZipFile(output, "w", ZIP_DEFLATED) as archive:
        archive.writestr("manifest.json", _json_bytes(manifest))
        for name, data in sorted(encoded.items()):
            archive.writestr(name, data)
    return output.getvalue(), manifest


def _read_bundle(data: bytes) -> tuple[dict[str, Any], dict[str, Any]]:
    if len(data) > MAX_BUNDLE_BYTES:
        raise NativeBundleError("Bundle exceeds the 64 MiB limit")
    try:
        with ZipFile(BytesIO(data)) as archive:
            infos = archive.infolist()
            if len(infos) > MAX_FILES or sum(item.file_size for item in infos) > MAX_BUNDLE_BYTES:
                raise NativeBundleError("Bundle expands beyond the safety limit")
            names = {item.filename for item in infos}
            if any(name.startswith(("/", "\\")) or ".." in name.replace("\\", "/").split("/") for name in names):
                raise NativeBundleError("Bundle contains an unsafe path")
            manifest = json.loads(archive.read("manifest.json"))
            if manifest.get("schema_version") != BUNDLE_SCHEMA:
                raise NativeBundleError("Unsupported native bundle schema")
            payloads: dict[str, Any] = {}
            for name, metadata in manifest.get("content", {}).items():
                if name not in names:
                    raise NativeBundleError(f"Missing bundle entry: {name}")
                raw = archive.read(name)
                if sha256(raw).hexdigest() != metadata.get("sha256"):
                    raise NativeBundleError(f"Checksum mismatch: {name}")
                payloads[name] = json.loads(raw)
            return manifest, payloads
    except (BadZipFile, KeyError, json.JSONDecodeError) as exc:
        raise NativeBundleError("Invalid native project bundle") from exc


def _insert_catalog_rows(db: Session, model_cls: type, rows: list[dict[str, Any]], keys: tuple[str, ...], issues: list[dict[str, Any]]) -> int:
    inserted = 0
    for row in rows:
        query = db.query(model_cls)
        for key in keys:
            query = query.filter(getattr(model_cls, key) == row.get(key))
        existing = query.first()
        if existing is not None:
            ignored = {"id", "created_at", "updated_at", "source_file"}
            mismatched = []
            for key, value in row.items():
                if key in ignored or not hasattr(existing, key):
                    continue
                existing_value = getattr(existing, key)
                expected_value = _parse_datetime(value) if isinstance(existing_value, datetime) else value
                if existing_value != expected_value:
                    mismatched.append(key)
            if mismatched:
                identity = {key: row.get(key) for key in keys}
                raise NativeBundleError(f"Catalog identity conflicts with destination: {identity}; fields={mismatched}")
            continue
        db.add(model_cls(**_model_kwargs(model_cls, row, drop={"id"} if model_cls is UnitDefinition else set())))
        inserted += 1
    return inserted


def import_project_bundle(db: Session, data: bytes, *, conflict_policy: str = "rename") -> dict[str, Any]:
    if conflict_policy not in {"rename", "fail"}:
        raise NativeBundleError("conflict_policy must be rename or fail")
    manifest, payloads = _read_bundle(data)
    source_project = payloads["project.json"]
    source_project_id = str(source_project["id"])
    target_project_id = source_project_id
    existing_project = db.query(Model).filter(Model.id == source_project_id).first()
    if existing_project is not None:
        if conflict_policy == "fail":
            raise NativeBundleError("Project identity already exists")
        target_project_id = str(uuid.uuid4())

    replacements = {source_project_id: target_project_id}
    issues: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    try:
        counts["flows"] = _insert_catalog_rows(db, FlowRecord, payloads.get("catalog/flows.json", []), ("flow_uuid",), issues)
        counts["flow_versions"] = _insert_catalog_rows(
            db, FlowVersionRecord, payloads.get("catalog/flow_versions.json", []),
            ("source_namespace", "flow_uuid", "source_version"), issues,
        )
        counts["unit_groups"] = _insert_catalog_rows(db, UnitGroup, payloads.get("catalog/unit_groups.json", []), ("name",), issues)
        counts["unit_definitions"] = _insert_catalog_rows(
            db, UnitDefinition, payloads.get("catalog/unit_definitions.json", []), ("unit_group", "unit_name"), issues,
        )

        project_payload = dict(source_project)
        project_payload["id"] = target_project_id
        if existing_project is not None:
            project_payload["name"] = f"{project_payload['name']} (restored)"
        db.add(Model(**_model_kwargs(Model, project_payload)))
        db.flush()

        pts_specs = (
            ("pts/definitions.json", PtsDefinition),
            ("pts/resources.json", PtsResource),
            ("pts/compile_artifacts.json", PtsCompileArtifact),
            ("pts/external_artifacts.json", PtsExternalArtifact),
        )
        pts_uuid_values = {
            str(row.get("pts_uuid")) for path, _model_cls in pts_specs
            for row in payloads.get(path, []) if row.get("pts_uuid")
        }
        for old_uuid in pts_uuid_values:
            if db.query(PtsResource).filter(PtsResource.pts_uuid == old_uuid).first() is not None:
                replacements[old_uuid] = str(uuid.uuid4())
        for path, model_cls in pts_specs:
            for row in payloads.get(path, []):
                old_id = str(row.get("id") or "")
                if old_id and db.query(model_cls).filter(model_cls.id == old_id).first() is not None:
                    replacements[old_id] = str(uuid.uuid4())

        version_id_map: dict[str, str] = {}
        hashes_preserved = True
        for row in payloads.get("versions.json", []):
            old_id = str(row["id"])
            new_id = old_id if db.query(ModelVersion).filter(ModelVersion.id == old_id).first() is None else str(uuid.uuid4())
            version_id_map[old_id] = new_id
            transformed = _replace_exact(row, replacements)
            transformed["id"] = new_id
            transformed["model_id"] = target_project_id
            if transformed.get("hybrid_graph_json") != row.get("hybrid_graph_json"):
                transformed["graph_hash"] = compute_graph_hash_from_slim_graph(transformed["hybrid_graph_json"])
                hashes_preserved = False
            db.add(ModelVersion(**_model_kwargs(ModelVersion, transformed)))

        run_count = 0
        for row in payloads.get("runs.json", []):
            transformed = _replace_exact(row, replacements)
            transformed["id"] = str(uuid.uuid4()) if db.query(RunJob).filter(RunJob.id == row.get("id")).first() else row.get("id")
            transformed["model_version_id"] = version_id_map.get(str(row.get("model_version_id")), row.get("model_version_id"))
            db.add(RunJob(**_model_kwargs(RunJob, transformed)))
            run_count += 1

        for path, model_cls in pts_specs:
            count = 0
            for row in payloads.get(path, []):
                transformed = _replace_exact(row, replacements)
                transformed["project_id"] = target_project_id
                transformed["id"] = replacements.get(str(row.get("id")), row.get("id"))
                db.add(model_cls(**_model_kwargs(model_cls, transformed)))
                count += 1
            counts[path.split("/")[-1].removesuffix(".json")] = count

        counts["versions"] = len(version_id_map)
        counts["runs"] = run_count
        dependencies = payloads.get("dependencies.json", [])
        for dependency in dependencies:
            issues.append({"code": "EXTERNAL_DEPENDENCY_NOT_BUNDLED", **dependency})
        db.commit()
    except Exception:
        db.rollback()
        raise

    return {
        "schema_version": BUNDLE_SCHEMA,
        "status": "restored_with_issues" if issues else "restored",
        "source_project_id": source_project_id,
        "project_id": target_project_id,
        "identity_remapped": source_project_id != target_project_id,
        "graph_hashes_preserved": hashes_preserved,
        "counts": counts,
        "issues": issues,
        "source_manifest_sha256": sha256(_json_bytes(manifest)).hexdigest(),
    }
