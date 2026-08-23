import json
import io
import zipfile

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from fastapi.testclient import TestClient

from app.database import Base, SessionLocal, engine
from app.main import app
from app.models import FlowRecord, FlowVersionRecord, ReferenceProcess
from app.schema_maintenance import ensure_flow_catalog_tidas_columns, ensure_flow_version_storage
from app.services.flow_versions import (
    TG_LEGACY_NAMESPACE,
    TG_LEGACY_VERSION,
    TIDAS_NAMESPACE,
    backfill_tg_legacy_flow_versions,
    create_flow_version_snapshot,
)
from app.services.tidas_import_core import import_tidas_flow_rows, import_tidas_process_rows


Base.metadata.create_all(bind=engine)


def _flow_row(flow_uuid: str, version: str | None, unit: str, unit_group: str) -> dict:
    return {
        "id": flow_uuid,
        "version": version,
        "name": f"Flow {version or 'legacy'}",
        "flow_type": "Product flow",
        "default_unit": unit,
        "unit_group": unit_group,
        "source": "tiangong",
    }


def test_tidas_versions_are_immutable_and_do_not_overwrite_compatibility_catalog() -> None:
    flow_uuid = "versioned-flow-regression"
    db = SessionLocal()
    try:
        first = import_tidas_flow_rows(
            db,
            [_flow_row(flow_uuid, "01.01.002", "kg", "Units of mass")],
            source_path="tiangong://flows/versioned-flow-regression",
            source_label="tiangong",
            source_namespace=TIDAS_NAMESPACE,
            require_source_version=True,
            persist_report=False,
            with_transaction=True,
        )
        second = import_tidas_flow_rows(
            db,
            [_flow_row(flow_uuid, "01.01.003", "MJ", "Units of energy")],
            source_path="tiangong://flows/versioned-flow-regression",
            source_label="tiangong",
            source_namespace=TIDAS_NAMESPACE,
            require_source_version=True,
            persist_report=False,
            with_transaction=True,
        )
        db.flush()

        assert first.failed == 0
        assert second.failed == 0
        catalog = db.get(FlowRecord, flow_uuid)
        assert catalog is not None
        assert (catalog.source_version, catalog.default_unit, catalog.unit_group) == (
            "01.01.002",
            "kg",
            "Units of mass",
        )
        snapshots = (
            db.query(FlowVersionRecord)
            .filter_by(source_namespace=TIDAS_NAMESPACE, flow_uuid=flow_uuid)
            .order_by(FlowVersionRecord.source_version)
            .all()
        )
        assert [(row.source_version, row.default_unit, row.unit_group) for row in snapshots] == [
            ("01.01.002", "kg", "Units of mass"),
            ("01.01.003", "MJ", "Units of energy"),
        ]
    finally:
        db.rollback()
        db.close()


def test_flow_upload_api_persists_same_uuid_as_distinct_exact_versions() -> None:
    flow_uuid = "flow-upload-api-version-regression"
    with TestClient(app) as client:
        for version, unit, unit_group in (
            ("01.01.002", "kg", "Units of mass"),
            ("01.01.003", "MJ", "Units of energy"),
        ):
            response = client.post(
                "/api/import/tidas/flows",
                files={
                    "file": (
                        f"{flow_uuid}-{version}.json",
                        json.dumps(_flow_row(flow_uuid, version, unit, unit_group)),
                        "application/json",
                    )
                },
            )
            assert response.status_code == 200, response.text
            assert response.json()["failed"] == 0

    db = SessionLocal()
    try:
        snapshots = (
            db.query(FlowVersionRecord)
            .filter_by(source_namespace=TIDAS_NAMESPACE, flow_uuid=flow_uuid)
            .order_by(FlowVersionRecord.source_version)
            .all()
        )
        assert [(row.source_version, row.default_unit, row.unit_group) for row in snapshots] == [
            ("01.01.002", "kg", "Units of mass"),
            ("01.01.003", "MJ", "Units of energy"),
        ]
    finally:
        db.query(FlowVersionRecord).filter_by(flow_uuid=flow_uuid).delete()
        db.query(FlowRecord).filter_by(flow_uuid=flow_uuid).delete()
        db.commit()
        db.close()


def test_open_source_package_flow_is_pinned_as_tg_1_0() -> None:
    flow_uuid = "legacy-flow-regression"
    db = SessionLocal()
    try:
        report = import_tidas_flow_rows(
            db,
            [_flow_row(flow_uuid, None, "kg", "Units of mass")],
            source_path="legacy-package.7z",
            source_label="tiangong",
            persist_report=False,
            with_transaction=True,
        )
        db.flush()

        assert report.failed == 0
        catalog = db.get(FlowRecord, flow_uuid)
        assert catalog is not None
        assert (catalog.source_namespace, catalog.source_version, catalog.version_label) == (
            TG_LEGACY_NAMESPACE,
            TG_LEGACY_VERSION,
            "TG 1.0",
        )
        snapshot = (
            db.query(FlowVersionRecord)
            .filter_by(
                source_namespace=TG_LEGACY_NAMESPACE,
                flow_uuid=flow_uuid,
                source_version=TG_LEGACY_VERSION,
            )
            .one()
        )
        assert snapshot.default_unit == "kg"
    finally:
        db.rollback()
        db.close()


def test_authoritative_package_replaces_only_provisional_tg_1_0_snapshot() -> None:
    flow_uuid = "provisional-legacy-flow-regression"
    db = SessionLocal()
    try:
        provisional = _flow_row(flow_uuid, None, "MJ", "Units of energy")
        provisional["flow_uuid"] = flow_uuid
        provisional["flow_name"] = provisional.pop("name")
        create_flow_version_snapshot(
            db,
            flow_record=provisional,
            source_namespace=TG_LEGACY_NAMESPACE,
            source_version=TG_LEGACY_VERSION,
            metadata={"migration": "existing_open_source_catalog"},
        )
        db.add(FlowRecord(
            flow_uuid=flow_uuid,
            flow_name="Provisional flow",
            flow_type="Product flow",
            default_unit="MJ",
            unit_group="Units of energy",
            source="tiangong",
            is_custom=False,
            source_namespace=TG_LEGACY_NAMESPACE,
            source_version=TG_LEGACY_VERSION,
            version_label="TG 1.0",
        ))
        db.flush()

        report = import_tidas_flow_rows(
            db,
            [_flow_row(flow_uuid, None, "kg", "Units of mass")],
            source_path="authoritative-tg-1.0.zip",
            source_label="tiangong",
            persist_report=False,
            with_transaction=True,
        )
        db.flush()

        assert report.failed == 0
        snapshot = db.query(FlowVersionRecord).filter_by(flow_uuid=flow_uuid).one()
        assert (snapshot.default_unit, snapshot.unit_group) == ("kg", "Units of mass")
        assert snapshot.metadata_json["replaced_provisional_migration"] is True
        catalog = db.get(FlowRecord, flow_uuid)
        assert catalog is not None
        assert (catalog.default_unit, catalog.unit_group) == ("kg", "Units of mass")
    finally:
        db.rollback()
        db.close()


def test_existing_mutable_catalog_is_not_guessed_as_tg_1_0() -> None:
    flow_uuid = "existing-tg-catalog-regression"
    db = SessionLocal()
    try:
        db.add(FlowRecord(
            flow_uuid=flow_uuid,
            flow_name="Existing TianGong flow",
            flow_type="Product flow",
            default_unit="kg",
            unit_group="Units of mass",
            source="tiangong",
            is_custom=False,
        ))
        db.flush()

        assert backfill_tg_legacy_flow_versions(db) == 0

        catalog = db.get(FlowRecord, flow_uuid)
        assert catalog is not None
        assert (catalog.source_namespace, catalog.source_version, catalog.version_label) == (None, None, None)
        assert db.query(FlowVersionRecord).filter_by(flow_uuid=flow_uuid).count() == 0
    finally:
        db.rollback()
        db.close()


def test_legacy_database_schema_migrates_without_changing_flow_units() -> None:
    legacy_engine = create_engine("sqlite:///:memory:")
    with legacy_engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE flow_catalog (
                flow_uuid VARCHAR(64) PRIMARY KEY,
                flow_name VARCHAR(255) NOT NULL,
                flow_name_en VARCHAR(255),
                flow_type VARCHAR(64) NOT NULL,
                default_unit VARCHAR(64) NOT NULL,
                unit_group VARCHAR(128) NOT NULL,
                compartment VARCHAR(255),
                subcompartment VARCHAR(255),
                source_updated_at VARCHAR(64),
                source VARCHAR(64),
                is_custom BOOLEAN NOT NULL DEFAULT 0,
                tidas_compatible BOOLEAN NOT NULL DEFAULT 0,
                tidas_unit_group VARCHAR(128),
                tidas_flow_property_uuid VARCHAR(64),
                tidas_reference_source VARCHAR(128),
                allocation_properties JSON
            )
        """))
        connection.execute(text("""
            INSERT INTO flow_catalog (
                flow_uuid, flow_name, flow_type, default_unit, unit_group,
                source, is_custom, tidas_compatible
            ) VALUES (
                'legacy-schema-flow', 'Legacy flow', 'Product flow', 'kg',
                'Units of mass', 'tiangong', 0, 1
            )
        """))

    migration = ensure_flow_catalog_tidas_columns(legacy_engine)
    assert set(migration["added_columns"]) == {"source_namespace", "source_version", "version_label"}

    with Session(legacy_engine) as db:
        first = ensure_flow_version_storage(legacy_engine, db)
        second = ensure_flow_version_storage(legacy_engine, db)
        assert first["legacy_snapshots_created"] == 0
        assert second["legacy_snapshots_created"] == 0
        catalog = db.get(FlowRecord, "legacy-schema-flow")
        assert catalog is not None
        assert (catalog.default_unit, catalog.unit_group) == ("kg", "Units of mass")
        assert db.query(FlowVersionRecord).count() == 0


def test_open_source_process_exchange_is_pinned_to_tg_1_0() -> None:
    flow_uuid = "legacy-process-flow-regression"
    process_uuid = "legacy-process-regression"
    db = SessionLocal()
    try:
        import_tidas_flow_rows(
            db,
            [_flow_row(flow_uuid, "01.01.009", "kg", "Units of mass")],
            source_path="legacy-package.zip",
            source_label="tiangong",
            persist_report=False,
            with_transaction=True,
        )
        report = import_tidas_process_rows(
            db,
            [{
                "process_uuid": process_uuid,
                "process_name": "Legacy package process",
                "reference_flow_uuid": flow_uuid,
                "exchanges": [{
                    "flow_uuid": flow_uuid,
                    "flow_name": "Legacy package flow",
                    "flow_version": "01.01.009",
                    "direction": "output",
                    "amount": 1,
                    "is_reference_flow": True,
                }],
            }],
            source_path="legacy-package.zip",
            valid_flow_uuids={flow_uuid},
            persist_report=False,
            with_transaction=True,
        )
        db.flush()

        assert report.failed == 0
        process = db.get(ReferenceProcess, process_uuid)
        assert process is not None
        exchange = process.process_json["exchanges"][0]
        assert exchange["flow_source_namespace"] == TG_LEGACY_NAMESPACE
        assert exchange["flow_version"] == TG_LEGACY_VERSION
        assert exchange["unit_group"] == "Units of mass"
    finally:
        db.rollback()
        db.close()


def test_process_bundle_api_pins_flow_and_exchange_to_same_tg_1_0_identity() -> None:
    flow_uuid = "bundle-api-flow-version-regression"
    process_uuid = "bundle-api-process-version-regression"
    flow_payload = _flow_row(flow_uuid, "01.01.009", "kg", "Units of mass")
    process_payload = {
        "process_uuid": process_uuid,
        "process_name": "Bundled process",
        "reference_flow_uuid": flow_uuid,
        "exchanges": [{
            "flow_uuid": flow_uuid,
            "flow_name": "Bundled flow",
            "flow_version": "01.01.009",
            "direction": "output",
            "amount": 1,
            "is_reference_flow": True,
        }],
    }
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("manifest.json", json.dumps({
            "bundle_schema_version": "tidas-lca-bundle-v1",
            "flow_dir": "flow",
            "process_dir": "process",
        }))
        archive.writestr("flow/flow.json", json.dumps(flow_payload))
        archive.writestr("process/process.json", json.dumps(process_payload))

    with TestClient(app) as client:
        response = client.post(
            "/api/import/tidas/processes",
            files={"file": ("tg-1.0.zip", buffer.getvalue(), "application/zip")},
        )
        assert response.status_code == 200, response.text
        assert response.json()["failed"] == 0

    db = SessionLocal()
    try:
        snapshot = db.query(FlowVersionRecord).filter_by(
            source_namespace=TG_LEGACY_NAMESPACE,
            flow_uuid=flow_uuid,
            source_version=TG_LEGACY_VERSION,
        ).one()
        assert (snapshot.default_unit, snapshot.unit_group) == ("kg", "Units of mass")
        process = db.get(ReferenceProcess, process_uuid)
        assert process is not None
        exchange = process.process_json["exchanges"][0]
        assert exchange["flow_source_namespace"] == TG_LEGACY_NAMESPACE
        assert exchange["flow_version"] == TG_LEGACY_VERSION
        assert (exchange["unit"], exchange["unit_group"]) == ("kg", "Units of mass")
    finally:
        db.query(ReferenceProcess).filter_by(process_uuid=process_uuid).delete()
        db.query(FlowVersionRecord).filter_by(flow_uuid=flow_uuid).delete()
        db.query(FlowRecord).filter_by(flow_uuid=flow_uuid).delete()
        db.commit()
        db.close()


def test_process_exchange_resolves_exact_flow_version_unit_group() -> None:
    flow_uuid = "process-versioned-flow-regression"
    process_uuid = "process-versioned-regression"
    db = SessionLocal()
    try:
        for version, unit, unit_group in (
            ("01.01.002", "kg", "Units of mass"),
            ("01.01.003", "MJ", "Units of energy"),
        ):
            import_tidas_flow_rows(
                db,
                [_flow_row(flow_uuid, version, unit, unit_group)],
                source_path=f"tiangong://flows/{flow_uuid}",
                source_label="tiangong",
                source_namespace=TIDAS_NAMESPACE,
                require_source_version=True,
                persist_report=False,
                with_transaction=True,
            )
        report = import_tidas_process_rows(
            db,
            [{
                "process_uuid": process_uuid,
                "process_name": "Versioned process",
                "reference_flow_uuid": flow_uuid,
                "exchanges": [{
                    "flow_uuid": flow_uuid,
                    "flow_name": "Versioned flow",
                    "flow_version": "01.01.003",
                    "direction": "output",
                    "amount": 1,
                    "is_reference_flow": True,
                }],
            }],
            source_path=f"tiangong://processes/{process_uuid}",
            valid_flow_uuids={flow_uuid},
            flow_source_namespace=TIDAS_NAMESPACE,
            require_flow_versions=True,
            persist_report=False,
            with_transaction=True,
        )
        db.flush()

        assert report.failed == 0
        process = db.get(ReferenceProcess, process_uuid)
        assert process is not None
        exchange = process.process_json["exchanges"][0]
        assert exchange["flow_version"] == "01.01.003"
        assert exchange["unit"] == "MJ"
        assert exchange["unit_group"] == "Units of energy"
    finally:
        db.rollback()
        db.close()
