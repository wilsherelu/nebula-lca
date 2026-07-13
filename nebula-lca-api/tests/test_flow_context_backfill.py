from pathlib import Path

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session

from app.database import Base
from app.flow_context_backfill import backfill_ecoinvent_flow_contexts
from app.models import FlowRecord, LciBiosphereFlowKey
from app.schema_maintenance import ensure_flow_catalog_context_columns
from app.services.lci_runtime import inventory_with_flow_metadata


def _flow(flow_uuid: str, *, source: str = "ecoinvent_3.11", flow_type: str = "Elementary flow") -> FlowRecord:
    return FlowRecord(
        flow_uuid=flow_uuid,
        flow_name=flow_uuid,
        flow_type=flow_type,
        default_unit="kg",
        unit_group="mass",
        compartment="air",
        source=source,
    )


def test_context_schema_maintenance_is_idempotent(tmp_path: Path) -> None:
    engine = create_engine(f"sqlite:///{(tmp_path / 'schema.db').as_posix()}")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE flow_catalog (flow_uuid VARCHAR(64) PRIMARY KEY)"))

    first = ensure_flow_catalog_context_columns(engine)
    second = ensure_flow_catalog_context_columns(engine)

    assert first["status"] == "ok"
    assert second["status"] == "already_complete"
    assert "subcompartment" in {column["name"] for column in inspect(engine).get_columns("flow_catalog")}


def test_backfill_is_uuid_based_and_preserves_conflicting_source(tmp_path: Path) -> None:
    engine = create_engine(f"sqlite:///{(tmp_path / 'backfill.db').as_posix()}")
    Base.metadata.create_all(engine)
    masterdata = Path(__file__).parent / "fixtures" / "ef31_masterdata"

    with Session(engine) as db:
        db.add(_flow("elem_uuid_co2_air"))
        db.add(_flow("elem_uuid_ch4_air", source="tiangong", flow_type="Product flow"))
        db.commit()

        dry_run = backfill_ecoinvent_flow_contexts(db, masterdata_dir=masterdata, apply=False)
        assert dry_run["changed"] == 1
        assert dry_run["conflicting_flow_uuids"] == ["elem_uuid_ch4_air"]
        assert db.get(FlowRecord, "elem_uuid_co2_air").subcompartment is None

        applied = backfill_ecoinvent_flow_contexts(db, masterdata_dir=masterdata, apply=True)
        assert applied["changed"] == 1
        assert db.get(FlowRecord, "elem_uuid_co2_air").subcompartment == "non-urban air"
        assert db.get(FlowRecord, "elem_uuid_ch4_air").source == "tiangong"


def test_legacy_flow_key_reads_catalog_subcompartment(tmp_path: Path) -> None:
    engine = create_engine(f"sqlite:///{(tmp_path / 'legacy-key.db').as_posix()}")
    Base.metadata.create_all(engine)

    with Session(engine) as db:
        db.add(_flow("eco-co2"))
        db.flush()
        flow = db.get(FlowRecord, "eco-co2")
        flow.subcompartment = "unspecified"
        key = LciBiosphereFlowKey(
            flow_uuid="eco-co2",
            compartment="air",
            subcompartment="",
            direction="output",
            canonical_unit="kg",
        )
        db.add(key)
        db.commit()

        result = inventory_with_flow_metadata(db, {key.flow_key_id: 1.0})

    assert result[0]["compartment"] == "air"
    assert result[0]["subcompartment"] == "unspecified"
