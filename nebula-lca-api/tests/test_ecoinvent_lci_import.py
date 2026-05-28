"""Tests for ecoinvent LCI data foundation import.

Covers:
- lci_exchange_matrix schema creation
- Unit catalog import from Units.xml + UnitConversions.xml
- Elementary flow import from ElementaryExchanges.xml
- Intermediate flow import from IntermediateExchanges.xml
- SPOLD process metadata import (lightweight)
- Elementary exchange aggregation to lci_exchange_matrix
"""

import textwrap
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.lci_vector_codec import pack_lci_vector, unpack_lci_flow_key_ids, unpack_lci_vector
from app.schemas import HybridGraph
from app.services.lci_runtime import (
    expand_graph_lci_inventory,
    expand_lci_vectors_into_graph,
    inventory_with_flow_metadata,
    load_process_vectors,
    top_process_vector_exchanges,
)
from app.models import (
    Base,
    FlowRecord,
    LciBiosphereFlowKey,
    LciExchangeMatrix,
    LciProcessVector,
    ReferenceProcess,
    UnitDefinition,
    UnitGroup,
)
from app.schema_maintenance import ensure_lci_exchange_matrix_table
from app.ingest_ecoinvent import (
    import_ecoinvent_elementary_flows,
    import_ecoinvent_intermediate_flows,
    import_ecoinvent_processes,
    import_ecoinvent_units,
    write_ecoinvent_exchanges_to_matrix,
    write_ecoinvent_process_vector,
    _guess_unit_group_from_name,
)
from app.ecoinvent_ef31_loader import parse_filename_to_activity, parse_spold_filename_dataset_ids


# ======================================================================
# Fixtures
# ======================================================================


@pytest.fixture()
def db_session(tmp_path: Path):
    """Create an in-memory SQLite database with all tables."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    ensure_lci_exchange_matrix_table(engine)
    Session = sessionmaker(bind=engine)
    db = Session()
    yield db
    db.close()


@pytest.fixture()
def sample_units_xml(tmp_path: Path) -> Path:
    """Create a minimal Units.xml."""
    xml = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <units xmlns="http://www.ecoinvent.org/1.0/spold">
      <unit id="unit-kg"><name>kg</name></unit>
      <unit id="unit-g"><name>g</name></unit>
      <unit id="unit-mj"><name>MJ</name></unit>
      <unit id="unit-kwh"><name>kWh</name></unit>
      <unit id="unit-m3"><name>m3</name></unit>
      <unit id="unit-l"><name>l</name></unit>
      <unit id="unit-kbq"><name>kBq</name></unit>
    </units>
    """)
    p = tmp_path / "Units.xml"
    p.write_text(xml, encoding="utf-8")
    return p


@pytest.fixture()
def sample_conversions_xml(tmp_path: Path) -> Path:
    """Create a minimal UnitConversions.xml."""
    xml = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <unitConversions xmlns="http://www.ecoinvent.org/1.0/spold">
      <unitConversion id="conv-1" factor="0.001">
        <unitFromName>g</unitFromName>
        <unitToName>kg</unitToName>
        <unitType>mass</unitType>
      </unitConversion>
      <unitConversion id="conv-2" factor="3.6">
        <unitFromName>kWh</unitFromName>
        <unitToName>MJ</unitToName>
        <unitType>energy</unitType>
      </unitConversion>
    </unitConversions>
    """)
    p = tmp_path / "UnitConversions.xml"
    p.write_text(xml, encoding="utf-8")
    return p


@pytest.fixture()
def sample_elementary_xml(tmp_path: Path) -> Path:
    """Create a minimal ElementaryExchanges.xml."""
    xml = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <ElementaryExchanges xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
      <ElementaryFlow>
        <uuid>flow-co2-air-001</uuid>
        <shortNameEN>CO2</shortNameEN>
        <nameEN>Carbon dioxide</nameEN>
        <flowType>Elementary flow</flowType>
        <unit>kg</unit>
        <unitGroup>mass</unitGroup>
        <compartment>air</compartment>
        <formula>CO2</formula>
      </ElementaryFlow>
      <ElementaryFlow>
        <uuid>flow-ch4-air-002</uuid>
        <shortNameEN>Methane</shortNameEN>
        <nameEN>Methane</nameEN>
        <flowType>Elementary flow</flowType>
        <unit>kg</unit>
        <unitGroup>mass</unitGroup>
        <compartment>air</compartment>
        <formula>CH4</formula>
      </ElementaryFlow>
      <ElementaryFlow>
        <uuid>flow-phosph-lake-003</uuid>
        <shortNameEN>Phosphorus (lake)</shortNameEN>
        <nameEN>Phosphorus to lake</nameEN>
        <flowType>Elementary flow</flowType>
        <unit>kg</unit>
        <unitGroup>mass</unitGroup>
        <compartment>water</compartment>
      </ElementaryFlow>
    </ElementaryExchanges>
    """)
    p = tmp_path / "ElementaryExchanges.xml"
    p.write_text(xml, encoding="utf-8")
    return p


@pytest.fixture()
def sample_intermediate_xml(tmp_path: Path) -> Path:
    """Create a minimal IntermediateExchanges.xml."""
    xml = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <IntermediateExchanges xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
      <intermediateExchange id="flow-electric-ac-001" unitId="unit-kg">
        <name>Electricity, medium voltage</name>
      </intermediateExchange>
      <intermediateExchange id="flow-natural-gas-002" unitId="unit-mj">
        <name>Natural gas</name>
      </intermediateExchange>
      <intermediateExchange id="flow-waste-003" unitId="unit-kg" classification="By-product classification" classificationValue="waste">
        <name>Waste to incineration</name>
      </intermediateExchange>
    </IntermediateExchanges>
    """)
    p = tmp_path / "IntermediateExchanges.xml"
    p.write_text(xml, encoding="utf-8")
    return p


@pytest.fixture()
def sample_spold_files(tmp_path: Path) -> list[Path]:
    """Create 3 minimal .spold XML files."""
    files = []
    base_ns = 'xmlns:es="http://www.EcoInvent.org/EcoSpold02"'
    spolds = [
        {
            "name": "dataset-001.spold",
            "activity_id": "proc-aaa-001",
            "activity_name": "Cement production",
            "location": "DE",
            "ref_product": "cement",
            "ref_unit": "kg",
            "ref_amount": "1",
            "exchanges": [
                '<es:elementaryExchange elementaryExchangeId="flow-co2-air-001" amount="0.8" outputGroup="output"><es:name>CO2</es:name><es:unitName>kg</es:unitName></es:elementaryExchange>',
                '<es:elementaryExchange elementaryExchangeId="flow-ch4-air-002" amount="0.001" outputGroup="output"><es:name>Methane</es:name><es:unitName>kg</es:unitName></es:elementaryExchange>',
            ],
        },
        {
            "name": "dataset-002.spold",
            "activity_id": "proc-bbb-002",
            "activity_name": "Steel production",
            "location": "DE",
            "ref_product": "steel",
            "ref_unit": "kg",
            "ref_amount": "1",
            "exchanges": [
                '<es:elementaryExchange elementaryExchangeId="flow-co2-air-001" amount="1.5" outputGroup="output"><es:name>CO2</es:name><es:unitName>kg</es:unitName></es:elementaryExchange>',
                '<es:elementaryExchange elementaryExchangeId="flow-phosph-lake-003" amount="0.0001" outputGroup="-1"><es:name>Phosphorus</es:name><es:unitName>kg</es:unitName></es:elementaryExchange>',
            ],
        },
        {
            "name": "dataset-003.spold",
            "activity_id": "proc-ccc-003",
            "activity_name": "Plastic molding",
            "location": "DE",
            "ref_product": "plastic parts",
            "ref_unit": "kg",
            "ref_amount": "1",
            "exchanges": [
                '<es:elementaryExchange elementaryExchangeId="flow-co2-air-001" amount="0.5" outputGroup="output"><es:name>CO2</es:name><es:unitName>kg</es:unitName></es:elementaryExchange>',
            ],
        },
    ]

    for s in spolds:
        xml = textwrap.dedent(f"""\
        <?xml version="1.0" encoding="UTF-8"?>
        <es:generalData es:ecoSpoldVersion="0.2" {base_ns}>
          <es:activity id="{s['activity_id']}" location="{s['location']}">
            <es:activityName>{s['activity_name']}</es:activityName>
            <es:intermediateExchange id="rp-001" variableName="RP" outputGroup="0" amount="{s['ref_amount']}">
              <es:name>{s['ref_product']}</es:name>
              <es:unitName>{s['ref_unit']}</es:unitName>
            </es:intermediateExchange>
            {''.join(s['exchanges'])}
          </es:activity>
        </es:generalData>
        """)
        p = tmp_path / s["name"]
        p.write_text(xml, encoding="utf-8")
        files.append(p)

    return files


# ======================================================================
# Tests: Unit group name guessing
# ======================================================================


class TestUnitGroupGuessing:
    def test_mass_units(self):
        assert _guess_unit_group_from_name("kg") == "mass"
        assert _guess_unit_group_from_name("g") == "mass"
        assert _guess_unit_group_from_name("mg") == "mass"
        assert _guess_unit_group_from_name("metric ton") == "mass"

    def test_energy_units(self):
        assert _guess_unit_group_from_name("MJ") == "energy"
        assert _guess_unit_group_from_name("kWh") == "energy"
        assert _guess_unit_group_from_name("kilojoule") == "energy"

    def test_volume_units(self):
        assert _guess_unit_group_from_name("m3") == "volume"
        assert _guess_unit_group_from_name("liter") == "volume"
        assert _guess_unit_group_from_name("l") == "volume"

    def test_unknown_unit(self):
        assert _guess_unit_group_from_name("unknown_unit") is None


# ======================================================================
# Tests: Unit catalog import
# ======================================================================


class TestUnitImport:
    def test_import_units_creates_groups(self, db_session, sample_units_xml):
        result = import_ecoinvent_units(
            db_session,
            data_dir=str(sample_units_xml.parent),
            package_version="ecoinvent_3.11",
        )
        assert result["groups_inserted"] >= 3  # mass, energy, volume
        assert result["units_inserted"] >= 6

    def test_import_units_persists(self, db_session, sample_units_xml):
        import_ecoinvent_units(
            db_session,
            data_dir=str(sample_units_xml.parent),
            package_version="ecoinvent_3.11",
        )
        groups = db_session.query(UnitGroup).all()
        assert len(groups) >= 3
        group_names = {g.name for g in groups}
        assert "mass" in group_names
        assert "energy" in group_names

    def test_import_conversions_no_error(self, db_session, sample_units_xml, sample_conversions_xml):
        result = import_ecoinvent_units(
            db_session,
            data_dir=str(sample_units_xml.parent),
            package_version="ecoinvent_3.11",
        )
        assert "conversions_count" in result

    def test_import_conversions_set_reference_factors(self, db_session, sample_units_xml, sample_conversions_xml):
        import_ecoinvent_units(
            db_session,
            data_dir=str(sample_units_xml.parent),
            package_version="ecoinvent_3.11",
        )
        gram = db_session.query(UnitDefinition).filter_by(unit_name="g").first()
        kwh = db_session.query(UnitDefinition).filter_by(unit_name="kWh").first()
        assert gram is not None
        assert kwh is not None
        assert abs(gram.factor_to_reference - 0.001) < 1e-12
        assert abs(kwh.factor_to_reference - 3.6) < 1e-12

    def test_import_conversions_keeps_fallback_units_not_in_conversion_graph(self, db_session, sample_units_xml, sample_conversions_xml):
        import_ecoinvent_units(
            db_session,
            data_dir=str(sample_units_xml.parent),
            package_version="ecoinvent_3.11",
        )
        kbq = db_session.query(UnitDefinition).filter_by(unit_name="kBq").first()
        assert kbq is not None
        assert kbq.unit_group == "radioactivity"
        assert kbq.is_reference is True


# ======================================================================
# Tests: Elementary flow import
# ======================================================================


class TestElementaryFlowImport:
    def test_import_elementary_flows(self, db_session, sample_units_xml, sample_elementary_xml):
        result = import_ecoinvent_elementary_flows(
            db_session,
            data_dir=str(sample_units_xml.parent),
            source="ecoinvent_3.11",
        )
        assert result["inserted"] == 3
        assert result["skipped"] == 0

    def test_elementary_flows_have_correct_type(self, db_session, sample_units_xml, sample_elementary_xml):
        import_ecoinvent_elementary_flows(
            db_session,
            data_dir=str(sample_units_xml.parent),
            source="ecoinvent_3.11",
        )
        flows = db_session.query(FlowRecord).all()
        for f in flows:
            assert f.flow_type == "Elementary flow"
            assert f.source == "ecoinvent_3.11"

    def test_elementary_flows_have_compartment(self, db_session, sample_units_xml, sample_elementary_xml):
        import_ecoinvent_elementary_flows(
            db_session,
            data_dir=str(sample_units_xml.parent),
            source="ecoinvent_3.11",
        )
        co2 = db_session.query(FlowRecord).filter_by(flow_uuid="flow-co2-air-001").first()
        assert co2 is not None
        assert co2.compartment == "air"

    def test_duplicate_import_skips(self, db_session, sample_units_xml, sample_elementary_xml):
        import_ecoinvent_elementary_flows(
            db_session,
            data_dir=str(sample_units_xml.parent),
            source="ecoinvent_3.11",
        )
        result2 = import_ecoinvent_elementary_flows(
            db_session,
            data_dir=str(sample_units_xml.parent),
            source="ecoinvent_3.11",
        )
        # Second import should skip (no replace mode)
        assert result2["skipped"] == 3 or result2["inserted"] == 0

    def test_ecoinvent_elementary_flow_overwrites_conflicting_product_flow(self, db_session, sample_units_xml, sample_elementary_xml):
        db_session.add(
            FlowRecord(
                flow_uuid="flow-co2-air-001",
                flow_name="Legacy product",
                flow_name_en="Legacy product",
                flow_type="Product flow",
                default_unit="kg",
                unit_group="Units of mass",
                compartment="legacy",
                source="tiangong",
                is_custom=True,
                tidas_compatible=True,
                allocation_properties={"legacy": True},
            )
        )
        db_session.commit()

        result = import_ecoinvent_elementary_flows(
            db_session,
            data_dir=str(sample_units_xml.parent),
            source="ecoinvent_3.11",
        )

        flow = db_session.get(FlowRecord, "flow-co2-air-001")
        assert result["conflicts_overwritten"] == 1
        assert flow.flow_name == "CO2"
        assert flow.flow_name_en == "CO2"
        assert flow.flow_type == "Elementary flow"
        assert flow.compartment == "air"
        assert flow.source == "ecoinvent_3.11"
        assert flow.is_custom is False
        assert flow.tidas_compatible is False
        assert flow.allocation_properties is None

    def test_ecoinvent_elementary_flow_preserves_existing_builtin_elementary_source(self, db_session, sample_units_xml, sample_elementary_xml):
        db_session.add(
            FlowRecord(
                flow_uuid="flow-co2-air-001",
                flow_name="CO2 builtin",
                flow_name_en="CO2 builtin",
                flow_type="Elementary flow",
                default_unit="kg",
                unit_group="Units of mass",
                compartment="air",
                source="ef3.1",
                is_custom=False,
                tidas_compatible=True,
                tidas_reference_source="tiangong",
            )
        )
        db_session.commit()

        result = import_ecoinvent_elementary_flows(
            db_session,
            data_dir=str(sample_units_xml.parent),
            source="ecoinvent_3.11",
        )

        flow = db_session.get(FlowRecord, "flow-co2-air-001")
        assert result["conflicts_overwritten"] == 1
        assert flow.flow_name == "CO2"
        assert flow.flow_type == "Elementary flow"
        assert flow.source == "ef3.1"
        assert flow.tidas_compatible is True
        assert flow.tidas_reference_source == "tiangong"


# ======================================================================
# Tests: Intermediate flow import
# ======================================================================


class TestIntermediateFlowImport:
    def test_import_intermediate_flows(self, db_session, sample_units_xml, sample_intermediate_xml):
        result = import_ecoinvent_intermediate_flows(
            db_session,
            data_dir=str(sample_units_xml.parent),
            source="ecoinvent_3.11",
        )
        assert result["inserted"] == 3

    def test_waste_flow_type(self, db_session, sample_units_xml, sample_intermediate_xml):
        import_ecoinvent_intermediate_flows(
            db_session,
            data_dir=str(sample_units_xml.parent),
            source="ecoinvent_3.11",
        )
        waste = db_session.query(FlowRecord).filter_by(flow_uuid="flow-waste-003").first()
        assert waste is not None
        assert waste.flow_type == "Waste flow"

    def test_product_flow_type(self, db_session, sample_units_xml, sample_intermediate_xml):
        import_ecoinvent_intermediate_flows(
            db_session,
            data_dir=str(sample_units_xml.parent),
            source="ecoinvent_3.11",
        )
        elec = db_session.query(FlowRecord).filter_by(flow_uuid="flow-electric-ac-001").first()
        assert elec is not None
        assert elec.flow_type == "Product flow"


# ======================================================================
# Tests: SPOLD process metadata import
# ======================================================================


class TestSPOLDProcessImport:
    def test_import_processes_metadata_only(self, db_session, sample_spold_files, sample_elementary_xml, sample_units_xml):
        # First import flows so reference_flow_uuid can resolve
        import_ecoinvent_elementary_flows(
            db_session,
            data_dir=str(sample_units_xml.parent),
            source="ecoinvent_3.11",
        )

        spold_dir = sample_spold_files[0].parent
        result = import_ecoinvent_processes(
            db_session,
            spold_dir=str(spold_dir),
            package_version="ecoinvent_3.11",
        )
        assert result["processes_inserted"] == 3
        assert result["exchange_count"] > 0
        assert result["matrix_rows_inserted"] == 0
        assert result["vector_rows_written"] == 3
        assert result["vector_nnz_total"] > 0
        assert db_session.query(LciExchangeMatrix).count() == 0
        assert db_session.query(LciProcessVector).count() == 3

    def test_processes_have_lightweight_json(self, db_session, sample_spold_files):
        spold_dir = sample_spold_files[0].parent
        import_ecoinvent_processes(
            db_session,
            spold_dir=str(spold_dir),
            package_version="ecoinvent_3.11",
        )
        procs = db_session.query(ReferenceProcess).all()
        assert len(procs) == 3
        for p in procs:
            assert p.import_mode == "ecoinvent_ef31_lci"
            assert p.process_type == "lci_dataset"
            assert p.process_json is not None
            # process_json should NOT contain full exchanges list
            pj = p.process_json
            # It should have lightweight metadata
            assert "exchange_count" in pj
            assert "process_name" in pj or "process_uuid" in pj

    def test_processes_have_import_mode(self, db_session, sample_spold_files):
        spold_dir = sample_spold_files[0].parent
        import_ecoinvent_processes(
            db_session,
            spold_dir=str(spold_dir),
            package_version="ecoinvent_3.11",
        )
        procs = db_session.query(ReferenceProcess).filter_by(
            import_mode="ecoinvent_ef31_lci"
        ).all()
        assert len(procs) == 3

    def test_limit_parameter(self, db_session, sample_spold_files):
        spold_dir = sample_spold_files[0].parent
        result = import_ecoinvent_processes(
            db_session,
            spold_dir=str(spold_dir),
            limit=1,
        )
        assert result["processes_inserted"] == 1

    def test_duplicate_process_updates(self, db_session, sample_spold_files):
        spold_dir = sample_spold_files[0].parent
        r1 = import_ecoinvent_processes(
            db_session,
            spold_dir=str(spold_dir),
        )
        r2 = import_ecoinvent_processes(
            db_session,
            spold_dir=str(spold_dir),
        )
        assert r1["processes_inserted"] == 3
        assert r2["processes_updated"] == 3
        assert r2["processes_inserted"] == 0

    def test_same_activity_different_reference_products_do_not_merge(self, db_session, tmp_path: Path):
        xml_template = """\
        <?xml version="1.0" encoding="UTF-8"?>
        <es:generalData es:ecoSpoldVersion="0.2" xmlns:es="http://www.EcoInvent.org/EcoSpold02">
          <es:activity id="activity-shared-001" location="GLO">
            <es:activityName>Shared activity</es:activityName>
            <es:intermediateExchange id="{rp_id}" variableName="RP" outputGroup="0" amount="1">
              <es:name>{rp_name}</es:name>
              <es:unitName>kg</es:unitName>
            </es:intermediateExchange>
            <es:elementaryExchange elementaryExchangeId="flow-co2-air-001" amount="{amount}" outputGroup="1">
              <es:name>CO2</es:name>
              <es:unitName>kg</es:unitName>
            </es:elementaryExchange>
          </es:activity>
        </es:generalData>
        """
        (tmp_path / "shared-a.spold").write_text(
            textwrap.dedent(xml_template.format(rp_id="rp-a", rp_name="product A", amount="0.1")),
            encoding="utf-8",
        )
        (tmp_path / "shared-b.spold").write_text(
            textwrap.dedent(xml_template.format(rp_id="rp-b", rp_name="product B", amount="0.2")),
            encoding="utf-8",
        )
        result = import_ecoinvent_processes(db_session, spold_dir=str(tmp_path))
        assert result["processes_inserted"] == 2
        assert db_session.query(ReferenceProcess).count() == 2
        assert db_session.query(LciProcessVector).count() == 2

    def test_import_processes_vector_can_unpack(self, db_session, sample_spold_files, sample_units_xml):
        import_ecoinvent_units(db_session, data_dir=str(sample_units_xml.parent))
        import_ecoinvent_processes(db_session, spold_dir=str(sample_spold_files[0].parent))
        vector = db_session.query(LciProcessVector).first()
        assert vector is not None
        flow_key_ids, amounts = unpack_lci_vector(
            flow_key_ids_blob=vector.flow_key_ids_blob,
            amounts_blob=vector.amounts_blob,
            nnz=vector.nnz,
            compression=vector.compression,
        )
        assert len(flow_key_ids) == vector.nnz
        assert len(amounts) == vector.nnz
        assert all(isinstance(item, int) for item in flow_key_ids)
        assert any(abs(amount) > 0 for amount in amounts)

    def test_lci_runtime_expands_vector_with_demand_scale(self, db_session, sample_spold_files, sample_units_xml):
        import_ecoinvent_units(db_session, data_dir=str(sample_units_xml.parent))
        import_ecoinvent_processes(db_session, spold_dir=str(sample_spold_files[0].parent))
        process = db_session.query(ReferenceProcess).filter_by(process_name="Cement production").first()
        assert process is not None

        graph = {
            "nodes": [
                {
                    "id": "node-cement",
                    "node_kind": "lci_dataset",
                    "process_uuid": process.process_uuid,
                    "outputs": [
                        {
                            "id": "out-cement",
                            "flowUuid": "flow-cement",
                            "amount": 2.0,
                            "unit": "kg",
                            "type": "technosphere",
                            "isProduct": True,
                        }
                    ],
                }
            ]
        }
        expanded = expand_graph_lci_inventory(db_session, graph)
        rows = inventory_with_flow_metadata(db_session, expanded.inventory)
        by_flow = {row["flow_uuid"]: row for row in rows}
        assert expanded.missing_vectors == []
        assert expanded.provenance[0]["scale"] == 2.0
        assert abs(by_flow["flow-co2-air-001"]["amount"] - 1.6) < 1e-12
        assert abs(by_flow["flow-ch4-air-002"]["amount"] - 0.002) < 1e-12

    def test_load_process_vectors_batches_by_uuid(self, db_session, sample_spold_files, sample_units_xml):
        import_ecoinvent_units(db_session, data_dir=str(sample_units_xml.parent))
        import_ecoinvent_processes(db_session, spold_dir=str(sample_spold_files[0].parent))
        process_uuids = [row.process_uuid for row in db_session.query(ReferenceProcess).all()]
        vectors = load_process_vectors(db_session, process_uuids)
        assert set(vectors) == set(process_uuids)
        assert all(vectors[process_uuid] for process_uuid in process_uuids)

    def test_lci_vector_expands_into_solver_graph_ports(self, db_session, sample_spold_files, sample_units_xml, sample_elementary_xml):
        import_ecoinvent_units(db_session, data_dir=str(sample_units_xml.parent))
        import_ecoinvent_elementary_flows(db_session, data_dir=str(sample_units_xml.parent), source="ecoinvent_3.11")
        import_ecoinvent_processes(db_session, spold_dir=str(sample_spold_files[0].parent))
        process = db_session.query(ReferenceProcess).filter_by(process_name="Cement production").first()
        assert process is not None

        graph = HybridGraph.model_validate(
            {
                "functionalUnit": "2 kg cement",
                "nodes": [
                    {
                        "id": "node-cement",
                        "node_kind": "lci_dataset",
                        "mode": "normalized",
                        "process_uuid": process.process_uuid,
                        "name": "Cement production",
                        "location": "DE",
                        "reference_product": "cement",
                        "inputs": [],
                        "outputs": [
                            {
                                "id": "out-cement",
                                "flowUuid": "flow-cement",
                                "name": "cement",
                                "unit": "kg",
                                "unitGroup": "mass",
                                "amount": 2.0,
                                "type": "technosphere",
                                "direction": "output",
                                "isProduct": True,
                            }
                        ],
                        "emissions": [],
                    }
                ],
                "exchanges": [],
                "metadata": {},
            }
        )
        expanded = expand_lci_vectors_into_graph(db_session, graph)
        node = expanded.graph.nodes[0]
        biosphere_ports = [port for port in node.outputs if port.type == "biosphere"]
        assert expanded.expanded_process_count == 1
        assert expanded.expanded_port_count == 2
        assert {port.flowUuid for port in biosphere_ports} == {"flow-co2-air-001", "flow-ch4-air-002"}
        co2 = next(port for port in biosphere_ports if port.flowUuid == "flow-co2-air-001")
        assert abs(co2.amount - 1.6) < 1e-12

    def test_manual_lci_node_does_not_require_vector(self, db_session):
        graph = HybridGraph.model_validate(
            {
                "functionalUnit": "1 kg product",
                "nodes": [
                    {
                        "id": "node-manual-lci",
                        "node_kind": "lci_dataset",
                        "mode": "normalized",
                        "process_uuid": "lci_manual_001",
                        "name": "Manual LCI",
                        "location": "GLO",
                        "reference_product": "product",
                        "inputs": [
                            {
                                "id": "in-co2",
                                "flowUuid": "flow-co2-air-001",
                                "name": "Carbon dioxide",
                                "unit": "kg",
                                "unitGroup": "Units of mass",
                                "amount": 1.0,
                                "type": "biosphere",
                                "direction": "input",
                                "showOnNode": True,
                            }
                        ],
                        "outputs": [
                            {
                                "id": "out-product",
                                "flowUuid": "flow-product",
                                "name": "product",
                                "unit": "kg",
                                "unitGroup": "Units of mass",
                                "amount": 1.0,
                                "type": "technosphere",
                                "direction": "output",
                                "isProduct": True,
                            }
                        ],
                        "emissions": [],
                    }
                ],
                "exchanges": [],
                "metadata": {},
            }
        )

        expanded = expand_lci_vectors_into_graph(db_session, graph)

        assert expanded.expanded_process_count == 0
        assert expanded.expanded_port_count == 0
        assert expanded.missing_vectors == []
        assert expanded.graph.nodes[0].inputs[0].flowUuid == "flow-co2-air-001"


class TestLciVectorCodec:
    def test_pack_unpack_roundtrip(self):
        packed = pack_lci_vector([1, 7, 9], [0.5, -2.0, 3.25])
        flow_key_ids, amounts = unpack_lci_vector(
            flow_key_ids_blob=packed.flow_key_ids_blob,
            amounts_blob=packed.amounts_blob,
            nnz=packed.nnz,
            compression=packed.compression,
        )
        assert flow_key_ids == [1, 7, 9]
        assert amounts == [0.5, -2.0, 3.25]

    def test_fast_compression_roundtrip_keeps_checksum(self):
        flow_key_ids = [1, 7, 9]
        amounts = [0.5, -2.0, 3.25]
        fast = pack_lci_vector(flow_key_ids, amounts, compression_level=1)
        default = pack_lci_vector(flow_key_ids, amounts, compression_level=6)

        assert fast.compression == "zlib"
        assert fast.checksum == default.checksum
        assert unpack_lci_vector(
            flow_key_ids_blob=fast.flow_key_ids_blob,
            amounts_blob=fast.amounts_blob,
            nnz=fast.nnz,
            compression=fast.compression,
        ) == (flow_key_ids, amounts)

    def test_no_compression_roundtrip_keeps_checksum(self):
        flow_key_ids = [1, 7, 9]
        amounts = [0.5, -2.0, 3.25]
        raw = pack_lci_vector(flow_key_ids, amounts, compression_level=0)
        zlib = pack_lci_vector(flow_key_ids, amounts, compression_level=1)

        assert raw.compression == "none"
        assert raw.checksum == zlib.checksum
        assert unpack_lci_vector(
            flow_key_ids_blob=raw.flow_key_ids_blob,
            amounts_blob=raw.amounts_blob,
            nnz=raw.nnz,
            compression=raw.compression,
        ) == (flow_key_ids, amounts)
        assert unpack_lci_flow_key_ids(
            flow_key_ids_blob=raw.flow_key_ids_blob,
            nnz=raw.nnz,
            compression=raw.compression,
        ) == flow_key_ids

    def test_pack_requires_sorted_keys(self):
        with pytest.raises(ValueError):
            pack_lci_vector([2, 1], [1.0, 2.0])


class TestEcoinventFilenameMetadata:
    def test_parse_spold_filename_dataset_ids(self):
        parsed = parse_spold_filename_dataset_ids(
            "00082bd6-67b0-509f-a229-7428fb2418ca_ad5a20dd-4c4d-499d-8dc1-254ebab8f3bf.spold"
        )

        assert parsed == (
            "00082bd6-67b0-509f-a229-7428fb2418ca",
            "ad5a20dd-4c4d-499d-8dc1-254ebab8f3bf",
            "00082bd6-67b0-509f-a229-7428fb2418ca:ad5a20dd-4c4d-499d-8dc1-254ebab8f3bf",
        )
        assert parse_spold_filename_dataset_ids("not-a-dataset.spold") is None

    def test_filename_lookup_supports_semicolon_csv(self, tmp_path):
        csv_path = tmp_path / "FilenameToActivityLookup.csv"
        csv_path.write_text(
            "Filename;ActivityName;Location;ReferenceProduct\n"
            "one.spold;Process;GLO;Product\n",
            encoding="utf-8",
        )

        rows = parse_filename_to_activity(csv_path)

        assert rows == [{
            "filename": "one.spold",
            "activity_name": "Process",
            "location": "GLO",
            "reference_product": "Product",
        }]


# ======================================================================
# Tests: Exchange matrix write
# ======================================================================


class TestExchangeMatrixWrite:
    def test_write_exchanges_to_matrix(self, db_session, sample_spold_files, sample_elementary_xml, sample_units_xml):
        import_ecoinvent_elementary_flows(
            db_session,
            data_dir=str(sample_units_xml.parent),
            source="ecoinvent_3.11",
        )
        spold_dir = sample_spold_files[0].parent
        proc_result = import_ecoinvent_processes(
            db_session,
            spold_dir=str(spold_dir),
            package_version="ecoinvent_3.11",
            write_matrix_debug=True,
        )

        # Collect exchanges from result (in real usage they'd be passed directly)
        exchanges = [
            LciExchangeMatrix(
                process_uuid="proc-aaa-001",
                flow_uuid="flow-co2-air-001",
                amount=0.8,
                unit="kg",
                direction="output",
                source="ecoinvent_3.11",
                source_package_version="ecoinvent_3.11",
            ),
            LciExchangeMatrix(
                process_uuid="proc-aaa-001",
                flow_uuid="flow-ch4-air-002",
                amount=0.001,
                unit="kg",
                direction="output",
                source="ecoinvent_3.11",
                source_package_version="ecoinvent_3.11",
            ),
            LciExchangeMatrix(
                process_uuid="proc-bbb-002",
                flow_uuid="flow-co2-air-001",
                amount=1.5,
                unit="kg",
                direction="output",
                source="ecoinvent_3.11",
                source_package_version="ecoinvent_3.11",
            ),
            LciExchangeMatrix(
                process_uuid="proc-bbb-002",
                flow_uuid="flow-phosph-lake-003",
                amount=0.0001,
                unit="kg",
                direction="input",
                source="ecoinvent_3.11",
                source_package_version="ecoinvent_3.11",
            ),
            LciExchangeMatrix(
                process_uuid="proc-ccc-003",
                flow_uuid="flow-co2-air-001",
                amount=0.5,
                unit="kg",
                direction="output",
                source="ecoinvent_3.11",
                source_package_version="ecoinvent_3.11",
            ),
        ]

        matrix_result = write_ecoinvent_exchanges_to_matrix(db_session, exchanges)
        assert matrix_result["rows_inserted"] == 5
        assert matrix_result["rows_aggregated"] == 0

    def test_matrix_aggregation(self, db_session):
        exchanges = [
            LciExchangeMatrix(
                process_uuid="proc-agg-001",
                flow_uuid="flow-co2-air-001",
                amount=0.5,
                unit="kg",
                direction="output",
                source="ecoinvent_3.11",
                source_package_version="ecoinvent_3.11",
            ),
            LciExchangeMatrix(
                process_uuid="proc-agg-001",
                flow_uuid="flow-co2-air-001",
                amount=0.3,
                unit="kg",
                direction="output",
                source="ecoinvent_3.11",
                source_package_version="ecoinvent_3.11",
            ),
        ]
        result = write_ecoinvent_exchanges_to_matrix(db_session, exchanges)
        # Should aggregate into 1 row
        assert result["rows_inserted"] == 1
        assert result["rows_aggregated"] == 1
        row = db_session.query(LciExchangeMatrix).filter_by(
            process_uuid="proc-agg-001",
            flow_uuid="flow-co2-air-001",
            direction="output",
        ).first()
        assert row is not None
        assert abs(row.amount - 0.8) < 1e-10

    def test_matrix_unique_constraint(self, db_session):
        exchanges = [
            LciExchangeMatrix(
                process_uuid="proc-unique-001",
                flow_uuid="flow-co2-air-001",
                amount=1.0,
                unit="kg",
                direction="output",
                source="ecoinvent_3.11",
                source_package_version="ecoinvent_3.11",
            ),
        ]
        r1 = write_ecoinvent_exchanges_to_matrix(db_session, exchanges)
        # Insert same key again via direct add
        existing = db_session.query(LciExchangeMatrix).first()
        existing.amount = 2.0
        r2 = write_ecoinvent_exchanges_to_matrix(db_session, exchanges)
        # Should update existing
        assert r2["rows_updated"] == 1

    def test_matrix_has_source(self, db_session, sample_spold_files, sample_elementary_xml, sample_units_xml):
        import_ecoinvent_elementary_flows(
            db_session,
            data_dir=str(sample_units_xml.parent),
            source="ecoinvent_3.11",
        )
        exchanges = [
            LciExchangeMatrix(
                process_uuid="proc-src-001",
                flow_uuid="flow-co2-air-001",
                amount=1.0,
                unit="kg",
                direction="output",
                source="ecoinvent_3.11",
                source_package_version="ecoinvent_3.11",
            ),
        ]
        write_ecoinvent_exchanges_to_matrix(db_session, exchanges)
        row = db_session.query(LciExchangeMatrix).first()
        assert row is not None
        assert row.source == "ecoinvent_3.11"
        assert row.source_package_version == "ecoinvent_3.11"

    def test_process_vector_canonicalizes_known_units(self, db_session, sample_units_xml, sample_conversions_xml):
        import_ecoinvent_units(db_session, data_dir=str(sample_units_xml.parent))
        exchanges = [
            LciExchangeMatrix(
                process_uuid="proc-vector-001",
                flow_uuid="flow-co2-air-001",
                amount=1000.0,
                unit="g",
                direction="output",
                source="ecoinvent_3.11",
                source_package_version="ecoinvent_3.11",
            ),
        ]
        result = write_ecoinvent_process_vector(
            db_session,
            process_uuid="proc-vector-001",
            exchanges=exchanges,
            package_version="ecoinvent_3.11",
        )
        assert result["vectors_written"] == 1
        assert result["canonicalized"] is True

        vector = db_session.get(LciProcessVector, "proc-vector-001")
        assert vector is not None
        flow_key_ids, amounts = unpack_lci_vector(
            flow_key_ids_blob=vector.flow_key_ids_blob,
            amounts_blob=vector.amounts_blob,
            nnz=vector.nnz,
            compression=vector.compression,
        )
        assert amounts == [1.0]
        key = db_session.get(LciBiosphereFlowKey, flow_key_ids[0])
        assert key is not None
        assert key.canonical_unit == "kg"

    def test_process_vector_preserves_unknown_units_with_warning(self, db_session):
        exchanges = [
            LciExchangeMatrix(
                process_uuid="proc-vector-unknown",
                flow_uuid="flow-x",
                amount=2.0,
                unit="mystery_unit",
                direction="output",
                source="ecoinvent_3.11",
                source_package_version="ecoinvent_3.11",
            ),
        ]
        result = write_ecoinvent_process_vector(
            db_session,
            process_uuid="proc-vector-unknown",
            exchanges=exchanges,
            package_version="ecoinvent_3.11",
        )
        assert result["canonicalized"] is False
        assert result["warnings"]
        vector = db_session.get(LciProcessVector, "proc-vector-unknown")
        assert vector is not None
        assert vector.canonicalized is False

    def test_top_process_vector_exchanges_filters_pages_and_searches(self, db_session):
        db_session.add_all([
            FlowRecord(
                flow_uuid="flow-co2-air-001",
                flow_name="Carbon dioxide fossil",
                flow_type="Elementary flow",
                default_unit="kg",
                unit_group="mass",
                compartment="air",
                source="ecoinvent_3.11",
            ),
            FlowRecord(
                flow_uuid="flow-water-002",
                flow_name="Water resource",
                flow_type="Elementary flow",
                default_unit="m3",
                unit_group="volume",
                compartment="water",
                source="ecoinvent_3.11",
            ),
            FlowRecord(
                flow_uuid="flow-ch4-air-003",
                flow_name="Methane fossil",
                flow_type="Elementary flow",
                default_unit="kg",
                unit_group="mass",
                compartment="air",
                source="ecoinvent_3.11",
            ),
        ])
        exchanges = [
            LciExchangeMatrix(
                process_uuid="proc-top",
                flow_uuid="flow-co2-air-001",
                amount=2.0,
                unit="kg",
                direction="output",
                source="ecoinvent_3.11",
                source_package_version="ecoinvent_3.11",
            ),
            LciExchangeMatrix(
                process_uuid="proc-top",
                flow_uuid="flow-water-002",
                amount=5.0,
                unit="m3",
                direction="input",
                source="ecoinvent_3.11",
                source_package_version="ecoinvent_3.11",
            ),
            LciExchangeMatrix(
                process_uuid="proc-top",
                flow_uuid="flow-ch4-air-003",
                amount=1.0,
                unit="kg",
                direction="output",
                source="ecoinvent_3.11",
                source_package_version="ecoinvent_3.11",
            ),
        ]
        write_ecoinvent_process_vector(
            db_session,
            process_uuid="proc-top",
            exchanges=exchanges,
            package_version="ecoinvent_3.11",
        )

        total_output, first_output = top_process_vector_exchanges(
            db_session,
            "proc-top",
            page=1,
            page_size=1,
            direction="output",
        )
        assert total_output == 2
        assert len(first_output) == 1
        assert first_output[0]["flow_uuid"] == "flow-co2-air-001"
        assert first_output[0]["direction"] == "output"

        total_input, input_items = top_process_vector_exchanges(
            db_session,
            "proc-top",
            page=1,
            page_size=10,
            direction="input",
        )
        assert total_input == 1
        assert input_items[0]["flow_uuid"] == "flow-water-002"

        search_total, search_items = top_process_vector_exchanges(
            db_session,
            "proc-top",
            page=1,
            page_size=10,
            direction="output",
            q="methane",
        )
        assert search_total == 1
        assert search_items[0]["flow_uuid"] == "flow-ch4-air-003"


# ======================================================================
# Tests: Schema ensure (idempotency)
# ======================================================================


class TestSchemaEnsure:
    def test_ensure_table_creates_if_missing(self, db_session):
        # Table should already exist from fixture setup
        from sqlalchemy import text as sa_text
        count = db_session.execute(sa_text("SELECT COUNT(*) FROM lci_exchange_matrix")).scalar()
        assert count == 0  # empty but table exists

    def test_ensure_table_is_idempotent(self, db_session, sample_units_xml):
        # Call twice - second call should see table exists
        result1 = ensure_lci_exchange_matrix_table(db_session.get_bind())
        result2 = ensure_lci_exchange_matrix_table(db_session.get_bind())
        assert result1["status"] in ("ok", "already_complete")
        assert result2["status"] == "already_complete"
