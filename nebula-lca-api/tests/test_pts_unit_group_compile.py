from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.api.pts import get_pts_artifact_diagnostics
from app.database import Base
from app.models import FlowRecord, PtsCompileArtifact, PtsExternalArtifact, PtsResource, UnitDefinition, UnitGroup
from app.pts_compile import compile_pts
from app.schemas import HybridGraph
from app.services.pts_resources import build_pts_external_payload


class _FakeQuery:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _FakeDb:
    def __init__(self):
        self.flow_by_uuid = {
            "gas-flow": SimpleNamespace(flow_uuid="gas-flow", default_unit="kg", unit_group="Units of mass"),
            "transport-flow": SimpleNamespace(flow_uuid="transport-flow", default_unit="kg", unit_group="Units of mass"),
            "water-flow": SimpleNamespace(flow_uuid="water-flow", default_unit="m3", unit_group="Units of volume"),
            "co2-flow": SimpleNamespace(flow_uuid="co2-flow", default_unit="kg", unit_group="Units of mass"),
        }
        self.unit_defs = [
            SimpleNamespace(unit_group="Units of mass", unit_name="kg", factor_to_reference=1.0, is_reference=True),
            SimpleNamespace(unit_group="Units of energy", unit_name="MJ", factor_to_reference=1.0, is_reference=True),
            SimpleNamespace(unit_group="Units of energy", unit_name="GJ", factor_to_reference=1000.0, is_reference=False),
            SimpleNamespace(unit_group="Units of volume", unit_name="m3", factor_to_reference=1.0, is_reference=True),
        ]
        self.unit_groups = [
            SimpleNamespace(name="Units of mass", reference_unit="kg"),
            SimpleNamespace(name="Units of energy", reference_unit="MJ"),
            SimpleNamespace(name="Units of volume", reference_unit="m3"),
        ]

    def get(self, model, key):
        if model is FlowRecord:
            return self.flow_by_uuid.get(key)
        return None

    def query(self, model):
        if model is UnitDefinition:
            return _FakeQuery(self.unit_defs)
        if model is UnitGroup:
            return _FakeQuery(self.unit_groups)
        return _FakeQuery([])


def _unit_group_switch(flow_uuid: str, factor: float) -> dict:
    return {
        "sourceFlowUuid": flow_uuid,
        "sourceUnitGroup": "Units of mass",
        "sourceUnit": "kg",
        "sourceReferenceUnit": "kg",
        "sourceAmount": 1,
        "targetUnitGroup": "Units of energy",
        "targetUnit": "MJ",
        "targetReferenceUnit": "MJ",
        "factor": factor,
        "source": "user_declared",
    }


def _pts_graph(*, transport_amount=0.8, transport_unit="GJ", manual=False) -> HybridGraph:
    transport_port = {
        "id": "out-transport",
        "flowUuid": "transport-flow",
        "name": "transport",
        "type": "technosphere",
        "direction": "output",
        "isProduct": True,
        "amount": transport_amount,
        "unit": transport_unit,
        "unitGroup": "Units of energy",
        "unitGroupSwitch": _unit_group_switch("transport-flow", 600),
    }
    if manual:
        transport_port = {
            "id": "out-water",
            "flowUuid": "water-flow",
            "name": "water",
            "type": "technosphere",
            "direction": "output",
            "isProduct": True,
            "amount": 2,
            "unit": "m3",
            "unitGroup": "Units of volume",
            "allocationFactor": 0.8,
            "allocationBasis": {"method": "manual_factor"},
        }
    gas_port = {
        "id": "out-gas",
        "flowUuid": "gas-flow",
        "name": "gas",
        "type": "technosphere",
        "direction": "output",
        "isProduct": True,
        "amount": 1000,
        "unit": "MJ",
        "unitGroup": "Units of energy",
        "unitGroupSwitch": _unit_group_switch("gas-flow", 1000),
    }
    if manual:
        gas_port["allocationFactor"] = 0.2
        gas_port["allocationBasis"] = {"method": "manual_factor"}

    return HybridGraph.model_validate(
        {
            "functionalUnit": "PTS",
            "nodes": [
                {
                    "id": "pts-node",
                    "node_kind": "pts_module",
                    "mode": "normalized",
                    "pts_uuid": "pts-unit-test",
                    "process_uuid": "pts-unit-test",
                    "name": "PTS",
                    "location": "CN",
                    "reference_product": "PTS",
                    "inputs": [],
                    "outputs": [],
                }
            ],
            "exchanges": [],
            "metadata": {
                "canvases": [
                    {
                        "id": "canvas-pts",
                        "kind": "pts_internal",
                        "parentPtsNodeId": "pts-node",
                        "nodes": [
                            {
                                "id": "internal-node",
                                "node_kind": "unit_process",
                                "mode": "normalized",
                                "process_uuid": "internal-process",
                                "name": "internal process",
                                "location": "CN",
                                "reference_product": "gas",
                                "inputs": [],
                                "outputs": [
                                    gas_port,
                                    transport_port,
                                    {
                                        "id": "em-co2",
                                        "flowUuid": "co2-flow",
                                        "name": "CO2",
                                        "type": "biosphere",
                                        "direction": "output",
                                        "amount": 100,
                                        "unit": "kg",
                                        "unitGroup": "Units of mass",
                                    },
                                ],
                                "emissions": [],
                            }
                        ],
                        "edges": [],
                    }
                ]
            },
        }
    )


def _fake_pts_solver(payload: dict) -> dict:
    target = payload["pts_outputs"][0]
    target_node_id = target["sourceNodeId"]
    node = next(item for item in payload["internal_nodes"] if item["id"] == target_node_id)
    products = [item for item in node["outputs"] if item.get("type") != "biosphere" and item.get("isProduct")]
    denominator = sum(float(item["amount"]) for item in products)
    emission = node["emissions"][0]
    return {
        "warnings": [],
        "virtual_processes": [
            {
                "process_uuid": f"solver::{target['flowUuid']}",
                "process_name": target["name"],
                "reference_product": {
                    "flowUuid": target["flowUuid"],
                    "name": target["name"],
                    "unit": target["unit"],
                    "unitGroup": target.get("unitGroup"),
                },
                "elementary_flows": [
                    {
                        "flowUuid": emission["flowUuid"],
                        "name": emission["name"],
                        "unit": emission["unit"],
                        "unitGroup": emission.get("unitGroup"),
                        "direction": "output",
                        "amount": float(emission["amount"]) / denominator,
                    }
                ],
                "technosphere_inputs": [],
            }
        ],
    }


def _co2_by_source_port(result: dict) -> dict[str, float]:
    rows = result["artifact"]["virtual_processes"]
    return {
        row["source_port_id"]: row["elementary_flows"][0]["amount"]
        for row in rows
    }


def test_pts_compile_converts_internal_products_before_quantity_allocation(monkeypatch):
    monkeypatch.setattr("app.pts_compile.run_tiangong_pts_compile", _fake_pts_solver)

    result_gj = compile_pts(_pts_graph(transport_amount=0.8, transport_unit="GJ"), "pts-node", db=_FakeDb())
    result_mj = compile_pts(_pts_graph(transport_amount=800, transport_unit="MJ"), "pts-node", db=_FakeDb())

    assert _co2_by_source_port(result_gj) == pytest.approx({
        "out-gas": 55.55555555555556,
        "out-transport": 33.33333333333333,
    })
    assert _co2_by_source_port(result_mj) == pytest.approx(_co2_by_source_port(result_gj))
    transport_vp = next(row for row in result_gj["artifact"]["virtual_processes"] if row["source_port_id"] == "out-transport")
    assert transport_vp["reference_unit"] == "kg"
    assert transport_vp["reference_unit_group"] == "Units of mass"
    assert transport_vp["allocation_fraction"] == pytest.approx(0.4444444444444444)
    assert transport_vp["normalization_reference_amount"] == pytest.approx(1.3333333333333333)


def test_pts_compile_respects_manual_allocation_across_unit_groups(monkeypatch):
    monkeypatch.setattr("app.pts_compile.run_tiangong_pts_compile", _fake_pts_solver)

    result = compile_pts(_pts_graph(manual=True), "pts-node", db=_FakeDb())

    assert _co2_by_source_port(result) == pytest.approx({
        "out-gas": 20.0,
        "out-water": 40.0,
    })
    water_vp = next(row for row in result["artifact"]["virtual_processes"] if row["source_port_id"] == "out-water")
    assert water_vp["reference_unit"] == "m3"
    assert water_vp["reference_unit_group"] == "Units of volume"
    assert water_vp["allocation_fraction"] == pytest.approx(0.8)


def test_pts_publish_payload_preserves_scaled_virtual_process_audit(monkeypatch):
    monkeypatch.setattr("app.pts_compile.run_tiangong_pts_compile", _fake_pts_solver)
    result = compile_pts(_pts_graph(transport_amount=0.8, transport_unit="GJ"), "pts-node", db=_FakeDb())
    compile_row = PtsCompileArtifact(
        project_id="project-1",
        pts_node_id="pts-node",
        pts_uuid="pts-unit-test",
        graph_hash=result["graph_hash"],
        ok=True,
        matrix_size=result["matrix_size"],
        invertible=result["invertible"],
        errors_json=[],
        warnings_json=[],
        artifact_json=result["artifact"],
    )
    definition = {
        "pts_node_id": "pts-node",
        "ports_policy": {
            "outputs": [
                {
                    "flowUuid": "transport-flow",
                    "name": "transport",
                    "unit": "kg",
                    "unitGroup": "Units of mass",
                    "isProduct": True,
                    "internalExposed": True,
                    "showOnNode": True,
                    "sourceProcessUuid": "internal-process",
                    "sourceNodeId": "internal-node",
                }
            ]
        },
    }

    payload = build_pts_external_payload(
        project_id="project-1",
        pts_uuid="pts-unit-test",
        definition=definition,
        compile_row=compile_row,
    )

    assert len(payload["virtual_processes"]) == 1
    vp = payload["virtual_processes"][0]
    assert vp["source_port_id"] == "out-transport"
    assert vp["allocation_fraction"] == pytest.approx(0.4444444444444444)
    assert vp["normalization_reference_amount"] == pytest.approx(1.3333333333333333)
    assert vp["elementary_flows"][0]["amount"] == pytest.approx(33.33333333333333)
    assert payload["frontend_ports"]["outputs"][0]["unitGroup"] == "Units of mass"


def test_pts_artifact_diagnostics_exposes_payload_and_stale_hash(tmp_path):
    db_path = tmp_path / "pts-artifact-diagnostics.db"
    engine = create_engine(f"sqlite:///{db_path}", future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    db = Session()
    artifact = {
        "virtual_processes": [
            {
                "process_uuid": "vp-1",
                "process_name": "virtual gas",
                "product_key": "proc::gas",
                "source_process_uuid": "proc-1",
                "source_node_id": "node-1",
                "source_port_id": "out-gas",
                "allocation_fraction": 0.5,
                "normalization_reference_amount": 2.0,
                "reference_unit": "kg",
                "reference_unit_group": "Units of mass",
                "reference_product": {"flowUuid": "gas-flow", "name": "gas", "amount": 1.0, "unit": "kg"},
                "technosphere_inputs": [{"flowUuid": "input-flow", "amount": 3.0, "unit": "kg"}],
                "elementary_flows": [{"flowUuid": "co2-flow", "amount": 4.0, "unit": "kg"}],
            }
        ]
    }
    compile_row = PtsCompileArtifact(
        project_id="project-1",
        pts_node_id="pts-node",
        pts_uuid="pts-diag",
        graph_hash="compiled-hash",
        compile_version=7,
        ok=True,
        matrix_size=1,
        invertible=True,
        errors_json=[],
        warnings_json=[],
        artifact_json=artifact,
    )
    db.add(
        PtsResource(
            project_id="project-1",
            pts_uuid="pts-diag",
            pts_node_id="pts-node",
            latest_graph_hash="current-hash",
            active_published_version=3,
        )
    )
    db.add(compile_row)
    db.commit()
    db.refresh(compile_row)
    db.add(
        PtsExternalArtifact(
            project_id="project-1",
            pts_uuid="pts-diag",
            pts_node_id="pts-node",
            graph_hash="compiled-hash",
            published_version=3,
            source_compile_id=compile_row.id,
            source_compile_version=7,
            artifact_json=artifact,
        )
    )
    db.commit()

    compile_diag = get_pts_artifact_diagnostics(
        pts_uuid="pts-diag",
        project_id="project-1",
        kind="compile",
        artifact_id=None,
        version=7,
        db=db,
    )
    published_diag = get_pts_artifact_diagnostics(
        pts_uuid="pts-diag",
        project_id="project-1",
        kind="published",
        artifact_id=None,
        version=3,
        db=db,
    )

    assert compile_diag.stale_against_resource is True
    assert compile_diag.summary["virtual_process_count"] == 1
    assert compile_diag.virtual_processes[0]["source_port_id"] == "out-gas"
    assert compile_diag.virtual_processes[0]["allocation_fraction"] == pytest.approx(0.5)
    assert published_diag.source_compile_version == 7
    assert published_diag.artifact["virtual_processes"][0]["product_key"] == "proc::gas"
    db.close()
