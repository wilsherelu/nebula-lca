from types import SimpleNamespace

import pytest

from app.provider_schemas import ProviderProjectBackgroundPin
from app.schemas import HybridGraph
from app.services.provider_background_leaf import ProviderBackgroundLeafError
from app.services.provider_project_background import build_project_background_receipts, expand_project_background_pins


POWER = "3d76981f-964a-4865-b588-0e067a2a1163"
CO2 = "08a91e70-3ddc-11dd-923d-0050c2490048"
GROUP = "93a60a57-a4c8-11da-a746-0800200c9a66"
PROPERTY = "93a60a56-a3c8-11da-a746-0800200c9a66"


def port(port_id, flow, unit, amount, direction, kind, product=False):
    return {"id": port_id, "flowUuid": flow, "flowSourceNamespace": "tiangong_open_data",
            "flowVersion": "01.01.003" if flow == POWER else "03.00.004",
            "flowPropertyUuid": PROPERTY, "flowPropertyVersion": "01.00.000",
            "unitGroupUuid": GROUP, "unitGroupVersion": "01.00.000", "unitGroup": "Energy",
            "name": port_id, "unit": unit, "amount": amount, "direction": direction,
            "type": kind, "isProduct": product}


def graph(node_id, process_id, inputs, outputs):
    return HybridGraph.model_validate({"functionalUnit": "one product", "nodes": [{"id": node_id, "node_kind": "unit_process",
        "mode": "normalized", "process_uuid": process_id, "name": node_id, "location": "CN",
        "reference_product": "electricity", "reference_product_flow_uuid": POWER,
        "reference_product_direction": "output", "inputs": inputs, "outputs": outputs}],
        "exchanges": []})


def test_user_project_supply_scales_once_and_retains_exact_source(monkeypatch):
    consumer = graph("sinter", "consumer-process", [port("power", POWER, "MJ", 610, "input", "technosphere")],
                     [port("steel", "steel-flow", "kg", 1000, "output", "technosphere", True)])
    source = graph("supply", "supply-process", [], [
        port("electricity", POWER, "kWh", 1, "output", "technosphere", True),
        port("co2", CO2, "kg", 0.5306, "output", "biosphere")])
    snapshot = SimpleNamespace(graph_hash="a" * 64, graph=source)
    monkeypatch.setattr("app.services.provider_project_background.convert_unit_value",
                        lambda *_args, **_kwargs: {"converted_value": 1 / 3.6})
    catalog = SimpleNamespace(resolve=lambda *_args: {"source_namespace": "tiangong_open_data", "compartment": "air"})
    pin = ProviderProjectBackgroundPin(consumer_exchange_id="sinter::power", project_id="supplier-project",
        version=3, expected_graph_hash=snapshot.graph_hash, source_node_id="supply",
        reference_port_id="electricity", provider_amount_per_consumer_unit=1 / 3.6)
    result = expand_project_background_pins(None, graph=consumer, pins=[pin],
        get_snapshot=lambda *_args: snapshot, flow_snapshot=catalog)
    assert len(result.graph.nodes) == 2
    assert len(result.graph.exchanges) == 1
    edge = result.graph.exchanges[0]
    assert edge.consumerAmount == 610
    assert edge.providerAmount == pytest.approx(610 / 3.6)
    assert result.graph.nodes[1].outputs[1].amount == 0.5306
    assert result.elementary_refs[0].exchange_id == f"{result.graph.nodes[1].id}::co2"
    assert result.receipt_drafts[0]["source_graph_hash"] == snapshot.graph_hash
    assert consumer.exchanges == []


def test_one_saved_supply_version_can_serve_two_consumer_inputs_without_double_counting(monkeypatch):
    consumer = graph("sinter", "consumer-process", [
        port("power-a", POWER, "MJ", 600, "input", "technosphere"),
        port("power-b", POWER, "MJ", 25, "input", "technosphere")],
        [port("steel", "steel-flow", "kg", 1000, "output", "technosphere", True)])
    source = graph("supply", "supply-process", [], [
        port("electricity", POWER, "kWh", 1, "output", "technosphere", True),
        port("co2", CO2, "kg", 0.5306, "output", "biosphere")])
    snapshot = SimpleNamespace(graph_hash="a" * 64, graph=source)
    monkeypatch.setattr("app.services.provider_project_background.convert_unit_value",
                        lambda *_args, **_kwargs: {"converted_value": 1 / 3.6})
    catalog = SimpleNamespace(resolve=lambda *_args: {"source_namespace": "tiangong_open_data", "compartment": "air"})
    pins = [ProviderProjectBackgroundPin(consumer_exchange_id=f"sinter::{port_id}", project_id="supplier-project",
        version=3, expected_graph_hash=snapshot.graph_hash, source_node_id="supply",
        reference_port_id="electricity", provider_amount_per_consumer_unit=1 / 3.6)
        for port_id in ("power-a", "power-b")]
    result = expand_project_background_pins(None, graph=consumer, pins=pins,
        get_snapshot=lambda *_args: snapshot, flow_snapshot=catalog)
    processes = [node.process_uuid for node in result.graph.nodes[1:]]
    assert len(set(processes)) == 2
    assert len(result.graph.exchanges) == 2
    assert sum(edge.providerAmount for edge in result.graph.exchanges) == pytest.approx(625 / 3.6)
    receipts = build_project_background_receipts(result, dict(zip(processes, (600 / 3.6, 25 / 3.6))))
    assert sum(row["activity_amount"] for row in receipts) == pytest.approx(625 / 3.6)
    assert {row["source_model_process_uuid"] for row in receipts} == {"supply-process"}


def test_user_project_pin_rejects_changed_source_and_open_supply(monkeypatch):
    consumer = graph("sinter", "consumer", [port("power", POWER, "MJ", 1, "input", "technosphere")], [])
    source = graph("supply", "provider", [port("fuel", POWER, "MJ", 1, "input", "technosphere")],
                   [port("electricity", POWER, "MJ", 1, "output", "technosphere", True)])
    snapshot = SimpleNamespace(graph_hash="a" * 64, graph=source)
    pin = ProviderProjectBackgroundPin(consumer_exchange_id="sinter::power", project_id="supplier",
        version=2, expected_graph_hash="b" * 64, source_node_id="supply", reference_port_id="electricity",
        provider_amount_per_consumer_unit=1)
    with pytest.raises(ProviderBackgroundLeafError, match="changed"):
        expand_project_background_pins(None, graph=consumer, pins=[pin], get_snapshot=lambda *_args: snapshot,
                                       flow_snapshot=None)
    with pytest.raises(ProviderBackgroundLeafError, match="open supply"):
        expand_project_background_pins(None, graph=consumer, pins=[pin.model_copy(update={"expected_graph_hash": snapshot.graph_hash})],
            get_snapshot=lambda *_args: snapshot, flow_snapshot=None)
