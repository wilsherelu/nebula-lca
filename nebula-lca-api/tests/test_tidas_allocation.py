"""Regression tests for TIDAS multi-product allocation export."""
from types import SimpleNamespace

from app.tidas_export import (
    ExportReport,
    _build_tidas_exchange,
    _calculate_allocation_factors,
)


def _product_port(
    port_id: str,
    unit_group: str,
    amount: float,
    allocation_factor=None,
    *,
    unit: str | None = None,
    allocation_basis: dict | None = None,
) -> dict:
    port = {
        "id": port_id,
        "flowUuid": f"flow-{port_id}",
        "unit": unit or unit_group,
        "unitGroup": unit_group,
        "amount": amount,
        "isProduct": True,
    }
    if allocation_factor is not None:
        port["allocationFactor"] = allocation_factor
    if allocation_basis is not None:
        port["allocationBasis"] = allocation_basis
    return port


class _FakeQuery:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _FakeDb:
    def __init__(self, rows):
        self._rows = rows

    def query(self, _model):
        return _FakeQuery(self._rows)


def _unit_def(group: str, name: str, factor: float):
    return SimpleNamespace(unit_group=group, unit_name=name, factor_to_reference=factor)


def _output_exchange(internal_id: str) -> dict:
    return {
        "internal_id": internal_id,
        "flow_uuid": f"flow-{internal_id}",
        "direction": "output",
        "amount": 1,
        "unit": "kg",
    }


def test_manual_required_allocation_does_not_write_reference_100_percent():
    """Different unit groups require manual allocation and must not fake 100%."""
    report = ExportReport()
    factors = _calculate_allocation_factors(
        [
            _product_port("p1", "kg", 1),
            _product_port("p2", "m3", 1),
        ],
        None,
        "process-1",
        report,
        db=None,
    )

    assert factors is None
    assert "process-1" in report.manual_allocation_required_processes

    exchange = _build_tidas_exchange(_output_exchange("p1"), factors, ref_internal_id="p1")

    assert exchange["allocations"]["allocation"] == {}
    assert exchange["json_tg_allocation"]["manualAllocationRequired"] is True
    assert exchange["json_tg_allocation"]["isReferenceFlow"] is True


def test_incomplete_user_allocation_requires_manual_allocation():
    """User allocation factors are trusted only when every product has one."""
    report = ExportReport()
    factors = _calculate_allocation_factors(
        [
            _product_port("p1", "kg", 1, allocation_factor=1.0),
            _product_port("p2", "kg", 1),
        ],
        None,
        "process-2",
        report,
        db=None,
    )

    assert factors is None
    assert "process-2" in report.manual_allocation_required_processes
    assert any(w.category == "allocation" for w in report.allocation_warnings)


def test_complete_user_allocation_is_written():
    report = ExportReport()
    factors = _calculate_allocation_factors(
        [
            _product_port("p1", "kg", 1, allocation_factor=0.25),
            _product_port("p2", "kg", 1, allocation_factor=0.75),
        ],
        None,
        "process-3",
        report,
        db=None,
    )

    assert factors == {"p1": 0.25, "p2": 0.75}
    exchange = _build_tidas_exchange(_output_exchange("p2"), factors, ref_internal_id="p1")
    assert exchange["allocations"]["allocation"]["@allocatedFraction"] == "75%"


def test_same_unit_group_converts_units_before_allocation():
    report = ExportReport()
    factors = _calculate_allocation_factors(
        [
            _product_port("p1", "Units of mass", 1, unit="kg"),
            _product_port("p2", "Units of mass", 500, unit="g"),
        ],
        None,
        "process-4",
        report,
        db=_FakeDb([
            _unit_def("Units of mass", "kg", 1.0),
            _unit_def("Units of mass", "g", 0.001),
        ]),
    )

    assert factors == {"p1": 2 / 3, "p2": 1 / 3}
    assert report.manual_allocation_required_processes == []


def test_density_basis_allocates_volume_and_mass_products():
    report = ExportReport()
    factors = _calculate_allocation_factors(
        [
            _product_port(
                "p1",
                "Units of volume",
                2,
                unit="m3",
                allocation_basis={"method": "density", "value": 800, "targetUnitGroup": "Units of mass"},
            ),
            _product_port("p2", "Units of mass", 400, unit="kg"),
        ],
        None,
        "process-5",
        report,
        db=_FakeDb([
            _unit_def("Units of volume", "m3", 1.0),
            _unit_def("Units of mass", "kg", 1.0),
        ]),
    )

    assert factors == {"p1": 0.8, "p2": 0.2}
    assert report.manual_allocation_required_processes == []


def test_heating_value_basis_allocates_mass_and_energy_products():
    report = ExportReport()
    factors = _calculate_allocation_factors(
        [
            _product_port(
                "p1",
                "Units of mass",
                10,
                unit="kg",
                allocation_basis={"method": "heating_value", "value": 20, "targetUnitGroup": "Units of energy"},
            ),
            _product_port("p2", "Units of energy", 100, unit="MJ"),
        ],
        None,
        "process-6",
        report,
        db=_FakeDb([
            _unit_def("Units of mass", "kg", 1.0),
            _unit_def("Units of energy", "MJ", 1.0),
        ]),
    )

    assert factors == {"p1": 2 / 3, "p2": 1 / 3}
    assert report.manual_allocation_required_processes == []


def test_incomplete_cross_unit_basis_requires_manual_allocation():
    report = ExportReport()
    factors = _calculate_allocation_factors(
        [
            _product_port("p1", "Units of volume", 2, unit="m3", allocation_basis={"method": "density"}),
            _product_port("p2", "Units of mass", 400, unit="kg"),
        ],
        None,
        "process-7",
        report,
        db=_FakeDb([
            _unit_def("Units of volume", "m3", 1.0),
            _unit_def("Units of mass", "kg", 1.0),
        ]),
    )

    assert factors is None
    assert "process-7" in report.manual_allocation_required_processes
