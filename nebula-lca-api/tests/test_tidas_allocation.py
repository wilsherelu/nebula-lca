"""Regression tests for TIDAS multi-product allocation export."""

from app.tidas_export import (
    ExportReport,
    _build_tidas_exchange,
    _calculate_allocation_factors,
)


def _product_port(port_id: str, unit_group: str, amount: float, allocation_factor=None) -> dict:
    port = {
        "id": port_id,
        "flowUuid": f"flow-{port_id}",
        "unit": unit_group,
        "unitGroup": unit_group,
        "amount": amount,
        "isProduct": True,
    }
    if allocation_factor is not None:
        port["allocationFactor"] = allocation_factor
    return port


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
