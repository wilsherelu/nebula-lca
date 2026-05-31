"""Tests for startup maintenance (repair / backfill) optimization.

Verifies:
1. Maintenance markers prevent repeated execution of startup backfills.
2. When markers are absent, repair/backfill runs and writes markers.
3. AUTO_STARTUP_MAINTENANCE_ON_STARTUP=false skips all maintenance.
4. _repair_builtin_elementary_flow_sources remains available as manual migration.
"""

import pytest
from app import main
from app.models import FlowRecord, UnitGroup, DebugDiagnostic


# ---------------------------------------------------------------------------
# Test 1: Marker prevents re-execution
# ---------------------------------------------------------------------------


def test_startup_maintenance_does_not_run_manual_repair(monkeypatch):
    main.Base.metadata.create_all(bind=main.engine)
    db = main.SessionLocal()
    try:
        monkeypatch.setattr(
            main,
            "_repair_builtin_elementary_flow_sources",
            lambda *, db: pytest.fail("manual repair must not run during startup maintenance"),
        )

        # Insert a FlowRecord that would have matched the old repair path.
        fr = FlowRecord(
            flow_uuid="test-marker-skip-flow",
            flow_name="marker skip test",
            flow_type="Elementary flow",
            default_unit="kg",
            unit_group="Units of mass",
            source="ecoinvent",  # should be repaired to ef3.1
        )
        db.add(fr)
        db.commit()

        main._run_startup_maintenance(db=db)
        db.rollback()

        # The flow should NOT have been touched because repair is no longer a startup step.
        repaired = (
            db.query(FlowRecord)
            .filter(FlowRecord.flow_uuid == "test-marker-skip-flow")
            .first()
        )
        assert repaired.source == "ecoinvent", (
            "Flow should NOT have been repaired when marker was present"
        )
    finally:
        db.close()


def test_maintenance_marker_skips_backfill(monkeypatch):
    main.Base.metadata.create_all(bind=main.engine)
    db = main.SessionLocal()
    try:
        # Pre-insert markers for both backfill steps
        monkeypatch.setattr(
            main,
            "_is_maintenance_done",
            lambda d, key: key in (
                "backfill_tidas_unit_group_sources:v1",
                "backfill_ecoinvent_unit_group_sources:v1",
            ),
        )

        # Create a UnitGroup that looks like ecoinvent (no source_uuid)
        ug = UnitGroup(
            name="test-marker-backfill-group",
            reference_unit="kg",
            source_uuid=None,
            source_version="",
            source_package_version="",
            source_file="",
        )
        db.add(ug)
        db.commit()

        main._run_startup_maintenance(db=db)
        db.rollback()

        ug_after = (
            db.query(UnitGroup)
            .filter(UnitGroup.name == "test-marker-backfill-group")
            .first()
        )
        assert not ug_after.source_uuid, (
            "UnitGroup should NOT have been backfilled when marker was present"
        )
    finally:
        db.close()


def test_maintenance_marker_skips_backfill_simple(monkeypatch):
    """Minimal backfill skip test that avoids heavy CSV-dependent repair step."""
    main.Base.metadata.create_all(bind=main.engine)
    db = main.SessionLocal()
    try:
        # Pre-insert markers for ALL steps
        monkeypatch.setattr(
            main,
            "_is_maintenance_done",
            lambda d, key: True,
        )

        # Create a UnitGroup that looks like ecoinvent (no source_uuid)
        ug = UnitGroup(
            name="test-marker-backfill-simple-group",
            reference_unit="kg",
            source_uuid=None,
            source_version="",
            source_package_version="",
            source_file="",
        )
        db.add(ug)
        db.commit()

        main._run_startup_maintenance(db=db)
        db.rollback()

        ug_after = (
            db.query(UnitGroup)
            .filter(UnitGroup.name == "test-marker-backfill-simple-group")
            .first()
        )
        assert not ug_after.source_uuid, (
            "UnitGroup should NOT have been backfilled when marker was present"
        )
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Test 2: Absent marker → repair runs and marker is written
# ---------------------------------------------------------------------------


def test_maintenance_no_marker_runs_and_writes_backfill_marker(monkeypatch):
    main.Base.metadata.create_all(bind=main.engine)
    db = main.SessionLocal()
    try:
        monkeypatch.setattr(main, "_is_maintenance_done", lambda d, key: False)
        monkeypatch.setattr(main, "backfill_tidas_unit_group_sources", lambda d: {"updated": 0})
        monkeypatch.setattr(main, "backfill_ecoinvent_unit_group_sources", lambda d: {"updated": 0})
        db.commit()

        main._run_startup_maintenance(db=db)
        db.commit()

        # Marker should exist in DebugDiagnostic for a startup backfill step.
        marker = (
            db.query(DebugDiagnostic)
            .filter(
                DebugDiagnostic.diagnostic_type
                == "backfill_tidas_unit_group_sources:v1"
            )
            .first()
        )
        assert marker is not None, "Marker should have been written after backfill"
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Test 3: Setting disabled → all steps skipped
# ---------------------------------------------------------------------------


def test_maintenance_disabled_skips_all(monkeypatch):
    main.Base.metadata.create_all(bind=main.engine)
    db = main.SessionLocal()
    try:
        # Count markers before (from previous tests)
        count_before = (
            db.query(DebugDiagnostic)
            .filter(
                DebugDiagnostic.diagnostic_type
                == "backfill_tidas_unit_group_sources:v1"
            )
            .count()
        )

        # Monkeypatch settings to disable maintenance
        monkeypatch.setattr(main.settings, "auto_startup_maintenance_on_startup", False)

        main._run_startup_maintenance(db=db)
        # Should complete without error; no new markers should be written

        count_after = (
            db.query(DebugDiagnostic)
            .filter(
                DebugDiagnostic.diagnostic_type
                == "backfill_tidas_unit_group_sources:v1"
            )
            .count()
        )
        assert count_after == count_before, (
            f"No new maintenance markers should be written when disabled "
            f"(before={count_before}, after={count_after})"
        )
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Test 4: Batch repair behavior
# ---------------------------------------------------------------------------


class TestBatchRepair:
    """Verify _repair_builtin_elementary_flow_sources uses batch query."""

    @pytest.fixture
    def repair_db(self, monkeypatch, request):
        """Provide a clean DB session with pre-seeded test data.

        Uses the test method name to generate unique UUIDs so tests don't
        collide with each other when reusing the same engine.
        """
        main.Base.metadata.create_all(bind=main.engine)
        db = main.SessionLocal()

        test_suffix = request.function.__name__.replace("test_", "")
        uuids = [f"br-{test_suffix}-1", f"br-{test_suffix}-2", f"br-{test_suffix}-3"]

        def fake_read():
            return uuids

        try:
            monkeypatch.setattr(main, "_read_builtin_flow_uuids", fake_read)

            # Create flows: only historical ecoinvent sources should be repaired.
            for i, src in enumerate(["ecoinvent", "unknown_source", "ecoinvent_3.11"], 1):
                fr = FlowRecord(
                    flow_uuid=uuids[i - 1],
                    flow_name=f"batch repair test {test_suffix} {i}",
                    flow_type="Elementary flow",
                    default_unit="kg",
                    unit_group="Units of mass",
                    source=src,
                )
                db.add(fr)
            db.commit()
            yield db
            db.rollback()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def test_batch_repair_updates_needing_repair(self, repair_db, request):
        """Only historical ecoinvent sources should be updated."""
        test_suffix = request.function.__name__.replace("test_", "")
        expected_uuids = {f"br-{test_suffix}-1", f"br-{test_suffix}-3"}
        need_repair_uuids = [f"br-{test_suffix}-1", f"br-{test_suffix}-2", f"br-{test_suffix}-3"]

        result = main._repair_builtin_elementary_flow_sources(db=repair_db)
        assert result["updated"] == 2

        for uuid in expected_uuids:
            flow = repair_db.query(FlowRecord).filter(
                FlowRecord.flow_uuid == uuid
            ).first()
            assert flow.source == main.BUILTIN_ELEMENTARY_FLOW_SOURCE

        # Non-ecoinvent rows should NOT be changed.
        for uuid in need_repair_uuids:
            if uuid in expected_uuids:
                continue
            flow = repair_db.query(FlowRecord).filter(
                FlowRecord.flow_uuid == uuid
            ).first()
            assert flow.source == "unknown_source"

    def test_batch_repair_no_false_positives(self, monkeypatch):
        """Flows outside the UUID list should never be touched."""
        main.Base.metadata.create_all(bind=main.engine)
        db = main.SessionLocal()
        try:
            test_uuids = ["false-pos-1", "false-pos-2"]

            def fake_read():
                return test_uuids

            monkeypatch.setattr(main, "_read_builtin_flow_uuids", fake_read)

            # Create flows that need repair
            for i, src in enumerate(["ecoinvent", "unknown_source"], 1):
                fr = FlowRecord(
                    flow_uuid=f"false-pos-{i}",
                    flow_name=f"false pos test {i}",
                    flow_type="Elementary flow",
                    default_unit="kg",
                    unit_group="Units of mass",
                    source=src,
                )
                db.add(fr)

            # Add a flow with same source but not in CSV list
            fringe = FlowRecord(
                flow_uuid="false-pos-outside-list",
                flow_name="should not touch",
                flow_type="Elementary flow",
                default_unit="kg",
                unit_group="Units of mass",
                source="some_other_source",
            )
            db.add(fringe)
            db.commit()

            result = main._repair_builtin_elementary_flow_sources(db=db)
            assert result["updated"] == 1  # only false-pos-1 has historical ecoinvent source

            fringe_after = (
                db.query(FlowRecord)
                .filter(FlowRecord.flow_uuid == "false-pos-outside-list")
                .first()
            )
            assert fringe_after.source == "some_other_source"
        finally:
            db.close()
