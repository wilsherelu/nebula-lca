from app import main
from app.models import FlowRecord, ReferenceProcess, UnitDefinition, UnitGroup


def test_startup_bootstrap_skips_existing_builtin_reference_data(monkeypatch):
    main.Base.metadata.create_all(bind=main.engine)
    db = main.SessionLocal()
    try:
        if db.get(UnitGroup, "test-startup-unit-group") is None:
            db.add(UnitGroup(name="test-startup-unit-group", reference_unit="kg"))
        if db.query(UnitDefinition).count() == 0:
            db.add(
                UnitDefinition(
                    unit_group="test-startup-unit-group",
                    unit_name="kg",
                    factor_to_reference=1.0,
                    is_reference=True,
                )
            )
        if db.query(FlowRecord).filter(FlowRecord.source == main.BUILTIN_ELEMENTARY_FLOW_SOURCE).count() == 0:
            db.add(
                FlowRecord(
                    flow_uuid="test-startup-elementary-flow",
                    flow_name="startup elementary",
                    flow_type="Elementary flow",
                    default_unit="kg",
                    unit_group="Units of mass",
                    source=main.BUILTIN_ELEMENTARY_FLOW_SOURCE,
                )
            )
        if db.query(FlowRecord).filter(FlowRecord.source == main.BUILTIN_INTERMEDIATE_FLOW_SOURCE).count() == 0:
            db.add(
                FlowRecord(
                    flow_uuid="test-startup-intermediate-flow",
                    flow_name="startup intermediate",
                    flow_type="Product flow",
                    default_unit="kg",
                    unit_group="Units of mass",
                    source=main.BUILTIN_INTERMEDIATE_FLOW_SOURCE,
                )
            )
        if db.query(ReferenceProcess).count() == 0:
            db.add(
                ReferenceProcess(
                    process_uuid="test-startup-reference-process",
                    process_name="startup reference process",
                    process_type="unit_process",
                )
            )
        db.commit()

        def fail_import(*args, **kwargs):
            raise AssertionError("startup bootstrap should skip existing reference data")

        monkeypatch.setattr(main, "import_unit_groups_from_excel", fail_import)
        monkeypatch.setattr(main, "import_flows_from_file", fail_import)
        monkeypatch.setattr(main, "import_processes_from_json", fail_import)

        main._bootstrap_reference_data_if_needed(db=db)
    finally:
        db.close()
