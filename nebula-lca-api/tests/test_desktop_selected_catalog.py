import csv
import hashlib
import json
from pathlib import Path

from app.services import provider_ef31
from app.services.selected_reference_bootstrap import bootstrap_selected_references
from app.database import Base
from app.models import FlowRecord, UnitGroup, UnitDefinition
from sqlalchemy import create_engine
from sqlalchemy.orm import Session


DATA = Path(__file__).resolve().parents[1] / 'data'


def test_desktop_metadata_is_complete_exact_selection_without_factors(monkeypatch):
    selected = DATA / 'provider_profiles/steel_elementary_catalog.v1.csv'
    provenance = json.loads(selected.with_suffix('.provenance.json').read_text(encoding='utf-8'))
    snapshot = DATA / 'provider_profiles' / provenance['selection_snapshot_asset']
    source = DATA / provenance['source_asset']
    digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    assert digest(source) == provenance['source_sha256']
    assert digest(snapshot) == provenance['selection_snapshot_sha256']
    assert digest(selected) == provenance['selected_sha256']
    uuids = {r['source_object_id'] for r in json.loads(snapshot.read_text(encoding='utf-8'))['records']
             if r['type_of_data_set'] == 'Elementary flow'}
    assert set(provenance['flow_uuids']) == uuids
    expected = source.read_bytes().splitlines(keepends=True)[0] + b''.join(line for line in source.read_bytes().splitlines(keepends=True)
                        if next(csv.reader([line.decode('utf-8-sig')]))[0] in uuids)
    assert selected.read_bytes() == expected
    provider_ef31._elementary_catalog.cache_clear()
    original = provider_ef31._elementary_catalog()
    try:
        monkeypatch.setattr(provider_ef31, '_ELEMENTARY_CATALOG', selected)
        provider_ef31._elementary_catalog.cache_clear()
        narrow = provider_ef31._elementary_catalog()
        assert set(narrow) == uuids
        assert narrow == {key: original[key] for key in uuids}
    finally:
        provider_ef31._elementary_catalog.cache_clear()


def test_selected_profile_initializes_original_source_and_replays_without_overwriting_user_rows():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    profile = DATA / 'provider_profiles/steel_elementary_catalog.v1.provenance.json'
    with Session(engine) as db:
        result = bootstrap_selected_references(db, profile, ef31_dir=str(DATA / 'EF3.1'), elementary_source='ef3.1')
        assert not result['errors']
        co2 = db.get(FlowRecord, '08a91e70-3ddc-11dd-923d-0050c2490048')
        assert co2.source == 'ef3.1'
        assert co2.compartment == 'Emissions;Emissions to air;Emissions to air, unspecified'
        assert co2.source_namespace is None
        mass = db.get(UnitGroup, 'Units of mass')
        assert mass.source_uuid == '93a60a57-a4c8-11da-a746-0800200c9a66'
        assert mass.source_version == '03.00.003'
        assert db.query(UnitDefinition).filter_by(unit_group='Units of energy', unit_name='kWh').one().factor_to_reference == 3.6
        counts = (db.query(FlowRecord).count(), db.query(UnitDefinition).count())
        co2.flow_name = 'User-owned label'
        db.commit()
        bootstrap_selected_references(db, profile, ef31_dir=str(DATA / 'EF3.1'), elementary_source='ef3.1')
        assert db.get(FlowRecord, co2.flow_uuid).flow_name == 'User-owned label'
        assert (db.query(FlowRecord).count(), db.query(UnitDefinition).count()) == counts


def test_startup_hook_uses_selected_profile_even_without_legacy_bootstrap(monkeypatch):
    from app import main

    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    monkeypatch.setenv('NEBULA_REFERENCE_BOOTSTRAP_PROFILE', str(DATA / 'provider_profiles/steel_elementary_catalog.v1.provenance.json'))
    monkeypatch.setattr(main.settings, 'auto_bootstrap_reference_data_on_startup', False)
    monkeypatch.setattr(main.settings, 'nebula_lca_ef31_dir', str(DATA / 'EF3.1'))
    with Session(engine) as db:
        main._bootstrap_reference_data_if_needed(db=db)
        assert db.get(FlowRecord, '08a91e70-3ddc-11dd-923d-0050c2490048').source == main.BUILTIN_ELEMENTARY_FLOW_SOURCE
        assert db.get(UnitGroup, 'Units of energy').source_uuid is not None
