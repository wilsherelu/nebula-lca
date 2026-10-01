"""Initialize an explicitly selected reference profile without legacy archives."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from sqlalchemy.orm import Session

from ..config import PROJECT_ROOT
from ..ingest import import_flows_from_file
from ..models import UnitDefinition, UnitGroup


def bootstrap_selected_references(db: Session, profile_path: Path, *, ef31_dir: str, elementary_source: str) -> dict:
    profile = json.loads(profile_path.read_text(encoding='utf-8'))
    if profile.get('schema_version') != 'nebula_lca.selected-elementary-catalog.v1':
        raise ValueError('Unsupported selected reference profile')
    catalog = profile_path.parent / profile['selected_asset']
    reference_path = PROJECT_ROOT / 'data/Tiangong/tidas_reference_seed.json'
    for path, expected in ((catalog, profile['selected_sha256']),
                           (reference_path, profile['reference_seed_sha256'])):
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise ValueError(f'Selected reference asset hash mismatch: {path.name}')
    seed = json.loads(reference_path.read_text(encoding='utf-8'))
    by_name = {row['name']: row for row in seed['unit_groups']}
    for name in profile['unit_group_names']:
        row = by_name[name]
        group = db.get(UnitGroup, name)
        if group is None:
            group = UnitGroup(name=name, reference_unit=row['reference_unit'], source_uuid=row['uuid'],
                              source_version=row['version'], source_package_version=seed['source_package_version'],
                              source_file='Tiangong/tidas_reference_seed.json')
            db.add(group)
            db.flush()
        # Existing user rows are never replaced by the bundled profile.
        for unit in row['units']:
            existing = db.query(UnitDefinition).filter_by(unit_group=name, unit_name=unit['name']).first()
            if existing is None:
                db.add(UnitDefinition(unit_group=name, unit_name=unit['name'],
                                      factor_to_reference=float(unit['meanValue']),
                                      is_reference=unit['name'] == row['reference_unit']))
    db.commit()
    return import_flows_from_file(db, str(catalog), replace_existing=False,
                                  default_flow_type='Elementary flow', ef31_flow_index_path=ef31_dir,
                                  default_source=elementary_source)
