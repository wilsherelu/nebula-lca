from app.services.provider_tidas_snapshot import _flow_display_names
from app.services.provider_tidas_snapshot import _load_snapshot
from pathlib import Path
import json


def test_exact_payload_language_tags_are_preserved_without_translation():
    payload = {'flowDataSet': {'flowInformation': {'dataSetInformation': {'name': {'baseName': [
        {'@xml:lang': 'zh', '#text': '交流电'}, {'@xml:lang': 'en', '#text': 'Alternating current'}]}}}}}
    assert _flow_display_names(payload) == {'zh': '交流电', 'en': 'Alternating current'}
    assert _flow_display_names({'flowDataSet': {'flowInformation': {'dataSetInformation': {
        'name': {'baseName': 'Untagged source name'}}}}}) == {}


def test_resolved_names_come_from_same_hashed_exact_snapshot():
    path = Path(__file__).parent / 'fixtures/provider_cases/tidas_flow_snapshot/minimal_crude_oil_snapshot.json'
    document = json.loads(path.read_text(encoding='utf-8'))
    source = document['records'][0]
    snapshot = _load_snapshot(path, None)
    answer = snapshot.resolve(source['source_object_id'], source['source_version'])
    assert answer['display_names'] == _flow_display_names(source['payload'])
    assert answer['content_hash'] == source['content_hash']
    assert answer['snapshot_hash'] == document['snapshot_hash']
    assert snapshot.resolve(source['source_object_id'], '99.99.999') is None
