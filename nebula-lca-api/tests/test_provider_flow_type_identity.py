"""Exact type propagation through both public solve snapshot sources."""
from types import SimpleNamespace
import pytest

from test_provider_v1_api import client, isolated_database, _port, _inline_snapshot
import app.database as database
import app.services.provider_v1 as provider
from app.models import Model, ModelVersion
from app.schemas import HybridGraph
from app.services.graph_storage import compute_graph_hash_from_graph


def graph():
    waste = [_port(f'waste-{i}', f'waste-flow-{i}', 'Waste', i+10, 'output', 'technosphere') for i in range(3)]
    for port in waste:
        port['flowSourceNamespace'] = 'tiangong_open_data'
    return HybridGraph.model_validate({'functionalUnit':'1000 kg product','nodes':[{
        'id':'single','node_kind':'unit_process','mode':'normalized','process_uuid':'single-process',
        'name':'Single product operation','location':'CN','reference_product':'product',
        'inputs':[_port('raw','raw','Raw',20,'input','technosphere'),
                  _port('water','water','Water',30,'input','biosphere'),
                  _port('energy','energy','Energy',40,'input','technosphere')],
        'outputs':[_port('product','product','Product',1000,'output','technosphere',product=True),
                   _port('emission','emission','Emission',1,'output','biosphere'),*waste]}], 'exchanges':[]})


def configure(monkeypatch):
    def resolve(uuid, version, **kwargs):
        if uuid.startswith('waste-flow-') and version == '1.0':
            return {'flow_type':'Waste flow','content_hash':'a'*64,'snapshot_hash':'b'*64,
                    'flow_property_uuid':'fp-mass','flow_property_version':'1.0',
                    'unit_group_uuid':'ug-mass','unit_group_version':'1.0','default_unit':'kg'}
        return None
    monkeypatch.setattr(provider,'_configured_tidas_snapshot',lambda:SimpleNamespace(resolve=resolve,snapshot_hash='b'*64,reference_dependencies=None))


def request_for(g, source):
    if source == 'inline':
        snapshot={'inline_snapshot':_inline_snapshot(g)}
    else:
        with database.SessionLocal() as db:
            db.add(Model(id='exact-single',name='Single',functional_unit='1000 kg product',source_policy='open_mixed',allowed_lcia_scope='ef31_only'))
            db.add(ModelVersion(id='exact-single-v1',model_id='exact-single',version=1,
                graph_hash=compute_graph_hash_from_graph(g),hybrid_graph_json=g.model_dump(mode='json',by_alias=True)))
            db.commit()
        snapshot={'snapshot_ref':{'project_id':'exact-single','version':1,'graph_hash':compute_graph_hash_from_graph(g)}}
    return {**snapshot,'demand':[{'process_uuid':'single-process','amount':1000,'unit':'kg'}]}


@pytest.mark.parametrize('source',['inline','saved'])
def test_exact_waste_types_and_single_product_allocation_reach_solver(client,monkeypatch,source):
    configure(monkeypatch)
    captured=[]; original=provider.to_tiangong_like
    def capture(g,**kwargs):
        result=original(g,**kwargs); captured.append(result); return result
    monkeypatch.setattr(provider,'to_tiangong_like',capture)
    response=client.post('/api/provider/v1/solve',json=request_for(graph(),source))
    assert response.status_code==200,response.text
    answer=response.json()
    assert len(answer['scaled_exchanges'])==8
    assert all(row['allocation_total']==1000 for row in answer['scaled_exchanges'])
    assert len([r for r in captured[0]['exchanges'] if r['allocation_fraction'] is not None])==1
    assert len([f for f in captured[0]['flows'] if f['flow_type']=='Waste flow'])==3
    assert [r['flow_type'] for r in answer['technosphere_flow_receipts'] if r['flow_uuid'].startswith('waste-flow-')]==['Waste flow']*3


@pytest.mark.parametrize('source',['inline','saved'])
def test_wrong_exact_waste_version_is_rejected(client,monkeypatch,source):
    configure(monkeypatch); g=graph(); g.nodes[0].outputs[-1].flow_version='missing'
    response=client.post('/api/provider/v1/solve',json=request_for(g,source))
    assert response.status_code==422
    assert response.json()['detail']['code']=='TIDAS_FLOW_EXACT_VERSION_NOT_IN_SNAPSHOT'


def test_uuid_keyed_adapter_refuses_conflicting_exact_types():
    g=graph(); waste=g.nodes[0].outputs[-1]
    g.nodes[0].inputs[0].flowUuid=waste.flowUuid
    receipt=SimpleNamespace(exchange_id=f'single::{waste.id}',flow_type='Waste flow')
    with pytest.raises(provider.ProviderContractError) as error:
        provider._solver_flow_types(g,[receipt])
    assert error.value.code=='SOLVER_FLOW_TYPE_IDENTITY_CONFLICT'
