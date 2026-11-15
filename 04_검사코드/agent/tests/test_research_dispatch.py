"""Dispatch-day limits and crash recovery use offline providers only."""
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import copy
import pytest
from test_ontology import isolated,C
from test_research_capacity import configure
import improvement_store as S
import research_budget as B
import research_calls as R


def test_old_unsent_reservation_is_rechecked_against_actual_dispatch_day(monkeypatch):
    old=C.now()
    B.reserve('old','owner',2,{})
    monkeypatch.setattr(C,'now',lambda:old+timedelta(days=1))
    B.reserve('today','owner',4,{})
    B.mark_sent('today');B.settle('today',4,'today-provider')
    with pytest.raises(B.DispatchBlocked,match='BUDGET_BLOCKED'):
        B.mark_sent('old')
    assert B.lookup('old')['state']=='reserved'
    assert B.status()['actual_usd']==4


def test_resume_moves_reservation_but_keeps_original_creation_and_settlement_day(monkeypatch):
    old=C.now()
    B.reserve('old','owner',2,{})
    original=B.lookup('old')['created_at']
    monkeypatch.setattr(C,'now',lambda:old+timedelta(days=1))
    sent=B.mark_sent('old')
    assert sent['created_at']==original and sent['sent_at']==S.now()
    assert sent['day']==C.today().isoformat()
    assert B.status(old.isoformat())['reserved_usd']==0
    monkeypatch.setattr(C,'now',lambda:old+timedelta(days=2))
    B.settle('old',.5,'provider')
    assert B.status(sent['sent_at'])['actual_usd']==.5
    assert B.status()['actual_usd']==0
    # New reservations also work after upgrading an existing calls table.
    assert B.reserve('new','owner',1,{})['dispatch_allowed']


def test_pre_reserved_second_call_cannot_dispatch_after_an_ambiguous_first():
    for identifier in ('first','second'):B.reserve(identifier,'owner',1,{})
    B.mark_sent('first');B.settle('first',None,'provider')
    with pytest.raises(B.DispatchBlocked,match='COST_UNRESOLVED'):
        B.mark_sent('second')
    assert B.lookup('second')['state']=='reserved'
    B.settle('first',.2,'provider')
    assert B.mark_sent('second')['state']=='sent'


def test_adapter_resumes_its_persisted_cap_without_hash_conflict(monkeypatch):
    configure(monkeypatch)
    command=['claude','-p','fixed input','--max-budget-usd','3.0']
    B.reserve('resume','owner',3,{'command':command})
    assert R.before('resume','owner',command)==command
    assert B.lookup('resume')['state']=='sent'
    with pytest.raises(ValueError,match='RESEARCH_SENT'):
        R.before('resume','owner',command)
    assert S.get('capacity_reservation','call:resume')['state']=='reserved'


def test_concurrent_resume_has_one_dispatch_winner_and_keeps_its_capacity(monkeypatch):
    configure(monkeypatch)
    B.reserve('same','owner',1,{'fixed':'input'})
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(lambda _:R.claim('same','owner',1,{'fixed':'input'}),range(2)))
    assert sum(r['dispatch_allowed'] for r in results)==1
    assert S.get('capacity_reservation','call:same')['state']=='reserved'
    assert B.lookup('same')['state']=='sent'


def test_blocked_dispatch_releases_only_unsent_capacity_and_can_resume(monkeypatch):
    configure(monkeypatch)
    B.reserve('later','owner',1,{})
    B.reserve('ambiguous','owner',1,{})
    B.mark_sent('ambiguous')
    blocked=R.claim('later','owner',1,{})
    assert blocked['state']=='cost_unresolved' and not blocked['dispatch_allowed']
    assert B.lookup('later')['state']=='reserved'
    assert S.get('capacity_reservation','call:later')['state']=='cancelled_before_dispatch'
    B.settle('ambiguous',.1,'provider')
    assert R.claim('later','owner',1,{})['dispatch_allowed']
    assert S.get('capacity_reservation','call:later')['state']=='reserved'


def test_provider_is_never_invoked_when_dispatch_day_has_no_budget(monkeypatch):
    configure(monkeypatch)
    import semantic_experiments as E
    request={'call_id':'old','experiment_id':'owner','maximum_usd':1,'input':{},
             'guidance':'fixture','model':'alias','model_version':'version'}
    old=C.now();B.reserve('old','owner',1,request)
    monkeypatch.setattr(C,'now',lambda:old+timedelta(days=1))
    B.reserve('spent','owner',5,{});B.mark_sent('spent');B.settle('spent',5,'paid')
    def forbidden(*args,**kwargs):raise AssertionError('provider must not run')
    monkeypatch.setattr(E.subprocess,'run',forbidden)
    assert E.cli_provider(request)['status']=='budget_blocked'
    assert B.lookup('old')['state']=='reserved'


def test_capacity_configuration_pending_does_not_mark_provider_sent(monkeypatch):
    import semantic_experiments as E
    from test_improvement import configured
    configured(monkeypatch,storage_bytes=None)
    def forbidden(*args,**kwargs):raise AssertionError('provider must not run')
    monkeypatch.setattr(E.subprocess,'run',forbidden)
    result=E.cli_provider({'call_id':'pending','experiment_id':'owner','maximum_usd':1,'input':{},
                          'guidance':'fixture','model':'alias','model_version':'version'})
    assert result['status']=='configuration_pending'
    assert B.lookup('pending') is None


@pytest.mark.semantic
@pytest.mark.parametrize('blocked',['capacity_blocked','configuration_pending'])
def test_paired_experiment_resumes_admission_without_changing_denominator(monkeypatch,blocked):
    import semantic_experiments as E
    import ai_judgment as J
    from test_semantic_layer import interpretation,evaluation
    _,_,context,data=interpretation(monkeypatch)
    receipts=[J._load('deliveries',identifier) for identifier in data['delivery_ids']]
    experiment=E.register('fixture',{'model':'fixture','model_version':'fixed','criteria':{},'maximum_usd':1,
        'data_cutoff':S.now(),'tool_permissions':[],'repetitions':1},[{'chain':context['chain'],'receipts':receipts}])
    first=E.run(experiment['id'],provider=lambda request:{'status':blocked})
    assert first['state']=='partial' and first['expected_stage_calls']==8
    calls=[]
    def provider(request):
        calls.append(request['call_id']);role=request['role']
        if role=='interpretation':out=copy.deepcopy(data)
        elif role in ('bull','bear'):out={'evaluations':[evaluation(role)],'objections':[]}
        else:out={'confidence_band_id':'clear_risk','confidence_raw':.7,'recommended_action':'매수','decision':'고정 위험 확인',
                  'adopted_claim_ids':['inst','h1'],'salient_risk_path_ids':['p1'],
                  'dispositions':[{'evaluation_id':r+'-eval','state':'adopted','reason':'위험 채택'} for r in ('bull','bear')]}
        return {'status':'completed','data':out}
    final=E.run(experiment['id'],provider=provider)
    assert final['state']=='completed' and final['expected_stage_calls']==8 and len(calls)==8
    assert final['actual_ai_calls']==0 and not final['adoption_eligible']
