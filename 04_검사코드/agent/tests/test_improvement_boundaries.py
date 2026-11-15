"""Admission, corrected-input consumption and real isolated defect checks."""
import copy
import os
from datetime import timedelta
from types import SimpleNamespace
import pytest
from test_ontology import isolated, C
import improvement_store as S


def test_real_provider_cost_is_settled_but_wrong_model_cannot_pass(monkeypatch):
    from test_research_capacity import configure
    configure(monkeypatch)
    import semantic_experiments as E
    import research_budget as B
    response={'total_cost_usd':.1,'session_id':'provider-fixture','result':'{}','modelUsage':{'different-model':{}}}
    monkeypatch.setattr(E.subprocess,'run',lambda *a,**k:SimpleNamespace(returncode=0,stdout=S.canonical(response)))
    result=E.cli_provider({'call_id':'fixture','experiment_id':'fixture','maximum_usd':.5,'input':{},
        'guidance':'fixture','model':'alias','model_version':'fixed-version'})
    assert result['status']=='model_version_unverified' and B.status()['actual_usd']==.1


def test_detail_scope_uses_original_judgment_and_live_pin():
    import retention_store as R
    import retention_policy as P
    from test_improvement import calendar
    bound=P.boundaries(calendar());old=(C.now()-timedelta(days=90)).isoformat()
    assert R.detail_support(old,boundary=bound)['reason']=='detailed_replay_expired'
    assert R.detail_support(S.now(),boundary=bound)['allowed']
    with S.transaction() as db:
        S.put(db,'retention_pin','validation:v',{'state':'active','until':(C.now()+timedelta(days=1)).isoformat(),
            'period':{'start':old,'end':old},'operations':['semantic_reexperiment']})
    assert R.detail_support(old,'v',boundary=bound)['allowed']
    assert R.detail_support(old,'other',boundary=bound)['allowed'] is False


def test_concurrent_research_reservations_cannot_both_exceed_daily_limit():
    from concurrent.futures import ThreadPoolExecutor
    import research_budget as B
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(lambda key:B.reserve(key,'concurrent',3,{}),('a','b')))
    assert sum(r['dispatch_allowed'] for r in results)==1
    assert B.status()['reserved_usd']==3


@pytest.mark.semantic
def test_full_stage_investigation_keeps_actual_reasons_when_original_readers_are_masked(monkeypatch):
    from test_semantic_layer import completed
    from test_improvement import calendar
    import opportunity_cases as O
    import judgment_coverage as JC
    import improvement_issues as I
    import investigation as Q
    import ai_judgment as J
    import semantic_layer as SL
    import retention_store as R
    import retention_policy as P
    _,manifest,_,_,_=completed(monkeypatch)
    attempt=manifest['run_attempt_id'];JC.finish_attempt(attempt,'normal');O.sync_attempt(attempt)
    case=S.get('opportunity',O.identifier(attempt,'000001','매수'))
    monkeypatch.setattr(R,'current_boundary',lambda *_:P.boundaries(calendar()))
    with S.transaction() as db:
        issue=I.record(db,'fixture_path',case['id'],'fixture','어떤 반론을 유보했는가?',
            expected='recorded treatment',actual='deferred',source_revision='fixture')
    session=Q.start(issue['id'])
    before=Q.query_investigation(session['id'],limit=50)['items']
    def forbidden(*a,**kw):pytest.fail('Original evidence reader called')
    monkeypatch.setattr(J,'_load',forbidden);monkeypatch.setattr(J,'load_judgment',forbidden)
    monkeypatch.setattr(SL,'_read',forbidden)
    after=Q.query_investigation(session['id'],limit=50)['items']
    assert before==after
    content=S.canonical(after)
    assert 'deferred' in content and '매도가 단기 일회성일 수 있다' in content
    assert '기존 구간 판단에서 검토함' in content and '기관 매도 경로' in content


@pytest.mark.semantic
def test_corrected_input_worker_runs_all_roles_without_order_or_live_requalification(monkeypatch):
    from test_semantic_layer import interpretation,evaluation
    import ai_judgment as J
    import semantic_dependencies as D
    import semantic_layer as SL
    _,_,context,data=interpretation(monkeypatch)
    chain=context['chain']
    D.register(chain,'interpretation',queries=[{'type':'fixture_relation','filters':{'code':chain['code']},
        'definition_id':'fixture@v1','as_of':chain['created_at'],'completeness':'complete'}])
    D.changed({'event_id':'new-relation','reason':'late corrected membership','type':'fixture_relation','attributes':{'code':chain['code']}})
    assert D.advance(no_ai=True)[0]['state']=='inputs_pending'
    job=S.page('reassessment')[0]
    receipts=[J._load('deliveries',identifier) for identifier in data['delivery_ids']]
    D.supply_inputs(job['id'],chain,receipts,{role:'fixture correction' for role in SL.ROLES})
    calls=[]
    def provider(request):
        role=request['input']['context']['role'];calls.append(role)
        if role=='interpretation':output=copy.deepcopy(data)
        elif role in ('bull','bear'):output={'evaluations':[evaluation(role)],'objections':[]}
        else:output={'confidence_band_id':'clear_risk','confidence_raw':.7,'recommended_action':'매수','decision':'재평가 기록',
            'adopted_claim_ids':['inst','h1'],'salient_risk_path_ids':['p1'],
            'dispositions':[{'evaluation_id':r+'-eval','state':'adopted','reason':'위험 검토'} for r in ('bull','bear')]}
        return {'status':'completed','data':output}
    result=D.advance(provider=provider)[0]
    assert result['state']=='completed' and calls==list(SL.ROLES)
    assert result['result_kind']=='local_fake' and not result['orders_replayed']
    assert result['current_live_eligibility']=='invalidated_until_new_live_judgment'
    assert D.advance(provider=provider)==[] and len(calls)==4


@pytest.mark.skipif(os.environ.get('SION_TEST_OS_SANDBOX')!='1',reason='explicit isolated OS sandbox integration run')
def test_real_os_sandbox_reproduces_defect_and_passes_candidate(monkeypatch,tmp_path):
    from test_improvement_lifecycle import source_fixture
    import source_validation_runner as V
    path,change=source_fixture(monkeypatch,tmp_path)
    root=path.parents[1]
    (root/'CLAUDE.md').write_text('fixture')
    (root/'run.py').write_text('')
    (root/'tests').mkdir()
    outside=tmp_path/'not_an_input.txt';outside.write_text('not visible in sandbox')
    (root/'tests/test_sample.py').write_text('from sample import VALUE\nimport pytest\nfrom pathlib import Path\n'
        'def test_unit():\n    assert VALUE == 2\ndef test_normal():\n    assert isinstance(VALUE, int)\n'
        'def test_read_boundary():\n    with pytest.raises(PermissionError):\n        Path('+repr(str(outside))+').read_text()\n')
    result=V.run(change['id'],timeout_seconds=30)
    assert result['baseline_defects']['failed']==1,result
    assert result['candidate_regression']['passed']==3,result
    assert result['functional_passed'] and result['adoption_eligible'],result


@pytest.mark.semantic
@pytest.mark.skipif(os.environ.get('SION_TEST_OS_SANDBOX')!='1',reason='explicit isolated OS sandbox integration run')
def test_actual_candidate_runtime_accepts_and_forwards_frozen_stage(monkeypatch,tmp_path):
    import shutil
    from pathlib import Path
    import ai_judgment as J
    import semantic_patch_runtime as P
    from test_semantic_layer import interpretation,evaluation
    _,_,context,data=interpretation(monkeypatch)
    receipts=[J._load('deliveries',identifier) for identifier in data['delivery_ids']]
    root=tmp_path/'candidate';root.mkdir()
    for name in ('src','config'):
        shutil.copytree(Path(C.AGENT)/name,root/name,ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
    runtime=P.SandboxSemantic(context['chain'],receipts,root)
    incoming=runtime.context('interpretation')
    assert runtime.submit(incoming['input_id'],data)['status']=='accepted'
    incoming=runtime.context('bull')
    assert incoming['predecessors'][0]['arguments'][1]['id']=='h1'
    assert runtime.submit(incoming['input_id'],{'evaluations':[evaluation('bull')],'objections':[]})['status']=='accepted'
