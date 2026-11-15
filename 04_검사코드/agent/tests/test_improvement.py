"""Plan 4 adversarial, offline contracts. No provider or broker is invoked."""
import copy
from datetime import timedelta

import pytest
from test_ontology import isolated, C
from test_judgment_integration import setup_run
import improvement_store as S
import research_budget as B
import assessment_contracts as A
import opportunity_cases as O
import improvement_issues as I
import investigation as Q


def configured(monkeypatch,**overrides):
    config=copy.deepcopy(S.config());config.update(overrides)
    monkeypatch.setattr(S,'config',lambda:copy.deepcopy(config))
    return config


def test_budget_reservations_are_atomic_and_persist(monkeypatch):
    first=B.reserve('a','i',3,{'stage':'bull'})
    assert first['dispatch_allowed']
    assert not B.reserve('a','i',3,{'stage':'bull'})['dispatch_allowed']
    assert B.reserve('b','i',3,{})['state']=='budget_blocked'
    with pytest.raises(ValueError,match='CONFLICT'):B.reserve('a','i',3,{'stage':'bear'})
    B.mark_sent('a');B.settle('a',2,'provider-a')
    assert B.settle('a',2,'provider-a')['actual']==2_000_000
    assert B.reserve('b','i',3,{})['dispatch_allowed']
    assert B.status()['actual_usd']==2


def test_uncertain_cost_blocks_next_kst_day_and_no_refund(monkeypatch):
    B.reserve('a','i',1,{})
    B.mark_sent('a');B.settle('a',None,'provider-a')
    with pytest.raises(ValueError):B.cancel_unsent('a')
    next_day=C.now()+timedelta(days=1)
    assert B.reserve('b','i',1,{},at=next_day.isoformat())['state']=='cost_unresolved'
    B.settle('a',.3,'provider-a')
    assert B.reserve('b','i',1,{},at=next_day.isoformat())['dispatch_allowed']
    assert B.status(next_day.isoformat())['actual_usd']==0


def test_overrun_stops_calls_and_nan_rejected():
    with pytest.raises(ValueError): B.reserve('nan','i',float('nan'),{})
    B.reserve('a','i',1,{});B.mark_sent('a')
    assert B.settle('a',1.1,'p')['state']=='overrun'
    assert B.reserve('b','i',.1,{})['state']=='cost_unresolved'


def spec(monkeypatch,op='all'):
    configured(monkeypatch,assessment_max_sessions=20,assessment_definitions={'inst@v1':{'source':'fixture','unit':'KRW','method':'net_value','collector':'trusted_fixture'}})
    now=C.now()
    import market_calendar as M
    def fixture_calendar(start,end,*args,**kwargs):
        dates=[now-timedelta(days=2)+timedelta(days=n) for n in range(4)]
        return {'status':'complete','calendar_id':'fixture-future-calendar','sessions':[
            {'date':d.date().isoformat(),'close_at':d.replace(hour=15,minute=30).isoformat()} for d in dates if d.weekday()<5]}
    monkeypatch.setattr(M,'find_calendar_snapshot',fixture_calendar)
    raw={'text':'기관 순매수가 모든 관측에서 양수다','claim_id':'h','path_id':'p','kind':'set_condition',
         'start_at':(now-timedelta(days=2)).isoformat(),'end_at':(now+timedelta(days=1)).isoformat(),
         'expected_observations':['a','b'],
         'fields':{'institution':{'unit':'KRW','method':'net_value','source':'fixture','definition_id':'inst@v1'}},
         'expression':{'op':op,'arg':{'op':'compare','operator':'gt','left':{'op':'value','field':'institution'},
                                    'right':{'op':'literal','value':0,'unit':'KRW'}}}}
    return A.normalize(raw,known_ids={'h','p'},code='000001',origin_at=now.isoformat())


def assessment(monkeypatch,op='all'):
    contract=spec(monkeypatch,op)
    chain={'chain_id':'c','run_attempt_id':'a','code':'000001','confidence_for':'매수','created_at':C.now().isoformat()}
    item=A.register_stage({'stage_id':'c-interpretation'},chain,[contract])[0]
    A.mark_usage(chain,'fixture-judgment',{'h'},{'p'})
    return S.get('assessment',item['id'])


def observation(item,key,value,available=None):
    definition=item['spec']['fields']['institution']
    return {'key':key,'observed_at':available or C.now().isoformat(),'available_at':available or C.now().isoformat(),
            'source_identity':'source-'+key,'fields':{'institution':{**definition,'value':value}}}


def test_all_counterexample_refutes_without_complete_horizon(monkeypatch):
    item=assessment(monkeypatch)
    A.ingest(item['id'],observation(item,'a',0))
    result=A.evaluate(item['id'])
    assert result['verdict']=='refuted'
    assert not result['complete'] and not result['period_complete'] and not result['performance_eligible']
    assert len(S.page('issue'))==1
    assert A.evaluate(item['id'])['revision']==result['revision']


def test_all_positive_partial_is_unknown_any_positive_is_supported(monkeypatch):
    item=assessment(monkeypatch)
    A.ingest(item['id'],observation(item,'a',10))
    assert A.evaluate(item['id'])['verdict']=='not_due'
    item=spec(monkeypatch,'any')
    value,unit=A._evaluate(item['expression'],[observation({'spec':item},'a',10)],False)
    assert value is True and unit=='bool'


def test_corrections_preserve_as_of_and_mechanism_not_proved(monkeypatch):
    item=assessment(monkeypatch)
    first=C.now()-timedelta(hours=1)
    A.ingest(item['id'],observation(item,'a',-1,first.isoformat()))
    A.ingest(item['id'],observation(item,'a',1))
    assert A.evaluate(item['id'],as_of=first.isoformat())['verdict']=='refuted'
    assert A.evaluate(item['id'])['verdict']=='not_due'
    mechanism=A.normalize({'text':'매수가 가격을 지지한다','claim_id':'h','path_id':'p','kind':'mechanism'},
                          known_ids={'h','p'},code='000001',origin_at=C.now().isoformat())
    assert mechanism['support']=='evaluation_unsupported'


def test_opportunity_missing_submission_and_idempotent_retry(monkeypatch):
    import judgment_coverage as JC
    _,manifest=setup_run(monkeypatch)
    attempt=manifest['run_attempt_id']
    assert JC.finish_attempt(attempt,'normal')['recording_status']=='recorded'
    case=S.page('opportunity')[0]
    assert case['coverage_status']=='not_submitted' and case['denominator']==1
    before=S.page('issue')[0]['occurrence_count']
    O.sync_attempt(attempt)
    assert len(S.page('opportunity'))==1
    assert S.page('issue')[0]['occurrence_count']==before


def investigation_fixture(monkeypatch):
    configured(monkeypatch)
    import retention_store as R
    import retention_policy as RP
    monkeypatch.setattr(R,'current_boundary',lambda *_:RP.boundaries(calendar()))
    with S.transaction() as db:
        refs=[]
        for period in ('intraday','20_sessions'):
            refs.append(O.evidence(db,{'subject':'000001','unit':'KRW','period':period,
                                      'method':'net_value','measurement_kind':'flow','value':10},owner='o'))
        case=S.put(db,'opportunity','o',{'owner':'a','code':'000001','confidence_for':'매수','origin_at':C.now().isoformat(),
                   'account_scope':{'mode':'mock'},'evidence_refs':refs,'application_ids':[],'state':'completed','closed_at':S.now(),
                   'capabilities':{'cause_investigation':True}})
        issue=I.record(db,'comparison','o','s','기간이 비교 가능한가?',expected='same_period',actual='different',source_revision='1')
    return Q.start(issue['id']),case


def test_investigation_actual_refs_checks_and_no_undeclared_execution(monkeypatch):
    session,case=investigation_fixture(monkeypatch)
    with pytest.raises(ValueError,match='NOT_EXPOSED'):
        Q.run_investigation_check(session['id'],'equality@v1',[{'ref':'evidence/'+case['evidence_refs'][0]}])
    page=Q.query_investigation(session['id'])
    check=Q.run_investigation_check(session['id'],'measurement_comparability@v1',
         [{'ref':'evidence/'+r,'path':['content']} for r in case['evidence_refs']])
    assert check['result']['verdict']=='fail'
    assert check['result']['differences'][0]['field']=='period'
    with pytest.raises(ValueError,match='UNREGISTERED'):
        Q.run_investigation_check(session['id'],'shell',[{'ref':page['actual_returned_refs'][0]}])
    diagnosis=Q.submit_diagnosis(session['id'],{'question':'기간이 같은가','explanation':'동일 기간 비교 계약 위반',
        'uncertainties':['시장 인과는 확인하지 않음'],'alternatives':[{'explanation':'원천 값 수집 오류','expected_result':'동일 기간도 값 불일치',
        'distinguishing_check':'measurement_comparability@v1'}], 'checks':[check['check_id']],
        'refs':page['actual_returned_refs'],'conclusion':'confirmed_defect'})
    assert diagnosis['acceptance']=='schema_and_recorded_checks_only'
    assert not diagnosis['causal_proof']


def test_holdout_checked_before_context_and_repeat_query(monkeypatch):
    session,case=investigation_fixture(monkeypatch)
    import validation_holdout as VH
    monkeypatch.setattr(VH,'is_reserved_judgment',lambda _:['reserved'])
    with pytest.raises(ValueError,match='RESERVED'):Q.get_investigation_context(session['id'])
    with pytest.raises(ValueError,match='RESERVED'):Q.query_investigation(session['id'])


def test_cumulative_query_bytes_and_cursor_scope(monkeypatch):
    session,case=investigation_fixture(monkeypatch)
    page=Q.query_investigation(session['id'],limit=1)
    assert page['next_cursor']
    with pytest.raises(ValueError,match='CURSOR'):
        Q.query_investigation(session['id'],'opportunity',cursor=page['next_cursor'])
    with S.transaction() as db:
        current=S.get('investigation',session['id'],conn=db)
        S.put(db,'investigation',session['id'],{**current,'limits':{**current['limits'],'bytes':1}})
    with pytest.raises(ValueError,match='BYTES'):Q.query_investigation(session['id'])


@pytest.mark.semantic
def test_frozen_four_stage_checker_forwards_new_output(monkeypatch):
    from test_semantic_layer import interpretation,evaluation
    import semantic_experiments as E
    import ai_judgment as J
    _,_,context,data=interpretation(monkeypatch)
    receipts=[J._load('deliveries',d) for d in data['delivery_ids']]
    runtime=E.FrozenSemantic(context['chain'],receipts)
    first=runtime.context('interpretation')
    data['arguments'][1]['text']='새로운 해석을 후속 단계에 전달한다'
    assert runtime.submit(first['input_id'],data)['status']=='accepted'
    for role in ('bull','bear'):
        incoming=runtime.context(role)
        assert incoming['predecessors'][0]['arguments'][1]['text']=='새로운 해석을 후속 단계에 전달한다'
        assert runtime.submit(incoming['input_id'],{'evaluations':[evaluation(role)],'objections':[]})['status']=='accepted'
    incoming=runtime.context('lead')
    final={'confidence_band_id':'clear_risk','confidence_raw':.7,'recommended_action':'매수','decision':'기록된 위험을 검토함',
           'adopted_claim_ids':['inst','h1'],'salient_risk_path_ids':['p1'],
           'dispositions':[{'evaluation_id':r+'-eval','state':'adopted','reason':'해당 위험 채택'} for r in ('bull','bear')]}
    assert runtime.submit(incoming['input_id'],final)['status']=='accepted'
    assert runtime.judgments[0]['scope']=='isolated_experiment'
    assert not list((C.ontology_root()/'judgments').glob('*.json'))


def calendar():
    days=[C.now()-timedelta(days=n) for n in reversed(range(60))]
    return {'status':'complete','calendar_id':'synthetic-reviewed','sessions':[
        {'date':d.date().isoformat(),'close_at':d.replace(hour=15,minute=30).isoformat()} for d in days if d.weekday()<5]}


def test_retention_shared_evidence_and_rootless_cycle(monkeypatch):
    import retention_gc as G
    configured(monkeypatch)
    with S.transaction() as db:
        ref=O.evidence(db,{'actual':'shared content'},owner='old')
        for identifier,age in [('old',50),('recent',1)]:
            S.put(db,'opportunity',identifier,{'owner':identifier,'state':'completed',
                'origin_at':(C.now()-timedelta(days=age)).isoformat(),'closed_at':(C.now()-timedelta(days=age)).isoformat()})
            S.link(db,'opportunity',identifier,'evidence',ref)
        for identifier in ('cycle-a','cycle-b'):
            S.put(db,'evidence',identifier,{'content':identifier})
        S.link(db,'evidence','cycle-a','evidence','cycle-b');S.link(db,'evidence','cycle-b','evidence','cycle-a')
    result=G.collect(calendar(),dry_run=False)
    assert result['state']=='reclaimed'
    assert S.get('opportunity','old') is None and S.get('opportunity','recent')
    assert S.get('evidence',ref)['content']=={'actual':'shared content'}
    assert S.get('evidence','cycle-a') is None and S.get('evidence','cycle-b') is None


def test_missing_calendar_and_corrupt_required_content_block_gc(monkeypatch):
    import retention_gc as G
    assert not G.collect({})['deletion_allowed']
    with S.transaction() as db:
        S.put(db,'opportunity','p',{'state':'open'})
        S.link(db,'opportunity','p','evidence','missing')
    with pytest.raises(ValueError,match='REQUIRED_CONTENT'):G.collect(calendar(),dry_run=False)


def test_two_twenty_session_windows_differ_for_long_premise():
    import retention_policy as P
    case={'origin_at':(C.now()-timedelta(days=55)).isoformat(),'closed_at':C.now().isoformat(),
          'capabilities':dict.fromkeys(('cause_investigation','premise_reevaluation','semantic_reexperiment','portfolio_reexperiment'),True)}
    support=P.capabilities(case,P.boundaries(calendar()))
    assert support['cause_investigation'] and support['premise_reevaluation']
    assert not support['semantic_reexperiment'] and not support['portfolio_reexperiment']


def test_query_membership_change_invalidates_unread_new_member():
    import semantic_dependencies as D
    chain={'chain_id':'chain-c','run_attempt_id':'attempt-a'}
    D.register(chain,'interpretation',records=[{'id':'original-read'}],
        queries=[{'type':'company_relation','filters':{'company':'A'},'definition_id':'mapping-v1',
                  'as_of':S.now(),'completeness':'complete'}])
    result=D.changed({'event_id':'correction-a','reason':'new relation','changed_ids':['never-read-before'],
                      'type':'company_relation','attributes':{'company':'A'}})
    assert result['affected']==['chain-c'] and result['orders_replayed'] is False
    assert S.page('reassessment')[0]['impact']=='query_membership_candidate'


def test_observation_requires_actual_use_and_complete_behavior(monkeypatch):
    import change_observation as V
    with S.transaction() as db:
        V.register(db,{'id':'app','after_revision':{'version':'new'},'observation_criteria':{
            'end_at':(C.now()+timedelta(days=1)).isoformat(),'minimum_uses':1,'max_recurrences':0,'max_side_effects':0}})
    assert V.evaluate('app')['state']=='applied'
    with pytest.raises(ValueError,match='CONSUMPTION'):V.record('app','e',consumption_id='none',expected_behavior=True,
                    recurrence=False,side_effect=False,complete=True,evidence_refs=['check'])
    V.consume({'version':'old'},attempt_id='a',judgment_id='j',stage='record',evidence_ref='e')
    assert not S.page('change_consumption')
    V.consume({'version':'new'},attempt_id='a',judgment_id='j',stage='record',evidence_ref='e')
    use=S.page('change_consumption')[0]
    V.record('app','e',consumption_id=use['id'],expected_behavior=True,recurrence=False,side_effect=False,complete=True,evidence_refs=['check'])
    assert V.evaluate('app')['state']=='observing'
    assert V.evaluate('app',at=(C.now()+timedelta(days=2)).isoformat())['state']=='improved'


def test_tuning_cannot_write_settings_or_lessons(monkeypatch):
    import tuning
    original=C.params()
    result=tuning._apply_authorized([{'key':'decision.buy_min_confidence','to':.6,'reason':'test'}],'test',['lesson'])
    assert result['applied']==[] and result['status']=='investigation_pending'
    assert C.params()==original
    assert not __import__('pathlib').Path(tuning.LESSONS).exists()
    with pytest.raises(ValueError,match='RETIRED'):tuning.write_params([])
