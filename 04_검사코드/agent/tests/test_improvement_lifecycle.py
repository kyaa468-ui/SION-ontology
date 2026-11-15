"""Offline source deployment, recovery and honest post-use reporting."""
import copy
from datetime import timedelta
import pytest
from test_ontology import isolated, C
import improvement_store as S


def source_fixture(monkeypatch,tmp_path):
    import change_registry as R
    from test_improvement import configured
    agent=tmp_path/'agent';(agent/'src').mkdir(parents=True)
    path=agent/'src'/'sample.py';path.write_text('VALUE = 1\n')
    monkeypatch.setattr(C,'AGENT',str(agent))
    scope={'codes':['000001']};criteria={'source_tests':{'defects':['test_sample.py::test_unit'],'controls':['test_sample.py::test_normal']}}
    observation={'end_at':(C.now()+timedelta(days=1)).isoformat(),'minimum_uses':2,'max_recurrences':0,'max_side_effects':0,
        'behavior_checks':[{'id':'fixture','path':['case','coverage_status'],'operator':'equals','expected':'linked','specification_ref':'fixture@v1'}],
        'recurrence_rules':[],'side_effect_rules':[]}
    configured(monkeypatch,storage_bytes=1_000_000_000,case_count=1000,research_job_bytes=10_000_000,
        auto_apply=[{'operator':'fixture','targets':['source.sample.py'],'classes':['implementation'],
        'scope':scope,'max_changes_per_day':2,'cooldown_seconds':0,'max_cumulative_changes':5,
        'expires_at':(C.now()+timedelta(days=1)).isoformat(),'rollback':True,
        'change_limits':{'max_patch_bytes':100,'max_cumulative_patch_bytes':1000,'max_changed_lines':10}}],
        validation_criteria={'implementation':criteria},observation_criteria={'source.sample.py':observation})
    monkeypatch.setattr(R,'base_revision',lambda:{'definition_hash':S.digest(path.read_text()),'settings_hash':'fixture'})
    monkeypatch.setattr(R,'targets',lambda:{'source.sample.py':{'deployment_mode':'offline_single_file','ai_impact':False}})
    patch={'before_hash':S.digest(path.read_text()),'content':'VALUE = 2\n'}
    with S.transaction() as db:
        change=S.put(db,'change','source-change',{'adapter':'source_patch','target_id':'source.sample.py',
             'target_path':'src/sample.py','change_class':'implementation','scope':scope,'base_revision':R.base_revision(),
             'patch':patch,'patch_hash':S.digest(patch),'before_content':path.read_text(),
             'validation':{'criteria_hash':S.digest(criteria)},'observation':observation,'state':'validation_pending'})
        S.put(db,'source_validation',change['id'],{'adoption_eligible':True,'base_revision':change['base_revision'],
                  'patch_hash':change['patch_hash'],'criteria_hash':S.digest(criteria)})
        S.put(db,'deployment_lease',change['target_id'],{'state':'quiescent','base_revision':change['base_revision'],
                  'expires_at':(C.now()+timedelta(minutes=5)).isoformat()})
    return path,change


def test_source_patch_apply_idempotent_and_rollback(monkeypatch,tmp_path):
    import source_application as A
    path,change=source_fixture(monkeypatch,tmp_path)
    result=A.apply(change['id'],operator='fixture')
    assert result['state']=='applied' and path.read_text()=='VALUE = 2\n'
    assert A.apply(change['id'],operator='fixture')['replayed']
    assert S.get('observation_job',result['id'])['state']=='applied'
    assert A.rollback(result['id'],operator='fixture',reason='fixture')['state']=='rolled_back'
    assert path.read_text()=='VALUE = 1\n'
    assert S.page('application_quota')[0]['total_count']==1


def test_source_patch_crash_after_replace_recovers_same_intent(monkeypatch,tmp_path):
    import source_application as A
    path,change=source_fixture(monkeypatch,tmp_path)
    original=S.put
    def fail(db,kind,identifier,body,**kwargs):
        if kind=='application' and body.get('state')=='applied':raise OSError('fixture crash')
        return original(db,kind,identifier,body,**kwargs)
    monkeypatch.setattr(S,'put',fail)
    result=A.apply(change['id'],operator='fixture')
    assert result['state']=='application_pending' and path.read_text()=='VALUE = 2\n'
    assert S.get('application',result['id'])['state']=='intent'
    monkeypatch.setattr(S,'put',original)
    result=A.apply(change['id'],operator='fixture')
    assert result['state']=='applied'
    assert len(S.page('application'))==1


def test_source_patch_rollback_conflict_holds_future_apply(monkeypatch,tmp_path):
    import source_application as A
    path,change=source_fixture(monkeypatch,tmp_path)
    result=A.apply(change['id'],operator='fixture')
    path.write_text('VALUE = 3\n')
    assert A.rollback(result['id'],operator='fixture',reason='fixture')['state']=='rollback_conflict'
    assert path.read_text()=='VALUE = 3\n' and S.get('application_hold','global')


def test_notification_retry_never_reapplies_and_deduplicates(monkeypatch):
    from test_improvement import configured
    import improvement_notifications as N
    configured(monkeypatch,notification_channels=[{'id':'fixture','kind':'webhook','url':'https://example.invalid',
        'supports_idempotency_key':True,'max_attempts':2,'retry_seconds':1}])
    with S.transaction() as db:S.put(db,'notification','n',{'state':'queued','channel_id':'fixture','event':'applied','application_id':'a','attempts':0})
    calls=[]
    def ambiguous(channel,body,key):calls.append(key);raise TimeoutError('fixture')
    assert N.dispatch(transport=ambiguous)[0]['state']=='retrying'
    monkeypatch.setattr(S,'now',lambda:(C.now()+timedelta(seconds=2)).isoformat())
    def delivered(channel,body,key):calls.append(key);return {'state':'delivered'}
    assert N.dispatch(transport=delivered)[0]['state']=='delivered'
    assert N.dispatch(transport=delivered)==[] and calls==['n','n']
    assert S.page('application')==[]


def test_business_check_does_not_treat_confidence_as_causal_proof():
    from semantic_validation import check
    assert check({'measurement':{'unit':'KRW'}},{'path':['measurement','unit'],'operator':'equals','expected':'KRW','specification_ref':'unit@v1'})
    with pytest.raises(ValueError,match='SPECIFICATION'):
        check({'confidence':.9},{'path':['confidence'],'operator':'range','minimum':0,'maximum':1})


@pytest.mark.semantic
def test_paired_stages_resume_without_recalling_or_promoting_fake(monkeypatch):
    from test_semantic_layer import interpretation,evaluation
    import semantic_experiments as E
    import ai_judgment as J
    _,_,context,data=interpretation(monkeypatch)
    receipts=[J._load('deliveries',identifier) for identifier in data['delivery_ids']]
    experiment=E.register('fixture',{'model':'fixture','model_version':'fixture@v1','criteria':{},'maximum_usd':1,
         'data_cutoff':C.now().isoformat(),'tool_permissions':[],'repetitions':2},[{'chain':context['chain'],'receipts':receipts}])
    calls=[]
    def provider(request):
        calls.append((request['arm'],request['role'],request['repetition']))
        role=request['role']
        if role=='interpretation':output=copy.deepcopy(data)
        elif role in ('bull','bear'):output={'evaluations':[evaluation(role)],'objections':[]}
        else:output={'confidence_band_id':'clear_risk','confidence_raw':.7,'recommended_action':'매수','decision':'고정 위험 확인',
              'adopted_claim_ids':['inst','h1'],'salient_risk_path_ids':['p1'],
              'dispositions':[{'evaluation_id':r+'-eval','state':'adopted','reason':'위험 채택'} for r in ('bull','bear')]}
        return {'status':'completed','data':output}
    first=E.run(experiment['id'],provider=provider)
    assert first['state']=='completed' and first['expected_stage_calls']==16 and len(calls)==16
    import retention_gc as G
    from test_improvement import calendar
    G.collect(calendar(),dry_run=False)
    assert S.get('experiment_result',experiment['id'])
    assert len(S.page('semantic_trial',owner=experiment['id']))==16
    monkeypatch.setattr(S,'now',lambda:(C.now()+timedelta(days=1)).isoformat())
    assert E.register('fixture',experiment['spec'],experiment['cases'])['id']==experiment['id']
    repeated=E.run(experiment['id'],provider=provider)
    assert repeated['state']=='completed' and len(calls)==16
    assert repeated['actual_ai_calls']==0 and not repeated['adoption_eligible']
    with pytest.raises(ValueError,match='RESULT_KIND'):E.run(experiment['id'],with_ai=True)


def test_large_required_evidence_is_shared_bounded_parts_and_gc_keeps_closure():
    import opportunity_cases as O
    import retention_gc as G
    from test_improvement import calendar
    with S.transaction() as db:
        identifier=O.evidence(db,{'text':'long evidence '*100000},owner='case')
        S.put(db,'opportunity','case',{'state':'open'})
        S.link(db,'opportunity','case','evidence',identifier)
    root=S.get('evidence',identifier)
    assert root['content_encoding']=='required_parts'
    before=len(S.page('evidence',limit=200))
    G.collect(calendar(),dry_run=False)
    assert len(S.page('evidence',limit=200))==before


def test_gc_report_does_not_create_database_and_boolean_aggregates_are_typed():
    import retention_gc as G
    import assessment_contracts as A
    from test_improvement import calendar
    assert not S.path().exists()
    assert G.collect(calendar())['state']=='empty'
    assert not S.path().exists()
    with pytest.raises(ValueError,match='BOOLEAN_REQUIRED'):
        A._evaluate({'op':'all','arg':{'op':'literal','value':0,'unit':'KRW'}},[{}],True)
