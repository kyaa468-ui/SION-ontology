"""Default trace preparation selects bounded independent evidence, before decode."""
from datetime import timedelta
import pytest
from test_ontology import isolated,C
from test_improvement import calendar
import improvement_store as S
import opportunity_cases as O
import holdout_scopes as H
import retention_store as R
import retention_policy as P
import scoped_trace as T
import trace_snapshot as V


def setup(monkeypatch,count=1):
    from test_improvement import configured
    configured(monkeypatch,storage_bytes=1_000_000_000,case_count=1000,research_job_bytes=10_000_000)
    H.migrate()
    monkeypatch.setattr(R,'current_boundary',lambda *a:P.boundaries(calendar()))
    with S.transaction() as db:
        evidence=O.evidence(db,{'stage_id':'stage','role':'bull','claim':'stored evidence, no reconstruction'},owner='a')
        for n in range(count):
            S.put(db,'opportunity',f'case-{n:03}',{'owner':f'run-{n}','run_attempt_id':f'run-{n}',
                'code':'000001','confidence_for':'buy','origin_at':S.now(),'state':'open','coverage_status':'not_submitted',
                'evidence_refs':[evidence],'stages':[{'role':'bull','evidence_ref':evidence}],
                'judgments':[],'application_ids':[],'execution':[],'delivery_refs':[],
                'capabilities':{'cause_investigation':True}})
    return evidence


def test_view_does_not_scan_original_archives_and_freezes_selected_revision(monkeypatch):
    evidence=setup(monkeypatch)
    def forbidden(*a,**kw):raise AssertionError('unbounded original source read')
    monkeypatch.setattr(V,'_capture_sources',forbidden)
    import trace_query
    monkeypatch.setattr(trace_query,'build_projection',forbidden)
    result=T.build();assert result['status']=='complete',result
    loaded=V.load_view_snapshot(result['view_id'])
    trace=next(iter(loaded['projection']['traces'].values()))
    assert trace['evidence_records'][evidence]['content']['claim']=='stored evidence, no reconstruction'
    assert trace['semantic_execution']['stages']['bull']['stage_id']=='stage'
    with S.transaction() as db:S.put(db,'evidence',evidence,{'content':{'claim':'later'}})
    again=V.load_view_snapshot(result['view_id'])
    assert next(iter(again['projection']['traces'].values()))['evidence_records'][evidence]['content']['claim']=='stored evidence, no reconstruction'
    assert result['manifest']['scope']['legacy_archive_scan'] is False


def test_count_scope_applied_before_evidence_decode(monkeypatch):
    setup(monkeypatch,8)
    result=T.build(limit=2)
    assert result['status']=='partial'
    assert result['manifest']['scope']['expected_count']==8
    loaded=V.load_view_snapshot(result['view_id'])
    assert len(loaded['projection']['targets'])==2
    assert {r['id'] for r in loaded['projection']['source_records'] if r['kind']=='opportunity'}=={'case-000','case-001'}
    chosen=T.build(case_ids=['case-007'])
    assert chosen['status']=='complete' and chosen['manifest']['scope']['selected_count']==1


def test_frozen_judgment_reason_and_action_reach_existing_trace_screen(monkeypatch):
    setup(monkeypatch)
    judgment={'judgment_id':'j','recommended_action':'관망','confidence_for':'매수',
              'confidence_raw':.55,'decision':'기록된 반론을 검토하여 대기한다','arguments':[{'text':'당시 주장'}]}
    with S.transaction() as db:
        case=S.get('opportunity','case-000',conn=db)
        ref=O.evidence(db,judgment,owner=case['id'])
        S.put(db,'opportunity',case['id'],{**case,'evidence_refs':[*case['evidence_refs'],ref],
            'judgments':[{'judgment_id':'j','action':'관망','confidence_raw':.55}]})
    import ai_judgment as J
    def forbidden(*args,**kwargs):raise AssertionError('raw judgment reader must not run')
    monkeypatch.setattr(J,'load_judgment',forbidden)
    result=T.build();projection=V.load_view_snapshot(result['view_id'])['projection']
    trace=next(iter(projection['traces'].values()))
    assert trace['target']['action']=='관망'
    assert trace['judgments'][0]['decision']==judgment['decision']
    assert trace['judgments'][0]['arguments']==judgment['arguments']


def test_large_evidence_is_explicitly_limited_before_decode(monkeypatch):
    evidence=setup(monkeypatch)
    with S.transaction() as db:S.put(db,'evidence',evidence,{'content':'big'*10000})
    result=T.build(maximum_bytes=7000)
    assert result['status'] in {'partial','capacity_blocked'}
    if result['status']=='partial':assert any(e.get('status')=='detail_limit' for e in result['errors'])


def test_calendar_and_consumer_migration_are_explicit_requirements(monkeypatch):
    monkeypatch.setattr(R,'current_boundary',lambda *a:{'deletion_allowed':False})
    assert T.build()['status']=='configuration_pending'
    monkeypatch.setattr(R,'current_boundary',lambda *a:P.boundaries(calendar()))
    assert T.build()['status']=='migration_pending'


def test_shared_evidence_is_withheld_when_holdout_visibility_was_not_recorded(monkeypatch):
    setup(monkeypatch)
    # Scope differs from the visible target; old receipt text might still share
    # a briefing with it, so absence of explicit scope proof is not permission.
    import validation_store as W
    W.archive('holdout_reservations','reserved',{'validation_id':'reserved',
        'spec':{'scope':{'codes':['000002'],'confidence_for':['buy']},
            'period':{'start':(C.now()-timedelta(days=1)).isoformat(),'end':(C.now()+timedelta(days=1)).isoformat()}},
        'registered_at':(C.now()-timedelta(days=2)).isoformat()})
    result=T.build();loaded=V.load_view_snapshot(result['view_id'])
    trace=next(iter(loaded['projection']['traces'].values()))
    assert trace['visibility']=='protected' and 'evidence_records' not in trace
    assert loaded['projection']['application_ids']==[]


def test_unconfigured_capacity_cannot_prepare_optional_view_or_expose_case(monkeypatch):
    setup(monkeypatch)
    from test_improvement import configured
    configured(monkeypatch,storage_bytes=None)
    result=T.build()
    assert result['status']=='configuration_pending' and 'CONFIGURATION_REQUIRED' in result['reason']
    assert not V.root().exists()
    with S.transaction(readonly=True) as db:
        assert db.execute('SELECT count(*) FROM exposure').fetchone()[0]==0
        assert db.execute("SELECT count(*) FROM records WHERE kind='capacity_reservation'").fetchone()[0]==0
