"""Discontinued model archives retire only after real consumer checks."""
from datetime import timedelta
import copy
import gc
import pytest

from test_ontology import isolated, C
from test_improvement import calendar
import archive_reclamation as A
import case_store as S
import case_prediction as P
import improvement_store as I
import model_retention as M
import record_store as R


def fixture_archive(key='1', *, state='completed', prediction=True, commit=True, recent=False, publication_age=None):
    origin=C.now()-timedelta(days=2 if recent else 90)
    mid='model-'+key;app='premise-'+key
    pid='prediction-'+S.digest([P.PREDICTION_VERSION,app])
    model={'model_id':mid,'status':'ready','training_cutoff':(origin-timedelta(days=10)).isoformat(),
        'training_inputs':('separate large model '+key+'\n')*3000}
    S.archive('models',mid,model)
    published=origin if publication_age is None else C.now()-timedelta(days=publication_age)
    S.archive('model_publications','publication-'+key,{'publication_id':'publication-'+key,'review_id':'review-'+key,
        'model_id':mid,'available_at':published.isoformat(),'valid_from':origin.isoformat(),
        'valid_until':(origin+timedelta(days=7)).isoformat(),'status':'published'})
    if commit:
        S.archive('publication_commits','review-'+key,{'review_id':'review-'+key,
            'publication_ids':['publication-'+key],'published_at':published.isoformat()})
    target={'prediction_id':pid,'application_id':app,'origin_judgment_id':'judgment-'+key,
        'code':'000001','origin_at':origin.isoformat(),'started_at':origin.isoformat(),
        'kind':'prospective','prediction_version':P.PREDICTION_VERSION}
    if prediction is not None:S.archive('prediction_targets',pid,target)
    if prediction:
        S.archive('predictions',pid,{**target,'created_at':origin.isoformat(),'status':'predicted',
            'model_id':mid,'probability':.3,'inputs':'old forecast detail\n'*500})
    with I.transaction() as db:
        I.put(db,'premise_case',app,{'state':state,'origin_at':origin.isoformat(),
            'closed_at':(origin+timedelta(days=10)).isoformat() if state=='completed' else None})
    return {'model':mid,'application':app,'prediction':pid,'origin':origin.isoformat(),'model_body':model}


def contents():
    return {str(p.relative_to(C.ontology_root())):p.read_bytes()
        for p in C.ontology_root().rglob('*') if p.is_file()}


def test_enrollment_and_dry_collection_are_explicit_and_read_only():
    row=fixture_archive();before=contents()
    assert A.collect(calendar())['model_retirement']['state']=='migration_required'
    assert M.enroll(dry_run=True)['original_counts']['models']==1
    assert contents()==before and not M.enabled()
    assert M.enroll(dry_run=False)['state']=='enabled'
    before=contents()
    plan=A.collect(calendar())['model_retirement']
    assert len(plan['records'])==3 and contents()==before
    assert S.load('models',row['model'])['model_id']==row['model']


def test_finished_artifacts_physically_retire_without_changing_review_markers():
    row=fixture_archive();M.enroll(dry_run=False)
    publication=S.digest(S.load('model_publications','publication-1'))
    commit=S.digest(S.load('publication_commits','review-1'))
    result=A.collect(calendar(),dry_run=False)
    assert result['state']=='reclaimed',result
    assert len(result['model_retirement']['records'])==3
    assert result['candidate_blob_bytes']>0
    assert result['after']['bytes']<result['before']['bytes']
    for kind,key in [('models',row['model']),('predictions',row['prediction']),('prediction_targets',row['prediction'])]:
        assert not S._path(kind,key).exists() and not R.commit_path(S._path(kind,key)).exists()
        with pytest.raises(M.DetailExpired):S.load(kind,key)
    assert S.digest(S.load('model_publications','publication-1'))==publication
    assert S.digest(S.load('publication_commits','review-1'))==commit
    with A.database(C.ontology_root()) as db:
        assert set(M.state(db)['closed_details'])==set(M.DETAIL_KINDS)
        assert db.execute('SELECT count(*) FROM model_expired').fetchone()[0]==0
    assert A.collect(calendar(),dry_run=False)['model_retirement']['records']==[]


def test_no_old_callback_or_direct_publisher_can_recreate_retired_models():
    row=fixture_archive();M.enroll(dry_run=False);A.collect(calendar(),dry_run=False)
    before=contents()
    assert P.record_new_premises({'premise_applications':[]})['status']=='disabled_by_design'
    with pytest.raises(ValueError,match='RETIRED|CLOSED'):S.archive('models',row['model'],row['model_body'])
    with pytest.raises(ValueError,match='RETIRED|CLOSED'):S.archive('models','new-model',{'model_id':'new-model'})
    assert contents()==before


@pytest.mark.parametrize('condition',['active','recent','uncommitted','unfinished','required_reference'])
def test_active_recent_unfinished_and_required_content_are_preserved(condition):
    row=fixture_archive(state='open' if condition=='active' else 'completed',
        recent=condition=='recent',commit=condition!='uncommitted',prediction=condition!='unfinished')
    if condition=='required_reference':
        with I.transaction() as db:
            I.put(db,'investigation','owner',{'state':'open'})
            I.link(db,'investigation','owner','premise_case',row['application'])
    M.enroll(dry_run=False)
    before=S.digest(S.load('models',row['model']))
    result=A.collect(calendar(),dry_run=False)
    assert S.digest(S.load('models',row['model']))==before
    if condition!='uncommitted':assert result['model_retirement']['records']==[]


def test_registered_old_range_and_late_registration_prevent_retirement(monkeypatch):
    row=fixture_archive();M.enroll(dry_run=False)
    original=A._plan
    def insert(*args):
        plan,pins=original(*args)
        with I.transaction() as db:
            I.put(db,'retention_pin','old-range',{'state':'active','until':(C.now()+timedelta(days=5)).isoformat(),
                'period':{'start':row['origin'],'end':C.now().isoformat()},'scope':{'codes':['000001']}})
        return plan,pins
    monkeypatch.setattr(A,'_plan',insert)
    with pytest.raises(ValueError,match='RANGE_CHANGED'):A.collect(calendar(),dry_run=False)
    monkeypatch.setattr(A,'_plan',original)
    result=A.collect(calendar(),dry_run=False)
    assert result['model_retirement']['records']==[]
    assert S.load('models',row['model']) is not None


def test_lifecycle_reopening_after_mark_blocks_sweep(monkeypatch):
    row=fixture_archive();M.enroll(dry_run=False);original=A._plan
    def reopen(*args):
        plan,pins=original(*args)
        with I.transaction() as db:
            old=I.get('premise_case',row['application'],conn=db)
            I.put(db,'premise_case',row['application'],{**old,'state':'open'})
        return plan,pins
    monkeypatch.setattr(A,'_plan',reopen)
    with pytest.raises(ValueError,match='LIFECYCLE_CHANGED'):A.collect(calendar(),dry_run=False)
    assert S.load('models',row['model']) is not None


@pytest.mark.parametrize('phase',['expired','records','blobs','index','checkpoint'])
def test_interrupted_collection_recovers_and_never_recreates_originals(phase):
    row=fixture_archive();M.enroll(dry_run=False)
    def fail(where,path):
        if where==phase:raise RuntimeError('injected crash')
    with pytest.raises(RuntimeError,match='injected'):A.collect(calendar(),dry_run=False,fault=fail)
    with pytest.raises(M.DetailExpired):S.load('models',row['model'])
    if phase!='checkpoint':
        # A committed retirement plan does not need a newly available calendar.
        assert A.collect({'status':'unavailable'},dry_run=False)['state']=='recovered'
    else:assert A.collect(calendar(),dry_run=False)['state']=='reclaimed'
    assert not S._path('models',row['model']).exists()


def test_improvement_gc_hands_completion_to_model_gc_then_retires_ownership():
    import retention_gc as G
    row=fixture_archive();M.enroll(dry_run=False)
    G.collect(calendar(),dry_run=False)
    assert I.get('premise_case',row['application']) is None
    assert I.get('model_retirement_owner',row['application'])['prediction_id']==row['prediction']
    assert A.collect(calendar(),dry_run=False)['state']=='reclaimed'
    assert I.get('model_retirement_owner',row['application']) is None
    with I.transaction(readonly=True) as db:
        assert db.execute("SELECT count(*) FROM revisions WHERE kind='model_retirement_owner'").fetchone()[0]==0


def test_shared_content_and_reachable_readers_are_preserved():
    row=fixture_archive();M.enroll(dry_run=False)
    original=R.load(S._path('models',row['model']))
    R.publish(C.ontology_root()/'required-copy.json',copy.deepcopy(original))
    expected=S.digest(original)
    assert A.collect(calendar(),dry_run=False)['state']=='deferred'
    del original;gc.collect()
    result=A.collect(calendar(),dry_run=False)
    assert result['state']=='reclaimed' and result['shared_blob_bytes']>0
    assert S.digest(R.load(C.ontology_root()/'required-copy.json'))==expected


@pytest.mark.parametrize('corruption',['state','marker'])
def test_corrupt_retirement_control_cannot_reenable_producers(corruption):
    fixture_archive();M.enroll(dry_run=False)
    with A.database(C.ontology_root(),True) as db:
        if corruption=='state':db.execute("UPDATE model_retention SET hash='changed'")
        else:db.execute('DROP TABLE model_enrollment')
    with pytest.raises(ValueError,match='MODEL_RETENTION'):M.enabled()
    with pytest.raises(ValueError,match='MODEL_RETENTION'):S.archive('models','new',{'model_id':'new'})


def test_supported_forecast_performance_is_identical_without_full_history_scan(monkeypatch):
    from test_case_prediction import fresh_case,judgment,mock_context,ORIGIN
    monkeypatch.setattr(C,'now',lambda:ORIGIN)
    case=fresh_case();mock_context(monkeypatch,case)
    assert P.record_new_premises(judgment(case))['items'][0]['status']=='no_model'
    before=P.prospective_performance([case],{},C.now().isoformat())
    M.enroll(dry_run=False)
    original=S.records
    monkeypatch.setattr(S,'records',lambda kind:pytest.fail('Full history scan') if kind in ('predictions','prediction_targets') else original(kind))
    assert P.prospective_performance([case],{},C.now().isoformat())==before


def test_expired_scope_does_not_claim_empty_or_complete_performance():
    row=fixture_archive();M.enroll(dry_run=False);A.collect(calendar(),dry_run=False)
    result=P.prospective_performance([{'application_id':row['application'],'origin_at':row['origin']}],{},C.now().isoformat())
    assert result['status']=='expired' and result['requested_count']==1
    assert result['expected_count'] is None and result['target_set_incomplete'] and result['coverage_rate'] is None
    with pytest.raises(M.DetailExpired):P._choose_model({},row['origin'])


def test_numerical_forecast_summary_is_unchanged_by_scoped_reader(monkeypatch):
    from test_case_prediction import forecasts_for
    from test_case_statistics import synthetic,NOW
    monkeypatch.setattr(C,'now',lambda:NOW)
    cases,cal=synthetic(10,6)
    for row in forecasts_for(cases):
        row['prediction_id']='prediction-'+S.digest([P.PREDICTION_VERSION,row['application_id']])
        row['prediction_version']=P.PREDICTION_VERSION
        S.archive('predictions',row['prediction_id'],row)
    before=P.prospective_performance(cases,cal,NOW.isoformat())
    M.enroll(dry_run=False)
    after=P.prospective_performance(cases,cal,NOW.isoformat())
    assert before==after and after['counts']=={'scorable':60}


def test_unverifiable_forecast_namespace_cannot_be_enrolled():
    S.archive('predictions','arbitrary-forecast',{'prediction_id':'arbitrary-forecast','application_id':'a'})
    before=contents()
    with pytest.raises(ValueError,match='NAMESPACE_UNVERIFIED'):M.enroll(dry_run=False)
    assert not M.enabled() and contents()==before


def test_missing_original_is_not_certified_as_normal_expiration():
    row=fixture_archive();M.enroll(dry_run=False)
    source=S._path('models',row['model'])
    source.unlink();R.commit_path(source).unlink()
    # Remove the derived index so the namespace certificate, independently of
    # INDEX_SOURCE_MISSING, must detect this accidental loss.
    import storage_index
    storage_index.path(C.ontology_root()).unlink()
    before=contents()
    with pytest.raises(ValueError,match='MEMBERSHIP_CHANGED'):A.collect(calendar(),dry_run=False)
    assert contents()==before
    with A.database(C.ontology_root()) as db:
        assert 'models' not in M.state(db)['closed_details']


def test_failed_publication_commit_blocks_enrollment_without_writes():
    row=fixture_archive();source=S._path('models',row['model']);source.unlink()
    before=contents()
    with pytest.raises(ValueError,match='INCOMPLETE_RECORD'):M.enroll(dry_run=False)
    assert contents()==before


def test_small_batches_keep_model_until_its_last_forecast_is_retired():
    row=fixture_archive();M.enroll(dry_run=False)
    narrow=A.collect(calendar(),dry_run=True,limit=1)['model_retirement']
    assert narrow['records']==[] and 'requires_limit' in narrow['batch_reason']
    first=A.collect(calendar(),dry_run=False,limit=2)['model_retirement']
    assert {r['kind'] for r in first['records']}=={'predictions','prediction_targets'}
    assert S.load('models',row['model']) is not None
    second=A.collect(calendar(),dry_run=False,limit=2)['model_retirement']
    assert [r['kind'] for r in second['records']]==['models']


def test_live_reader_defers_enrollment_and_dry_empty_store_writes_nothing():
    before=contents();assert M.enroll(dry_run=True)['state']=='dry_run';assert contents()==before
    row=fixture_archive();reader=S.load('models',row['model'])
    from storage_leases import StorageBusy
    with pytest.raises(StorageBusy):M.enroll(dry_run=False)
    assert not M.enabled() and reader['model_id']==row['model']
    del reader;gc.collect()
    assert M.enroll(dry_run=False)['state']=='enabled'


def test_second_enrollment_cannot_reset_an_already_advanced_checkpoint(monkeypatch):
    from contextlib import contextmanager
    import storage_leases as L
    fixture_archive();original=L.exclusive;intervened=False
    @contextmanager
    def interleaved(base):
        nonlocal intervened
        if not intervened:
            intervened=True
            M.enroll(dry_run=False)
            A.collect(calendar(),dry_run=False)
        with original(base):yield
    monkeypatch.setattr(L,'exclusive',interleaved)
    result=M.enroll(dry_run=False)
    assert result['replayed'] and result['cutoff'] and result['retired_records']==3


def test_legacy_review_remains_complete_after_its_model_original_is_deleted():
    import case_review as CR
    row=fixture_archive()
    S.archive('jobs','review-1',{'review_id':'review-1','spec_version':CR.SPEC_VERSION,'started_at':row['origin']})
    S.archive('results','review-1',{'review_id':'review-1','spec_version':CR.SPEC_VERSION,
        'analysis_status':'complete','recording_status':'recorded','expected_count':0,
        'input_hash':'same-input','statistics':{'model_ids':[row['model']]},'errors':[]})
    S.archive('datasets','review-1',{'review_id':'review-1','input_hash':'same-input','cases':[]})
    assert CR.get_case_review('review-1')['run_status']=='complete'
    before=S.digest(CR.get_case_review('review-1'))
    M.enroll(dry_run=False);A.collect(calendar(),dry_run=False)
    assert not S._path('models',row['model']).exists()
    assert S.digest(CR.get_case_review('review-1'))==before


@pytest.mark.parametrize('publication_age',[1,-1])
def test_recent_or_future_publication_is_not_retired_just_because_its_model_window_is_old(publication_age):
    row=fixture_archive(prediction=None,publication_age=publication_age)
    M.enroll(dry_run=False)
    result=A.collect(calendar(),dry_run=False)
    assert result['model_retirement']['records']==[]
    assert S.load('models',row['model'])['model_id']==row['model']
