"""Live scoring target migration, durable acceptance and finite completion."""
from datetime import timedelta
import copy
import gc
import pytest

from test_ontology import isolated,C
from test_improvement import calendar as retention_calendar
from test_close_evaluation import calendar,at,price_provider
from test_case_dataset import traced_delivery as traced_source,premise_item,accept as accept_source
from storage_objects import materialize
import ai_judgment as J
import improvement_store as I
import scoring_targets as T
import legacy_assessments as L


def accept(payload):
    row,app=accept_source(payload)
    return materialize(row),materialize(app)


def traced_delivery(monkeypatch):
    text,row=traced_source(monkeypatch)
    return text,materialize(row)


def seed(key='one', *, age=0, app=None, committed=True):
    now=(C.now()-timedelta(days=age)).isoformat()
    identifier='judgment-'+key
    app=copy.deepcopy(app) if app else {'application_id':'premise-'+key,'judgment_id':identifier,
        'code':'000001','created_at':now,'judgment_date':now[:10],
        'definition_id':'MA20_MAINTENANCE@v2','definition_snapshot':copy.deepcopy(J.PREMISE_DEFINITION_V2),
        'days':3,'direction':'above'}
    row={'judgment_id':identifier,'acceptance_event_id':'acceptance-'+key,'code':app['code'],
        'created_at':now,'run_attempt_id':'fixture-attempt','premise_applications':[app]}
    if app['judgment_id']==identifier:J._archive('premises',app['application_id'],app)
    J._archive('judgments',identifier,row)
    if committed:publish_event(row)
    return row,app


def publish_event(row):
    J._archive('submissions',row['acceptance_event_id'],{'event_id':row['acceptance_event_id'],
        'status':'accepted','judgment_id':row['judgment_id'],'created_at':row['created_at']})


def select(**kwargs):
    return T.select(C.now().isoformat(),C.today().isoformat(),calendar=retention_calendar(),**kwargs)


def inventory():
    return {str(p.relative_to(C.ontology_root())):p.read_bytes() for p in C.ontology_root().rglob('*') if p.is_file()}


def comparable(entries):
    return sorted([{'application':r['application'],'judgment_ids':sorted(r['judgment_ids'])} for r in entries],key=lambda r:r['application']['application_id'])


def finish(app, *, closed_age=0, unknown=False):
    now=C.now()-timedelta(days=closed_age)
    value={'state':'finalized_unknown' if unknown else 'finalized','application_id':app['application_id'],
        'owner':app['code'],'code':app['code'],'origin_at':app['created_at'],'closed_at':now.isoformat(),
        'known_at':now.isoformat(),'dirty':False,'verdict':'unknown' if unknown else 'refuted',
        'evaluation_id':'evaluation-'+app['application_id'],'support_ended':True}
    with I.transaction() as db:
        row=I.put(db,'legacy_assessment_state',app['application_id'],value)
        T.lifecycle(db,app['application_id'],row)
    return row


def test_explicit_dry_migration_and_selected_reader_parity(monkeypatch):
    _,app=seed();seed('reuse',app=app);seed('two')
    J._archive('premises','orphan',{'application_id':'orphan'})
    before=inventory();proof=T.migrate()
    assert proof['target_count']==2 and not proof['raw_deletion_authorized']
    assert inventory()==before and not T.enabled()
    old=materialize(J.committed_premises(C.now().isoformat(),C.today().isoformat()))
    assert T.migrate(dry_run=False)['state']=='enabled'
    monkeypatch.setattr(J,'committed_premises',lambda *a,**k:pytest.fail('Original history read'))
    monkeypatch.setattr(T,'_source',lambda *a,**k:pytest.fail('Accepted source reread'))
    new=select()
    assert new['expected_count']==2 and not new['target_set_incomplete']
    assert comparable(new['applications'])==comparable(old['applications'])
    assert T.migrate(dry_run=False)['replayed']


def test_empty_dry_migration_creates_no_files(tmp_path):
    assert T.migrate()['target_count']==0
    assert not list(tmp_path.rglob('*'))


def test_source_loss_does_not_become_a_complete_empty_migration():
    row,_=seed()
    J._path('judgments',row['judgment_id']).unlink()
    with pytest.raises(T.TargetError,match='SOURCE_INCOMPLETE'):T.migrate(dry_run=False)
    assert not T.enabled()


def test_late_acceptance_between_inventories_aborts_migration(monkeypatch):
    seed();original=T._inventory;calls=[]
    def race(at):
        result=original(at);calls.append(at)
        if len(calls)==1:
            point=C.now()+timedelta(seconds=1)
            monkeypatch.setattr(C,'now',lambda:point)
            seed('late')
        return result
    monkeypatch.setattr(T,'_inventory',race)
    with pytest.raises(T.TargetError,match='SOURCE_CHANGED'):T.migrate(dry_run=False)
    assert not T.enabled()


def test_active_acceptance_pin_blocks_migration(monkeypatch):
    import storage_leases
    text,_=traced_delivery(monkeypatch)
    original=T.prepare
    def prepare(*args,**kwargs):
        with pytest.raises(storage_leases.StorageBusy):T.migrate(dry_run=False)
        return original(*args,**kwargs)
    monkeypatch.setattr(T,'prepare',prepare)
    accept(premise_item(text))
    assert not T.enabled()


@pytest.mark.parametrize('failure',['control','marker','target'])
def test_corrupt_or_missing_target_state_never_falls_back_to_empty_history(failure):
    _,app=seed();T.migrate(dry_run=False)
    with I.transaction() as db:
        if failure=='control':db.execute("UPDATE scoring_target_control SET hash='bad'")
        elif failure=='marker':db.execute('DROP TABLE scoring_target_enrollment')
        else:db.execute("DELETE FROM records WHERE kind='scoring_target' AND id=?",(app['application_id'],))
    with pytest.raises((T.TargetError,ValueError)):select()


def test_real_acceptance_and_idempotent_retry_register_once(monkeypatch):
    text,_=traced_delivery(monkeypatch);T.migrate(dry_run=False)
    payload=premise_item(text)
    row,app=accept(payload)
    target=I.get('scoring_target',app['application_id'])
    assert select()['expected_count']==1
    assert J.record_ai_judgments([payload])[0]['idempotent']
    assert I.get('scoring_target',app['application_id'])==target
    assert I.page('scoring_acceptance')==[]
    reuse=premise_item(text,key='reuse');reuse['premises']=[{'application_id':app['application_id']}]
    other,_=accept(reuse)
    assert select()['expected_count']==1
    assert set(select()['applications'][0]['judgment_ids'])=={row['judgment_id'],other['judgment_id']}


@pytest.mark.parametrize('phase',['event','targets'])
def test_crash_after_acceptance_file_recovers_without_duplicate_submission(monkeypatch,phase):
    text,_=traced_delivery(monkeypatch);T.migrate(dry_run=False)
    payload=premise_item(text);original=T._finish
    original_event=J._event
    if phase=='targets':
        monkeypatch.setattr(T,'_finish',lambda *a,**k:(_ for _ in ()).throw(RuntimeError('injected after commit')))
    else:
        def event(*args,**kwargs):
            result=original_event(*args,**kwargs)
            if args[1]=='accepted':raise OSError('response lost after commit')
            return result
        monkeypatch.setattr(J,'_event',event)
    result=J.record_ai_judgments([payload])[0]
    assert result['status']=='recording_error'
    pending=I.page('scoring_acceptance');assert len(pending)==1
    assert J.load_judgment(pending[0]['judgment_id'])
    assert I.page('scoring_target')==[]
    monkeypatch.setattr(T,'_finish',original)
    monkeypatch.setattr(J,'_event',original_event)
    assert J.record_ai_judgments([payload])[0]['status']=='accepted'
    assert select()['expected_count']==1 and not I.page('scoring_acceptance')
    assert T.recover()['recovered']==0


def test_uncommitted_intent_is_not_a_scoring_target_and_can_be_retried(monkeypatch):
    text,_=traced_delivery(monkeypatch);T.migrate(dry_run=False)
    original=J._event
    def event(*args,**kwargs):
        if args[1]=='accepted':raise OSError('before acceptance')
        return original(*args,**kwargs)
    monkeypatch.setattr(J,'_event',event)
    payload=premise_item(text)
    assert J.record_ai_judgments([payload])[0]['status']=='recording_error'
    assert select()['expected_count']==0
    assert len(I.page('scoring_acceptance'))==1
    monkeypatch.setattr(J,'_event',original)
    assert J.record_ai_judgments([payload])[0]['status']=='accepted'
    assert select()['expected_count']==1 and not I.page('scoring_acceptance')


def test_pending_accepted_judgment_loss_is_an_error(monkeypatch):
    text,_=traced_delivery(monkeypatch);T.migrate(dry_run=False)
    original=T._finish
    monkeypatch.setattr(T,'_finish',lambda *a,**k:(_ for _ in ()).throw(RuntimeError('crash')))
    J.record_ai_judgments([premise_item(text)])
    row=I.page('scoring_acceptance')[0]
    J._path('judgments',row['judgment_id']).unlink()
    monkeypatch.setattr(T,'_finish',original)
    with pytest.raises(T.TargetError,match='ACCEPTED_JUDGMENT_UNAVAILABLE'):select()
    assert I.page('scoring_acceptance')


def test_lifecycle_correction_and_unknown_closure_are_preserved():
    _,app=seed();T.migrate(dry_run=False)
    result={'knowledge_as_of':I.now(),'period_status':'in_progress','verdict':'unknown','evaluation_id':'e','semantic_hash':'h'}
    old=L.record(app,result,{},retention_calendar())
    assert select()['applications'][0]['lifecycle']==old
    target=I.get('scoring_target',app['application_id'])
    L.record(app,result,{},retention_calendar())
    assert I.get('scoring_target',app['application_id'])==target
    L.mark_correction(app['code'],'receipt-correction')
    assert select()['applications'][0]['lifecycle']['dirty']
    final=finish(app,unknown=True)
    assert not T.pending(select()['applications'][0],I.now())
    L.mark_correction(app['code'],'too-late')
    assert select()['applications'][0]['lifecycle']==final


def test_correction_reaches_more_than_two_hundred_pending_assessments():
    with I.transaction() as db:
        for n in range(205):
            I.put(db,'legacy_assessment_state',str(n),{'owner':'000001','state':'evaluating','dirty':False})
    L.mark_correction('000001','all-targets')
    with I.transaction(readonly=True) as db:
        assert db.execute("SELECT count(*) FROM records WHERE kind='legacy_assessment_state' AND json_extract(body,'$.dirty')=1").fetchone()[0]==205


def test_page_limits_keep_denominator_and_rotate_without_starvation():
    for n in range(5):seed(str(n))
    T.migrate(dry_run=False)
    seen=set()
    for _ in range(3):
        page=select(limit=2,advance=True)
        assert page['expected_count']==5 and page['target_set_incomplete']
        seen.update(x['application']['application_id'] for x in page['applications'])
    assert len(seen)==5
    page=select(maximum_bytes=1024)
    assert page['expected_count']==5 and page['target_set_incomplete']


def test_expired_queue_history_has_unknown_denominator():
    seed();T.migrate(dry_run=False)
    result=T.select((C.now()-timedelta(seconds=1)).isoformat(),C.today().isoformat(),calendar=retention_calendar())
    assert result['state']=='expired' and result['expected_count'] is None and result['target_set_incomplete']


def test_live_job_matches_legacy_scoring_with_target_history_reader_blocked(monkeypatch,calendar):
    import close_evaluation as E
    import premise_evaluation as PE
    text,_=traced_delivery(monkeypatch);accept(premise_item(text))
    at(monkeypatch,'2026-11-09T16:00:00+09:00')
    old=E.run_evaluation(calendar=calendar,provider=price_provider(calendar))
    T.migrate(dry_run=False)
    monkeypatch.setattr(PE,'committed_premises',lambda *a,**k:pytest.fail('Full target history scan'))
    new=E.run_evaluation(calendar=calendar,provider=price_provider(calendar))
    keys=('expected_count','counts','conclusive_count','conclusive_rate','rate_status','target_set_incomplete')
    assert {k:old[k] for k in keys}=={k:new[k] for k in keys}
    assert old['items'][0]['evaluation_id']==new['items'][0]['evaluation_id']
    assert new['target_source']=='producer_scoring_targets'


@pytest.mark.parametrize('unknown',[True,False])
def test_completed_job_reuses_frozen_verdict(monkeypatch,calendar,unknown):
    import close_evaluation as E
    import premise_evaluation as PE
    text,_=traced_delivery(monkeypatch);_,app=accept(premise_item(text))
    final=finish(app,unknown=unknown)
    old=E.run_evaluation(calendar=calendar,provider=price_provider(calendar))
    T.migrate(dry_run=False)
    monkeypatch.setattr(PE,'committed_premises',lambda *a,**k:pytest.fail('Full target history scan'))
    monkeypatch.setattr(PE,'evaluate_premise',lambda *a,**k:pytest.fail('Final target rescored'))
    report=E.run_evaluation(calendar=calendar,provider=price_provider(calendar))
    assert report['expected_count']==1 and report['counts']==old['counts']=={final['verdict']:1}
    assert report['items'][0]['reused_finalized'] and report['items'][0]['evaluation_id']==final['evaluation_id']


def test_gc_protects_active_targets_and_retires_closed_targets_atomically():
    import retention_gc as G
    _,old=seed('old',age=100);_,active=seed('active',age=100)
    finish(old,closed_age=90);T.migrate(dry_run=False)
    assert select()['expected_count']==1
    before=inventory();G.collect(retention_calendar(),dry_run=True)
    assert inventory()==before
    G.collect(retention_calendar(),dry_run=False)
    assert I.get('scoring_target',old['application_id']) is None
    assert I.get('scoring_target',active['application_id'])
    assert select()['expected_count']==1
    with I.transaction(readonly=True) as db:
        assert T.state(db)['target_count']==1
        assert db.execute("SELECT count(*) FROM revisions WHERE kind='scoring_target' AND id=?",(old['application_id'],)).fetchone()[0]==0
    finish(old)  # Historical evaluation cannot recreate the live target.
    assert I.get('scoring_target',old['application_id']) is None


def test_registered_range_and_required_reference_keep_finished_targets():
    import retention_gc as G
    _,app=seed(age=100);finish(app,closed_age=90);T.migrate(dry_run=False)
    with I.transaction() as db:
        I.put(db,'retention_pin','range',{'state':'active','until':(C.now()+timedelta(days=2)).isoformat(),
            'period':{'start':app['created_at'],'end':I.now()},'scope':{'codes':[app['code']]}})
    G.collect(retention_calendar(),dry_run=False)
    assert I.get('scoring_target',app['application_id'])
    assert I.get('legacy_assessment_state',app['application_id'])


def test_future_acceptance_bindings_do_not_leak_into_the_known_target_set(monkeypatch):
    _,app=seed();T.migrate(dry_run=False);before=I.now()
    point=C.now()+timedelta(seconds=1);monkeypatch.setattr(C,'now',lambda:point)
    for key,original in [('reuse',app),('new',None)]:
        row,_=seed(key,app=original,committed=False)
        T.prepare(row)
        with T.acceptance_transaction(row):publish_event(row)
    historical=T.select(before,C.today().isoformat(),calendar=retention_calendar())
    assert historical['expected_count']==1 and not historical['target_set_incomplete']
    assert historical['applications'][0]['judgment_ids']==['judgment-one']
    assert select()['expected_count']==2


def test_retired_reuse_is_rejected_without_recreating_queue_rows():
    import retention_gc as G
    _,app=seed(age=100);finish(app,closed_age=90);T.migrate(dry_run=False)
    G.collect(retention_calendar(),dry_run=False)
    row={"judgment_id":"new-reuse","acceptance_event_id":"new-event","run_attempt_id":"new-run",
        "created_at":I.now(),"premise_applications":[app]}
    with pytest.raises(T.TargetError,match='REUSE_UNAVAILABLE'):T.prepare(row)
    assert not I.page('scoring_target') and not I.page('scoring_acceptance')


def test_pending_capacity_does_not_claim_acceptance_or_grow_versions(monkeypatch):
    T.migrate(dry_run=False)
    monkeypatch.setattr(T,'MAX_PENDING',2)
    rows=[]
    for key in ('a','b','c'):
        app={'application_id':'app-'+key,'judgment_id':key,'created_at':I.now(),'code':'000001'}
        rows.append({'judgment_id':key,'acceptance_event_id':'event-'+key,'run_attempt_id':'fixture',
            'created_at':I.now(),'premise_applications':[app]})
    T.prepare(rows[0]);T.prepare(rows[1])
    for n in range(5):T.prepare({**rows[0],'acceptance_event_id':'retried-event-'+str(n)})
    with pytest.raises(T.TargetError,match='PENDING_CAPACITY'):T.prepare(rows[2])
    assert select()['expected_count']==0
    with I.transaction(readonly=True) as db:
        assert T.state(db)['pending_count']==2
        assert db.execute("SELECT count(*) FROM revisions WHERE kind='scoring_acceptance'").fetchone()[0]==2


def test_target_heartbeat_versions_do_not_grow_without_bound():
    _,app=seed();T.migrate(dry_run=False)
    result={'knowledge_as_of':I.now(),'period_status':'in_progress','verdict':'unknown','evaluation_id':'e','semantic_hash':'h'}
    L.record(app,result,{},retention_calendar())
    for n in range(6):L.mark_correction(app['code'],'receipt-'+str(n))
    with I.transaction(readonly=True) as db:
        assert db.execute("SELECT count(*) FROM revisions WHERE kind='scoring_target'").fetchone()[0]==1


def test_dirty_final_state_stays_in_live_scoring_and_survives_gc():
    import retention_gc as G
    _,app=seed(age=100);finish(app,closed_age=90);T.migrate(dry_run=False)
    with I.transaction() as db:
        old=I.get('legacy_assessment_state',app['application_id'],conn=db)
        value=I.put(db,'legacy_assessment_state',app['application_id'],{**old,'dirty':True})
        T.lifecycle(db,app['application_id'],value)
    assert select()['expected_count']==1
    G.collect(retention_calendar(),dry_run=False)
    assert I.get('scoring_target',app['application_id'])
    assert I.get('legacy_assessment_state',app['application_id'])


def test_job_target_failure_does_not_report_a_known_zero_denominator(monkeypatch,calendar):
    import close_evaluation as E
    text,_=traced_delivery(monkeypatch);accept(premise_item(text));T.migrate(dry_run=False)
    with I.transaction() as db:db.execute("UPDATE scoring_target_control SET hash='corrupt'")
    report=E.run_evaluation(calendar=calendar,provider=price_provider(calendar))
    assert report['expected_count'] is None and report['target_set_incomplete']
    assert report['conclusive_rate'] is None and report['run_status']=='recording_error'


def test_migration_counts_copied_lifecycle_bytes_before_writing_targets(monkeypatch):
    _,app=seed()
    with I.transaction() as db:
        I.put(db,'legacy_assessment_state',app['application_id'],{'owner':app['code'],'state':'evaluating',
            'known_at':I.now(),'window':{'diagnostic':'x'*12000}})
    before=inventory();monkeypatch.setattr(T,'MAX_MIGRATION_BYTES',10000)
    with pytest.raises(T.TargetError,match='BYTES_LIMIT'):T.migrate(dry_run=True)
    assert inventory()==before and not T.enabled()


def test_lifecycle_change_during_migration_does_not_mix_snapshots(monkeypatch):
    _,app=seed();original=T._inventory;calls=[]
    def change(at):
        result=original(at);calls.append(at)
        if len(calls)==1:finish(app)
        return result
    monkeypatch.setattr(T,'_inventory',change)
    with pytest.raises(T.TargetError,match='SOURCE_CHANGED'):T.migrate(dry_run=False)
    assert not T.enabled() and I.page('scoring_target')==[]
