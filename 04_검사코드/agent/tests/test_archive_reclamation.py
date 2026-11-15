"""Physical reclaim uses real v1/v2 files and survives interrupted unlink phases."""
from datetime import timedelta
import gc
from pathlib import Path
import uuid
import pytest
from test_ontology import isolated,C,NOW
from test_improvement import calendar
import archive_reclamation as G
import record_store as R
import storage_objects as O
import snapshot_store as V1
import storage_leases as L
import trace_snapshot as V


@pytest.mark.parametrize('publication',['publish','publish_control'])
def test_failed_connection_releases_pin_even_with_exception_traceback_retained(monkeypatch,publication):
    import sqlite3
    base=C.ontology_root();base.mkdir(parents=True)
    def unavailable(*args,**kwargs):raise sqlite3.OperationalError('injected connection failure')
    monkeypatch.setattr(sqlite3,'connect',unavailable)
    with pytest.raises(sqlite3.OperationalError) as caught:
        getattr(R,publication)(base/'record.json',{'value':1},base=base)
    assert caught.value.__traceback__ is not None
    with L.exclusive(base):pass


@pytest.mark.parametrize('interruption',['complete','expired'])
def test_index_prepared_before_collection_cannot_replace_after_it(monkeypatch,interruption):
    from contextlib import contextmanager
    import storage_index as index
    identifier,_=view(monkeypatch);advance(monkeypatch)
    base=C.ontology_root();original=L.exclusive;ran=False
    old_generation=R.generation(base)
    @contextmanager
    def interleaved(folder):
        nonlocal ran
        if not ran:
            ran=True
            def fault(phase,item):
                if phase=='expired':raise RuntimeError('stop after expiration')
            if interruption=='expired':
                with pytest.raises(RuntimeError,match='stop after expiration'):
                    G.collect(calendar(),dry_run=False,fault=fault)
            else:
                assert G.collect(calendar(),dry_run=False)['state']=='reclaimed'
        with original(folder):yield
    monkeypatch.setattr(L,'exclusive',interleaved)
    result=index.rebuild()
    assert result['state']=='deferred'
    if interruption=='complete':
        assert R.generation(base)>old_generation
        assert not any(identifier in path for path,_,_ in index.entries(base))
    else:assert 'RECOVERY_REQUIRED' in result['reason']
    assert not list(base.glob('.index-*'))


def view(monkeypatch,*,legacy=False):
    import trace_query as Q
    body={'schema_version':'TRACE_VIEW@v1','status':'complete','source_manifest':[],
        'application_ids':[],'protection_scopes':[],'targets':[],'traces':{},'cases':[],
        'reviews':[],'proposals':[],'validations':[],'policies':[],'errors':[],
        'content':('a large shared frozen result\n'*2000)}
    monkeypatch.setattr(Q,'build_projection',lambda *a:body)
    result=V.build_view_snapshot()
    assert result['status']=='complete',result
    return result['view_id'],body


def advance(monkeypatch,days=100):
    monkeypatch.setattr(C,'now',lambda:NOW+timedelta(days=days))


def contents():
    root=C.ontology_root()
    return {str(p.relative_to(root)):p.read_bytes() for p in root.rglob('*') if p.is_file()}


def test_shared_blob_survives_first_owner_and_is_physically_reclaimed_after_last(monkeypatch):
    a,expected=view(monkeypatch)
    advance(monkeypatch);b,_=view(monkeypatch)
    before=contents()
    dry=G.collect(calendar())
    assert dry['candidate_views']==1 and dry['shared_blob_bytes']>0
    assert contents()==before
    result=G.collect(calendar(),dry_run=False)
    assert result['state']=='reclaimed',result
    assert result['candidate_views']==1
    assert V.load_view_snapshot(a)['status']=='expired'
    assert V.load_view_snapshot(b)['projection']['content']==expected['content']
    advance(monkeypatch,200)
    result=G.collect(calendar(),dry_run=False)
    assert result['candidate_views']==1 and result['candidate_blob_bytes']>0
    assert result['net_physical_bytes_reclaimed']==result['before']['bytes']-result['after']['bytes']
    assert V.load_view_snapshot(b)['status']=='expired'
    import storage_index
    assert not [p for p,_,_ in storage_index.entries(C.ontology_root()) if '/'+a+'/' in p or '/'+b+'/' in p]


def test_v1_required_content_and_v2_literals_are_not_confused_with_references(monkeypatch):
    view(monkeypatch);base=C.ontology_root()
    source={'repeated':'preserve this original'*1000,'literal':{O.REF:{'sha256':'not-a-real-reference'}}}
    encoded=V1.encode(source,base=base)
    (base/'legacy.json').write_bytes(V1.canonical(encoded))
    R.publish(base/'literal.json',source)
    orphan=O.put_object({'orphan':'unused'*5000},base=base)
    orphan_path=V1._safe_path(O.ref_info(orphan)['sha256'],base)
    advance(monkeypatch)
    result=G.collect(calendar(),dry_run=False)
    assert result['state']=='reclaimed',result
    assert R.load(base/'legacy.json')==source
    assert R.load(base/'literal.json')==source
    assert not orphan_path.exists()


@pytest.mark.parametrize('phase',['expired','records','blobs','index'])
def test_crash_recovery_never_exposes_a_half_deleted_view(monkeypatch,phase):
    identifier,_=view(monkeypatch)
    base=C.ontology_root();R.publish(base/'live.json',{'current':'must survive'*2000})
    expected=O.fingerprint(R.load(base/'live.json'))
    advance(monkeypatch)
    def crash(where,item):
        if where==phase:raise RuntimeError('injected stop')
    with pytest.raises(RuntimeError,match='injected'):G.collect(calendar(),dry_run=False,fault=crash)
    assert V.load_view_snapshot(identifier)['status']=='expired'
    with pytest.raises(ValueError,match='RECOVERY_REQUIRED'):
        R.publish(base/'cannot-reference-sweeping-blobs.json',{'x':1})
    assert G.collect(calendar())['state']=='recovery_pending'
    assert G.collect(calendar(),dry_run=False)['state']=='recovered'
    assert O.fingerprint(R.load(base/'live.json'))==expected
    assert V.load_view_snapshot(identifier)['status']=='expired'
    R.publish(base/'after-recovery.json',{'x':1})


def test_live_lazy_reader_defers_and_active_server_session_protects_view(monkeypatch):
    identifier,_=view(monkeypatch);advance(monkeypatch)
    lazy=R.load(V.root()/'snapshots'/identifier/'projection.json')
    assert G.collect(calendar(),dry_run=False)['state']=='deferred'
    assert lazy['content']
    del lazy;gc.collect()
    session=L.Pin(V.root()/'snapshots'/identifier)
    try:
        result=G.collect(calendar(),dry_run=False)
        assert result['state']=='reclaimed' and result['candidate_views']==0
        assert any(p['reason']=='active_view_session' for p in result['protected'])
    finally:session.close()
    assert G.collect(calendar(),dry_run=False)['candidate_views']==1


def test_new_reference_during_mark_defers_sweep(monkeypatch):
    view(monkeypatch);advance(monkeypatch)
    original=G._plan
    def changed(*args):
        plan,pins=original(*args)
        R.publish(C.ontology_root()/'new-reference.json',{'made_after_mark':1})
        return plan,pins
    monkeypatch.setattr(G,'_plan',changed)
    result=G.collect(calendar(),dry_run=False)
    assert result=={'state':'deferred','reason':'STORAGE_CHANGED_BEFORE_SWEEP'}
    assert R.load(C.ontology_root()/'new-reference.json')['made_after_mark']==1


def test_corrupt_or_unknown_manifest_blocks_entire_blob_region_without_writes(monkeypatch):
    view(monkeypatch);advance(monkeypatch)
    root=C.ontology_root();(root/'unreadable.json').write_text('{bad')
    before=contents()
    with pytest.raises(ValueError):G.collect(calendar(),dry_run=False)
    assert contents()==before
    (root/'unreadable.json').write_text('{"format":"content-addressed-snapshot@v99"}')
    before=contents()
    with pytest.raises(ValueError,match='VERSION_UNKNOWN'):G.collect(calendar(),dry_run=False)
    assert contents()==before


def test_expired_namespace_cannot_publish_old_view_and_unknown_new_id_is_missing(monkeypatch):
    identifier,_=view(monkeypatch);advance(monkeypatch)
    G.collect(calendar(),dry_run=False)
    with pytest.raises(ValueError,match='EXPIRED'):
        V._write_immutable(V.root()/'snapshots'/identifier/'projection.json',{})
    unknown='view-'+C.now().strftime('%Y%m%d')+'-'+uuid.uuid4().hex
    assert V.load_view_snapshot(unknown)['status']=='unavailable'


def test_view_usage_origin_reclaimed_only_after_compact_consumer_migration(monkeypatch):
    import case_store as CS
    import research_usage_ledger as U
    identifier,_=view(monkeypatch)
    manifest=R.load(V.root()/'snapshots'/identifier/'manifest.json')
    usage_id=manifest['exposure_reservation_id'];del manifest
    advance(monkeypatch)
    assert G.collect(calendar(),dry_run=False)['candidate_views']==1
    assert CS.load('usage_events',usage_id) is not None
    other,_=view(monkeypatch)
    usage_id=R.load(V.root()/'snapshots'/other/'manifest.json')['exposure_reservation_id']
    U.migrate();advance(monkeypatch,200)
    assert G.collect(calendar(),dry_run=False)['candidate_views']==1
    assert CS.load('usage_events',usage_id) is None
    assert U.enabled() and U.status(['unknown'])['items'][0]['independent_unused'] is False


def test_legacy_derived_index_rebuilt_atomically_and_missing_index_has_same_live_results(monkeypatch):
    import sqlite3
    import storage_index as X
    base=C.ontology_root();base.mkdir(parents=True)
    db=sqlite3.connect(X.path(base))
    db.execute('CREATE TABLE records(path TEXT PRIMARY KEY,sha TEXT,metadata TEXT)')
    db.executemany('INSERT INTO records VALUES (?,?,?)',[(f'old-{n}.json','old','{}') for n in range(2000)])
    db.commit();db.close()
    R.publish(base/'live.json',{'id':'live','data':'keep'*5000})
    assert X.compact(base)['state']=='index_rebuild_required'
    result=X.rebuild()
    assert result['state']=='rebuilt' and result['records']==1
    assert result['database_bytes_after']<result['database_bytes_before']
    assert [r[0] for r in X.entries(base)]==['live.json']
    assert R.load(base/'live.json')['id']=='live'
    X.path(base).unlink()
    assert R.load(base/'live.json')['id']=='live'
    assert X.rebuild()['records']==1


def test_ended_view_owner_lock_is_reclaimed_and_cannot_be_recreated(monkeypatch):
    import case_store as CS
    identifier,_=view(monkeypatch)
    usage_id=R.load(V.root()/'snapshots'/identifier/'manifest.json')['exposure_reservation_id']
    lock=CS.root()/'locks'/(usage_id+'.sqlite3')
    assert lock.exists()
    advance(monkeypatch)
    assert G.collect(calendar(),dry_run=False)['candidate_views']==1
    assert not lock.exists()
    with pytest.raises(ValueError,match='EXPIRED'):
        import research_evidence
        research_evidence.reserve_usage(identifier,[],purpose='same_expired_owner')
    assert not lock.exists()


def test_live_usage_writer_defers_reclamation_without_ttl_guessing(monkeypatch):
    import case_store as CS
    identifier,_=view(monkeypatch);advance(monkeypatch)
    usage_id=R.load(V.root()/'snapshots'/identifier/'manifest.json')['exposure_reservation_id']
    with CS.locked(usage_id):
        assert G.collect(calendar(),dry_run=False)['state']=='deferred'
    assert G.collect(calendar(),dry_run=False)['candidate_views']==1
