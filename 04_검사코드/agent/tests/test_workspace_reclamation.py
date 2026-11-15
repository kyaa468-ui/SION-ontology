"""Physical patch copies follow the same active roots as their proofs."""
from datetime import timedelta
from pathlib import Path
import pytest
from test_ontology import isolated,C
from test_improvement import calendar
import improvement_store as S
import retention_gc as G
import workspace_reclamation as W


def fixture_workspace():
    directory=W.root()/'change-fixture';directory.mkdir(parents=True)
    (directory/'source.txt').write_text('candidate')
    baseline=W.root()/'change-fixture-baseline';baseline.mkdir()
    (baseline/'source.txt').write_text('baseline')
    with S.transaction() as db:
        S.put(db,'change','change-fixture',{'state':'validation_pending'})
        S.put(db,'patch_workspace','change-fixture',{'owner':'change-fixture','workspace':str(directory),'patch_hash':'fixture'})
        S.link(db,'change','change-fixture','patch_workspace','change-fixture')
    return directory,baseline


def test_active_workspace_protected_and_expired_copies_physically_reclaimed():
    directory,baseline=fixture_workspace()
    G.collect(calendar(),dry_run=False)
    assert directory.exists() and baseline.exists()
    with S.transaction() as db:
        row=S.get('change','change-fixture',conn=db)
        S.put(db,'change',row['id'],{**row,'state':'completed','closed_at':(C.now()-timedelta(days=90)).isoformat()})
    result=G.collect(calendar(),dry_run=False)
    assert not directory.exists() and not baseline.exists()
    assert result['workspace_reclamation'][0]['state']=='completed'
    assert result['workspace_reclamation'][0]['reclaimed_bytes']==len('candidatebaseline')


def test_reclaim_resumes_after_quarantine_commit(monkeypatch):
    directory,baseline=fixture_workspace()
    with S.transaction() as db:
        row=S.get('change','change-fixture',conn=db)
        S.put(db,'change',row['id'],{**row,'state':'completed','closed_at':(C.now()-timedelta(days=90)).isoformat()})
        W.enqueue(db,S.get('patch_workspace','change-fixture',conn=db),cutoff=C.now().date().isoformat())
    original=W.shutil.rmtree
    monkeypatch.setattr(W.shutil,'rmtree',lambda *_:(_ for _ in ()).throw(OSError('fixture failure')))
    assert W.resume()[0]['state']=='reclamation_pending'
    assert S.get('workspace_reclamation','change-fixture')['phases']['change-fixture']=='quarantined'
    monkeypatch.setattr(W.shutil,'rmtree',original)
    assert W.resume()[0]['state']=='completed'
    assert not directory.exists() and not baseline.exists()


def test_prepare_crash_after_rename_recovers_owned_manifest(monkeypatch,tmp_path):
    import source_change_validation as P
    from test_improvement_lifecycle import source_fixture
    target,change=source_fixture(monkeypatch,tmp_path)
    (target.parents[1]/'CLAUDE.md').write_text('fixture')
    (target.parents[1]/'run.py').write_text('')
    original=S.put
    def crash(db,kind,key,body,**kw):
        if kind=='patch_workspace' and body['state']=='prepared':raise OSError('fixture crash after rename')
        return original(db,kind,key,body,**kw)
    monkeypatch.setattr(S,'put',crash)
    with pytest.raises(OSError,match='after rename'):P.prepare(change['id'])
    assert S.get('patch_workspace',change['id'])['state']=='preparing'
    monkeypatch.setattr(S,'put',original)
    result=P.prepare(change['id'])
    assert result['state']=='prepared'
    assert (Path(result['workspace'])/'src/sample.py').read_text()=='VALUE = 2\n'
    assert S.get('capacity_reservation','workspace:'+change['id'])['state']=='completed'
    assert P.prepare(change['id'])['revision']==result['revision']


def test_prepare_rejects_symlink_input_before_copy(monkeypatch,tmp_path):
    import source_change_validation as P
    from test_improvement_lifecycle import source_fixture
    target,change=source_fixture(monkeypatch,tmp_path)
    (target.parent/'secret.py').symlink_to(tmp_path/'outside-secret')
    with pytest.raises(ValueError,match='SYMLINK'):P.prepare(change['id'])
    assert not (W.root()/change['id']).exists()


def test_broken_first_workspace_does_not_starve_later_copy():
    directory,baseline=fixture_workspace()
    with S.transaction() as db:
        row=S.get('change','change-fixture',conn=db)
        S.put(db,'change',row['id'],{**row,'state':'completed','closed_at':(C.now()-timedelta(days=90)).isoformat()})
        W.enqueue(db,S.get('patch_workspace','change-fixture',conn=db),cutoff=C.now().date().isoformat())
        S.put(db,'workspace_reclamation','a-missing',{'state':'pending','names':['a-missing'],'phases':{},'reclaimed_bytes':0})
    assert W.resume(limit=1)[0]['state']=='reclamation_pending'
    assert W.resume(limit=1)[0]['state']=='completed'
    assert not directory.exists() and not baseline.exists()
