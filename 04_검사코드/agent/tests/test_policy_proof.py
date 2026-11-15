"""Policy verification keeps its gates after historical evidence is masked."""
import copy
from datetime import timedelta
from pathlib import Path

import pytest
from test_ontology import isolated, C, NOW
from test_policy_adoption import authority, activate
import policy_registry as R
import policy_proof as P
import validation_store as V


def forbid_historical(monkeypatch):
    original=R.load
    def read(kind,identifier):
        if kind=='adoptions':pytest.fail('Historical adoption read after proof migration')
        return original(kind,identifier)
    monkeypatch.setattr(R,'load',read)
    monkeypatch.setattr(V,'load',lambda *args,**kwargs:pytest.fail('Historical validation read after proof migration'))


def test_verified_policy_and_lessons_survive_historical_reader_mask(monkeypatch,authority):
    activate(monkeypatch,effect={'operation':'prompt_guidance','text':'검증된 관측의 기간을 구분한다.'})
    before=R.snapshot();params=authority.read_bytes()
    expected=R.LessonBackend(before).list_lessons(NOW)
    report=P.migrate()
    assert report['state']=='verified' and not report['raw_deletion_authorized']
    forbid_historical(monkeypatch)
    assert R.snapshot()==before
    assert R.LessonBackend(R.snapshot()).list_lessons(NOW)==expected
    assert authority.read_bytes()==params
    assert P.migrate()['replayed']


def test_dry_proof_report_does_not_publish_or_change_parameters(monkeypatch,authority):
    activate(monkeypatch)
    before={str(p):p.read_bytes() for p in Path(C.WORK).rglob('*') if p.is_file()}
    assert P.migrate(dry_run=True)['state']=='dry_run'
    assert before=={str(p):p.read_bytes() for p in Path(C.WORK).rglob('*') if p.is_file()}
    assert not R.records('proof_bindings')['records']


@pytest.mark.parametrize('phase',['proof_committed','binding_committed'])
def test_interrupted_migration_reuses_immutable_proof(monkeypatch,authority,phase):
    activate(monkeypatch);before=R.snapshot()
    def stop(where):
        if where==phase:raise OSError('stopped')
    with pytest.raises(OSError):P.migrate(fault=stop)
    assert R.snapshot()==before
    assert P.migrate()['state']=='verified'
    assert len(R.records('policy_proofs')['records'])==1
    assert len(R.records('proof_bindings')['records'])==1


def test_corrupt_compact_proof_never_falls_back_to_legacy_success(monkeypatch,authority):
    activate(monkeypatch);P.migrate()
    original=R.load
    def read(kind,identifier):
        value=original(kind,identifier)
        if kind=='policy_proofs':
            value=copy.deepcopy(value);value['validation']['criteria']['status']='failed'
        return value
    monkeypatch.setattr(R,'load',read)
    result=R.snapshot()
    assert result['status']=='recording_error' and 'PROOF_UNAVAILABLE_OR_CHANGED' in result['reason']


def test_missing_old_proof_cannot_be_migrated_from_eligible_flag_alone(monkeypatch,authority):
    activate(monkeypatch)
    original=V.load
    monkeypatch.setattr(V,'load',lambda kind,key:None if kind=='result_commits' else original(kind,key))
    with pytest.raises(ValueError,match='SOURCE_UNAVAILABLE'):P.migrate()
    assert not R.records('proof_bindings')['records']


def test_compact_verifier_still_checks_base_expiry_and_candidate_params(monkeypatch,authority):
    activate(monkeypatch);P.migrate();forbid_historical(monkeypatch)
    assert R.snapshot(at=NOW+timedelta(days=10))['status']=='expired'
    base=R._current_base()
    monkeypatch.setattr(R,'_current_base',lambda:{**base,'definition_hash':'changed'})
    assert R.snapshot()['status']=='stale_base'
    changed=copy.deepcopy(C.params());changed['decision']['buy_min_confidence']+=.01
    assert R.snapshot(changed)['status']=='recording_error'


def test_proof_capacity_failure_leaves_legacy_consumer_active(monkeypatch,authority):
    activate(monkeypatch);before=R.snapshot()
    monkeypatch.setattr(P,'MAX_BYTES',1)
    with pytest.raises(ValueError,match='CAPACITY_BLOCKED'):P.migrate()
    assert not R.records('proof_bindings')['records']
    assert R.snapshot()==before
