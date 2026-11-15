"""Only verified projection retirement removes the full reservation scan."""
import copy
import pytest
from test_ontology import isolated,C
from test_validation_holdout import reserved
import validation_holdout as H
import validation_store as V
import holdout_scopes as P
import improvement_store as I


def test_migration_parity_and_scope_protection_survive_masked_raw_reader(monkeypatch):
    result,now=reserved(monkeypatch)
    before=H.reservations()
    assert P.migrate()['active_scopes']==1
    def forbidden(*args,**kwargs):raise AssertionError('historical reservation scan')
    monkeypatch.setattr(V,'records',forbidden)
    assert H.reservations()==before
    origin=before[0]['spec']['period']['start']
    assert H.discovery_exclusions([{'application_id':'new','code':'000001','origin_at':origin}])[0]['application_id']=='new'
    assert P.reservations(include_released=True)==before


def test_gap_between_raw_commit_and_projection_blocks_discovery_and_recovers(monkeypatch):
    assert P.migrate()['active_scopes']==0
    original=P.commit
    monkeypatch.setattr(P,'commit',lambda *a:(_ for _ in ()).throw(ValueError('injected projection failure')))
    from test_validation_data import spec
    from datetime import timedelta
    body={'validation_id':'held','spec':spec('prospective'),'registered_at':C.now().isoformat(),'status':'reserved'}
    body['spec']['period']={'start':(C.now()+timedelta(days=1)).isoformat(),'end':(C.now()+timedelta(days=2)).isoformat()}
    with pytest.raises(V.StoreError,match='injected'):V.archive('holdout_reservations','held',body)
    with pytest.raises(ValueError,match='UNFINISHED'):H.reservations()
    monkeypatch.setattr(P,'commit',original)
    V.archive('holdout_reservations','held',body)
    assert H.reservations()==[body]


def test_corrupt_checkpoint_cannot_turn_reserved_scope_into_empty(monkeypatch):
    result,now=reserved(monkeypatch);P.migrate()
    with I.transaction() as db:
        db.execute("UPDATE records SET hash='bad' WHERE kind='holdout_scope'")
    with pytest.raises(ValueError):H.reservations()


def test_unpublished_pending_source_cannot_be_silently_skipped_by_migration():
    P.prepare('holdout_reservations','pending',{'x':1})
    with pytest.raises(ValueError,match='UNFINISHED'):P.migrate()
    assert not P.enabled()


def test_scope_limit_blocks_instead_of_exposing_unread_tail(monkeypatch):
    result,now=reserved(monkeypatch);P.migrate()
    with pytest.raises(ValueError,match='CAPACITY_BLOCKED'):P.reservations(limit=0)


def test_missing_active_row_is_not_a_successful_empty_reservation_set(monkeypatch):
    reserved(monkeypatch);P.migrate()
    with I.transaction() as db:db.execute("DELETE FROM records WHERE kind='holdout_scope'")
    with pytest.raises(ValueError,match='MEMBERSHIP_CHANGED'):H.reservations()
