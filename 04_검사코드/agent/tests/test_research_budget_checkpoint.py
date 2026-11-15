"""Compaction preserves paid totals, unknown costs, and duplicate-call safety."""
from datetime import timedelta
import pytest
from test_ontology import isolated,C
from test_improvement import calendar
import improvement_store as S
import research_budget as B
import research_budget_checkpoint as K


def pay(identifier='paid',provider='provider',cost=.25):
    assert B.reserve(identifier,'finished-owner',1,{'stage':'fixture'})['dispatch_allowed']
    B.mark_sent(identifier);B.settle(identifier,cost,provider)


def future(monkeypatch):
    old=C.now();monkeypatch.setattr(C,'now',lambda:old+timedelta(days=90));return old


def test_retire_settled_rows_keeps_fixed_checkpoint_and_denies_reissued_call(monkeypatch):
    pay();old=future(monkeypatch)
    result=K.collect(calendar(),dry_run=False)
    assert result['retired_count']==1 and result['retired_actual_usd']==.25
    with S.transaction(readonly=True) as db:
        assert db.execute('SELECT count(*) FROM calls').fetchone()[0]==0
        checkpoint=K.load(db)
        assert checkpoint['actual_total']==250000 and checkpoint['retired_count']==1
        assert len(checkpoint['call_bits'])==K.BYTES
    assert B.reserve('paid','finished-owner',1,{'stage':'fixture'})['state']=='call_retired_or_checkpoint_collision'
    assert B.status(old.isoformat())['history_scope']=='expired'
    assert B.status(old.isoformat())['actual_usd'] is None
    assert B.reserve('new-backdated','x',1,{},at=old.isoformat())['state']=='research_support_expired'
    assert B.status()['actual_usd']==0 and B.status()['retired_actual_usd']==.25


def test_retired_provider_identity_is_not_charged_twice_or_refunded(monkeypatch):
    pay();future(monkeypatch);K.collect(calendar(),dry_run=False)
    assert B.reserve('new','x',1,{})['dispatch_allowed'];B.mark_sent('new')
    with pytest.raises(ValueError,match='RETIRED_OR_CHECKPOINT_COLLISION'):
        B.settle('new',.25,'provider')
    assert B.status()['unresolved_calls']==1 and B.status()['reserved_usd']==1
    assert B.reserve('later','x',1,{})['state']=='cost_unresolved'


def test_unsettled_and_active_owner_rows_are_kept(monkeypatch):
    pay('active','active-provider')
    B.reserve('unknown','finished-owner',1,{})
    assert B.reserve('reserved','other-owner',1,{})['dispatch_allowed']
    B.mark_sent('unknown')
    with S.transaction() as db:
        S.put(db,'investigation','finished-owner',{'state':'investigating'})
    future(monkeypatch)
    assert K.collect(calendar(),dry_run=False)['retired_count']==0
    with S.transaction(readonly=True) as db:
        assert db.execute('SELECT count(*) FROM calls').fetchone()[0]==3
    assert B.status()['unresolved_calls']==1


def test_hash_failure_blocks_reservation_and_no_false_negative_after_checkpoint(monkeypatch):
    pay();future(monkeypatch);K.collect(calendar(),dry_run=False)
    with S.transaction() as db:
        db.execute('UPDATE budget_retirement SET call_bits=? WHERE singleton=1',(bytes(K.BYTES),))
    with pytest.raises(ValueError,match='CHECKPOINT_CORRUPT'):B.reserve('paid','x',1,{})


def test_bounded_identity_filter_collision_fails_closed_without_claiming_duplicate(monkeypatch):
    monkeypatch.setattr(K,'BYTES',1)
    for n in range(8):
        B.reserve('old-'+str(n),'owner',.01,{})
        B.cancel_unsent('old-'+str(n))
    future(monkeypatch);K.collect(calendar(),dry_run=False)
    with S.transaction(readonly=True) as db:
        checkpoint=K.load(db)
        for n in range(8):assert K._contains(checkpoint['call_bits'],'old-'+str(n))
    unseen=next('unseen-'+str(n) for n in range(1000) if K._contains(checkpoint['call_bits'],'unseen-'+str(n)))
    result=B.reserve(unseen,'owner',1,{})
    assert not result['dispatch_allowed'] and result['state']=='call_retired_or_checkpoint_collision'


def test_multiple_batches_preserve_exact_total_without_per_call_history(monkeypatch):
    for n in range(4):pay('call-'+str(n),'provider-'+str(n),.1)
    future(monkeypatch)
    assert K.collect(calendar(),dry_run=False,limit=2)['retired_count']==2
    assert K.collect(calendar(),dry_run=False,limit=2)['retired_count']==2
    assert K.collect(calendar(),dry_run=False,limit=2)['retired_count']==0
    with S.transaction(readonly=True) as db:
        row=K.load(db);assert row['retired_count']==4 and row['actual_total']==400000
        assert db.execute('SELECT count(*) FROM budget_retirement').fetchone()[0]==1
    assert B.status()['unresolved_calls']==0
