"""Order retention must never turn an old request into a fresh transmission."""
import json
from datetime import timedelta
from pathlib import Path
import pytest
from test_execution_trace import isolated,C,E,NOW,PAYLOAD,reserve
from test_improvement import calendar
import order_lifecycle as L


def later(monkeypatch,days=100):
    monkeypatch.setattr(C,'now',lambda:NOW+timedelta(days=days))


def sent(kind='rejected'):
    scope,aid=reserve()
    E.dispatch_once(scope,aid,PAYLOAD,lambda:{'ok':kind!='rejected','broker_acknowledgment':kind,'order_no':'one','qty':100})
    return scope,aid


def proof(scope,aid,*,status='confirmed_full',quantity=100):
    event=E.record('execution_status',account_scope=scope,order_attempt_id=aid,
        summary={'confirmation_status':status,'query_complete':True,'confirmed_qty':quantity,
            'requested_qty':100,'conflicts':[],'ignored_rows':[],'confirmed_average_price':20,'order_no':'one'})
    return L.record_confirmation(event)


def test_retired_and_arbitrary_supplied_ids_cannot_be_reissued(monkeypatch):
    scope,aid=sent()
    assert 'order-g1-' in aid
    assert E.reserve_attempt(scope,attempt_id='arbitrary')['error_code']=='UNKNOWN_OR_EXPIRED_ORDER_ID'
    later(monkeypatch)
    result=L.collect(calendar(),dry_run=False)
    assert result['retired_count']==1 and result['generation']==2
    assert E.reserve_attempt(scope,attempt_id=aid)['error_code']=='UNKNOWN_OR_EXPIRED_ORDER_ID'
    calls=[]
    assert E.dispatch_once(scope,aid,PAYLOAD,lambda:calls.append(1))['ok'] is False
    assert not calls
    new=E.reserve_attempt(scope)
    assert new['order_attempt_id'].startswith('order-g2-')
    assert E.reserve_attempt(scope,attempt_id='order-g2-forged')['ok'] is False


def test_unknown_claim_and_partial_execution_survive_all_generations(monkeypatch):
    scope,unknown=reserve()
    E.dispatch_once(scope,unknown,PAYLOAD,lambda:(_ for _ in ()).throw(TimeoutError('receipt lost')))
    _,partial=sent('accepted');proof(scope,partial,status='confirmed_partial',quantity=40)
    later(monkeypatch)
    for _ in range(3):assert L.collect(calendar(),dry_run=False)['retired_count']==0
    assert E.reserve_attempt(scope,attempt_id=unknown)['status']=='complete'
    assert E.reserve_attempt(scope,attempt_id=partial)['status']=='complete'
    with E._connect(readonly=True) as db:
        assert db.execute('SELECT count(*) FROM order_attempts').fetchone()[0]==2


def test_completed_execution_needs_complete_account_and_no_live_holding(monkeypatch):
    import position_episodes as P
    scope,aid=sent('accepted')
    assert proof(scope,aid)['status']=='closed'
    later(monkeypatch)
    assert L.collect(calendar(),dry_run=False)['retired_count']==0
    P.observe({'holdings':[{'code':'000001','qty':100}],'complete':True})
    assert L.collect(calendar(),dry_run=False)['retired_count']==0
    P.observe({'holdings':[],'complete':False})
    assert L.collect(calendar(),dry_run=False)['retired_count']==0
    P.observe({'holdings':[],'complete':True})
    assert L.collect(calendar(),dry_run=False)['retired_count']==1


def test_conflicting_later_reconciliation_reopens_retention(monkeypatch):
    import position_episodes as P
    scope,aid=sent('accepted');proof(scope,aid)
    proof(scope,aid,status='conflict',quantity=None)
    later(monkeypatch);P.observe({'holdings':[],'complete':True})
    assert L.collect(calendar(),dry_run=False)['retired_count']==0


def test_legacy_intent_retries_bind_original_run_and_cannot_reappear_after_retirement(monkeypatch):
    import judgment_coverage as J
    J._write('attempts','original',{'run_attempt_id':'original','run_id':'run','created_at':NOW.isoformat()})
    scope=E.account_scope()
    first=L.reserve_intent(scope,'same-intent','original')
    second=L.reserve_intent(scope,'same-intent','original')
    assert first['order_attempt_id']==second['order_attempt_id']
    E.dispatch_once(scope,first['order_attempt_id'],PAYLOAD,lambda:{'ok':False,'broker_acknowledgment':'rejected'})
    later(monkeypatch);assert L.collect(calendar(),dry_run=False)['retired_count']==1
    with pytest.raises(ValueError,match='EXPIRED'):L.reserve_intent(scope,'same-intent','original')
    with pytest.raises(ValueError,match='EXPIRED'):L.reserve_intent(scope,'different-intent','original')


def test_readonly_report_and_live_validation_pin(monkeypatch):
    import improvement_store as I
    scope,aid=sent()
    later(monkeypatch)
    with I.transaction() as db:
        I.put(db,'retention_pin','validation-active',{'state':'active','until':(C.now()+timedelta(days=2)).isoformat(),
            'period':{'start':(NOW-timedelta(hours=1)).isoformat(),'end':(NOW+timedelta(days=1)).isoformat()},'scope':{'codes':['000001']}})
    before={str(p):p.read_bytes() for p in Path(C.WORK).rglob('*') if p.is_file()}
    assert L.collect(calendar(),dry_run=True)['protected_count']==1
    assert before=={str(p):p.read_bytes() for p in Path(C.WORK).rglob('*') if p.is_file()}
    assert L.collect(calendar(),dry_run=False)['retired_count']==0


def test_unsent_old_request_expires_without_sender_and_checkpoint_does_not_grow_per_id(monkeypatch):
    scope,aid=reserve();later(monkeypatch)
    assert L.collect(calendar(),dry_run=False)['retired_count']==1
    calls=[]
    assert not E.dispatch_once(scope,aid,PAYLOAD,lambda:calls.append(True))['ok']
    assert calls==[]
    with E._connect(readonly=True) as db:
        assert db.execute('SELECT count(*) FROM order_replay_contract').fetchone()[0]==1
        assert db.execute('SELECT count(*) FROM order_lifecycle').fetchone()[0]==0
        assert db.execute('SELECT count(*) FROM order_intents').fetchone()[0]==0
    monkeypatch.setattr(C,'now',lambda:NOW)
    with pytest.raises(ValueError,match='CANNOT_REVERSE'):L.collect(calendar(),at=NOW.isoformat(),dry_run=False)


def test_tampered_terminal_proof_stops_batch_without_removing_rows(monkeypatch):
    scope,aid=sent();later(monkeypatch)
    with E._connect() as db:
        db.execute("UPDATE order_lifecycle SET proof='{}' WHERE order_attempt_id=?",(aid,))
    with pytest.raises(ValueError,match='PROOF_CORRUPT'):L.collect(calendar(),dry_run=False)
    assert E.reserve_attempt(scope,attempt_id=aid)['status']=='complete'
