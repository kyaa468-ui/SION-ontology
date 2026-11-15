"""Optional admission cannot discard mandatory records or forgive unknown work."""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import pytest
from test_ontology import isolated,C
import improvement_store as S
import research_capacity as K


def configure(monkeypatch,**values):
    from test_improvement import configured
    return configured(monkeypatch,**{'storage_bytes':1_000_000_000,'case_count':1000,
        'research_job_bytes':10_000_000,**values})


def test_unconfigured_blocks_optional_calls_before_money_or_provider(monkeypatch):
    import research_calls as R
    import research_budget as B
    with pytest.raises(K.CapacityBlocked,match='CONFIGURATION_REQUIRED'):
        R.before('fixture','fixture',['claude','--max-budget-usd','1'])
    assert B.status()['reserved_usd']==0
    with S.transaction() as db:S.put(db,'opportunity','required',{'state':'open'})
    assert S.get('opportunity','required')  # Mandatory capture remains available.


def test_concurrent_reservations_and_crash_do_not_oversubscribe(monkeypatch):
    configure(monkeypatch,storage_bytes=1_500_000,research_job_bytes=1_000_000)
    def take(key):
        try:return K.reserve(key,key)['state']
        except K.CapacityBlocked:return 'blocked'
    with ThreadPoolExecutor(max_workers=2) as pool:states=list(pool.map(take,['a','b']))
    assert sorted(states)==['blocked','reserved']
    assert K.status()['reserved_bytes']==1_000_000
    with pytest.raises(K.CapacityBlocked):K.reserve('c','c')


def test_overage_preserves_required_files_and_stops_next_job(monkeypatch):
    configure(monkeypatch,storage_bytes=2_000_000,research_job_bytes=500_000)
    K.reserve('a','a')
    required=Path(C.WORK)/'journal.jsonl';required.write_bytes(b'x'*2_000_000)
    result=K.finish('a')
    assert result['capacity_exceeded'] and required.stat().st_size==2_000_000
    with pytest.raises(K.CapacityBlocked,match='STORAGE_BYTES'):K.reserve('b','b')


def test_case_overage_and_oversized_source_are_not_silently_truncated(monkeypatch):
    configure(monkeypatch,case_count=1)
    with S.transaction() as db:
        for key in ('a','b'):S.put(db,'opportunity',key,{'state':'open'})
    with pytest.raises(K.CapacityBlocked,match='CASE_COUNT'):K.reserve('a','a')
    with pytest.raises(K.CapacityBlocked,match='JOB_BYTES'):K.reserve('b','b',minimum_bytes=20_000_000)


def test_cancelled_unsent_capacity_can_resume_but_completed_cannot(monkeypatch):
    configure(monkeypatch)
    K.reserve('a','a');K.finish('a',outcome='cancelled_before_dispatch')
    assert K.reserve('a','a')['state']=='reserved'
    K.finish('a')
    with pytest.raises(K.CapacityBlocked,match='ALREADY_FINISHED'):K.reserve('a','a')


def test_readonly_status_creates_no_store():
    assert not S.path().exists()
    assert K.status()['state']=='configuration_pending'
    assert not S.path().exists()
