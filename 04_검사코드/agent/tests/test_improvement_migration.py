"""Independent case projection and concurrent SQLite reclamation, offline."""
import threading
from concurrent.futures import ThreadPoolExecutor
import pytest
from test_ontology import isolated,C
from test_close_evaluation import calendar
import improvement_store as S


def test_migrated_comparison_survives_masked_original_readers(monkeypatch,calendar):
    from test_case_dataset import traced_delivery,premise_item,accept,score
    import case_dataset as D
    import independent_cases as I
    import evaluation_context as E
    import premise_evaluation as P
    import retention_store as R
    import retention_policy as RP
    text,_=traced_delivery(monkeypatch);accept(premise_item(text))
    score(monkeypatch,calendar,['2026-11-11T16:00:00+09:00','2026-11-12T08:30:00+09:00'])
    market='2026-11-11';known=C.now().isoformat()
    old=D._build_legacy_cases(market,known)
    proof=I.migrate(market,known)
    assert proof['state']=='verified' and not proof['raw_deletion_authorized']
    monkeypatch.setattr(R,'current_boundary',lambda *_:RP.boundaries({**calendar,'calendar_id':'fixture'}))
    def forbidden(*args,**kwargs):pytest.fail('Independent projection reread original data')
    monkeypatch.setattr(D,'_read_original',forbidden)
    monkeypatch.setattr(E,'execution_context',forbidden)
    monkeypatch.setattr(P,'committed_premises',forbidden)
    new=D.build_cases(market,known)
    assert not new['target_set_incomplete'] and new['expected_count']==old['expected_count']
    assert D.compare_cases(new['cases'])['summary']==D.compare_cases(old['cases'])['summary']


def test_reader_snapshot_survives_gc_commit(monkeypatch):
    import retention_gc as G
    from test_improvement import calendar as calendar_fixture
    with S.transaction() as db:
        S.put(db,'opportunity','old',{'state':'completed','origin_at':'2020-01-01T00:00:00+09:00','closed_at':'2020-01-02T00:00:00+09:00','denominator':1})
    started=threading.Event();release=threading.Event()
    def reader():
        with S.transaction(readonly=True) as db:
            row=S.get('opportunity','old',conn=db)
            started.set();assert release.wait(10)
            assert S.get('opportunity','old',conn=db)==row
        return row
    with ThreadPoolExecutor(max_workers=2) as pool:
        held=pool.submit(reader);assert started.wait(10)
        sweep=pool.submit(G.collect,calendar_fixture(),dry_run=False)
        release.set()
        assert held.result()['denominator']==1
        assert sweep.result()['state']=='reclaimed'
    assert S.get('opportunity','old') is None
