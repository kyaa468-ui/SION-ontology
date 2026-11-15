"""Physical case removal must not make prior research an independent trial."""
import pytest
from test_ontology import isolated,C
from test_improvement import calendar
import improvement_store as S
import retention_gc as G
import exposure_checkpoint as E
from investigation import exposure_identity


def make_case():
    with S.transaction() as db:
        case=S.put(db,'opportunity','old',{'run_attempt_id':'run','code':'000001','confidence_for':'buy',
            'account_scope':'account','origin_at':'2020-01-01T00:00:00+09:00','state':'completed',
            'closed_at':'2020-01-02T00:00:00+09:00'})
        source=exposure_identity(case)
        db.executemany('INSERT INTO exposure VALUES (?,?,?)',[(source,'owner1',S.now()),(source,'owner2',S.now())])
    return case,source


def test_case_and_per_owner_rows_reclaimed_but_independence_still_blocked():
    case,source=make_case()
    result=G.collect(calendar(),dry_run=False)
    assert S.get('opportunity','old') is None
    assert result['opportunity_exposure']['retired_rows']==2
    with S.transaction(readonly=True) as db:
        assert db.execute('SELECT count(*) FROM exposure').fetchone()[0]==0
        assert E.status(db,source)=={'used':True,'basis':'prior_exposure_or_checkpoint_collision'}
        assert not E.status(db,'unseen')['used']


def test_live_case_keeps_exact_evidence_and_repeated_owner_growth_is_collapsed():
    case,source=make_case()
    with S.transaction() as db:S.put(db,'opportunity','old',{**case,'state':'open','closed_at':None})
    result=G.collect(calendar(),dry_run=False)
    assert result['opportunity_exposure']['retired_rows']==0
    with S.transaction() as db:
        assert E.status(db,source)['basis']=='recorded_exposure'
        assert db.execute('SELECT count(*) FROM exposure').fetchone()[0]==1
        db.execute('INSERT OR IGNORE INTO exposure VALUES (?,?,?)',(source,'owner3',S.now()))
        assert db.execute('SELECT count(*) FROM exposure').fetchone()[0]==1


def test_corruption_cannot_turn_compacted_exposure_into_nonuse():
    case,source=make_case();G.collect(calendar(),dry_run=False)
    with S.transaction() as db:db.execute("UPDATE exposure_retirement SET hash='bad'")
    with S.transaction(readonly=True) as db:
        with pytest.raises(ValueError,match='CORRUPT'):E.status(db,'unseen')
