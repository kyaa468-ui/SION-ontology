"""Archive-independent exposure, including a new ID for the same premise."""
import copy
from pathlib import Path
import pytest
from test_ontology import isolated,C
import case_store as CS
import research_evidence as RE
import research_usage_ledger as L
import improvement_store as S


def non_usage_state():
    """Every DB table except the two permitted monotone exposure record kinds."""
    result={name:[] for name in ('records','revisions','links','calls','exposure','meta')}
    with S.transaction(readonly=True) as db:
        if db:
            for table in result:
                where=" WHERE kind NOT IN ('research_exposure','usage_alias')" if table in {'records','revisions'} else ''
                result[table]=sorted(tuple(row) for row in db.execute('SELECT * FROM '+table+where))
    return result


def test_usage_reader_survives_archive_masking_after_verified_migration(monkeypatch,tmp_path):
    RE.reserve_usage('owner',['p1'],purpose='fixture')
    CS.archive('datasets','legacy',{'review_id':'legacy','cases':[{'application_id':'p2'}]})
    proof=L.migrate()
    assert proof['state']=='verified' and proof['applications']==2
    CS.root().rename(tmp_path/'masked-case-review')
    monkeypatch.setattr(CS,'records',lambda *a,**k:pytest.fail('Historical archive scan'))
    monkeypatch.setattr(CS,'iter_records',lambda *a,**k:pytest.fail('Historical archive scan'))
    result=RE.usage_status(['p1','p2','missing'])
    assert [r['status'] for r in result['items']]==['unknown','used','used']
    assert result['recording_status']=='recorded' and not any(r['independent_unused'] for r in result['items'])


def test_new_application_id_cannot_reset_exposure(monkeypatch):
    import ai_judgment as J
    base={'code':'000001','created_at':S.now(),'definition_id':'MA20_MAINTENANCE@v2','days':3,'direction':'above'}
    applications={key:{**base,'application_id':key,'judgment_id':'new-'+key} for key in ('old-id','new-id')}
    monkeypatch.setattr(J,'_load',lambda kind,key:copy.deepcopy(applications.get(key)))
    RE.reserve_usage('owner',['old-id'],purpose='fixture')
    L.migrate()
    result=RE.usage_status(['new-id'])['items'][0]
    assert result['status']=='used' and result['source_identity']==L.origin(base)


def test_invalid_usage_schema_prevents_reader_activation():
    CS.archive('usage_events','bad',{'application_ids':['x'],'version':'unknown','kind':'fresh'})
    with pytest.raises(ValueError):L.migrate()
    assert not L.enabled()


def test_new_use_after_migration_is_immediately_visible():
    L.migrate()
    RE.reserve_usage('later',['p1'],purpose='fixture')
    result=RE.usage_status(['p1'])
    assert result['items'][0]['status']=='used'
    assert result['source']=='independent_usage_ledger'


def test_exposure_reservation_inside_investigation_transaction_and_retry(monkeypatch):
    from test_improvement import investigation_fixture
    import investigation as Q
    session,_=investigation_fixture(monkeypatch)
    L.migrate()
    assert Q.get_investigation_context(session['id'])['cases']
    assert Q.get_investigation_context(session['id'])['cases']


def test_readonly_store_scope_cannot_be_upgraded():
    with S.transaction(readonly=True):
        with pytest.raises(ValueError,match='READONLY_TRANSACTION_SCOPE'):
            with S.transaction():pass
