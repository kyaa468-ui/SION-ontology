"""Expired usage cannot become independent after alias/source deletion."""
import copy
from datetime import timedelta
from pathlib import Path

import pytest
from test_ontology import isolated, C, NOW
from test_improvement import calendar
import ai_judgment as J
import case_store as CS
import improvement_store as S
import research_usage_ledger as U
import usage_checkpoint as K


def setup(monkeypatch, names=('a',), days=-80):
    applications = {key: {'application_id': key, 'code': '000001',
        'created_at': (NOW + timedelta(days=days)).isoformat(),
        'definition_id': 'MA20_MAINTENANCE@v2', 'days': 3, 'direction': 'above'} for key in names}
    monkeypatch.setattr(J, '_load', lambda kind, key: copy.deepcopy(applications.get(key)))
    for key in names:
        CS.archive('usage_events', 'usage-'+key, {'version':'RESEARCH_USAGE@v1', 'kind':'used',
            'usage_id':'usage-'+key, 'owner_id':'owner-'+key, 'purpose':'fixture',
            'created_at': (NOW + timedelta(days=days+1)).isoformat(), 'application_ids':[key]})
    U.migrate()
    return applications


def test_dry_report_is_readonly_and_raw_masked_aliases_stay_used(monkeypatch, tmp_path):
    applications=setup(monkeypatch)
    before={str(p):p.read_bytes() for p in Path(C.WORK).rglob('*') if p.is_file()}
    assert K.collect(calendar())['retired_aliases']==1
    assert before=={str(p):p.read_bytes() for p in Path(C.WORK).rglob('*') if p.is_file()}
    result=K.collect(calendar(),dry_run=False)
    assert result['retired_aliases']==result['retired_sources']==1
    assert S.get('usage_alias','a') is None
    assert S.get('research_exposure',U.origin(applications['a'])) is None
    CS.root().rename(tmp_path/'masked-raw-usage')
    applications['new-id']={**applications['a'],'application_id':'new-id'}
    for key in ('a','new-id'):
        item=U.status([key])['items'][0]
        assert item['status']=='used' and item['detail_scope']=='expired'
        assert item['reason']=='prior_exposure_or_checkpoint_collision'
        assert not item['independent_unused']
    applications.clear()
    assert U.status(['a'])['items'][0]['status']=='used'
    assert U.status(['unknown'])['items'][0]['status']=='unknown'


def test_scope_query_never_calls_expired_history_unexposed(monkeypatch):
    setup(monkeypatch);K.collect(calendar(),dry_run=False)
    result=U.exposed_in_scope({'codes':['000001']},{'start':(NOW-timedelta(days=90)).isoformat(),'end':NOW.isoformat()})
    assert any(e['reason']=='usage_detail_scope_expired' for e in result['errors'])
    recent=U.exposed_in_scope({'codes':['000001']},{'start':(NOW-timedelta(days=1)).isoformat(),'end':NOW.isoformat()})
    assert recent['errors']==[]


def test_retired_alias_status_needs_no_original_reader(monkeypatch):
    setup(monkeypatch);K.collect(calendar(),dry_run=False)
    monkeypatch.setattr(J,'_load',lambda *args:pytest.fail('Retired original premise read'))
    assert U.status(['a'])['items'][0]['status']=='used'


@pytest.mark.parametrize('protection',['range','active_owner','required_link','recent_use','unresolved_origin'])
def test_active_or_unknown_usage_state_is_protected(monkeypatch,protection):
    applications=setup(monkeypatch)
    with S.transaction() as db:
        if protection=='range':
            S.put(db,'retention_pin','range',{'state':'active','until':(NOW+timedelta(days=3)).isoformat(),
                'period':{'start':(NOW-timedelta(days=90)).isoformat(),'end':NOW.isoformat()}})
        elif protection=='active_owner':
            S.put(db,'investigation','owner-a',{'state':'running'})
        elif protection=='required_link':
            S.put(db,'application','policy',{'state':'applied'})
            S.link(db,'application','policy','usage_alias','a')
        elif protection=='unresolved_origin':
            value=S.get('usage_alias','a',conn=db);value['application_origin_at']=None
            S.put(db,'usage_alias','a',value)
    if protection=='recent_use':
        U.record_source('usage_events','recent',{'version':'RESEARCH_USAGE@v1','kind':'reserved',
            'created_at':S.now(),'application_ids':['a'],'owner_id':'new-job'})
    result=K.collect(calendar(),dry_run=False)
    assert result['retired_aliases']==result['retired_sources']==0
    assert S.get('usage_alias','a') and S.get('research_exposure',U.origin(applications['a']))


def test_batch_shared_source_and_replays_do_not_regrow_aliases(monkeypatch):
    apps=setup(monkeypatch,('a','b'))
    first=K.collect(calendar(),dry_run=False,limit=1)
    assert first['retired_aliases']==1 and first['retired_sources']==0
    second=K.collect(calendar(),dry_run=False,limit=1)
    assert second['retired_aliases']==second['retired_sources']==1
    for index in range(20):
        key='import-'+str(index);apps[key]={**apps['a'],'application_id':key}
        U.record_source('usage_events',key,{'version':'RESEARCH_USAGE@v1','kind':'used',
            'created_at':S.now(),'application_ids':[key]})
    with S.transaction(readonly=True) as db:
        assert db.execute("SELECT count(*) FROM records WHERE kind IN ('usage_alias','research_exposure')").fetchone()[0]==0
        state=K.load(db)
        assert len(state['bits'])==K.BYTES and state['retired_aliases']==2
    apps.clear()
    assert all(x['status']=='used' for x in U.status(['a','b','import-19'])['items'])


def test_new_identity_cannot_reuse_retired_id_as_fresh_data(monkeypatch):
    apps=setup(monkeypatch);K.collect(calendar(),dry_run=False)
    apps['a']['created_at']=S.now()
    with pytest.raises(ValueError,match='RETIRED_OR_CHECKPOINT_COLLISION'):
        U.record_source('usage_events','new',{'version':'RESEARCH_USAGE@v1','kind':'used','application_ids':['a']})
    assert S.get('usage_alias','a') is None


def test_missing_or_corrupt_checkpoint_is_never_unknown(monkeypatch):
    setup(monkeypatch);K.collect(calendar(),dry_run=False)
    with S.transaction() as db:
        db.execute('UPDATE usage_retirement SET bits=?',(bytes(K.BYTES),))
    with pytest.raises(ValueError,match='CHECKPOINT_CORRUPT'):U.status(['a'])
    with S.transaction() as db:db.execute('DROP TABLE usage_retirement')
    with pytest.raises(ValueError,match='IDENTITY_MISMATCH'):U.status(['a'])


def test_full_filter_disqualifies_without_claiming_exact_history(monkeypatch):
    setup(monkeypatch);K.collect(calendar(),dry_run=False)
    with S.transaction() as db:
        checkpoint=K.load(db);checkpoint['bits']=bytes([255])*K.BYTES;K._save(db,checkpoint)
    result=U.status(['unrelated'])['items'][0]
    assert result['reason']=='prior_exposure_or_checkpoint_collision'
    assert not result['independent_unused'] and not result['references']


def test_publication_pending_or_late_pin_blocks_retirement(monkeypatch):
    setup(monkeypatch)
    with S.transaction() as db:S.put(db,'holdout_sync','pending',{'state':'pending'})
    with pytest.raises(ValueError,match='PUBLICATION_PENDING'):K.collect(calendar(),dry_run=False)
    assert S.get('usage_alias','a')
    with S.transaction() as db:db.execute("DELETE FROM records WHERE kind='holdout_sync'")
    K.collect(calendar(),dry_run=False)
    with S.transaction() as db:
        S.put(db,'retention_pin','late',{'state':'active','until':(NOW+timedelta(days=1)).isoformat(),
            'period':{'start':(NOW-timedelta(days=90)).isoformat(),'end':NOW.isoformat()}})
    with pytest.raises(ValueError,match='BOUNDARY_CANNOT_REVERSE'):K.collect(calendar(),dry_run=False)


def test_failed_checkpoint_publication_rolls_back_alias_deletion(monkeypatch):
    setup(monkeypatch)
    def fail(*args):raise OSError('disk failure')
    monkeypatch.setattr(K,'_save',fail)
    with pytest.raises(OSError):K.collect(calendar(),dry_run=False)
    assert S.get('usage_alias','a') and U.status(['a'])['items'][0]['detail_scope']=='available'


def test_main_collector_includes_usage_projection_retirement(monkeypatch):
    setup(monkeypatch)
    import retention_gc as G
    result=G.collect(calendar(),dry_run=False)
    assert result['research_usage']['retired_aliases']==1
    assert U.status(['a'])['items'][0]['status']=='used'


def test_kst_retention_boundary_keeps_early_morning_origins(monkeypatch):
    import retention_policy
    cutoff=retention_policy.boundaries(calendar())['detailed_origin_cutoff']
    applications=setup(monkeypatch, names=())
    applications['morning']={'application_id':'morning','code':'000001',
        'created_at':cutoff+'T00:01:00+09:00','definition_id':'MA20_MAINTENANCE@v2','days':3,'direction':'above'}
    U.record_source('usage_events','morning',{'version':'RESEARCH_USAGE@v1','kind':'used',
        'created_at':cutoff+'T00:02:00+09:00','application_ids':['morning']})
    result=K.collect(calendar(),dry_run=False)
    assert result['retired_aliases']==0
    assert U.status(['morning'])['items'][0]['detail_scope']=='available'
