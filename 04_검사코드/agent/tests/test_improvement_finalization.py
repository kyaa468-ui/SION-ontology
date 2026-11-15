"""Correction-window closure and isolation from the original observation path."""
from datetime import timedelta
import pytest
from test_ontology import isolated, C
from test_close_evaluation import calendar, price_provider, at
import improvement_store as S


def test_finalized_ma20_is_not_dirtied_by_later_daily_receipts():
    from test_improvement import calendar as fixture_calendar
    import legacy_assessments as L
    old=(C.now()-timedelta(days=14)).date().isoformat()
    application={'application_id':'a','code':'000001','definition_id':'MA20_MAINTENANCE@v2','days':3,
        'created_at':(C.now()-timedelta(days=20)).isoformat()}
    result={'knowledge_as_of':S.now(),'period_status':'ended','verdict':'refuted','evaluation_id':'e','semantic_hash':'h'}
    final=L.record(application,result,{'end_trading_day':old},fixture_calendar())
    assert final['state']=='finalized' and final['support_ended']
    L.mark_correction('000001','late-receipt')
    assert S.get('legacy_assessment_state','a')==final
    assert not L.pending('a',S.now())
    assert L.pending('a',(C.now()-timedelta(days=1)).isoformat())


def test_improvement_callback_failure_does_not_reject_committed_daily_receipt(monkeypatch,calendar):
    import daily_observations as D
    import legacy_assessments as L
    def unavailable(*a,**k):raise OSError('fixture improvement store unavailable')
    monkeypatch.setattr(L,'mark_correction',unavailable)
    at(monkeypatch,'2026-11-11T16:00:00+09:00')
    result=D.collect_daily_observations('000001',['2026-11-10'],calendar,price_provider(calendar))
    assert result['recording_status']=='recorded'
    assert result['observations'] and result['improvement_errors']
    assert result['rows']['2026-11-10']['status']!='recording_error'


def test_active_investigation_keeps_query_cursor_and_checks_during_gc(monkeypatch):
    import investigation as Q
    import retention_gc as G
    from test_improvement import investigation_fixture,calendar as fixture_calendar
    session,case=investigation_fixture(monkeypatch)
    page=Q.query_investigation(session['id'],limit=1)
    check=Q.run_investigation_check(session['id'],'equality@v1',
        [{'ref':page['actual_returned_refs'][0],'path':['content','period']}]*2)
    G.collect(fixture_calendar(),dry_run=False)
    assert S.get('query',page['query_id']) and S.get('query_cursor',page['next_cursor'])
    assert S.get('check',check['check_id'])
    assert Q.query_investigation(session['id'],limit=1,cursor=page['next_cursor'])['returned']==1


def test_notification_registration_does_not_conflict_or_extend_progress_on_retry(monkeypatch):
    from test_improvement import configured
    import change_application as A
    configured(monkeypatch,notification_channels=[{'id':'fixture'}])
    with S.transaction() as db:A.enqueue_notification(db,'application-fixture','applied')
    first=S.page('notification')[0]
    monkeypatch.setattr(S,'now',lambda:(C.now()+timedelta(hours=1)).isoformat())
    with S.transaction() as db:A.enqueue_notification(db,'application-fixture','applied')
    assert S.page('notification')[0]==first
