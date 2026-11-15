"""Actual journal replacement preserves safety calculations and frozen inputs."""
import copy
import gc
import json
from datetime import timedelta
from pathlib import Path
import pytest
from test_ontology import isolated,C,NOW,P
from test_improvement import calendar,configured
import journal_checkpoint as J
import journal_retention as G
import log_store as L
import tuning as T
import logger
import risk
import improvement_store as S


def row(kind,days,run,**fields):
    return {'kind':kind,'time':C.stamp(NOW+timedelta(days=days)),'run':run,**fields}


def fill(days,run,side='매수',qty=10,price=100,code='000001',**fields):
    return row('fill',days,run,side=side,qty=qty,price=price,code=code,name=code,amount=qty*price,
               confidence=.7,bull='기록한 근거',bear='기록한 반론',decision='기록한 행동',**fields)


def setup(monkeypatch):
    params=copy.deepcopy(P);params['competition']['start']=(NOW-timedelta(days=60)).date().isoformat()
    monkeypatch.setattr(C,'params',lambda:copy.deepcopy(params))
    rows=[fill(-90,'before-competition',code='000002'),
          row('regime',-50,'old',regime='하락',cap=.2),fill(-50,'old'),
          row('decision',-50,'old',code='000001',action='관망',confidence=.4,mode='decide'),
          row('block',-50,'old',code='000001',side='매수',rule='RISK_03',detail='횟수'),
          row('tune',-49,'tune-old',applied=[{'key':'screen.max_vol20'}]),
          row('review',-49,'review-old',week=1,period='fixture',suggestion='옛 회고'),
          row('regime',-1,'recent',regime='상승',cap=.8,mode='decide'),
          fill(-1,'recent',qty=10,price=200),fill(-1,'recent',side='매도',qty=5,price=180),
          row('decision',-1,'recent',code='000001',action='관망',confidence=.6,confidence_adj=.62,mode='decide'),
          row('regime',0,'current',regime='횡보',cap=.5,mode='decide'),fill(0,'current',qty=1,price=160)]
    for r in rows:C.append_jsonl(C.JOURNAL,r)
    C.write_json(C.ACCOUNT,{'holdings':[{'code':'000001','qty':16,'price':180,'avg_price':150,'pnl_pct':.18}]})
    return rows,params


def snapshot():return {str(p):p.read_bytes() for p in Path(C.WORK).rglob('*') if p.is_file()}


def test_dry_run_has_no_write_and_complete_reducers_match_after_reclamation(monkeypatch):
    rows,params=setup(monkeypatch);before=snapshot()
    global_state=J.summarize(rows);competition=J.summarize(rows,scope='competition',start=params['competition']['start'])
    account=C.read_json(C.ACCOUNT)
    recent_trades=[x for x in T.trade_results(rows,account) if x.get('exit','9999')[:10]>=(NOW-timedelta(days=2)).date().isoformat()]
    recent_comp=[x for x in T.trade_results(J.Since(rows,params['competition']['start']),account) if x.get('exit','9999')[:10]>=(NOW-timedelta(days=2)).date().isoformat()]
    dry=G.collect(calendar());assert dry['candidate_rows']==7 and before==snapshot()
    assert G.collect(calendar(),dry_run=False)['state']=='reclaimed'
    compact=C.read_jsonl(C.JOURNAL)
    assert J.summarize(compact)==global_state
    assert J.summarize(compact,scope='competition',start=params['competition']['start'])==competition
    assert C.fill_count()==5 and C.buys_today('000001')==1
    assert T.trade_results(compact,account)==recent_trades
    assert T.trade_results(J.Since(compact,params['competition']['start']),account,scope='competition')==recent_comp
    assert T.cooldown_keys(compact,{'cooldown_runs':3})==T.cooldown_keys(rows,{'cooldown_runs':3})
    assert C.latest_regime()['run']=='current'
    assert C.prior_judgments(rows=compact,run='different')==C.prior_judgments(rows=rows,run='different')
    assert G.watermark(compact)['checkpoint_generation']==1


def test_old_partial_fill_anchor_preserves_recent_dashboard_quantity(monkeypatch):
    monkeypatch.setattr(logger,'FIXES',{})
    rows=[fill(-90,'old',qty=8,ordered=10),fill(-1,'new',qty=5)]
    for r in rows:C.append_jsonl(C.JOURNAL,r)
    C.write_json(C.ACCOUNT,{'holdings':[{'code':'000001','qty':15}]})
    old=logger.trades(rows)
    assert [r['qty'] for r in old]==[10,5]
    G.collect(calendar(),dry_run=False)
    current=logger.trades(C.read_jsonl(C.JOURNAL))
    assert current[0]['id']==2 and current[0]['qty']==5
    assert current[0]['amount']==old[-1]['amount']


def test_repeated_compaction_and_expired_run_replay_cannot_reset_counts(monkeypatch):
    setup(monkeypatch);G.collect(calendar(),dry_run=False)
    with pytest.raises(ValueError,match='EXPIRED_OR_CHECKPOINT_COLLISION'):
        list(C.journal_run_rows(C.read_jsonl(C.JOURNAL),'old'))
    with pytest.raises(ValueError,match='EXPIRED_OR_CHECKPOINT_COLLISION'):
        C.append_jsonl(C.JOURNAL,fill(0,'old'))
    with pytest.raises(ValueError,match='APPEND_SUPPORT_EXPIRED'):
        C.append_jsonl(C.JOURNAL,fill(-90,'fresh-but-backdated'))
    monkeypatch.setattr(C,'now',lambda:NOW+timedelta(days=90))
    result=G.collect(calendar(),dry_run=False)
    assert result['generation']==2 and C.fill_count()==5
    assert G.collect(calendar(),dry_run=False)['state']=='unchanged'
    assert not list(Path(C.WORK).glob('.journal-generation-*'))


@pytest.mark.parametrize('phase',['prepared','replaced'])
def test_crash_recovery_preserves_counts_and_refuses_reads_or_appends_mid_transition(monkeypatch,phase):
    setup(monkeypatch)
    def crash(where):
        if where==phase:raise RuntimeError('injected stop')
    with pytest.raises(RuntimeError,match='injected'):G.collect(calendar(),dry_run=False,fault=crash)
    with pytest.raises(ValueError,match='RECOVERY_REQUIRED'):C.read_jsonl(C.JOURNAL)
    with pytest.raises(ValueError,match='RECOVERY_REQUIRED'):C.append_jsonl(C.JOURNAL,fill(0,'fresh'))
    assert G.collect(calendar())['state']=='recovery_pending'
    assert G.collect(calendar(),dry_run=False)['state']=='recovered'
    assert C.fill_count()==5 and G.validate()['generation']==1


def test_live_reader_defers_collection_and_deleted_identity_cannot_recreate(monkeypatch):
    setup(monkeypatch);reader=L.LogView(C.JOURNAL)
    assert G.collect(calendar(),dry_run=False)['state']=='deferred'
    assert next(iter(reader))['kind']=='fill'
    del reader;gc.collect()
    assert G.collect(calendar(),dry_run=False)['state']=='reclaimed'
    Path(C.JOURNAL).unlink()
    with pytest.raises(ValueError,match='IDENTITY_MARKER_MISMATCH'):C.append_jsonl(C.JOURNAL,fill(0,'new'))
    assert not Path(C.JOURNAL).exists()


def test_active_registered_range_keeps_original_detail(monkeypatch):
    setup(monkeypatch)
    import improvement_store as S
    start=(NOW-timedelta(days=100)).isoformat()
    with S.transaction() as db:
        S.put(db,'retention_pin','validation',{'state':'active','until':(NOW+timedelta(days=5)).isoformat(),
            'period':{'start':start,'end':NOW.isoformat()}})
    assert G.collect(calendar(),dry_run=False)['candidate_rows']==0
    assert C.journal_checkpoint(C.read_jsonl(C.JOURNAL)) is None


def test_checkpoint_is_valid_frozen_engine_input_without_live_journal(monkeypatch):
    setup(monkeypatch);G.collect(calendar(),dry_run=False)
    frozen=list(C.read_jsonl(C.JOURNAL))
    import audit_inputs,replay_context as R
    monkeypatch.setenv('RUN_ID','current');monkeypatch.setenv('RUN_MODE','decide')
    runtime=audit_inputs.runtime_context()
    engine=R.Engine(runtime,[],strict=False,snapshots={'journal.jsonl':frozen})
    Path(C.JOURNAL).unlink()
    assert engine.common.fill_count()==5 and engine.risk.orders_this_run()==1
    assert engine.common.buys_today('000001')==1


def test_historical_dates_and_changed_old_competition_scope_are_explicitly_unavailable(monkeypatch):
    setup(monkeypatch);G.collect(calendar(),dry_run=False);rows=C.read_jsonl(C.JOURNAL)
    with pytest.raises(ValueError,match='DETAIL_EXPIRED'):C.journal_require_date(rows,(NOW-timedelta(days=90)).date().isoformat())
    with pytest.raises(ValueError,match='COMPETITION_SCOPE_EXPIRED'):
        J.state(rows,'competition',(NOW-timedelta(days=100)).date().isoformat())
    assert J.state(rows,'competition',NOW.date().isoformat())['fills']==0


def test_bounded_tuning_context_keeps_competition_totals(monkeypatch):
    rows,params=setup(monkeypatch)
    configured(monkeypatch,query_limits={'rows':2,'bytes':2*1024*1024,'queries':40,'checks':20,'depth':3})
    monkeypatch.setattr(T,'rules',lambda:{'cooldown_runs':2,'weekdays':list(range(7)),'max_changes_per_run':2,'keys':{}})
    monkeypatch.setattr(T,'outcomes',lambda *a,**k:{'scope':'fixture'})
    before=T.context();G.collect(calendar(),dry_run=False);after=T.context()
    for field in ('competition','decisions','blocks_by_rule','decision_confidence','cooldown_keys','trade_summary'):
        assert before[field]==after[field],field
    assert after['competition']['fills']==4 and len(after['trades'])<=2
    assert after['detail_scope']['full_history'] is False


def test_append_during_preparation_defers_without_losing_new_row(monkeypatch):
    setup(monkeypatch);original=G._file_hash;added=False
    def interleaved(path):
        nonlocal added
        if Path(path).name.startswith('.journal-generation-') and not added:
            added=True;C.append_jsonl(C.JOURNAL,fill(0,'arrived-during-prepare'))
        return original(path)
    monkeypatch.setattr(G,'_file_hash',interleaved)
    result=G.collect(calendar(),dry_run=False)
    assert result['state']=='deferred' and result['reason']=='JOURNAL_CHANGED_DURING_PREPARATION'
    assert C.fill_count()==6 and C.journal_checkpoint(C.read_jsonl(C.JOURNAL)) is None
    assert not list(Path(C.WORK).glob('.journal-generation-*'))


def test_actual_process_exit_releases_staging_owner_and_recovery_keeps_original(monkeypatch):
    import subprocess,sys,textwrap
    rows,params=setup(monkeypatch)
    config=Path(C.WORK)/'fixture-params.json';config.write_text(json.dumps(params))
    script=textwrap.dedent('''
        import sys,json,os
        from pathlib import Path
        from datetime import datetime,timedelta
        sys.path.insert(0,sys.argv[1])
        import common as C
        C.WORK=sys.argv[2];C.JOURNAL=str(Path(C.WORK)/'journal.jsonl')
        stamp=datetime.fromisoformat(sys.argv[3]);C.now=lambda:stamp
        C.params=lambda:json.loads((Path(C.WORK)/'fixture-params.json').read_text())
        import journal_retention as G
        days=[stamp-timedelta(days=n) for n in reversed(range(60))]
        calendar={'status':'complete','calendar_id':'synthetic-process-crash','sessions':[
            {'date':d.date().isoformat(),'close_at':d.replace(hour=15,minute=30).isoformat()} for d in days if d.weekday()<5]}
        G._file_hash=lambda path:os._exit(71)
        G.collect(calendar,dry_run=False)
    ''')
    original=Path(C.JOURNAL).read_bytes()
    child=subprocess.run([sys.executable,'-c',script,str(Path(C.AGENT)/'src'),C.WORK,NOW.isoformat()],capture_output=True,text=True,timeout=20)
    assert child.returncode==71,child.stderr
    assert Path(C.JOURNAL).read_bytes()==original
    assert G.collect(calendar())['state']=='preparation_recovery_pending'
    assert G.collect(calendar(),dry_run=False)['state']=='preparation_recovered'
    assert not list(Path(C.WORK).glob('.journal-generation-*'))
    assert G.collect(calendar(),dry_run=False)['state']=='reclaimed' and C.fill_count()==5


def test_required_writer_waits_for_short_swap_instead_of_dropping_journal(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    import threading
    import storage_leases
    setup(monkeypatch);started=threading.Event()
    def write():
        started.set();C.append_jsonl(C.JOURNAL,fill(0,'after-swap'))
    with ThreadPoolExecutor(max_workers=1) as pool:
        with storage_leases.exclusive(C.WORK):
            future=pool.submit(write);assert started.wait(1)
            assert not future.done()
        future.result(timeout=2)
    assert C.fill_count()==6


def test_names_for_retained_blocks_survive_and_corrupt_checkpoint_is_not_zero(monkeypatch):
    monkeypatch.setattr(C,'load_briefing',lambda **kw:{})
    C.write_json(C.ACCOUNT,{'holdings':[]})
    C.append_jsonl(C.JOURNAL,fill(-90,'old',name_unused='ignored'))
    # A name appeared only before the boundary; the retained risk event still uses it.
    C.append_jsonl(C.JOURNAL,row('decision',-80,'named',code='000001',name='보존할 이름',action='보유',confidence=.5))
    C.append_jsonl(C.JOURNAL,row('block',0,'current',code='000001',side='매수',rule='RISK_03',detail='횟수'))
    before=logger.names();G.collect(calendar(),dry_run=False)
    assert logger.names()==before
    values=Path(C.JOURNAL).read_text().splitlines();first=json.loads(values[0])
    first['checkpoint']['all']['fills']=0
    Path(C.JOURNAL).write_text(json.dumps(first)+'\n'+'\n'.join(values[1:])+'\n')
    with pytest.raises(ValueError,match='CHECKPOINT_CORRUPT'):C.fill_count()


def test_changed_old_fill_fix_is_unavailable_and_public_expiry_is_explicit(monkeypatch):
    setup(monkeypatch);G.collect(calendar(),dry_run=False);rows=C.read_jsonl(C.JOURNAL)
    scope={'supported_after':C.journal_checkpoint(rows)['supported_after']}
    public=logger.public_payload('status',{'journal_detail_scope':scope})
    assert public['journal_detail_scope']['older_detail']=='expired'
    assert '보관 종료' in public['note']
    monkeypatch.setattr(logger,'FIXES',{'old-reference':100})
    with pytest.raises(ValueError,match='DEFINITION_CHANGED'):logger.trades(rows)
    before=Path(C.JOURNAL).read_bytes()
    with pytest.raises(ValueError,match='DEFINITION_CHANGED'):G.collect(calendar(),dry_run=False)
    assert Path(C.JOURNAL).read_bytes()==before


def test_recovery_without_calendar(monkeypatch):
    setup(monkeypatch)
    def crash(where):
        if where == 'prepared':
            raise RuntimeError('stop')
    with pytest.raises(RuntimeError):
        G.collect(calendar(), dry_run=False, fault=crash)
    assert G.collect({'status':'unavailable'}, dry_run=False)['state'] == 'recovered'
    assert C.fill_count() == 5


def test_large_checkpoint_reverse(monkeypatch):
    monkeypatch.setattr(C, 'params', lambda: {**P, 'competition': {**P['competition'], 'start': '2020-01-01'}})
    C.append_jsonl(C.JOURNAL, row('review', -50, 'large', suggestion='x' * 600000))
    G.collect(calendar(), dry_run=False)
    rows = C.read_jsonl(C.JOURNAL)
    assert list(reversed(rows)) == list(rows)[::-1]


@pytest.mark.parametrize('protection', ['range', 'holdout_publication'])
def test_new_range_during_preparation(monkeypatch, protection):
    setup(monkeypatch)
    before = Path(C.JOURNAL).read_bytes()
    original = G._file_hash
    def changed(path):
        value = original(path)
        if Path(path).name.startswith('.journal-generation-'):
            with S.transaction() as db:
                if protection == 'holdout_publication':
                    S.put(db, 'holdout_sync', 'pending', {'state':'pending'})
                    return value
                S.put(db, 'retention_pin', 'late-range', {'state':'active',
                    'until': (NOW + timedelta(days=5)).isoformat(),
                    'period': {'start': (NOW - timedelta(days=100)).isoformat(), 'end': NOW.isoformat()}})
        return value
    monkeypatch.setattr(G, '_file_hash', changed)
    assert G.collect(calendar(), dry_run=False)['state'] == 'deferred'
    assert Path(C.JOURNAL).read_bytes() == before
