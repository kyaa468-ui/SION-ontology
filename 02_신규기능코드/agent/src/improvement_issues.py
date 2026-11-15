"""Cheap issue detection and durable claims; a signal is not a proved defect."""
from __future__ import annotations
from datetime import timedelta
import improvement_store as S

RULE_VERSION = 'BASIC_CHECKS@v1'


def record(db, rule, opportunity_id, source_id, question, *, expected, actual, source_revision):
    issue_id = 'issue-' + S.digest([rule, opportunity_id])[:40]
    occurrence_id = 'occurrence-' + S.digest([issue_id, source_id, source_revision])[:40]
    if S.get('occurrence', occurrence_id, conn=db):
        return S.get('issue', issue_id, conn=db)
    S.put(db, 'occurrence', occurrence_id, {'issue_id':issue_id,'opportunity_id':opportunity_id,
          'owner':issue_id,'source_id':source_id,'source_revision':source_revision,
          'expected':expected,'actual':actual,'rule':rule,'rule_version':RULE_VERSION})
    old = S.get('issue',issue_id,conn=db)
    state=(old or {}).get('state','queued')
    if state in {'resolved','expired','cancelled'}:state='queued'
    row = S.put(db,'issue',issue_id,{**(old or {}),'question':question,'rule':rule,'rule_version':RULE_VERSION,
                'opportunity_id':opportunity_id,'owner':opportunity_id,'occurrence_count':(old or {}).get('occurrence_count',0)+1,
                'first_at':(old or {}).get('first_at',S.now()),'last_at':S.now(),
                'last_progress_at':S.now(),'state':state,
                'causal_status':'not_diagnosed','last_occurrence_id':occurrence_id})
    S.link(db,'issue',issue_id,'occurrence',occurrence_id)
    S.link(db,'issue',issue_id,'opportunity',opportunity_id)
    return row


def inspect_case(db, case):
    signals = []
    if case['coverage_status'] != 'linked':
        signals.append(('judgment_gap','대상 판단이 제출·수락·소비되지 않은 이유는 무엇인가?',
                        'accepted judgment with terminal consumption',case['coverage_status']))
    for call in case.get('execution',[]):
        for result in call.get('results',[]):
            outcome = result.get('outcome') or {}
            if outcome.get('ok') is False:
                signals.append(('execution_difference','판단과 실행의 차이는 정상 제약으로 설명되는가?',
                                {'stage':call.get('stage'),'judgment_id':call.get('judgment_id')},outcome))
    for stage in case.get('stages',[]):
        if stage.get('unreviewed_ids'):
            signals.append(('unreviewed_objection','검토되지 않은 반론이 최종 판단에 어떤 영향을 주는가?',
                            'explicit treatment of each objection',stage['unreviewed_ids']))
    for rule,question,expected,actual in signals:
        record(db,rule,case['id'],case['id'],question,expected=expected,actual=actual,source_revision=S.digest(actual))


def claim(worker, *, lease_seconds=300):
    if not isinstance(worker,str) or not worker: raise ValueError('WORKER_REQUIRED')
    with S.transaction() as db:
        stamp = S.now()
        active = db.execute("SELECT body,hash FROM records WHERE kind='worker' AND id='singleton'").fetchone()
        active = S.decode(active)
        if active and active.get('lease_until','') > stamp and active.get('worker') != worker:
            return {'state':'busy'}
        S.put(db,'worker','singleton',{'worker':worker,'lease_until':(S.instant(stamp)+timedelta(seconds=lease_seconds)).isoformat()})
        raw = db.execute("SELECT body,hash FROM records WHERE kind='issue' AND (state='queued' OR (state='claimed' AND json_extract(body,'$.lease_until')<=?)) ORDER BY origin_at,id LIMIT 1",(stamp,)).fetchone()
        if not raw: return {'state':'empty'}
        issue = S.decode(raw)
        return S.put(db,'issue',issue['id'],{**issue,'state':'claimed','worker':worker,
                     'lease_until':(S.instant(stamp)+timedelta(seconds=lease_seconds)).isoformat()})


def expire_stalled(at=None):
    cutoff = (S.instant(at or S.now())-timedelta(days=30)).isoformat()
    count=0
    with S.transaction() as db:
        rows = db.execute("SELECT body,hash FROM records WHERE kind IN ('issue','investigation','change','experiment','reassessment','notification') AND state NOT IN ('resolved','completed','applied','observing','expired','cancelled','delivered','failed') AND json_extract(body,'$.last_progress_at')<? LIMIT 200",(cutoff,)).fetchall()
        for raw in rows:
            row=S.decode(raw)
            if row.get('wait_until') and row['wait_until'] > (at or S.now()): continue
            S.put(db,row['kind'],row['id'],{**row,'resume_state':row['state'],'state':'expired','closed_at':at or S.now()})
            count+=1
    return count
