"""MA20 adapter: original verdict semantics, finite correction lifecycle."""
import improvement_store as S


def pending(application_id, knowledge_as_of):
    item=S.get('legacy_assessment_state',application_id)
    if not item: return True
    if S.instant(knowledge_as_of)<S.instant(item['known_at']): return True
    return item['state'] not in {'finalized','finalized_unknown'} or item.get('dirty',False)


def record(application, result, window, calendar):
    identifier=application['application_id']
    cutoff=S.instant(result['knowledge_as_of'])
    state='evaluating'
    if result.get('period_status')=='ended':
        state='correction_pending'
        if calendar and calendar.get('status')=='complete' and window.get('end_trading_day'):
            closes=sorted(S.instant(x['close_at']) for x in calendar['sessions'] if x['date']>window['end_trading_day'])
            if len(closes)>=5 and cutoff>=closes[4]:
                state='finalized' if result['verdict'] in {'maintained','refuted'} else 'finalized_unknown'
    with S.transaction() as db:
        import scoring_targets
        old=S.get('legacy_assessment_state',identifier,conn=db)
        if old and S.instant(old['known_at'])>cutoff:
            scoring_targets.lifecycle(db,identifier,old)
            return old
        body={'owner':application['code'],'code':application['code'],'application_id':identifier,
              'definition_id':application['definition_id'],'days':application['days'],'state':state,
              'verdict':result['verdict'],'known_at':result['knowledge_as_of'],'dirty':False,
              'evaluation_id':result.get('evaluation_id'),'semantic_hash':result.get('semantic_hash'),
              'window':window,'calendar_id':(calendar or {}).get('calendar_id'),
              'origin_at':application.get('created_at',S.now())}
        if state in {'finalized','finalized_unknown'}:
            body.update(closed_at=(old or {}).get('closed_at',S.now()),support_ended=True,correction_end_at=closes[4].isoformat())
        value=old if old and all(old.get(k)==v for k,v in body.items()) else S.put(db,'legacy_assessment_state',identifier,body)
        scoring_targets.lifecycle(db,identifier,value)
        return value


def mark_correction(code, receipt_id):
    with S.transaction() as db:
        import scoring_targets
        after=''
        while True:
            rows=db.execute("SELECT body,hash,id FROM records WHERE kind='legacy_assessment_state' AND owner=? AND state NOT IN ('finalized','finalized_unknown') AND id>? ORDER BY id LIMIT 200",(code,after)).fetchall()
            for raw in rows:
                item=S.decode(raw)
                if not item.get('support_ended'):
                    value=S.put(db,'legacy_assessment_state',item['id'],{**item,'dirty':True,'correction_receipt_id':receipt_id})
                    scoring_targets.lifecycle(db,item['id'],value)
            if len(rows)<200:break
            after=rows[-1]['id']
