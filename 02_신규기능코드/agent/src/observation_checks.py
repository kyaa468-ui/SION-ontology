"""Registered post-use behavior checks, distinct from version consumption."""
import improvement_store as S


def observe(application_id, *, limit=100):
    import change_observation as O
    import semantic_layer as SL
    import ai_judgment as J
    import opportunity_cases as P
    from semantic_validation import check
    job=S.get('observation_job',application_id)
    if not job:raise ValueError('OBSERVATION_NOT_FOUND')
    criteria=job['criteria']
    rules=criteria.get('behavior_checks')
    if not rules or 'recurrence_rules' not in criteria or 'side_effect_rules' not in criteria:
        return {'state':'observation_pending','reason':'registered_behavior_checks_required'}
    count=0
    with S.transaction(readonly=True) as db:
        pending=[] if db is None else [S.decode(r) for r in db.execute("SELECT c.body,c.hash FROM records c WHERE c.kind='change_consumption' AND c.owner=? AND NOT EXISTS (SELECT 1 FROM records e WHERE e.kind='observation_event' AND e.owner=? AND json_extract(e.body,'$.consumption_id')=c.id) ORDER BY c.id LIMIT ?",(application_id,application_id,limit))]
    for use in pending:
        event_id='behavior-'+S.digest([application_id,use['id']])[:40]
        if S.get('observation_event',event_id):continue
        judgment=J.load_judgment(use['judgment_id'])
        if not judgment:continue
        case=S.get('opportunity',P.identifier(use['attempt_id'],judgment['code'],judgment['confidence_for']))
        if not case:continue
        actual={'judgment':judgment,'input':SL._read('inputs',use['evidence_ref'],use['attempt_id']),
                'execution':case.get('execution'),'case':case}
        results=[];complete=True
        for rule in rules:
            try:results.append(check(actual,rule))
            except (ValueError,TypeError,KeyError,IndexError):complete=False
        if not complete and case['state']!='completed':continue
        issues=S.page('issue',owner=case['id'],limit=200)
        signals={i['rule'] for i in issues}
        recurrence=bool(signals & set(criteria['recurrence_rules']))
        side_effect=bool(signals & set(criteria['side_effect_rules']))
        O.record(application_id,event_id,consumption_id=use['id'],expected_behavior=bool(results) and all(results),
                 recurrence=recurrence,side_effect=side_effect,complete=complete,
                 evidence_refs=[{'kind':'opportunity','id':case['id'],'hash':S.digest(case)},
                                {'kind':'judgment','id':judgment['judgment_id'],'hash':S.digest(judgment)},
                                {'kind':'semantic_input','id':use['evidence_ref'],'hash':S.digest(actual['input'])}])
        count+=1
    return {'state':'observed','new_events':count}


def advance(*, limit=100):
    import change_observation as O
    import change_application as A
    results=[]
    with S.transaction(readonly=True) as db:
        jobs=[] if db is None else [S.decode(r) for r in db.execute("SELECT body,hash FROM records WHERE kind='observation_job' AND state IN ('applied','actual_use_confirmed','observing','deteriorated') ORDER BY updated_at,id LIMIT ?",(limit,))]
    for job in jobs:
        try:
            observation=observe(job['id'])
            outcome=O.evaluate(job['id'])
            if outcome['state']=='deteriorated':
                try:rollback=A.rollback(job['id'],operator='improvement-worker',reason='predeclared_observation_deterioration')
                except ValueError as exc:rollback={'state':'rollback_pending','reason':str(exc)}
            else:rollback=None
            if outcome['state']!=job['state']:
                with S.transaction() as db:A.enqueue_notification(db,job['id'],outcome['state'])
            results.append({'application_id':job['id'],'state':outcome['state'],'observation':observation,'rollback':rollback})
        except (ValueError,KeyError,OSError) as exc:
            results.append({'application_id':job['id'],'state':'observation_pending','reason':str(exc)})
    return results
