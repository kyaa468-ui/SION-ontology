"""Producer-built MA20 comparison contexts under opportunity parents.

The historical reader changes only after an explicit parity-checked migration.
Normal reads then select indexed origins and frozen revisions before decoding.
"""
from __future__ import annotations
import copy
import improvement_store as S


def compact(context):
    context=copy.deepcopy(context)
    judgment=context.get('judgment_snapshot') or {}
    context['judgment_snapshot']={k:judgment[k] for k in ('judgment_id','run_attempt_id','created_at','code','confidence_for','confidence_raw','recommended_action','semantic') if k in judgment}
    for key in ('selection_definition_snapshot','selection_observation_snapshot'):
        if key in context:context[key+'_hash']=S.digest(context.pop(key))
    execution=context.get('execution_context')
    if execution:
        execution=copy.deepcopy(execution)
        for key in ('engine_definitions','rule_context','unlinked_references','unlinked_non_order_references'):
            if key in execution:execution[key+'_hash']=S.digest(execution.pop(key))
        for order in execution.get('orders',[]):
            for key in ('original_calls','link_events','events','execution_observations'):
                if key in order:
                    values=order.pop(key);order[key+'_hash']=S.digest(values);order[key+'_count']=len(values)
        execution['detail_availability']='summary_only'
        context['execution_context']=execution
    return context


def capture(judgment):
    import case_dataset as D
    with S.transaction() as db:
        for app in judgment.get('premise_applications',[]):
            old=S.get('premise_case',app['application_id'],conn=db)
            if old:
                context=copy.deepcopy(old['context'])
                if judgment['judgment_id'] not in context['judgment_ids']:
                    context['judgment_ids'].append(judgment['judgment_id'])
                    S.put(db,'premise_case',app['application_id'],{**old,'context':context})
                continue
            if app['judgment_id']!=judgment['judgment_id']: continue
            context=D.extract_case_context(judgment,app)
            context.update(judgment_ids=[judgment['judgment_id']],evaluation_history=[],scoring_history=[],
                           execution_context={'recording_status':'not_recorded'},reference_returns=[],
                           source_recording_status='recorded')
            S.put(db,'premise_case',app['application_id'],{'owner':judgment['run_attempt_id'],
                  'origin_at':judgment['created_at'],'context':compact(context),'state':'open'})


def evaluated(application_id,result):
    with S.transaction() as db:
        current=S.get('premise_case',application_id,conn=db)
        if current is None: return {'state':'migration_required'}
        context=copy.deepcopy(current['context'])
        history=context.setdefault('evaluation_history',[])
        if any(r['evaluation_id']==result['evaluation_id'] for r in history): return current
        history.append(copy.deepcopy(result))
        if result.get('scoring_job_id'):
            context.setdefault('scoring_history',[]).append({'scoring_job_id':result['scoring_job_id'],
                'market_as_of':result['market_as_of'],'available_at':result['created_at'],
                'recording_status':'recording_error','reason':'unfinished_scoring_job'})
        return S.put(db,'premise_case',application_id,{**current,'context':context})


def scoring_finished(report):
    import evaluation_context as E
    import scoring_store as SS
    import case_dataset as D
    legacy=SS.load('legacy_contexts',report['scoring_job_id'])
    changed=0
    for item in report['items']:
        identifier=item.get('application_id');stored=S.get('premise_case',identifier)
        if not stored:continue
        context=copy.deepcopy(stored['context'])
        context['scoring_history']=[x for x in context.get('scoring_history',[]) if x['scoring_job_id']!=report['scoring_job_id']]
        context['scoring_history'].append({'scoring_job_id':report['scoring_job_id'],'market_as_of':report['market_as_of'],
            'available_at':report['finished_at'],'recording_status':item.get('recording_status','recording_error'),
            'reason':None if item.get('recording_status')=='recorded' else 'scoring_recording_error'})
        context['execution_context']=E.execution_context(context['judgment_ids'],report['finished_at'])
        if legacy:
            context['reference_returns']=D._reference_returns([legacy],context['execution_context'],set(context['judgment_ids']),S.instant(report['finished_at']),report['market_as_of'])
        lifecycle=S.get('legacy_assessment_state',identifier)
        closed=bool(lifecycle and lifecycle['state'] in {'finalized','finalized_unknown'})
        with S.transaction() as db:
            S.put(db,'premise_case',identifier,{**stored,'context':compact(context),'state':'completed' if closed else 'open',
                **({'closed_at':lifecycle['closed_at']} if closed else {})})
        changed+=1
    return {'updated':changed}


def enabled():
    with S.transaction(readonly=True) as db:
        return bool(db and db.execute("SELECT 1 FROM meta WHERE key='independent_cases_ready'").fetchone())


def build(market_as_of,knowledge_as_of,*,limit=200,enforce_window=True):
    import case_dataset as D
    import validation_holdout as VH
    cases=[];withheld=[];errors=[]
    boundary=None
    if enforce_window:
        import retention_store
        boundary=retention_store.current_boundary(knowledge_as_of)
        if not boundary.get('deletion_allowed'):
            return {'cases':[],'errors':[{'reason':'case_calendar_unavailable'}],'expected_count':None,'target_set_incomplete':True}
    with S.transaction(readonly=True) as db:
        if db is None: return {'cases':[],'errors':[],'expected_count':0,'target_set_incomplete':False}
        selector="kind='premise_case' AND origin_at<=?";args=[knowledge_as_of]
        if boundary:selector+=' AND (closed_at IS NULL OR substr(closed_at,1,10)>=?)';args.append(boundary['case_closed_cutoff'])
        total=db.execute('SELECT count(*) FROM records WHERE '+selector,args).fetchone()[0]
        raw=db.execute('SELECT id FROM records WHERE '+selector+' ORDER BY origin_at,id LIMIT ?',[*args,limit])
        for row in raw:
            snapshot=db.execute("SELECT body,hash FROM revisions WHERE kind='premise_case' AND id=? AND known_at<=? ORDER BY revision DESC LIMIT 1",(row['id'],knowledge_as_of)).fetchone()
            if not snapshot:
                imported=db.execute("SELECT body,hash FROM revisions WHERE kind='premise_case' AND id=? AND json_extract(body,'$.migration_source_as_of')<=? ORDER BY revision LIMIT 1",(row['id'],knowledge_as_of)).fetchone()
                snapshot=imported
            if not snapshot:errors.append({'reason':'case_revision_unavailable_as_of','application_id':row['id']});continue
            stored=S.decode(snapshot);context=stored['context']
            excluded=VH.discovery_exclusions([context.get('application_snapshot',context)])
            if excluded: withheld+=excluded;continue
            cases.append(D.project_case(context,market_as_of,knowledge_as_of))
    if total>limit:errors.append({'reason':'case_count_limit','excluded_count':total-limit})
    return {'dataset_version':'INDEPENDENT_CASES@v1','cases':cases,'expected_count':total,
            'target_set_incomplete':bool(errors),'errors':errors,'source_manifest':[],
            'source_definitions':{},'withheld_for_validation':withheld,'withheld_count':len(withheld),
            'market_as_of':market_as_of,'knowledge_as_of':knowledge_as_of,'source_availability':'independent_case_content'}


def migrate(market_as_of,knowledge_as_of):
    import case_dataset as D
    # First inventory may scan history. It is explicitly separate from the
    # incrementally maintained reader and must not be called from a judgment.
    legacy=D._build_legacy_cases(market_as_of,knowledge_as_of)
    if legacy['target_set_incomplete']: raise ValueError('MIGRATION_SOURCE_INCOMPLETE')
    with S.transaction() as db:
        for case in legacy['cases']:
            old=S.get('premise_case',case['application_id'],conn=db) or {}
            lifecycle=S.get('legacy_assessment_state',case['application_id'],conn=db) or {}
            closed=old.get('closed_at') or lifecycle.get('closed_at')
            S.put(db,'premise_case',case['application_id'],{'owner':(case.get('judgment_snapshot') or {}).get('run_attempt_id','legacy'),
                  'origin_at':case['origin_at'],'state':'completed' if closed else 'open',
                  **({'closed_at':closed} if closed else {}),
                  'completion_basis':'preserved_confirmed_completion' if closed else 'not_yet_confirmed',
                  'migration_source_as_of':knowledge_as_of,'context':compact(case)})
    projected=build(market_as_of,knowledge_as_of,limit=max(1,len(legacy['cases'])),enforce_window=False)
    keys=('application_id','features','signed_flows','stratum','adoption','label','verdict','label_available_at',
          'recording_status','period_status','data_status','outcome_eligible')
    comparable=lambda rows:sorted([{k:r.get(k) for k in keys} for r in rows],key=lambda r:r['application_id'])
    if comparable(legacy['cases'])!=comparable(projected['cases']): raise ValueError('INDEPENDENT_CASE_PARITY_FAILED')
    proof={'state':'verified','case_count':len(legacy['cases']),'as_of':knowledge_as_of,
           'projection_hash':S.digest(comparable(projected['cases'])),'raw_deletion_authorized':False,
           'remaining_requirement':'investigation_evidence_and_other_consumers'}
    with S.transaction() as db:
        S.put(db,'migration_proof','independent_cases',proof)
        db.execute('INSERT OR REPLACE INTO meta VALUES (?,?)',('independent_cases_ready',S.canonical(proof)))
    return proof
