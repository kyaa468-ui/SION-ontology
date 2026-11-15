"""Read-record and query-membership dependencies, including unseen new members."""
import improvement_store as S


def register(chain, stage, *, records=(), queries=()):
    with S.transaction() as db:
        if not S.get('semantic_dependency_owner',chain['chain_id'],conn=db):
            S.put(db,'semantic_dependency_owner',chain['chain_id'],{'owner':chain['run_attempt_id'],'state':'active'})
        for kind,values in (('record',records),('query',queries)):
            for value in values:
                if not isinstance(value,dict): raise ValueError('DEPENDENCY_OBJECT_REQUIRED')
                if kind=='query' and any(k not in value for k in ('type','filters','definition_id','as_of','completeness')):
                    raise ValueError('QUERY_DEPENDENCY_CONTRACT_REQUIRED')
                key='dependency-'+S.digest([chain['chain_id'],stage,kind,value])[:40]
                S.save_once('semantic_dependency',key,{'owner':chain['chain_id'],'chain_id':chain['chain_id'],
                    'run_attempt_id':chain['run_attempt_id'],'stage':stage,'dependency_kind':kind,'dependency':value},conn=db)
                S.link(db,'semantic_dependency_owner',chain['chain_id'],'semantic_dependency',key)


def changed(event):
    """Trusted data/definition correction hook, not a free-form AI mutation."""
    import common as C
    import record_store
    import semantic_layer as SL
    if not event.get('event_id') or not event.get('reason'): raise ValueError('CORRECTION_ID_AND_REASON_REQUIRED')
    candidates={}
    with S.transaction() as db:
        for raw in db.execute("SELECT body,hash FROM records WHERE kind='semantic_dependency'"):
            row=S.decode(raw);dependency=row['dependency']
            actual=row['dependency_kind']=='record' and dependency.get('id') in event.get('changed_ids',[])
            query=(row['dependency_kind']=='query' and
                   (dependency['definition_id'] in event.get('changed_ids',[]) or
                    dependency['type']==event.get('type') and all(event.get('attributes',{}).get(k)==v for k,v in dependency['filters'].items())))
            if not actual and not query: continue
            key=row['chain_id'];previous=candidates.get(key)
            if not previous or SL.ROLES.index(row['stage'])<SL.ROLES.index(previous['stage']):
                candidates[key]={**row,'impact':'actual_read' if actual else 'query_membership_candidate'}
        for cid,row in candidates.items():
            key='reassessment-'+S.digest([event['event_id'],cid])[:40]
            S.save_once('reassessment',key,{'owner':cid,'run_attempt_id':row['run_attempt_id'],'chain_id':cid,
                        'event':event,'from_stage':row['stage'],'impact':row['impact'],'state':'queued',
                        'orders_replayed':False,'last_progress_at':S.now()},conn=db)
            path=SL._path('invalidations',cid,row['run_attempt_id'])
            if not path.exists():
                record_store.publish(path,{'chain_id':cid,'dependency_id':event['event_id'],'reason':event['reason'],
                    'created_at':C.now().isoformat(),'status':'reassessment_required','impact':row['impact']},kind='semantic_invalidation')
    return {'affected':list(candidates),'orders_replayed':False}


def supply_inputs(identifier,chain,receipts,guidance):
    """Host-only correction adapter. Never regenerate missing historical facts."""
    import ai_judgment as J
    import semantic_layer as SL
    job=S.get('reassessment',identifier)
    if not job or job['state'] in {'completed','expired','cancelled'}:raise ValueError('REASSESSMENT_NOT_OPEN')
    original=SL._read('chains',job['chain_id'],job['run_attempt_id'])
    if not original:raise ValueError('ORIGINAL_CHAIN_UNAVAILABLE')
    import validation_holdout as VH
    if VH.is_reserved_judgment(original):raise ValueError('RESERVED_VALIDATION_DATA')
    if any(original[k]!=chain[k] for k in ('chain_id','run_attempt_id','code','confidence_for')):raise ValueError('CORRECTION_TARGET_MISMATCH')
    if not receipts or set(guidance)!=set(SL.ROLES):raise ValueError('CORRECTION_RECEIPTS_AND_GUIDANCE_REQUIRED')
    for receipt in receipts:
        stored=J._load('deliveries',receipt['delivery_id'])
        if not stored or S.digest(stored)!=S.digest(receipt):raise ValueError('CORRECTION_RECEIPT_NOT_COMMITTED')
        if S.instant(receipt['created_at'])>S.instant(chain['created_at']):raise ValueError('CORRECTION_INPUT_AFTER_CUTOFF')
    with S.transaction() as db:
        inputs=S.save_once('reassessment_inputs',identifier,{'owner':identifier,'chain':chain,'receipts':receipts,
            'guidance':guidance,'original_hash':S.digest(original),'original_origin_at':original['created_at'],
            'event_id':job['event']['event_id']},conn=db)
        S.link(db,'reassessment',identifier,'reassessment_inputs',identifier)
        return S.put(db,'reassessment',identifier,{**job,'state':'ready','input_hash':S.digest(inputs),'last_progress_at':S.now()})


def advance(*,no_ai=False,limit=10,provider=None):
    """Reevaluate a correction separately; original eligibility stays invalidated."""
    import copy
    import semantic_layer as SL
    import semantic_experiments as E
    import retention_store as R
    cfg=S.config();output=[]
    with S.transaction(readonly=True) as db:
        jobs=[] if db is None else [S.decode(r) for r in db.execute("SELECT body,hash FROM records WHERE kind='reassessment' AND state IN ('queued','inputs_pending','ready','budget_pending','configuration_pending') ORDER BY updated_at,id LIMIT ?",(limit,))]
    for job in jobs:
        inputs=S.get('reassessment_inputs',job['id'])
        state='inputs_pending';reason='committed_corrected_inputs_required';trials=[]
        if inputs:
            state='ready';reason='real_ai_authorization_required'
            if not no_ai or provider:
                state='configuration_pending';reason='pinned_research_model_required'
                if provider or cfg.get('research_model') and cfg.get('research_model_version'):
                    permission=R.detail_support(inputs['original_origin_at'])
                    if provider or permission['allowed']:
                        import validation_holdout as VH
                        if VH.is_reserved_judgment(inputs['chain']):raise ValueError('RESERVED_VALIDATION_DATA')
                        if not provider:
                            from investigation import exposure_identity
                            with S.transaction() as db:
                                db.execute('INSERT OR IGNORE INTO exposure VALUES (?,?,?)',
                                    (exposure_identity(inputs['chain']),job['id'],S.now()))
                        runtime=E.FrozenSemantic(inputs['chain'],inputs['receipts']);state='completed';reason=None
                        # Rerun all roles so a changed interpretation cannot inherit
                        # a stale predecessor. This never produces a live order.
                        for role in SL.ROLES:
                            key='reassessment-trial-'+S.digest([job['id'],job['input_hash'],role])[:40]
                            context=runtime.context(role)
                            request={'call_id':key,'experiment_id':job['id'],'model':cfg.get('research_model'),
                                'model_version':cfg.get('research_model_version'),'maximum_usd':cfg.get('research_call_usd',1)/4,
                                'guidance':inputs['guidance'][role],'input':{'context':context,'receipts':inputs['receipts']}}
                            old=S.get('reassessment_trial',key)
                            kind='local_fake' if provider else 'real_ai'
                            if old and old['result_kind']!=kind:raise ValueError('REASSESSMENT_RESULT_KIND_IMMUTABLE')
                            if old and old['input_hash']!=S.digest(request):raise ValueError('REASSESSMENT_INPUT_CHANGED')
                            response=old['output'] if old else (provider or E.cli_provider)(copy.deepcopy(request))
                            if response.get('status')!='completed':
                                state='budget_pending' if response.get('status') in {'budget_blocked','cost_unresolved'} else 'failed'
                                reason=response.get('status');break
                            try:accepted=runtime.submit(context['input_id'],response['data'])
                            except ValueError as exc:state='failed';reason=str(exc);break
                            with S.transaction() as db:
                                if not old:S.save_once('reassessment_trial',key,{'owner':job['id'],'result_kind':kind,
                                    'output':response,'accepted':accepted,'input_hash':S.digest(request),'stage':role},conn=db)
                                S.link(db,'reassessment',job['id'],'reassessment_trial',key)
                            trials.append(key)
                    else:state='inputs_pending';reason=permission['reason']
        with S.transaction() as db:
            row={**job,'state':state,'reason':reason,'trial_ids':trials,'orders_replayed':False,
                 'current_live_eligibility':'invalidated_until_new_live_judgment'}
            if state=='completed':row.update(closed_at=S.now(),last_progress_at=S.now(),result_kind='local_fake' if provider else 'real_ai')
            if any(row.get(k)!=job.get(k) for k in ('state','reason','trial_ids')):row=S.put(db,'reassessment',job['id'],row)
            output.append(row)
    return output
