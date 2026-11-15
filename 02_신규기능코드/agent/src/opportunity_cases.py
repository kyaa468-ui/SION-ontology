"""One independently committed case per attempt/instrument/direction.

Producer callbacks are incremental and idempotent. Historical migration is an
explicit command, never a full-history scan on a new trading judgment.
"""
from __future__ import annotations
import copy
import storage_objects as objects

import common as C
import improvement_store as S


def identifier(attempt_id, code, direction):
    return 'opportunity-' + S.digest([attempt_id,code,direction])[:40]


def evidence(db, value, *, owner):
    """Deduplicate the necessary content; avoid root/dataset copies in each case."""
    hashed,size=objects.fingerprint(value)
    eid = 'evidence-' + hashed
    if not S.get('evidence',eid,conn=db):
        if size<=512*1024:
            S.put(db,'evidence',eid,{'content':value,'content_hash':hashed,'owner':owner})
        else:
            # Store bounded reusable parts, not a giant body under a summary
            # name. Every required child is a typed GC edge.
            if isinstance(value,dict):parts=[(k,evidence(db,v,owner=owner)) for k,v in value.items()];kind='dict'
            elif isinstance(value,list):parts=[(i,evidence(db,v,owner=owner)) for i,v in enumerate(value)];kind='list'
            elif isinstance(value,str):parts=[(i,evidence(db,value[i:i+65536],owner=owner)) for i in range(0,len(value),65536)];kind='string'
            else:raise ValueError('UNSUPPORTED_LARGE_EVIDENCE')
            S.put(db,'evidence',eid,{'content_encoding':'required_parts','content_type':kind,'parts':parts,
                                  'content_hash':hashed,'logical_bytes':size,'owner':owner})
            for _,ref in parts:S.link(db,'evidence',eid,'evidence',ref)
    return eid


def execution_summary(call):
    keys=('event_id','stage','judgment_id','code','confidence_for','action','confidence','plan_id','order_attempt_id','link','created_at')
    outcome_keys=('ok','error','reason','status','code','side','qty','requested_qty','confirmed_qty','unconfirmed_qty','replayed','plan_id','order_attempt_id','order_id','terminal')
    return {**{k:call[k] for k in keys if k in call},'results':[
        {'event_id':r.get('event_id'),'outcome':{k:r['outcome'][k] for k in outcome_keys if k in (r.get('outcome') or {})},
         'boost_recorded':r.get('boost') is not None,
         'actual_calculation':investigation_slice(r.get('boost'))} for r in call.get('results',[])]}


def investigation_slice(value,depth=0):
    """Keep consumed values/reasons; broad source bundles have separate owners.

    V2 lazy views may describe enormous shared trees. Inspect known branches
    before encoding them; never materialize the entire graph to trim it later.
    """
    broad={'source_bundle','base_snapshot','briefing_snapshot','unmentioned_evidence',
           'source_files','runtime_inputs','target_snapshot','raw_response','raw_stdout','raw_stderr'}
    if depth>20:return {'availability':'original_required','reason':'investigation_depth_limit'}
    if isinstance(value,dict):
        output={}
        for key in value:
            if key in broad:
                output[key]={'availability':'original_required','reason':'broad_snapshot_kept_in_its_own_store'}
            else:output[key]=investigation_slice(value[key],depth+1)
        return output
    if isinstance(value,(list,tuple)):
        if len(value)>1000:return {'availability':'original_required','count':len(value),'reason':'registered_investigation_row_limit'}
        return [investigation_slice(v,depth+1) for v in value]
    return value


def sync_attempt(attempt_id):
    import judgment_coverage as JC
    import semantic_layer as SL
    import ai_judgment as J
    manifest = JC._manifest(attempt_id)
    coverage = JC.get_coverage(attempt_id)
    if coverage.get('global_storage_errors') or 'targets' not in coverage:
        raise ValueError('OPPORTUNITY_SOURCE_UNAVAILABLE')
    output=[]
    with S.transaction() as db:
        for target in coverage['targets']:
            oid = identifier(attempt_id,target['code'],target['confidence_for'])
            cid = 'chain-' + SL.digest([attempt_id,target['code'],target['confidence_for']])[:32]
            chain = SL._read('chains',cid,attempt_id)
            stages, refs, application_ids, judgments = [], [], [], []
            deliveries=set()
            if chain:
                refs.append(evidence(db,investigation_slice(chain),owner=oid))
            for role in SL.ROLES:
                stage = SL._read('stages',cid+'-'+role,attempt_id)
                if not stage:
                    stages.append({'role':role,'status':'not_recorded'})
                    continue
                ref = evidence(db,investigation_slice(stage),owner=oid); refs.append(ref)
                deliveries.update(stage.get('delivery_ids',[]))
                stages.append({'role':role,'stage_id':stage['stage_id'],'status':stage['status'],
                               'evidence_ref':ref,'input_id':stage['input_id'],
                               'unreviewed_ids':stage.get('decision',{}).get('unreviewed_ids',[])})
                # Actual predecessor inputs are stored, not reconstructed from today's definitions.
                incoming=SL._read('inputs',stage['input_id'],attempt_id)
                if incoming:
                    payload=incoming['payload']
                    frozen={k:incoming[k] for k in incoming if k!='payload'}
                    frozen['payload']={k:investigation_slice(payload[k]) for k in payload if k not in {'chain','predecessors'}}
                    frozen['chain_id']=cid
                    frozen['predecessors']=[{'stage_id':p['stage_id'],'input_hash':p['input_hash']} for p in payload['predecessors']]
                    refs.append(evidence(db,frozen,owner=oid))
            for submission in target['submissions']:
                jid=submission.get('judgment_id')
                judgment=J.load_judgment(jid) if jid else None
                if not judgment: continue
                application_ids += [a['application_id'] for a in judgment.get('premise_applications',[])]
                deliveries.update(judgment.get('delivery_ids',[]))
                refs.append(evidence(db,investigation_slice(judgment),owner=oid))
                judgments.append({'judgment_id':jid,'action':judgment['recommended_action'],
                                  'confidence_raw':judgment['confidence_raw']})
            delivery_refs=[];missing_deliveries=[]
            import evidence_wire as W
            for did in sorted(deliveries):
                receipt=J._load('deliveries',did)
                if receipt:
                    # Original detail/replay remains in its protected owner.
                    # This is the actual delivered text and target measurements,
                    # not a copy of every candidate's raw preparation snapshot.
                    frozen={k:receipt[k] for k in ('delivery_id','run_attempt_id','ai_run_id','created_at','kind','text','sha256') if k in receipt}
                    catalog=receipt.get('evidence_catalog',[])
                    frozen['evidence_catalog']=[investigation_slice(e) for e in catalog if e.get('subject') in {target['code'],'market','account','policy','system'}]
                    frozen['detail_source']={'delivery_id':did,'availability':'original_required_for_full_replay'}
                    ref=evidence(db,frozen,owner=oid);refs.append(ref);delivery_refs.append(ref)
                else:missing_deliveries.append(did)
            execution=[]
            for call in target['calls']:
                summary=execution_summary(call)
                ref=evidence(db,summary,owner=oid);refs.append(ref)
                execution.append({k:v for k,v in summary.items() if k!='results'} | {'results':[{'event_id':r['event_id'],'outcome':r['outcome']} for r in summary['results']],'evidence_ref':ref})
            assessments = [x['id'] for x in S.page('assessment',owner=attempt_id,limit=200,conn=db)
                           if x.get('chain_id')==cid]
            old=S.get('opportunity',oid,conn=db)
            bound=db.execute("SELECT value FROM meta WHERE key='support_lower_bound'").fetchone()
            if old is None and bound and manifest['created_at'][:10]<bound[0]:
                raise ValueError('EXPIRED_REQUEST')
            body={'id':oid,'owner':attempt_id,'run_attempt_id':attempt_id,'code':target['code'],
                  'confidence_for':target['confidence_for'],'origin_at':manifest['created_at'],
                  'account_scope':manifest.get('account_scope',{'status':'not_recorded'}),
                  'coverage_status':target['status'],'coverage_reasons':target['status_reasons'],
                  'selection_reasons':target['reasons'],'denominator':1,
                  'target_set_incomplete':coverage['target_set_incomplete'],
                  'stages':stages,'judgments':judgments,'evidence_refs':list(dict.fromkeys(refs)),
                  'application_ids':sorted(set(application_ids)),'assessment_ids':assessments,
                  'execution':execution,'delivery_refs':delivery_refs,'missing_deliveries':missing_deliveries,
                  'evaluation_status':'pending' if application_ids or assessments else 'not_expected',
                  'source_availability':'available' if chain or judgments else 'not_recorded',
                  'capabilities':{'cause_investigation':True,'premise_reevaluation':bool(application_ids or assessments),
                                  'semantic_reexperiment':bool(chain and delivery_refs and not missing_deliveries),'portfolio_reexperiment':False},
                  'state':'open'}
            if coverage['run_status']!='unfinished' and not application_ids and not assessments:
                body.update(state='completed',closed_at=(old or {}).get('closed_at') or coverage.get('first_finished_at') or S.now())
            for judgment in judgments:
                jid=judgment['judgment_id']
                if S.get('judgment_policy_inputs',jid,conn=db):
                    S.link(db,'opportunity',oid,'judgment_policy_inputs',jid)
            source_hash=S.digest(body)
            if old and old.get('source_hash')==source_hash:
                output.append(old);continue
            row=S.put(db,'opportunity',oid,{**body,'source_hash':source_hash})
            for ref in refs: S.link(db,'opportunity',oid,'evidence',ref)
            for aid in assessments: S.link(db,'opportunity',oid,'assessment',aid)
            for aid in application_ids:
                if S.get('premise_case',aid,conn=db):S.link(db,'opportunity',oid,'premise_case',aid)
                if S.get('legacy_assessment_state',aid,conn=db):S.link(db,'opportunity',oid,'legacy_assessment_state',aid)
            for dependency in S.page('semantic_dependency',owner=cid,limit=200,conn=db):
                S.link(db,'opportunity',oid,'semantic_dependency',dependency['id'])
            dependency_owner=S.get('semantic_dependency_owner',cid,conn=db)
            if dependency_owner:
                S.link(db,'opportunity',oid,'semantic_dependency_owner',cid)
                if coverage['run_status']!='unfinished' and dependency_owner['state']=='active':
                    S.put(db,'semantic_dependency_owner',cid,{**dependency_owner,'state':'completed','closed_at':S.now()})
            import improvement_issues as I
            I.inspect_case(db,row)
            output.append(row)
        # Cursor is committed after every case and issue in the same transaction.
        S.put(db,'producer_cursor',attempt_id,{'source_hash':S.digest([x['source_hash'] for x in output]),'owner':attempt_id,
              'case_ids':[x['id'] for x in output],'expected_count':coverage['expected_count']})
    return {'state':'recorded','expected_count':coverage['expected_count'],'case_ids':[x['id'] for x in output]}


def reconcile_assessments(limit=200):
    with S.transaction() as db:
        for case in S.page('opportunity',state='open',limit=limit,conn=db):
            children=[S.get('assessment',a,conn=db) for a in case.get('assessment_ids',[])]
            if case.get('application_ids'):
                # Legacy MA20 evaluations remain owned by their original adapter.
                import scoring_store
                final=[]
                for aid in case['application_ids']:
                    final.append(S.get('legacy_assessment_state',aid,conn=db))
                children+=final
            if not children or any(not x or x.get('state') not in {'finalized','finalized_unknown','evaluation_unsupported','capacity_blocked'} for x in children):
                continue
            S.put(db,'opportunity',case['id'],{**case,'state':'completed','closed_at':S.now(),
                  'evaluation_status':'finalized','assessment_summary':[{'id':x['id'],'verdict':x.get('verdict'),'state':x['state']} for x in children]})
            for child in children:S.link(db,'opportunity',case['id'],child['kind'],child['id'])


def migrate(*, after='', limit=100):
    """Explicit first inventory. Normal operation uses sync_attempt callbacks."""
    root=C.ontology_root()/'attempts'
    import heapq
    paths=heapq.nsmallest(limit+1,(p for p in root.glob('*.json') if p.stem>after),key=lambda p:p.stem)
    results=[]
    for path in paths[:limit]:
        try: results.append(sync_attempt(path.stem))
        except Exception as exc: results.append({'attempt_id':path.stem,'state':'recording_error','error':str(exc)})
    return {'items':results,'next_cursor':paths[limit-1].stem if len(paths)>limit else None,
            'mode':'explicit_inventory_scan'}
