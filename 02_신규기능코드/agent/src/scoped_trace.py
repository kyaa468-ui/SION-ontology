"""Bounded view preparation from committed independent opportunity records.

No full archive scan or live reconstruction is allowed. Missing migrations and
oversized details stay explicit. Evidence is frozen under one SQLite snapshot.
"""
import common as C
import improvement_store as S
import retention_store as R
import trace_snapshot as V
import uuid
import json


def build(market_as_of=None,knowledge_as_of=None,*,case_ids=(),limit=50,maximum_bytes=4*1024*1024):
    if type(limit) is not int or not 1<=limit<=200:raise ValueError('TRACE_SCOPE_LIMIT')
    if type(maximum_bytes) is not int or not 1024<=maximum_bytes<=16*1024*1024:raise ValueError('TRACE_SCOPE_BYTES_LIMIT')
    if len(case_ids)>limit or len(set(case_ids))!=len(case_ids):raise ValueError('TRACE_CASE_SCOPE_LIMIT')
    known=S.instant(knowledge_as_of or S.now())
    if known>S.instant(S.now()):raise ValueError('FUTURE_VIEW_CUTOFF')
    market=market_as_of or known.date().isoformat()
    if market>known.date().isoformat():raise ValueError('FUTURE_VIEW_CUTOFF')
    boundary=R.current_boundary()
    if not boundary.get('deletion_allowed'):return {'status':'configuration_pending','reason':'verified_calendar_required','boundary':boundary}
    import validation_holdout as H
    import holdout_scopes
    if not holdout_scopes.enabled():return {'status':'migration_pending','reason':'holdout_scopes_migration_required'}
    import research_evidence as RE
    import investigation
    reservations=H.reservations()
    view_id='view-'+C.now().strftime('%Y%m%d')+'-'+uuid.uuid4().hex
    projection={'schema_version':'TRACE_VIEW@v1','status':'complete','generated_at':S.now(),
        'market_as_of':market,'knowledge_as_of':known.isoformat(),'targets':[],'traces':{},'cases':[],
        'reviews':[],'proposals':[],'validations':[],'policies':[],'coverage':[],
        'source_manifest':[],'source_records':[],'errors':[],'protected_count':0,
        'application_ids':[],'protection_scopes':[],
        'scope':{'contract':'INDEPENDENT_TRACE_SCOPE@v1','boundary':boundary,'limit':limit,'maximum_bytes':maximum_bytes,
                 'case_ids':list(case_ids),'selection':'origin_and_id_order_before_outcomes',
                 'legacy_archive_scan':False,'historical_reconstruction':False}}
    with S.transaction() as db:
        conditions=["kind='opportunity'","origin_at<=?","(closed_at IS NULL OR substr(closed_at,1,10)>=?)"]
        args=[known.isoformat(),boundary['case_closed_cutoff']]
        if case_ids:conditions.append('id IN ('+','.join('?' for _ in case_ids)+')');args+=list(case_ids)
        where=' AND '.join(conditions)
        total=db.execute('SELECT count(*) FROM records WHERE '+where,args).fetchone()[0]
        selected=db.execute('SELECT id FROM records WHERE '+where+' ORDER BY origin_at,id LIMIT ?',[*args,limit]).fetchall()
        projection['scope'].update(expected_count=total,selected_count=len(selected))
        if case_ids and total<len(case_ids):projection['errors'].append({'reason':'requested_case_expired_or_unavailable','requested_count':len(case_ids),'available_count':total})
        if not total:
            migration=db.execute("SELECT 1 FROM records WHERE kind='producer_cursor' LIMIT 1").fetchone()
            if not migration:return {'status':'migration_pending','reason':'independent_opportunity_capture_required','scope':projection['scope']}
        if total>limit:projection['errors'].append({'reason':'scope_limit','remaining_count':total-limit})
        remaining=maximum_bytes
        def frozen(kind,key):
            nonlocal remaining
            meta=db.execute('SELECT revision,length(CAST(body AS BLOB)) AS bytes FROM revisions WHERE kind=? AND id=? AND known_at<=? ORDER BY revision DESC LIMIT 1',(kind,key,known.isoformat())).fetchone()
            if not meta:return {'status':'unavailable','kind':kind,'id':key,'reason':'revision_not_recorded_as_of'}
            if meta['bytes']>remaining:return {'status':'detail_limit','kind':kind,'id':key,'required_bytes':meta['bytes']}
            raw=db.execute('SELECT body,hash FROM revisions WHERE kind=? AND id=? AND revision=?',(kind,key,meta['revision'])).fetchone()
            value=S.decode(raw);remaining-=meta['bytes']
            projection['source_records'].append({'kind':kind,'id':key,'revision':meta['revision'],'sha256':raw['hash']})
            return value
        exposed=[]
        for item in selected:
            case=frozen('opportunity',item['id'])
            if case.get('status') in {'unavailable','detail_limit'}:
                projection['errors'].append(case);continue
            trace_id='trace-'+V.digest([case['run_attempt_id'],case['code'],case['confidence_for']])[:32]
            target={'trace_id':trace_id,'run_attempt_id':case['run_attempt_id'],'code':case['code'],
                'confidence_for':case['confidence_for'],'created_at':case['origin_at'],
                'coverage_status':case['coverage_status'],'judgment_ids':[j['judgment_id'] for j in case.get('judgments',[])],
                'connection_status':'linked' if case['coverage_status']=='linked' else 'unlinked','visibility':'available'}
            scope={'code':case['code'],'created_at':case['origin_at'],'confidence_for':case['confidence_for']}
            # Old independent receipts did not preserve the full shared-data
            # visibility scope. Do not infer that a target-specific row makes
            # every embedded delivery safe during another current holdout.
            scope_unknown=bool(case.get('evidence_refs') and any(S.instant(r['spec']['period']['start'])<=known for r in reservations))
            if H.is_reserved_judgment(scope) or scope_unknown:
                target.update(visibility='protected',coverage_status='protected',judgment_ids=[])
                projection['protected_count']+=1
                detail={'target':target,'visibility':'protected','reason':'shared_evidence_holdout_scope_unavailable' if scope_unknown else 'prospective_validation_reserved'}
            else:
                projection['protection_scopes'].append(scope);exposed.append(case)
                records={};queue=list(case.get('evidence_refs',[]));seen=set()
                while queue and len(seen)<1000:
                    key=queue.pop(0)
                    if key in seen:continue
                    seen.add(key);value=frozen('evidence',key);records[key]=value
                    if value.get('content_encoding')=='required_parts':queue.extend(p[1] for p in value['parts'])
                    if value.get('status') in {'unavailable','detail_limit'}:projection['errors'].append(value)
                if queue:projection['errors'].append({'reason':'evidence_scope_limit','case_id':case['id']})
                recorded={value['content']['judgment_id']:value['content'] for value in records.values()
                    if isinstance(value.get('content'),dict) and value['content'].get('judgment_id')
                    and value['content'].get('recommended_action')}
                judgments=[recorded.get(j['judgment_id'],{**j,'recommended_action':j.get('action'),
                    'confidence_for':case['confidence_for'],'detail_availability':'summary_only'})
                    for j in case.get('judgments',[])]
                target['action']=judgments[-1].get('recommended_action') if judgments else None
                detail={'target':target,'visibility':'available','opportunity':case,'evidence_records':records,
                    'judgments':judgments,'execution_context':{'scope':'recorded_summary','calls':case.get('execution',[])},
                    'semantic_execution':{'stages':{x['role']:records.get(x.get('evidence_ref'),{}).get('content') for x in case.get('stages',[])}},
                    'deliveries':[records[r].get('content',records[r]) for r in case.get('delivery_refs',[]) if r in records],
                    'capabilities':case.get('capabilities',{}),'detail_support':R.supports(case,'cause_investigation',boundary=boundary)}
                for aid in case.get('application_ids',[]):
                    premise=frozen('premise_case',aid)
                    if premise.get('context'):
                        import case_dataset
                        row=case_dataset.project_case(premise['context'],market,known.isoformat())
                        row['trace_ids']=[trace_id];projection['cases'].append(row)
                projection['application_ids']+=case.get('application_ids',[])
            projection['targets'].append(target);projection['traces'][trace_id]=detail
        projection['application_ids']=sorted(set(projection['application_ids']))
        projection['scope']['decoded_source_bytes']=maximum_bytes-remaining
        projection['status']='partial' if projection['errors'] else 'complete'
        import storage_objects
        logical_bytes=storage_objects.fingerprint(projection)[1]
        if logical_bytes>maximum_bytes:
            return {'status':'capacity_blocked','reason':'view_projection_bytes_limit','logical_bytes':logical_bytes,
                    'maximum_bytes':maximum_bytes,'scope':projection['scope'],'exposure_already_recorded':False}
        import research_capacity
        capacity_id='trace-view:'+view_id
        try:research_capacity.reserve(capacity_id,view_id,minimum_bytes=maximum_bytes)
        except research_capacity.CapacityBlocked as exc:
            return {'status':'configuration_pending' if 'CONFIGURATION_REQUIRED' in str(exc) else 'capacity_blocked',
                    'reason':str(exc),'scope':projection['scope']}
        V.check_visibility(projection)
        reservation=RE.reserve_usage(view_id,projection['application_ids'],source_refs=projection['source_records'],purpose='bounded_view_preparation')
        for case in exposed:
            db.execute('INSERT OR IGNORE INTO exposure VALUES (?,?,?)',(investigation.exposure_identity(case),view_id,S.now()))
        # Freeze to ordinary values before leaving this transaction. Publication
        # later has no reason to reopen raw judgments or a changing SQLite file.
        projection=json.loads(S.canonical(projection))
    V.check_visibility(projection)
    manifest={'schema_version':V.VERSION,'view_id':view_id,'created_at':S.now(),
        'market_as_of':market,'knowledge_as_of':known.isoformat(),'projection_schema':projection['schema_version'],
        'projection_hash':V.digest(projection),'source_manifest':[],'source_records':projection['source_records'],
        'scope':projection['scope'],'exposure_reservation_id':reservation['usage_id'],
        'exposure_reservation_hash':V.digest(reservation),'status':projection['status']}
    folder=V.root()/'snapshots'/view_id
    V._write_immutable(folder/'projection.json',projection)
    V._write_immutable(folder/'manifest.json',manifest)
    V.check_visibility(projection)
    V._write_immutable(folder/'commit.json',{'schema_version':V.VERSION,'view_id':view_id,
        'manifest_hash':V.digest(manifest),'projection_hash':V.digest(projection),'committed_at':S.now()})
    research_capacity.finish(capacity_id)
    return {'status':manifest['status'],'view_id':view_id,'manifest':manifest,'errors':projection['errors']}
