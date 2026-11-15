"""Monotone research exposure, independent of archived datasets.

An absent entry is unknown. Only an explicit verified migration enables this
reader for an existing store; recording callbacks are active before migration.
Identity conservatively spans accounts within one workspace when old records
do not contain account scope. A new application ID cannot reset known exposure.
"""
import improvement_store as S

KINDS=('usage_events','research_usage','datasets')


def origin(application):
    required=('code','created_at','definition_id','days','direction')
    if any(application.get(k) is None for k in required):return None
    return 'premise-origin:'+S.digest({k:application[k] for k in required})


def _application(identifier,provided=None):
    if provided:
        app=provided.get('application_snapshot') or provided.get('application') or provided
        if origin(app):return app
    import ai_judgment as J
    return J._load('premises',identifier)


def _merge(db,key,source):
    old=S.get('research_exposure',key,conn=db)
    refs=(old or {}).get('references',[])
    if any(r['usage_id']==source['usage_id'] for r in refs):return
    row={**(old or {}),'state':'used','first_used_at':(old or {}).get('first_used_at',source['created_at']),
         'references':(refs+[source])[-8:],'evidence_scope':'monotone_compact_usage',
         'independent_unused':False,'origin_at':(old or {}).get('origin_at',source['created_at'])}
    S.put(db,'research_exposure',key,row)


def record_source(kind,identifier,body):
    if kind not in KINDS:return
    if kind=='usage_events' and (body.get('version')!='RESEARCH_USAGE@v1' or body.get('kind') not in {'reserved','used'}):
        raise ValueError('UNKNOWN_RESEARCH_USAGE_SCHEMA')
    values=body.get('cases') if kind=='datasets' else body.get('application_ids')
    if not isinstance(values,list):raise ValueError('USAGE_SOURCE_APPLICATIONS_REQUIRED')
    ids=[];metadata={}
    for value in values:
        key=value.get('application_id') if isinstance(value,dict) else value
        if not isinstance(key,str) or not key:raise ValueError('INVALID_USAGE_APPLICATION')
        ids.append(key)
        if isinstance(value,dict):metadata[key]=value
    stamp=body.get('created_at') or body.get('started_at') or body.get('used_at') or S.now()
    source={'kind':kind,'owner_id':body.get('owner_id',body.get('review_id',identifier)),
            'usage_id':kind+'/'+identifier,'purpose':body.get('purpose','archived_research_dataset'),'created_at':stamp}
    bindings={key:_application(key,metadata.get(key)) for key in set(ids)}
    with S.transaction() as db:
        import usage_checkpoint as checkpoint_store
        checkpoint=checkpoint_store.load(db)
        for key,application in bindings.items():
            old=S.get('usage_alias',key,conn=db)
            normalized=origin(application or {}) or (old or {}).get('source_identity') or 'application-id:'+key
            if old and old.get('identity_resolved') and old['source_identity']!=normalized:raise ValueError('USAGE_ORIGIN_CONFLICT')
            if old and old['source_identity']!=normalized:
                prior=S.get('research_exposure',old['source_identity'],conn=db)
                for ref in (prior or {}).get('references',[]):_merge(db,normalized,ref)
            app_origin=(application or {}).get('created_at') or (old or {}).get('application_origin_at')
            app_origin=S.instant(app_origin).isoformat() if app_origin else None
            if not old and checkpoint_store.fold_expired_use(db,checkpoint,key,normalized,app_origin,source):
                continue
            app_code=(application or {}).get('code') or (old or {}).get('code')
            if not old or old['source_identity']!=normalized or old.get('application_origin_at')!=app_origin or old.get('code')!=app_code:
                S.put(db,'usage_alias',key,{'source_identity':normalized,'identity_resolved':not normalized.startswith('application-id:'),
                    'origin_at':(application or {}).get('created_at',stamp),'code':app_code,'application_origin_at':app_origin,
                    'state':'used','independent_unused':False})
            _merge(db,normalized,source)


def problem(kind,identifier,reason):
    with S.transaction() as db:
        S.put(db,'usage_ledger_health',kind+'/'+identifier,{'state':'blocked','source_kind':kind,'source_id':identifier,'reason':str(reason)})


def enabled():
    with S.transaction(readonly=True) as db:
        return bool(db and db.execute("SELECT 1 FROM meta WHERE key='research_usage_ready'").fetchone())


def status(application_ids):
    if len(application_ids)>2000:raise ValueError('USAGE_QUERY_SCOPE_LIMIT')
    output=[]
    with S.transaction(readonly=True) as db:
        import usage_checkpoint as checkpoint_store
        checkpoint=checkpoint_store.load(db)
        errors=[] if db is None else [{'reason':'usage_history_schema_unknown','source':r['id']} for r in S.page('usage_ledger_health',state='blocked',limit=200,conn=db)]
        for key in sorted(set(application_ids)):
            alias=S.get('usage_alias',key,conn=db) if db else None
            identity=(alias or {}).get('source_identity')
            if not identity:
                identity=('application-id:'+key if checkpoint_store.possible_use(checkpoint,key)
                          else origin(_application(key) or {}) or 'application-id:'+key)
            evidence=S.get('research_exposure',identity,conn=db) if db else None
            retired=checkpoint_store.possible_use(checkpoint,key,identity)
            output.append({'application_id':key,'status':'used' if evidence or retired else 'unknown','independent_unused':False,
                'references':(evidence or {}).get('references',[]),'source_identity':identity,
                'reason':'compact_monotone_exposure' if evidence else 'prior_exposure_or_checkpoint_collision' if retired else 'no_proof_of_nonuse',
                'detail_scope':'available' if evidence else 'expired' if retired else 'unknown'})
    return {'items':output,'recording_status':'recording_error' if errors else 'recorded','errors':errors,'source':'independent_usage_ledger'}


def exposed_in_scope(scope,period):
    """Supplement positive enrollment with exposure surviving raw deletion.

    Absence is not proof of nonuse; this guard can only disqualify a cohort.
    The independently verified pre-outcome enrollment remains mandatory.
    """
    with S.transaction(readonly=True) as db:
        if db is None:return {'application_ids':[],'errors':[],'truncated':False}
        import usage_checkpoint as checkpoint_store
        checkpoint=checkpoint_store.load(db)
        codes=scope['codes'];conditions=["a.kind='usage_alias'","e.kind='research_exposure'",
            "julianday(coalesce(json_extract(a.body,'$.application_origin_at'),json_extract(a.body,'$.origin_at')))>=julianday(?)",
            "julianday(coalesce(json_extract(a.body,'$.application_origin_at'),json_extract(a.body,'$.origin_at')))<julianday(?)",
            "json_extract(a.body,'$.code') IS NOT NULL"]
        args=[period['start'],period['end']]
        if '*' not in codes:
            conditions.append("json_extract(a.body,'$.code') IN ("+','.join('?' for _ in codes)+")");args+=codes
        rows=db.execute("SELECT a.body,a.hash,e.body AS exposure_body,e.hash AS exposure_hash FROM records a JOIN records e ON e.id=json_extract(a.body,'$.source_identity') WHERE "+' AND '.join(conditions)+" ORDER BY a.id LIMIT 201",args).fetchall()
        ids=[]
        for raw in rows[:200]:
            alias=S.decode(raw)
            S.decode({'body':raw['exposure_body'],'hash':raw['exposure_hash']})
            ids.append(alias['id'])
        errors=[{'reason':'usage_history_schema_unknown','source':r['id']} for r in S.page('usage_ledger_health',state='blocked',limit=200,conn=db)]
        expired=checkpoint_store.scope_error(checkpoint,period)
        if expired:errors.append(expired)
        unresolved=db.execute("""SELECT id FROM records WHERE kind='usage_alias'
            AND EXISTS (SELECT 1 FROM meta WHERE key='research_usage_ready')
            AND (json_extract(body,'$.application_origin_at') IS NULL OR json_extract(body,'$.code') IS NULL) LIMIT 1""").fetchone()
        if unresolved:errors.append({'reason':'exposed_origin_scope_unknown','source':unresolved['id']})
        return {'application_ids':ids,'errors':errors,'truncated':len(rows)>200}


def migrate():
    """Explicit streaming inventory. A failure leaves the legacy reader active."""
    import case_store as CS
    errors=[];count=0;applications=set()
    for kind in KINDS:
        for row in CS.iter_records(kind,errors=errors):
            identifier=row.get('usage_id') or row.get('review_id') or row.get('owner_id')
            if not identifier:raise ValueError('USAGE_SOURCE_ID_REQUIRED')
            record_source(kind,identifier,row);count+=1
            values=row['cases'] if kind=='datasets' else row['application_ids']
            applications.update(v['application_id'] if isinstance(v,dict) else v for v in values)
    if errors:raise ValueError('USAGE_MIGRATION_SOURCE_UNAVAILABLE')
    if S.page('usage_ledger_health',state='blocked',limit=1):raise ValueError('USAGE_LEDGER_HEALTH_UNRESOLVED')
    # Every application discovered in the source inventory must remain used.
    # Projection only needs small IDs, never all archived training features.
    keys=sorted(applications)
    for offset in range(0,len(keys),2000):
        if any(row['status']!='used' for row in status(keys[offset:offset+2000])['items']):
            raise ValueError('USAGE_MIGRATION_PARITY_FAILED')
    proof={'state':'verified','sources':count,'applications':len(keys),'projection_hash':S.digest(keys),
        'verified_at':S.now(),'raw_deletion_authorized':False,'source_reader':'case_store.iter_records'}
    with S.transaction() as db:
        S.put(db,'migration_proof','research_usage',proof)
        db.execute("CREATE INDEX IF NOT EXISTS usage_source_identity ON records(json_extract(body,'$.source_identity')) WHERE kind='usage_alias'")
        db.execute('INSERT OR REPLACE INTO meta VALUES (?,?)',('research_usage_ready',S.canonical(proof)))
    return proof
