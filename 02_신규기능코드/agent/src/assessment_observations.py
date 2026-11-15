"""Registered observation collector fed by already persisted deliveries."""
import improvement_store as S
import assessment_contracts as A


def from_delivery(receipt):
    definitions=S.config().get('assessment_definitions',{})
    if not definitions:return []
    registered={v.get('field') for v in definitions.values() if v.get('collector')=='delivered_evidence'}
    catalog=[e for e in receipt.get('evidence_catalog',[]) if e.get('field') in registered]
    if not catalog:return []
    identifier='delivery-observations-'+S.digest(receipt['delivery_id'])[:40]
    with S.transaction() as db:
        old=S.get('observation_delivery',identifier,conn=db)
        if old and old['state']=='completed':return []
        if not old:
            S.put(db,'observation_delivery',identifier,{'state':'pending','cursor':'',
                'receipt':{'delivery_id':receipt['delivery_id'],'created_at':receipt['created_at'],'evidence_catalog':catalog},
                'registered_definitions':definitions,'origin_at':receipt['created_at']})
    return _process(identifier)


def _process(identifier,limit=200):
    import case_store
    with case_store.locked('observation-delivery-'+identifier,timeout=0):
        job=S.get('observation_delivery',identifier)
        if not job or job['state']=='completed':return []
        return _consume(job,limit)


def _consume(job,limit):
    receipt=job['receipt'];definitions=job['registered_definitions']
    key=S.instant(receipt['created_at']).date().isoformat()
    codes=sorted({e.get('subject') for e in receipt['evidence_catalog'] if e.get('subject')})
    results=[]
    with S.transaction(readonly=True) as db:
        # Select this receipt's target and expected date before materialization.
        placeholders=','.join('?' for _ in codes)
        rows=[] if not codes or db is None else db.execute(f"""SELECT body,hash,id FROM records WHERE kind='assessment'
            AND state IN ('accepted','evaluating','correction_pending') AND id>? AND origin_at<=?
            AND json_extract(body,'$.spec.code') IN ({placeholders})
            AND EXISTS (SELECT 1 FROM json_each(body,'$.spec.expected_observations') WHERE value=?)
            ORDER BY id LIMIT ?""",(job['cursor'],receipt['created_at'],*codes,key,limit+1)).fetchall()
    for raw in rows[:limit]:
        assessment=S.decode(raw);spec=assessment['spec']
        if spec['support']!='supported':continue
        fields={}
        for name,contract in spec['fields'].items():
            definition=definitions.get(contract['definition_id'],{})
            if definition.get('collector')!='delivered_evidence':continue
            matches=[e for e in receipt.get('evidence_catalog',[]) if e.get('subject')==spec['code']
                     and e.get('field')==definition.get('field') and e.get('kind')==contract['method']
                     and e.get('unit')==contract['unit'] and e.get('status')!='unknown']
            if len(matches)!=1:continue
            e=matches[0]
            fields[name]={**contract,'value':e.get('value'),'code':spec['code'],
                          'evidence_id':e.get('evidence_id'),'period':e.get('period')}
        if not fields:continue
        observation={'key':key,'observed_at':receipt['created_at'],'available_at':receipt['created_at'],
                     'source_identity':receipt['delivery_id'],'fields':fields}
        try:results.append(A.ingest(assessment['id'],observation))
        except ValueError as exc:results.append({'assessment_id':assessment['id'],'state':'unavailable','reason':str(exc)})
    finished=len(rows)<=limit
    with S.transaction() as db:
        S.put(db,'observation_delivery',job['id'],{**job,'cursor':rows[min(len(rows),limit)-1]['id'] if rows else job['cursor'],
            'state':'completed' if finished else 'pending','last_progress_at':S.now(),
            **({'closed_at':S.now()} if finished else {})})
    return results


def advance(*,limit=10):
    import case_store
    results=[]
    with S.transaction(readonly=True) as db:
        rows=[] if db is None else db.execute("SELECT id FROM records WHERE kind='observation_delivery' AND state='pending' ORDER BY updated_at,id LIMIT ?",(limit,)).fetchall()
    for row in rows:
        try:results.extend(_process(row['id']))
        except case_store.StoreBusy:continue
    return results
