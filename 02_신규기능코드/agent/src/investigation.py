"""Scope-bound investigation sessions with exposure-before-return semantics."""
from __future__ import annotations
import copy
import os

import improvement_store as S

TOOLS=('get_investigation_context','query_investigation','run_investigation_check','submit_diagnosis','submit_change')


def exposure_identity(case):
    return 'opportunity-source:'+S.digest([case['run_attempt_id'],case['code'],case['confidence_for']])


def _case_allowed(case):
    import validation_holdout as VH
    if VH.is_reserved_judgment({'code':case['code'],'created_at':case['origin_at'],
                               'confidence_for':case['confidence_for']}):
        raise ValueError('RESERVED_VALIDATION_DATA')


def start(issue_id, *, controls=()):
    issue=S.get('issue',issue_id)
    if not issue: raise ValueError('UNKNOWN_ISSUE')
    cases=[cid for cid in [issue['opportunity_id'],*controls] if cid is not None]
    if len(cases)>20: raise ValueError('INVESTIGATION_SCOPE_LIMIT')
    frozen=[]
    for cid in dict.fromkeys(cases):
        case=S.get('opportunity',cid)
        if not case: raise ValueError('OPPORTUNITY_UNAVAILABLE')
        _case_allowed(case)
        if case.get('closed_at'):
            import retention_store as R
            support=R.supports(case,'cause_investigation')
            if not support['allowed']:raise ValueError('INVESTIGATION_SUPPORT_PENDING:'+support['reason'])
        frozen.append({'id':cid,'hash':S.digest(case),'revision':case['revision']})
    identifier='investigation-'+S.digest([issue_id,frozen])[:40]
    import learning_proposals as L
    base=L.target_snapshot()
    with S.transaction() as db:
        old=S.get('investigation',identifier,conn=db)
        if old: return old
        row=S.put(db,'investigation',identifier,{'issue_id':issue_id,'owner':issue_id,'question':issue['question'],'request':issue.get('request'),
            'scope':frozen,'scope_hash':S.digest(frozen),'knowledge_as_of':S.now(),
            'base_revision':{'definition_hash':base['definition_hash'],'settings_hash':base['settings_hash']},
            'state':'investigating','last_progress_at':S.now(),'queries':0,'checks':0,'returned_bytes':0,
            'limits':S.config()['query_limits'],'selection_rule':'issue_subject_and_explicit_controls',
            'control_case_ids':list(controls),'exposed_refs':[]})
        S.link(db,'investigation',identifier,'issue',issue_id)
        for c in frozen: S.link(db,'investigation',identifier,'opportunity',c['id'])
        current=S.get('issue',issue_id,conn=db)
        S.put(db,'issue',issue_id,{**current,'state':'investigating','investigation_id':identifier,'last_progress_at':S.now()})
        return row


def _session(identifier, db):
    row=S.get('investigation',identifier,conn=db)
    if not row: raise ValueError('UNKNOWN_INVESTIGATION')
    if row['state'] in {'expired','cancelled'}: raise ValueError('INVESTIGATION_EXPIRED')
    assigned=os.environ.get('INVESTIGATION_ID')
    if os.environ.get('RUN_MODE')=='investigation' and assigned != identifier:
        raise ValueError('INVESTIGATION_SCOPE_MISMATCH')
    return row


def _cases(session, db):
    cases=[]
    for ref in session['scope']:
        # Read the frozen revision, not a later correction disguised as the origin.
        raw=db.execute('SELECT body,hash FROM revisions WHERE kind=? AND id=? AND revision=?',
                       ('opportunity',ref['id'],ref['revision'])).fetchone()
        case=S.decode(raw)
        if not case: raise ValueError('SOURCE_EXPIRED_OR_UNAVAILABLE')
        if S.digest(case)!=ref['hash']: raise ValueError('SOURCE_REVISION_CORRUPT')
        _case_allowed(case)  # Recheck even on repeated/cache-like reads.
        cases.append(case)
    return cases


def _frozen(kind,identifier,session,db):
    if kind=='change_target':return S.get(kind,identifier,conn=db)
    return S.decode(db.execute('SELECT body,hash FROM revisions WHERE kind=? AND id=? AND known_at<=? ORDER BY revision DESC LIMIT 1',
                       (kind,identifier,session['knowledge_as_of'])).fetchone())


def _return(db, session, result, refs, *, query=False, check=False):
    cases=_cases(session,db)
    limits=session['limits']
    if session['queries']+int(query)>limits['queries'] or session['checks']+int(check)>limits['checks']:
        raise ValueError('INVESTIGATION_BUDGET_EXHAUSTED')
    envelope={'investigation_id':session['id'],'scope':session['scope_hash'],
              'knowledge_as_of':session['knowledge_as_of'],'base_revision':session['base_revision'],
              'original_availability':'available','actual_returned_refs':refs,**result}
    size=len(S.canonical(envelope).encode())
    if session['returned_bytes']+size>limits['bytes']: raise ValueError('INVESTIGATION_BYTES_EXHAUSTED')
    import research_evidence as RE
    appids=sorted({a for c in cases for a in c.get('application_ids',[])})
    # Existing holdout and usage consumers see the exposure too. It is committed
    # before the response; an exception after this is still a discovery exposure.
    RE.reserve_usage(session['id'],appids,source_refs={'scope':session['scope_hash']},purpose='cause_investigation')
    for case in cases:
        origin=S.digest([case['account_scope'],case['code'],case['confidence_for'],case['origin_at']])
        db.execute('INSERT OR IGNORE INTO exposure VALUES (?,?,?)',(origin,session['id'],S.now()))
        if case.get('run_attempt_id'):
            db.execute('INSERT OR IGNORE INTO exposure VALUES (?,?,?)',(exposure_identity(case),session['id'],S.now()))
    S.put(db,'investigation',session['id'],{**session,'queries':session['queries']+int(query),
          'checks':session['checks']+int(check),'returned_bytes':session['returned_bytes']+size,
          'exposed_refs':sorted(set(session['exposed_refs'])|set(refs))})
    for ref in refs:
        kind,key=ref.split('/',1)
        S.link(db,'investigation',session['id'],kind,key)
    return {**envelope,'consumed_budget':{'queries':session['queries']+int(query),'checks':session['checks']+int(check),'bytes':session['returned_bytes']+size}}


def get_investigation_context(investigation_id):
    import investigation_checks as IC
    import change_registry
    target_registry=change_registry.targets()
    with S.transaction() as db:
        session=_session(investigation_id,db)
        cases=_cases(session,db)
        return _return(db,session,{'question':session['question'],'request':session.get('request'),'cases':cases,
              'tools':list(TOOLS),'registered_checks':list(IC.CHECKS),'change_targets':list(target_registry.values()),
              'completeness':'fixed_scope_only','limits':session['limits'],
              'evidence_semantics':{'chain':'defined','input':'delivered_predecessors','stage':'submitted_and_accepted','execution':'actually_consumed_or_blocked'}},
              ['opportunity/'+c['id'] for c in cases],query=True)


def query_investigation(investigation_id, record_type='evidence', *, cursor=None, limit=20, relation=None, depth=1, target_id=None):
    if record_type not in {'evidence','opportunity','assessment','occurrence','change_target'}: raise ValueError('UNREGISTERED_QUERY_TYPE')
    if relation not in (None,'required_content','required_state'): raise ValueError('UNREGISTERED_RELATION')
    with S.transaction() as db:
        session=_session(investigation_id,db)
        if not isinstance(limit,int) or not 1<=limit<=min(50,session['limits']['rows']): raise ValueError('QUERY_ROW_LIMIT')
        if not isinstance(depth,int) or not 1<=depth<=session['limits']['depth']: raise ValueError('QUERY_DEPTH_LIMIT')
        cases=_cases(session,db)
        ids=sorted(set([c['id'] for c in cases] if record_type=='opportunity' else
                      [r for c in cases for r in c.get('evidence_refs' if record_type=='evidence' else 'assessment_ids',[])]))
        if record_type=='occurrence':
            ids=[x[0] for x in db.execute("SELECT id FROM records WHERE kind='occurrence' AND owner=? ORDER BY id LIMIT ?",(session['issue_id'],session['limits']['rows']+1))]
        if record_type=='change_target':
            import change_registry as R
            from pathlib import Path
            import common as C
            if R.base_revision()!=session['base_revision']:raise ValueError('INVESTIGATION_BASE_CHANGED')
            target=R.targets().get(target_id)
            if not target:raise ValueError('REGISTERED_TARGET_ID_REQUIRED')
            content={**target}
            if target['adapter']=='source_patch':
                path=Path(C.AGENT)/target['path']
                if path.is_symlink() or not path.resolve().is_relative_to(Path(C.AGENT).resolve()):raise ValueError('TARGET_PATH_FORBIDDEN')
                content['source']=path.read_text()
                content['source_hash']=S.digest(content['source'])
            key='target-'+S.digest([investigation_id,content])[:40]
            S.save_once('change_target',key,{'owner':investigation_id,'content':content},conn=db)
            S.link(db,'investigation',investigation_id,'change_target',key)
            ids=[key]
        truncated=False
        if relation and ids and record_type!='change_target':
            seed=','.join('(?,?,0)' for _ in ids)
            arguments=[value for identifier in ids for value in (record_type,identifier)]
            reached=db.execute(f"WITH RECURSIVE reach(kind,id,depth) AS (VALUES {seed} UNION SELECT l.target_kind,l.target_id,r.depth+1 FROM links l JOIN reach r ON l.owner_kind=r.kind AND l.owner_id=r.id WHERE l.relation=? AND r.depth<?) SELECT DISTINCT id FROM reach WHERE kind=? ORDER BY id LIMIT ?",
                               [*arguments,relation,depth,record_type,session['limits']['rows']+1]).fetchall()
            truncated=len(reached)>session['limits']['rows'];ids=[x[0] for x in reached[:session['limits']['rows']]]
        start=0
        scope=S.digest([session['scope_hash'],record_type,relation,depth,target_id])
        if cursor:
            token=S.get('query_cursor',cursor,conn=db)
            if not token or token['scope']!=scope or token['owner']!=investigation_id: raise ValueError('STALE_OR_FOREIGN_CURSOR')
            start=token['offset']
        selected=ids[start:start+limit]
        rows=[]
        for key in selected:
            value=_frozen(record_type,key,session,db)
            rows.append(value or {'id':key,'availability':'unavailable'})
        next_cursor=None
        if start+limit<len(ids):
            next_cursor='cursor-'+S.digest([scope,start+limit])
            S.save_once('query_cursor',next_cursor,{'scope':scope,'offset':start+limit,'owner':investigation_id},conn=db)
            S.link(db,'investigation',investigation_id,'query_cursor',next_cursor)
        qid='query-'+S.digest([investigation_id,scope,start,selected])
        result={'query_id':qid,'items':rows,'next_cursor':next_cursor,'total':len(ids),'returned':len(rows),
                'completeness':'partial' if start or next_cursor or truncated else 'complete_for_fixed_scope','traversal_limit_reached':truncated,
                'query_definition':{'type':record_type,'relation':relation,'depth':depth,'scope':scope},
                'absence_meaning':'not_returned_does_not_prove_nonexistence'}
        S.save_once('query',qid,{'owner':investigation_id,'scope':scope,'result_hash':S.digest(result),'returned_ids':selected},conn=db)
        S.link(db,'investigation',investigation_id,'query',qid)
        return _return(db,session,result,[record_type+'/'+x for x in selected],query=True)


def _select(value, path):
    # Structured field selection inside already exposed data, never a file path.
    if not isinstance(path,list) or len(path)>12: raise ValueError('INVALID_FIELD_PATH')
    for key in path:
        if isinstance(value,dict) and isinstance(key,str): value=value[key]
        elif isinstance(value,list) and isinstance(key,int) and 0<=key<len(value): value=value[key]
        else: raise ValueError('INVALID_FIELD_PATH')
    return copy.deepcopy(value)


def run_investigation_check(investigation_id, function_id, inputs, arguments=None):
    import investigation_checks as IC
    with S.transaction() as db:
        session=_session(investigation_id,db)
        _cases(session,db)
        if not isinstance(inputs,list) or not 1<=len(inputs)<=50: raise ValueError('CHECK_INPUTS_REQUIRED')
        values=[]
        for item in inputs:
            ref=item['ref']
            if ref not in session['exposed_refs']: raise ValueError('CHECK_REFERENCE_NOT_EXPOSED')
            kind,identifier=ref.split('/',1)
            source=_frozen(kind,identifier,session,db)
            if source is None: raise ValueError('CHECK_SOURCE_UNAVAILABLE')
            values.append(_select(source,item.get('path',[])))
        result=IC.execute(function_id,values,arguments)
        cid='check-'+S.digest([investigation_id,function_id,inputs,arguments,result])
        S.save_once('check',cid,{'owner':investigation_id,'inputs':inputs,**result},conn=db)
        session={**session,'last_progress_at':S.now()}
        return _return(db,session,{'check_id':cid,**result,'completeness':'provided_inputs_only'},['check/'+cid],check=True)


def submit_diagnosis(investigation_id, diagnosis):
    with S.transaction() as db:
        session=_session(investigation_id,db)
        _cases(session,db)
        for key in ('question','explanation','uncertainties','alternatives','checks','refs','conclusion'):
            if key not in diagnosis: raise ValueError('DIAGNOSIS_REQUIRED:'+key)
        if diagnosis['conclusion'] not in {'confirmed_defect','limited_support','undetermined','no_defect'}:
            raise ValueError('INVALID_DIAGNOSIS_CONCLUSION')
        if not isinstance(diagnosis['alternatives'],list) or not diagnosis['alternatives']:
            raise ValueError('COMPETING_EXPLANATION_REQUIRED')
        for alternative in diagnosis['alternatives']:
            if any(not alternative.get(k) for k in ('explanation','expected_result','distinguishing_check')):
                raise ValueError('DISTINGUISHING_TEST_REQUIRED')
        if not diagnosis['checks']: raise ValueError('EXECUTED_CHECK_REQUIRED')
        checks=[]
        for cid in diagnosis['checks']:
            check=S.get('check',cid,conn=db)
            if not check or check['owner']!=investigation_id: raise ValueError('CHECK_SCOPE_MISMATCH')
            checks.append(check)
        if any(ref not in session['exposed_refs'] for ref in diagnosis['refs']): raise ValueError('DIAGNOSIS_UNEXPOSED_REFERENCE')
        if diagnosis['conclusion']=='confirmed_defect' and not any(c['result']['verdict']=='fail' for c in checks):
            raise ValueError('REPRODUCIBLE_CONTRACT_FAILURE_REQUIRED')
        identifier='diagnosis-'+S.digest([investigation_id,diagnosis])[:40]
        row=S.save_once('diagnosis',identifier,{'owner':investigation_id,'investigation_id':investigation_id,
                        **diagnosis,'acceptance':'schema_and_recorded_checks_only','causal_proof':False},conn=db)
        state='resolved' if diagnosis['conclusion']=='no_defect' else 'diagnosed'
        S.put(db,'investigation',investigation_id,{**session,'state':state,'diagnosis_id':identifier,'last_progress_at':S.now(),
              **({'closed_at':S.now()} if state=='resolved' else {})})
        issue=S.get('issue',session['issue_id'],conn=db)
        S.put(db,'issue',issue['id'],{**issue,'state':state,'diagnosis_id':identifier,'last_progress_at':S.now(),
              **({'closed_at':S.now()} if state=='resolved' else {})})
        S.link(db,'diagnosis',identifier,'investigation',investigation_id)
        S.link(db,'investigation',investigation_id,'diagnosis',identifier)
        for check in checks: S.link(db,'diagnosis',identifier,'check',check['id'])
        return row
