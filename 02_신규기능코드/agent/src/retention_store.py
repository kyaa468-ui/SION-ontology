"""Scope before decode: completion-based case selection with honest limits."""
from datetime import timedelta
import improvement_store as S
import retention_policy as P


def current_boundary(at=None):
    import market_calendar as M
    point=S.instant(at or S.now())
    calendar=M.find_calendar_snapshot((point.date()-timedelta(days=90)).isoformat(),point.date().isoformat(),point.isoformat(),allow_partial=True)
    if calendar.get('coverage_end','')<point.date().isoformat():return {'status':'calendar_incomplete','deletion_allowed':False}
    return P.boundaries(calendar,point.isoformat())


def recent_cases(*, calendar, at=None, limit=50, cursor=None, maximum_bytes=1024*1024):
    if type(limit) is not int or not 1<=limit<=200:raise ValueError('CASE_PAGE_LIMIT')
    if type(maximum_bytes) is not int or not 1024<=maximum_bytes<=2*1024*1024:raise ValueError('CASE_BYTES_LIMIT')
    boundary=P.boundaries(calendar,at)
    if not boundary['deletion_allowed']:return {**boundary,'items':[],'scope_status':'unavailable'}
    scope=S.digest([{k:v for k,v in boundary.items() if k!='knowledge_as_of'},limit,maximum_bytes]);after='';items=[];used=0
    with S.transaction() as db:
        if cursor:
            saved=S.get('case_cursor',cursor,conn=db)
            if not saved or saved['scope']!=scope:raise ValueError('CASE_CURSOR_EXPIRED_OR_CHANGED')
            after=saved['after']
        selector="kind='opportunity' AND (closed_at IS NULL OR substr(closed_at,1,10)>=?)"
        total=db.execute('SELECT count(*) FROM records WHERE '+selector,(boundary['case_closed_cutoff'],)).fetchone()[0]
        candidates=db.execute('SELECT body,hash,id,length(CAST(body AS BLOB)) AS bytes FROM records WHERE '+selector+' AND id>? ORDER BY id LIMIT ?',
                              (boundary['case_closed_cutoff'],after,limit+1)).fetchall()
        for raw in candidates[:limit]:
            if used+raw['bytes']>maximum_bytes:break
            value=S.decode(raw);items.append({**value,'supported_operations':P.capabilities(value,boundary)});used+=raw['bytes']
        more=len(candidates)>len(items)
        next_cursor=None
        if more and items:
            next_cursor='case-cursor-'+S.digest([scope,items[-1]['id']])[:40]
            S.save_once('case_cursor',next_cursor,{'scope':scope,'after':items[-1]['id'],'state':'completed','closed_at':S.now()},conn=db)
        return {'items':items,'expected_count':total,'returned':len(items),'returned_body_bytes':used,
                'next_cursor':next_cursor,'boundary':boundary,'target_set_incomplete':more,
                'status':'capacity_blocked' if more and not items else 'partial' if more else 'complete',
                'reason':'first_record_exceeds_registered_bytes' if more and not items else None,
                'selection':'completion_window_and_id_order_independent_of_outcomes'}


def supports(case,operation,*,boundary=None):
    boundary=boundary or current_boundary()
    if not boundary['deletion_allowed']:return {'allowed':False,'reason':'calendar_unavailable'}
    support=P.capabilities(case,boundary)
    if support.get(operation):return {'allowed':True,'basis':'registered_window'}
    with S.transaction(readonly=True) as db:
        rows=[] if db is None else db.execute("SELECT r.body,r.hash FROM records r JOIN links l ON l.owner_kind=r.kind AND l.owner_id=r.id WHERE r.kind='retention_pin' AND l.target_kind='opportunity' AND l.target_id=?",(case['id'],))
        for raw in rows:
            pin=S.decode(raw)
            if pin['until']>S.now() and operation in pin.get('operations',[]):return {'allowed':True,'basis':'explicit_pin','pin_id':pin['id']}
    return {'allowed':False,'reason':'expired' if support['availability']=='expired' else 'operation_unavailable'}


def detail_support(origin_at,validation_id=None,*,boundary=None,operation='semantic_reexperiment'):
    """Q3 gate before real calls; an old registration alone is not a live pin."""
    if validation_id:
        pin=S.get('retention_pin','validation:'+validation_id)
        if pin and pin.get('state')=='active' and S.instant(pin['until'])>S.instant(S.now()):
            start,end=pin.get('period',{}).get('start'),pin.get('period',{}).get('end')
            if start and end and S.instant(start)<=S.instant(origin_at)<=S.instant(end) and operation in pin.get('operations',[]):
                return {'allowed':True,'basis':'preregistered_live_validation','pin_id':pin['id']}
    boundary=boundary or current_boundary()
    if not boundary.get('deletion_allowed'):return {'allowed':False,'reason':'calendar_unavailable'}
    if S.instant(origin_at).date().isoformat()>=boundary['detailed_origin_cutoff']:
        return {'allowed':True,'basis':'original_judgment_window'}
    return {'allowed':False,'reason':'detailed_replay_expired'}


def register_validation_support(identifier,spec,registered_at):
    """Host-only finite range registration, before any original inputs are read."""
    point=S.instant(registered_at);start=S.instant(spec['period']['start']);end=S.instant(spec['period']['end'])
    if spec['mode']=='prospective':
        if point>=start or S.instant(S.now())>=start:raise ValueError('DETAIL_RANGE_REGISTRATION_AFTER_START')
        permission={'allowed':True,'basis':'prospective_range_registered_before_start'}
    else:
        permission=detail_support(start.isoformat(),operation='portfolio_reexperiment')
        if not permission['allowed']:raise ValueError('VALIDATION_SUPPORT_PENDING:'+permission['reason'])
    until=max(point,end)+timedelta(days=30)
    body={'owner':identifier,'state':'active','until':until.isoformat(),'wait_until':end.isoformat(),
        'period':{'start':start.isoformat(),'end':end.isoformat()},'scope':spec['scope'],'registered_at':registered_at,
        'spec_hash':S.digest(spec),'operations':['semantic_reexperiment','portfolio_reexperiment'],
        'reason':'registered_validation_detail','admission':permission,'last_progress_at':registered_at}
    import research_capacity
    research_capacity.reserve('validation:'+identifier,identifier)
    with S.transaction() as db:
        old=S.get('retention_pin','validation:'+identifier,conn=db)
        if old:
            if old['spec_hash']!=body['spec_hash']:raise ValueError('VALIDATION_RANGE_CONFLICT')
            return old
        return S.put(db,'retention_pin','validation:'+identifier,body)


def require_validation_support(manifest):
    spec=manifest['spec'];identifier=manifest.get('validation_id')
    permission=detail_support(spec['period']['start'],identifier,operation='portfolio_reexperiment')
    if not permission['allowed']:raise ValueError('VALIDATION_SUPPORT_PENDING:'+permission['reason'])
    if permission['basis']=='preregistered_live_validation':
        pin=S.get('retention_pin',permission['pin_id'])
        if pin.get('spec_hash')!=S.digest(spec):raise ValueError('VALIDATION_RANGE_SPEC_CHANGED')
    return permission


def validation_completed(validation_id,result_id):
    """Close a range only from a hash-verified committed final result."""
    pin=S.get('retention_pin','validation:'+validation_id)
    if not pin:return None
    import validation_store as V
    result=V.load('results',result_id);commit=V.load('result_commits',result_id)
    if not result or not commit or result.get('validation_id')!=validation_id or commit.get('result_hash')!=V.digest(result):
        raise ValueError('VALIDATION_COMPLETION_UNVERIFIED')
    if result.get('status') not in {'complete','completed','insufficient_data','failed'}:return pin
    import research_capacity
    research_capacity.finish('validation:'+validation_id)
    if pin['state']=='completed':return pin
    with S.transaction() as db:
        return S.put(db,'retention_pin',pin['id'],{**pin,'state':'completed','closed_at':S.now(),
            'result_id':result_id,'result_hash':commit['result_hash']})
