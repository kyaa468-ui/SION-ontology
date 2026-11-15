"""Single policy application gate shared by CLI, workers and future UI.

Permissions and criteria are server configuration, never fields supplied by AI.
The existing intent/pointer/commit protocol performs recoverable policy writes.
"""
from __future__ import annotations
from contextvars import ContextVar
from datetime import timedelta

import common as C
import improvement_store as S

AUTHORIZED = ContextVar('authorized_change_application',default=False)


def _permission(change, operator, db):
    cfg=S.config()
    grants=[g for g in cfg['auto_apply'] if g.get('operator')==operator and
            change['target_id'] in g.get('targets',[]) and change['change_class'] in g.get('classes',[])]
    if not grants: raise ValueError('APPLICATION_PERMISSION_UNSET')
    grant=grants[0]
    for field in ('scope','max_changes_per_day','cooldown_seconds','max_cumulative_changes','expires_at'):
        if field not in grant: raise ValueError('APPLICATION_PERMISSION_INCOMPLETE:'+field)
    if change['scope']!=grant['scope']: raise ValueError('APPLICATION_SCOPE_NOT_AUTHORIZED')
    if S.instant(grant['expires_at'])<=S.instant(S.now()): raise ValueError('APPLICATION_PERMISSION_EXPIRED')
    history=db.execute("SELECT body,hash FROM records WHERE kind='application' AND state IN ('applied','rolled_back','revoked') AND json_extract(body,'$.operator')=? AND json_extract(body,'$.target_id')=?",(operator,change['target_id']))
    rows=[S.decode(x) for x in history]
    quota=S.get('application_quota',_quota_id(operator,change['target_id']),conn=db) or {}
    total=quota.get('total_count',len(rows))
    if total>=grant['max_cumulative_changes']: raise ValueError('APPLICATION_CUMULATIVE_LIMIT')
    today=[r for r in rows if S.instant(r['origin_at']).date()==S.instant(S.now()).date()]
    day=S.instant(S.now()).date().isoformat()
    daily=quota.get('day_count',0) if quota.get('day')==day else 0 if quota else len(today)
    if daily>=grant['max_changes_per_day']: raise ValueError('APPLICATION_DAILY_LIMIT')
    last=quota.get('last_at') or (max(r['origin_at'] for r in rows) if rows else None)
    if last and S.instant(S.now())-S.instant(last)<timedelta(seconds=grant['cooldown_seconds']):
        raise ValueError('APPLICATION_COOLDOWN')
    criteria=cfg['validation_criteria'].get(change['change_class'])
    observation=cfg['observation_criteria'].get(change['target_id'])
    if not criteria: raise ValueError('VALIDATION_CRITERIA_UNSET')
    if not observation: raise ValueError('OBSERVATION_CRITERIA_UNSET')
    import change_observation
    change_observation.validate_criteria(observation,for_application=True)
    if change['validation'].get('criteria_hash')!=S.digest(criteria): raise ValueError('VALIDATION_CRITERIA_CHANGED')
    if change['observation']!=observation: raise ValueError('OBSERVATION_CRITERIA_CHANGED')
    return grant,criteria,observation


def _quota_id(operator,target):return 'quota-'+S.digest([operator,target])[:40]


def _magnitude(change,grant,db,proposal=None):
    import math
    limits=grant.get('change_limits')
    if not isinstance(limits,dict):raise ValueError('APPLICATION_CHANGE_MAGNITUDE_UNSET')
    effect=(proposal or {}).get('effect',{})
    if effect.get('operation') in {'set_parameter','add_confidence'}:
        import tuning
        amount=effect.get('amount')
        if effect['operation']=='set_parameter':
            key=effect.get('key') or proposal.get('target_snapshot',{}).get('key')
            value=effect.get('value',effect.get('to'))
            amount=value-tuning.get_value(C.params(),key)
            if not limits.get('minimum',-math.inf)<=value<=limits.get('maximum',math.inf):raise ValueError('APPLICATION_VALUE_BOUND')
        if type(amount) not in (int,float) or not math.isfinite(amount):raise ValueError('APPLICATION_MAGNITUDE_UNAVAILABLE')
        amount=abs(amount);kind='numeric'
        maximum=limits.get('max_absolute_step');cumulative=limits.get('max_cumulative_step')
    else:
        before=change.get('before_content','');after=change['patch'].get('content',effect.get('text'))
        if not isinstance(after,str):raise ValueError('APPLICATION_PATCH_SIZE_UNAVAILABLE')
        amount=len(after.encode());kind='patch_bytes'
        maximum=limits.get('max_patch_bytes');cumulative=limits.get('max_cumulative_patch_bytes')
        changed=sum(line.startswith(('+','-')) and not line.startswith(('+++','---')) for line in change.get('diff','').splitlines())
        if type(limits.get('max_changed_lines')) is not int or changed>limits['max_changed_lines']:raise ValueError('APPLICATION_PATCH_LINE_BOUND')
    if any(type(v) not in (int,float) or not math.isfinite(v) or v<0 for v in (maximum,cumulative)):
        raise ValueError('APPLICATION_CHANGE_MAGNITUDE_UNSET')
    quota=S.get('application_quota',_quota_id(grant['operator'],change['target_id']),conn=db) or {}
    if amount>maximum or quota.get(kind,0)+amount>cumulative:raise ValueError('APPLICATION_CHANGE_MAGNITUDE_EXCEEDED')
    return {'kind':kind,'amount':amount}


def record_quota(db,application,change):
    key=_quota_id(application['operator'],change['target_id']);old=S.get('application_quota',key,conn=db) or {}
    day=S.instant(S.now()).date().isoformat();magnitude=application.get('magnitude') or {'kind':'numeric','amount':0}
    return S.put(db,'application_quota',key,{**old,'owner':application['operator'],'target_id':change['target_id'],
        'total_count':old.get('total_count',0)+1,'day':day,'day_count':old.get('day_count',0)+1 if old.get('day')==day else 1,
        magnitude['kind']:old.get(magnitude['kind'],0)+magnitude['amount'],'last_at':S.now(),'state':'checkpoint'})


def apply_prepared(adoption_id, expected_revision, request_key, *, operator):
    import policy_adoption as A
    import policy_registry as P
    import change_registry as R
    identifier='application-'+S.digest([adoption_id,request_key])[:40]
    try:
        with P.change_lock():
            A._operator(operator)
            prepared=P.load('adoptions',adoption_id)
            if not prepared: raise ValueError('PREPARATION_REQUIRED')
            with S.transaction() as db:
                old=S.get('application',identifier,conn=db)
                request_hash=S.digest([adoption_id,expected_revision,request_key,operator])
                if old and old['request_hash']!=request_hash: raise ValueError('APPLICATION_ID_CONFLICT')
                if old and old['state']=='applied':
                    return {**old['activation'],'application_id':identifier,'application_state':'applied','replayed':True}
                if S.get('application_hold','global',conn=db): raise ValueError('AUTO_APPLICATION_HELD')
                matches=db.execute("SELECT body,hash FROM records WHERE kind='change' AND json_extract(body,'$.validation.validation_id')=?",(prepared['validation_id'],)).fetchall()
                if len(matches)!=1: raise ValueError('DIAGNOSED_CHANGE_REQUIRED')
                change=S.decode(matches[0])
                if not old:
                    grant,criteria,observation=_permission(change,operator,db)
                else:
                    # Recovery finishes a durable authorized intent; it does not
                    # issue a new grant or silently replace its criteria.
                    grant,criteria,observation={}, {}, old['observation_criteria']
                if change['adapter']!='policy': raise ValueError('ADAPTER_MISMATCH')
                if not old and change['base_revision']!=R.base_revision(): raise ValueError('STALE_CHANGE_BASE')
                manifest,result=A._validated_result(prepared['validation_id'])
                if change['patch']['proposal_id']!=manifest['proposal_snapshot'].get('proposal_id'):
                    raise ValueError('VALIDATION_CHANGE_MISMATCH')
                if manifest['proposal_snapshot'].get('target_ref',{}).get('target_id',manifest['proposal_snapshot'].get('target_id')) not in (None,change['target_id']):
                    raise ValueError('VALIDATION_TARGET_MISMATCH')
                magnitude=(old or {}).get('magnitude') or _magnitude(change,grant,db,manifest['proposal_snapshot'])
                intent=old or S.put(db,'application',identifier,{'owner':change['id'],'change_id':change['id'],
                    'target_id':change['target_id'],
                    'adoption_id':adoption_id,'operator':operator,'state':'intent','request_hash':request_hash,
                    'expected_revision':expected_revision,'before_revision':change['base_revision'],
                    'permission_hash':S.digest(grant),'criteria_hash':S.digest(criteria),
                    'observation_criteria':observation,'validation_hash':P.digest(result),'request_key':request_key,'magnitude':magnitude})
            token=AUTHORIZED.set(True)
            try: applied=A._adopt_policy_impl(adoption_id,expected_revision,request_key,operator=operator)
            finally: AUTHORIZED.reset(token)
            if applied.get('status')!='active':
                return {**applied,'application_id':identifier,'state':'application_pending'}
            with S.transaction() as db:
                row=S.put(db,'application',identifier,{**intent,'state':'applied','activation':applied,
                      'after_revision':R.base_revision(),'last_progress_at':S.now()})
                change=S.get('change',change['id'],conn=db)
                S.put(db,'change',change['id'],{**change,'state':'applied','application_id':identifier,'last_progress_at':S.now()})
                import change_observation as O
                O.register(db,row)
                S.link(db,'application',identifier,'change',change['id'])
                record_quota(db,row,change)
                enqueue_notification(db,identifier,'applied')
                return {**applied,'application_id':identifier,'application_state':'applied'}
    except (ValueError,KeyError,TypeError) as exc:
        return {'id':identifier,'state':'application_pending','status':'rejected','error':str(exc),'reason':str(exc),'operational_activation':False}


def apply_change(change_id, *, operator='improvement-worker'):
    change=S.get('change',change_id)
    if not change: raise ValueError('UNKNOWN_CHANGE')
    if change['adapter']=='source_patch':
        import source_application
        return source_application.apply(change_id,operator=operator)
    if change['adapter']!='policy':raise ValueError('UNREGISTERED_CHANGE_ADAPTER')
    import policy_adoption as A
    spec=change['validation']
    if not spec.get('validation_id'): return {'state':'validation_pending','reason':'validation_not_registered'}
    period=change['scope'].get('effective_period',{})
    if not period: return {'state':'application_pending','reason':'effective_period_unset'}
    token=AUTHORIZED.set(True)
    try:
        prepared=A.prepare_adoption(spec['validation_id'],change_id,operator=operator,
                   effective_from=period['start'],effective_until=period['end'],scope=change['scope'].get('policy_scope'))
        if prepared.get('status')!='prepared': return prepared
        return apply_prepared(prepared['adoption_id'],prepared['expected_revision'],change_id,operator=operator)
    finally:
        AUTHORIZED.reset(token)


def rollback(application_id, *, operator, reason):
    import policy_registry as P
    import policy_adoption as A
    import change_registry as R
    initial=S.get('application',application_id)
    if initial and initial.get('adapter')=='source_patch':
        import source_application
        return source_application.rollback(application_id,operator=operator,reason=reason)
    with P.change_lock():
        row=S.get('application',application_id)
        if not row or row['state']!='applied': raise ValueError('APPLICATION_NOT_ACTIVE')
        if R.base_revision()!=row['after_revision']:
            with S.transaction() as db:
                S.put(db,'application_hold','global',{'state':'blocked','reason':'rollback_revision_conflict','application_id':application_id})
            return {'state':'rollback_conflict','additional_auto_apply':'blocked'}
        change=S.get('change',row['change_id'])
        with S.transaction() as db:
            grant,_,_=_permission_for_rollback(change,operator,db)
        token=AUTHORIZED.set(True)
        try:
            result=A._rollback_policy_impl(row['activation']['activation_id'],P.revision(A._read_params()),
                           'rollback-'+application_id,operator=operator,reason=reason)
        finally: AUTHORIZED.reset(token)
        if result.get('status') not in {'rolled_back','rollback'}: return result
        with S.transaction() as db:
            S.put(db,'application',application_id,{**row,'state':'rolled_back','rollback':result,'closed_at':S.now()})
            job=S.get('observation_job',application_id,conn=db)
            if job: S.put(db,'observation_job',application_id,{**job,'state':'rolled_back','closed_at':S.now()})
            S.put(db,'change',change['id'],{**change,'state':'rolled_back','closed_at':S.now()})
            enqueue_notification(db,application_id,'rolled_back')
        return result


def _permission_for_rollback(change,operator,db):
    grants=[g for g in S.config()['auto_apply'] if g.get('operator')==operator and
            g.get('rollback') is True and change['target_id'] in g.get('targets',[])]
    if not grants: raise ValueError('ROLLBACK_PERMISSION_UNSET')
    if S.instant(grants[0]['expires_at'])<=S.instant(S.now()): raise ValueError('ROLLBACK_PERMISSION_EXPIRED')
    return grants[0],None,None


def revoke(expected_revision,request_key,*,operator,reason):
    import policy_registry as P
    import policy_adoption as A
    with P.change_lock():
        A._operator(operator)
        params=A._read_params()
        if P.revision(params)!=expected_revision:return {'status':'rejected','error':'STALE_REVISION'}
        activation=(params.get(P.MARKER) or {}).get('activation_id')
        with S.transaction(readonly=True) as db:
            rows=[] if db is None else db.execute("SELECT body,hash FROM records WHERE kind='application' AND json_extract(body,'$.activation.activation_id')=?",(activation,)).fetchall()
        if len(rows)!=1:return {'status':'rejected','error':'UNIFIED_APPLICATION_REQUIRED'}
        application=S.decode(rows[0]);change=S.get('change',application['change_id'])
        with S.transaction() as db:
            try: _permission_for_rollback(change,operator,db)
            except ValueError as exc:return {'status':'rejected','error':str(exc)}
        token=AUTHORIZED.set(True)
        try: result=A._revoke_policy_impl(expected_revision,request_key,operator=operator,reason=reason)
        finally: AUTHORIZED.reset(token)
        if result.get('status')=='revoked':
            with S.transaction() as db:
                S.put(db,'application',application['id'],{**application,'state':'revoked','closed_at':S.now(),'revocation':result})
        return result


def enqueue_notification(db,application_id,state):
    # Delivery is deliberately separate from application. No channel configured
    # means no external message and no invented recipient.
    for channel in S.config()['notification_channels']:
        key='notification-'+S.digest([application_id,state,channel['id']])[:40]
        if S.get('notification',key,conn=db):continue
        S.save_once('notification',key,{'owner':application_id,'channel_id':channel['id'],
                    'state':'queued','event':state,'application_id':application_id,'attempts':0,'last_progress_at':S.now()},conn=db)
