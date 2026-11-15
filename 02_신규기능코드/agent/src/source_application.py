"""Recoverable, single-file source deployment through the shared apply gate.

Only an explicitly registered offline adapter may deploy. Running processes
must be stopped by the operator's deployment system; an unverified claim of
quiescence in an AI proposal is never accepted.
"""
from pathlib import Path
import os
import tempfile

import common as C
import improvement_store as S


def _target(change):
    import change_registry as R
    target=R.targets()[change['target_id']]
    if target.get('deployment_mode')!='offline_single_file':
        raise ValueError('SOURCE_OFFLINE_DEPLOYMENT_ADAPTER_UNSET')
    path=Path(C.AGENT)/change['target_path']
    if path.is_symlink() or not path.resolve().is_relative_to(Path(C.AGENT).resolve()):
        raise ValueError('SOURCE_PATH_FORBIDDEN')
    # This lease is written by the host deployment integration, never exposed as
    # an investigation tool. Without it, the next step is concrete review.
    lease=S.get('deployment_lease',change['target_id'])
    if not lease or lease.get('state')!='quiescent' or S.instant(lease['expires_at'])<=S.instant(S.now()):
        raise ValueError('SOURCE_QUIESCENCE_NOT_CONFIRMED')
    if lease.get('base_revision')!=change['base_revision']:
        raise ValueError('SOURCE_DEPLOYMENT_LEASE_CHANGED')
    return target,path,lease


def _write(path, content):
    # Same-filesystem rename, then fsync both the file and its directory.
    fd,name=tempfile.mkstemp(prefix='.sion-change-',dir=path.parent)
    try:
        with os.fdopen(fd,'w',encoding='utf-8') as stream:
            stream.write(content);stream.flush();os.fsync(stream.fileno())
        os.chmod(name,path.stat().st_mode & 0o777)
        os.replace(name,path)
        directory=os.open(path.parent,os.O_RDONLY)
        try:os.fsync(directory)
        finally:os.close(directory)
    finally:
        if os.path.exists(name):os.unlink(name)


def apply(change_id, *, operator):
    import change_registry as R
    import change_application as A
    import change_observation as O
    import policy_registry as P
    identifier='application-'+S.digest([change_id,'source'])[:40]
    try:
        with P.change_lock():
            change=S.get('change',change_id)
            if not change or change['adapter']!='source_patch':raise ValueError('SOURCE_CHANGE_REQUIRED')
            old=S.get('application',identifier)
            if old and old['state']=='applied':return {**old,'replayed':True}
            target,path,lease=_target(change)
            if S.get('application_hold','global'):raise ValueError('AUTO_APPLICATION_HELD')
            before=change['patch']['before_hash'];after=S.digest(change['patch']['content'])
            current=S.digest(path.read_text())
            if current not in ({before,after} if old else {before}):raise ValueError('SOURCE_REVISION_CONFLICT')
            with S.transaction() as db:
                if old:
                    intent=old
                else:
                    grant,criteria,observation=A._permission(change,operator,db)
                    magnitude=A._magnitude(change,grant,db)
                    if R.base_revision()!=change['base_revision']:raise ValueError('STALE_CHANGE_BASE')
                    validation=S.get('source_validation',change_id,conn=db)
                    if not validation or not validation.get('adoption_eligible'):raise ValueError('SOURCE_VALIDATION_INCOMPLETE')
                    if validation.get('patch_hash')!=change['patch_hash'] or validation.get('base_revision')!=change['base_revision']:
                        raise ValueError('SOURCE_VALIDATION_REVISION_MISMATCH')
                    if validation.get('criteria_hash')!=S.digest(criteria):raise ValueError('SOURCE_VALIDATION_CRITERIA_MISMATCH')
                    if target.get('ai_impact',True) and not validation.get('real_ai_criteria_passed'):
                        raise ValueError('SOURCE_AI_IMPACT_NOT_VALIDATED')
                    intent=S.put(db,'application',identifier,{'owner':change_id,'change_id':change_id,
                        'target_id':change['target_id'],
                        'adapter':'source_patch','state':'intent','operator':operator,'before_revision':change['base_revision'],
                        'before_hash':before,'after_hash':after,'permission_hash':S.digest(grant),
                        'validation_hash':S.digest(validation),'lease_hash':S.digest(lease),'observation_criteria':observation,'magnitude':magnitude})
            if current==before:_write(path,change['patch']['content'])
            if S.digest(path.read_text())!=after:raise ValueError('SOURCE_AFTER_WRITE_MISMATCH')
            with S.transaction() as db:
                result=S.put(db,'application',identifier,{**intent,'state':'applied','after_revision':R.base_revision(),
                             'last_progress_at':S.now(),'activation':'next_process_only'})
                S.put(db,'change',change_id,{**change,'state':'applied','application_id':identifier,'last_progress_at':S.now()})
                S.link(db,'application',identifier,'change',change_id)
                S.link(db,'application',identifier,'source_validation',change_id)
                O.register(db,result);A.enqueue_notification(db,identifier,'applied')
                A.record_quota(db,result,change)
            return result
    except (ValueError,KeyError,TypeError,OSError) as exc:
        return {'id':identifier,'change_id':change_id,'state':'application_pending','reason':str(exc)}


def rollback(application_id, *, operator, reason):
    import change_application as A
    import change_registry as R
    import policy_registry as P
    with P.change_lock():
        row=S.get('application',application_id)
        if not row or row.get('adapter')!='source_patch':raise ValueError('SOURCE_APPLICATION_REQUIRED')
        if row['state']=='rolled_back':return row
        change=S.get('change',row['change_id'])
        _,path,_=_target(change)
        with S.transaction() as db:A._permission_for_rollback(change,operator,db)
        current=S.digest(path.read_text())
        recovery=row['state']=='rollback_intent'
        if (not recovery and R.base_revision()!=row['after_revision']) or current not in ({row['after_hash'],row['before_hash']} if recovery else {row['after_hash']}):
            with S.transaction() as db:S.put(db,'application_hold','global',{'state':'blocked','reason':'rollback_revision_conflict','application_id':application_id})
            return {'state':'rollback_conflict','additional_auto_apply':'blocked'}
        if not recovery:
            with S.transaction() as db:row=S.put(db,'application',application_id,{**row,'state':'rollback_intent','rollback_reason':reason})
        if current==row['after_hash']:_write(path,change['before_content'])
        with S.transaction() as db:
            result=S.put(db,'application',application_id,{**row,'state':'rolled_back','closed_at':S.now()})
            job=S.get('observation_job',application_id,conn=db)
            if job:S.put(db,'observation_job',application_id,{**job,'state':'rolled_back','closed_at':S.now()})
            S.put(db,'change',change['id'],{**change,'state':'rolled_back','closed_at':S.now()})
            A.enqueue_notification(db,application_id,'rolled_back')
        return result
