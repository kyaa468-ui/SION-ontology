"""Reviewable changes tied to a diagnosis, fixed base and registered owner."""
from __future__ import annotations
import difflib
from pathlib import Path

import common as C
import improvement_store as S


def base_revision():
    import learning_proposals as L
    base=L.target_snapshot()
    return {k:base[k] for k in ('definition_hash','settings_hash')}


def targets():
    import learning_proposals as L
    base=L.target_snapshot()
    values={x['target_id']:{**x,'change_class':'strategy','adapter':'policy'} for x in base['targets']}
    for name,content in base['sources'].items():
        values['source.'+name]={'target_id':'source.'+name,'path':'src/'+name,
                                'change_class':'implementation','adapter':'source_patch','before_hash':S.digest(content)}
    for name in base['prompts']:
        values['prompt.'+name]={'target_id':'prompt.'+name,'path':name,'change_class':'interpretation','adapter':'source_patch'}
    for row in S.config().get('registered_change_targets',[]):
        if row['target_id'] in values: raise ValueError('DUPLICATE_CHANGE_TARGET')
        values[row['target_id']]=row
    for identifier,settings in S.config().get('source_deployment_targets',{}).items():
        if identifier not in values or values[identifier]['adapter']!='source_patch':raise ValueError('UNREGISTERED_DEPLOYMENT_TARGET')
        if set(settings)-{'deployment_mode','ai_impact'}:raise ValueError('INVALID_DEPLOYMENT_SETTINGS')
        values[identifier].update(settings)
    return values


def submit_change(investigation_id, change):
    from investigation import _session
    registry=targets()
    current=base_revision()
    required=('diagnosis_id','goal','invariants','alternatives','selection_reason','target_id',
              'base_revision','scope','expected_difference','validation','observation','rollback','patch')
    for field in required:
        if not change.get(field): raise ValueError('CHANGE_REQUIRED:'+field)
    target=registry.get(change['target_id'])
    if target is None: raise ValueError('UNREGISTERED_CHANGE_TARGET')
    if change['base_revision']!=current: raise ValueError('STALE_CHANGE_BASE')
    alternatives=change['alternatives']
    if not isinstance(alternatives,list) or not any(a.get('kind')=='retain_current' for a in alternatives):
        raise ValueError('RETAIN_CURRENT_ALTERNATIVE_REQUIRED')
    for option in alternatives:
        if any(not option.get(k) for k in ('description','impact','assumptions','validation','cost','rollback')):
            raise ValueError('CHANGE_ALTERNATIVE_INCOMPLETE')
    patch=change['patch']
    row={**change,'change_class':target['change_class'],'adapter':target['adapter']}
    # A caller cannot reclassify a strategy parameter as a deterministic defect.
    if change.get('change_class',target['change_class'])!=target['change_class']:
        raise ValueError('CHANGE_CLASSIFICATION_MISMATCH')
    if target['adapter']=='source_patch':
        path=Path(C.AGENT)/target['path']
        if not path.resolve().is_relative_to(Path(C.AGENT).resolve()) or path.is_symlink(): raise ValueError('TARGET_PATH_FORBIDDEN')
        before=path.read_text()
        after=patch.get('content')
        if not isinstance(after,str) or len(after.encode())>512*1024: raise ValueError('FINITE_PATCH_REQUIRED')
        if patch.get('before_hash')!=S.digest(before): raise ValueError('PATCH_BASE_MISMATCH')
        row.update(target_path=target['path'],before_content=before,
                   diff=''.join(difflib.unified_diff(before.splitlines(True),after.splitlines(True),fromfile='a/'+target['path'],tofile='b/'+target['path'])))
    elif target['adapter']=='policy':
        if not patch.get('proposal_id'): raise ValueError('VALIDATABLE_POLICY_PROPOSAL_REQUIRED')
    else:
        raise ValueError('CHANGE_ADAPTER_UNSUPPORTED')
    with S.transaction() as db:
        session=_session(investigation_id,db)
        diagnosis=S.get('diagnosis',change['diagnosis_id'],conn=db)
        if not diagnosis or diagnosis['owner']!=investigation_id: raise ValueError('DIAGNOSIS_SCOPE_MISMATCH')
        if diagnosis['conclusion'] not in {'confirmed_defect','limited_support'}: raise ValueError('DIAGNOSIS_NOT_READY_FOR_CHANGE')
        identifier='change-'+S.digest([investigation_id,row])[:40]
        old=S.get('change',identifier,conn=db)
        if old:return old
        saved=S.save_once('change',identifier,{**row,'owner':investigation_id,'state':'validation_pending',
                          'last_progress_at':S.now(),'patch_hash':S.digest(patch)},conn=db)
        S.link(db,'change',identifier,'diagnosis',diagnosis['id'])
        S.put(db,'investigation',investigation_id,{**session,'state':'validation_pending','change_id':identifier,'last_progress_at':S.now()})
        return saved


def submit_tuning(changes, summary, lessons=None):
    """Legacy tuning now creates an investigation request; it cannot apply."""
    payload={'changes':changes,'summary':str(summary),'lessons':lessons or []}
    identifier='tuning-request-'+S.digest([C.today().isoformat(),payload])[:40]
    with S.transaction() as db:
        row=S.get('tuning_request',identifier,conn=db) or S.save_once('tuning_request',identifier,{'payload':payload,'state':'investigation_pending',
                        'owner':'legacy_tuning','required_next':['diagnosis','change','validation','application'],
                        'last_progress_at':S.now()},conn=db)
        issue_id='issue-'+S.digest(['tuning_request',identifier])[:40]
        if not S.get('issue',issue_id,conn=db):
            S.put(db,'issue',issue_id,{'owner':identifier,'opportunity_id':None,'request_id':identifier,
                'rule':'tuning_request','question':str(summary),'request':payload,'occurrence_count':1,
                'state':'queued','last_progress_at':S.now(),'causal_status':'not_diagnosed'})
            S.link(db,'issue',issue_id,'tuning_request',identifier)
    C.journal('tune',summary=str(summary)[:300],applied=[],rejected=[],proposal_id=identifier)
    return {'ok':True,'status':row['state'],'proposal_id':identifier,'applied':[],
            'note':'조사·검증 대기 중입니다. 설정과 교훈은 변경하지 않았습니다.'}
