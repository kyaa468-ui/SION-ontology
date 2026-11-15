"""Applied, actually consumed, and improved are separate evidence states."""
from __future__ import annotations
import improvement_store as S


def validate_criteria(criteria,*,for_application=False):
    if any(k not in criteria for k in ('end_at','minimum_uses','max_recurrences','max_side_effects')):
        raise ValueError('OBSERVATION_CRITERIA_UNSET')
    end=S.instant(criteria['end_at'])
    for key in ('minimum_uses','max_recurrences','max_side_effects'):
        if type(criteria[key]) is not int or criteria[key]<(1 if key=='minimum_uses' else 0):raise ValueError('INVALID_OBSERVATION_CRITERIA')
    if for_application:
        if end<=S.instant(S.now()):raise ValueError('OBSERVATION_WINDOW_ALREADY_ENDED')
        if not criteria.get('behavior_checks') or any(k not in criteria for k in ('recurrence_rules','side_effect_rules')):
            raise ValueError('OBSERVATION_BEHAVIOR_CRITERIA_UNSET')
    return criteria


def register(db, application):
    identifier=application['id']
    criteria=application['observation_criteria']
    validate_criteria(criteria)
    old=S.get('observation_job',identifier,conn=db)
    if old: return old
    row=S.put(db,'observation_job',identifier,{'owner':identifier,'application_id':identifier,
        'state':'applied','criteria':criteria,'revision_bundle':application['after_revision'],
        'uses':0,'recurrences':0,'side_effects':0,'missing':0,'last_progress_at':S.now()})
    S.link(db,'observation_job',identifier,'application',identifier)
    return row


def consume(revision_bundle, *, attempt_id, judgment_id, stage, evidence_ref):
    with S.transaction() as db:
        active=db.execute("SELECT body,hash FROM records WHERE kind='observation_job' AND state IN ('applied','actual_use_confirmed','observing') ORDER BY id")
        for raw in active:
            job=S.decode(raw)
            if S.instant(S.now())>S.instant(job['criteria']['end_at']):continue
            if job['revision_bundle']!=revision_bundle: continue
            identifier='consumption-'+S.digest([job['id'],attempt_id,judgment_id,stage,evidence_ref])[:40]
            if S.get('change_consumption',identifier,conn=db): continue
            S.put(db,'change_consumption',identifier,{'owner':job['id'],'application_id':job['id'],
                'attempt_id':attempt_id,'judgment_id':judgment_id,'stage':stage,'evidence_ref':evidence_ref,
                'revision_bundle':revision_bundle})
            S.link(db,'observation_job',job['id'],'change_consumption',identifier)
            S.put(db,'observation_job',job['id'],{**job,'state':'actual_use_confirmed','uses':job['uses']+1,'last_progress_at':S.now()})


def record(application_id, event_id, *, consumption_id, expected_behavior, recurrence, side_effect, complete, evidence_refs):
    """Trusted runtime evaluator; not an AI self-evaluation tool."""
    if any(type(v) is not bool for v in (expected_behavior,recurrence,side_effect,complete)):
        raise ValueError('OBSERVATION_BOOLEAN_REQUIRED')
    if not evidence_refs: raise ValueError('OBSERVATION_EVIDENCE_REQUIRED')
    with S.transaction() as db:
        job=S.get('observation_job',application_id,conn=db)
        use=S.get('change_consumption',consumption_id,conn=db)
        if not job or not use or use['owner']!=application_id: raise ValueError('ACTUAL_CONSUMPTION_REQUIRED')
        payload={'owner':application_id,'consumption_id':consumption_id,'expected_behavior':expected_behavior,
                 'recurrence':recurrence,'side_effect':side_effect,'complete':complete,'evidence_refs':evidence_refs}
        if S.get('observation_event',event_id,conn=db):
            return S.save_once('observation_event',event_id,payload,conn=db)
        if job['state'] in {'improved','rolled_back','deteriorated','insufficient_data'}:raise ValueError('OBSERVATION_ALREADY_ENDED')
        row=S.save_once('observation_event',event_id,payload,conn=db)
        S.link(db,'observation_job',application_id,'observation_event',event_id)
        S.link(db,'observation_event',event_id,'change_consumption',consumption_id)
        for ref in evidence_refs:
            if isinstance(ref,dict) and ref.get('kind')=='opportunity':S.link(db,'observation_event',event_id,'opportunity',ref['id'])
        S.put(db,'observation_job',application_id,{**job,'state':'observing','recurrences':job['recurrences']+int(recurrence),
              'side_effects':job['side_effects']+int(side_effect),'missing':job['missing']+int(not complete),
              'behavior_passes':job.get('behavior_passes',0)+int(expected_behavior and complete),
              'behavior_observations':job.get('behavior_observations',0)+1,'last_progress_at':S.now()})
        return row


def evaluate(application_id, *, at=None):
    with S.transaction() as db:
        job=S.get('observation_job',application_id,conn=db)
        if not job: raise ValueError('OBSERVATION_NOT_FOUND')
        if job['state'] in {'improved','rolled_back','deteriorated','insufficient_data'}: return job
        criteria=job['criteria']; state='observing' if job['uses'] else 'applied'
        if job['recurrences']>criteria['max_recurrences'] or job['side_effects']>criteria['max_side_effects']:
            state='deteriorated'
        elif S.instant(at or S.now()) >= S.instant(criteria['end_at']):
            enough=(job['uses']>=criteria['minimum_uses'] and not job['missing'] and
                    job.get('behavior_observations',0)>=criteria['minimum_uses'])
            state='improved' if enough and job.get('behavior_passes',0)==job['behavior_observations'] else 'insufficient_data'
        return S.put(db,'observation_job',application_id,{**job,'state':state,
                    **({'closed_at':at or S.now()} if state in {'improved','deteriorated','insufficient_data'} else {})})
