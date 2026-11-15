"""Predeclared, deterministic business checks over paired semantic outputs.

An expected value and its specification reference are server configuration.
Schema acceptance and an AI's own confidence are not validity labels.
"""
import math
import improvement_store as S


def _value(row,path):
    if not isinstance(path,list) or not 1<=len(path)<=12:raise ValueError('BUSINESS_CHECK_PATH_REQUIRED')
    for key in path:
        if isinstance(row,dict) and isinstance(key,str):row=row[key]
        elif isinstance(row,list) and type(key) is int:row=row[key]
        else:raise ValueError('BUSINESS_CHECK_PATH_UNAVAILABLE')
    return row


def check(stage,rule):
    if not rule.get('specification_ref'):raise ValueError('EXPECTED_RESULT_SPECIFICATION_REQUIRED')
    value=_value(stage,rule['path'])
    if rule['operator']=='equals':return type(value) is type(rule['expected']) and value==rule['expected']
    if rule['operator']=='range':
        if type(value) not in (int,float) or not math.isfinite(value):return False
        return rule['minimum']<=value<=rule['maximum']
    raise ValueError('UNREGISTERED_BUSINESS_CHECK')


def evaluate(experiment_id):
    experiment=S.get('experiment',experiment_id);result=S.get('experiment_result',experiment_id)
    if not experiment or not result:raise ValueError('SEMANTIC_RESULT_REQUIRED')
    change=S.get('change',experiment['change_id'])
    criteria=experiment['spec']['criteria']
    required=('checks','minimum_candidate_rate','minimum_improvement','maximum_regressions','minimum_pairs')
    reasons=[]
    if any(k not in criteria for k in required) or not criteria.get('checks'):
        reasons.append('business_criteria_unset')
    configured=S.config().get('validation_criteria',{}).get((change or {}).get('change_class'))
    if criteria!=configured:reasons.append('business_criteria_not_registered')
    if not change or change['base_revision']!=experiment.get('base_revision'):reasons.append('change_base_not_bound')
    if result['state']!='completed' or result['result_kind']!='real_ai':reasons.append('complete_real_ai_comparison_required')
    if not reasons:
        import semantic_layer as SL
        try:
            if type(criteria['minimum_pairs']) is not int or criteria['minimum_pairs']<1:raise ValueError()
            if type(criteria['maximum_regressions']) is not int or criteria['maximum_regressions']<0:raise ValueError()
            for key in ('minimum_candidate_rate','minimum_improvement'):
                if type(criteria[key]) not in (int,float) or not math.isfinite(criteria[key]) or not 0<=criteria[key]<=1:raise ValueError()
            if len({r['id'] for r in criteria['checks']})!=len(criteria['checks']):raise ValueError()
            for rule in criteria['checks']:
                if rule['role'] not in SL.ROLES or not rule.get('specification_ref') or not rule.get('path'):raise ValueError()
        except (KeyError,TypeError,ValueError):reasons.append('invalid_business_criteria')
    proof=experiment.get('independent_proof') or {}
    if not proof.get('independent'):reasons.append('independent_pre_outcome_enrollment_required')
    else:
        import validation_holdout as VH
        current=VH.status(proof['validation_id'])
        if not current['independent'] or S.digest(current)!=S.digest(proof):reasons.append('independent_proof_changed')
    grouped={};failures=[]
    if not reasons:
        for descriptor in result['trials']:
            trial=S.get('semantic_trial',descriptor['id']);request=trial['request']
            if not trial['output'].get('model_version_verified') or trial['output'].get('reported_models')!=[experiment['spec']['model_version']]:
                reasons.append('reported_model_version_not_verified')
            key=(request['case_index'],request['repetition'],request['arm'])
            group=grouped.setdefault(key,[])
            rules=[r for r in criteria['checks'] if r['role']==request['role']]
            for rule in rules:
                try:passed=check(trial['stage'],rule)
                except (KeyError,ValueError,TypeError,IndexError):passed=False
                group.append(passed)
                if not passed:failures.append({'trial_id':trial['id'],'check':rule['id']})
        pairs=[]
        for case in range(len(experiment['cases'])):
            for repeat in range(experiment['spec']['repetitions']):
                a=grouped.get((case,repeat,'baseline'),[]);b=grouped.get((case,repeat,'candidate'),[])
                pairs.append({'case_index':case,'repetition':repeat,'baseline':bool(a) and all(a),'candidate':bool(b) and all(b)})
        count=len(pairs);base=sum(p['baseline'] for p in pairs);candidate=sum(p['candidate'] for p in pairs)
        regressions=sum(p['baseline'] and not p['candidate'] for p in pairs)
        rates={'pair_count':count,'baseline_passes':base,'candidate_passes':candidate,
               'candidate_rate':candidate/count if count else None,'paired_improvement':(candidate-base)/count if count else None,
               'regressions':regressions,'market_samples':len(experiment['cases']),
               'repetitions_are_independent_market_samples':False}
        if count<criteria['minimum_pairs']:reasons.append('insufficient_pairs')
        if not count or candidate/count<criteria['minimum_candidate_rate']:reasons.append('candidate_criterion_failed')
        if not count or (candidate-base)/count<criteria['minimum_improvement']:reasons.append('improvement_criterion_failed')
        if regressions>criteria['maximum_regressions']:reasons.append('normal_case_regression')
    else:rates={};pairs=[]
    body={'owner':experiment['change_id'],'state':'passed' if not reasons else 'validation_pending',
          'experiment_id':experiment_id,'experiment_result_hash':S.digest(result),'criteria_hash':S.digest(criteria),
          'base_revision':experiment.get('base_revision'),'patch_hash':(change or {}).get('patch_hash'),
          'candidate_implementation_hash':result.get('candidate_implementation_hash'),
          'adoption_eligible':not reasons,'reasons':reasons,'metrics':rates,'pairs':pairs,'failures':failures,
          'causal_validity':'not_established','profitability_validity':'not_established'}
    with S.transaction() as db:
        row=S.put(db,'semantic_validation',experiment_id,body)
        S.link(db,'semantic_validation',experiment_id,'experiment',experiment_id)
        S.link(db,'semantic_validation',experiment_id,'experiment_result',experiment_id)
        if change:S.link(db,'change',change['id'],'semantic_validation',experiment_id)
        return row
