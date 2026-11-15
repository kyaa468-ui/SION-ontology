"""Resume a diagnosed change using its registered experiment, never weaker gates."""
import improvement_store as S


def prepare_experiment(change):
    import validation_spec as V
    import validation_dataset as D
    import semantic_prompt_validation as P
    import semantic_experiments as E
    requested=change['validation'].get('experiment_spec')
    validation_id=change['validation'].get('validation_id')
    if not requested or not validation_id:
        return {'state':'validation_pending','reason':'registered_experiment_spec_and_data_required'}
    manifest=V.load_spec(validation_id);inputs=D.ensure_inputs(validation_id)
    frozen,gaps=P.cases(inputs,manifest['proposal_snapshot'])
    if gaps or not frozen:return {'state':'validation_pending','reason':'semantic_inputs_incomplete','gaps':gaps}
    if requested.get('validation_id')!=validation_id:raise ValueError('EXPERIMENT_VALIDATION_SCOPE_MISMATCH')
    criteria=S.config()['validation_criteria'].get(change['change_class'])
    if requested.get('criteria')!=criteria:raise ValueError('EXPERIMENT_CRITERIA_NOT_REGISTERED')
    experiment=E.register(change['id'],requested,frozen)
    with S.transaction() as db:
        current=S.get('change',change['id'],conn=db)
        S.put(db,'change',change['id'],{**current,'validation':{**current['validation'],'experiment_id':experiment['id']}})
        S.link(db,'change',change['id'],'experiment',experiment['id'])
    return {'state':'prepared','experiment_id':experiment['id']}


def advance(change_id,*,no_ai=False):
    import change_application as A
    change=S.get('change',change_id);results=[]
    if not change:raise ValueError('UNKNOWN_CHANGE')
    if change['adapter']=='source_patch':
        import source_change_validation as V
        import change_registry as R
        target=R.targets()[change['target_id']]
        functional=S.get('source_validation',change_id)
        if not functional or functional.get('patch_hash')!=change['patch_hash']:
            functional=V.run(change_id);results.append(functional)
        if not functional.get('functional_passed'):return {'state':'validation_pending','results':results,'reason':'functional_validation_incomplete'}
        if target.get('ai_impact',True):
            experiment_id=change['validation'].get('experiment_id')
            if not experiment_id:
                prepared=prepare_experiment(change);results.append(prepared)
                experiment_id=prepared.get('experiment_id')
            if not experiment_id or no_ai:return {'state':'validation_pending','reason':'real_ai_comparison_pending','results':results}
            import semantic_experiments as E
            import semantic_validation as SV
            results.append(E.run(experiment_id,with_ai=True));paired=SV.evaluate(experiment_id);results.append(paired)
            # Reuse a verified functional result, while binding the separate AI
            # proof. Repeating the whole suite cannot cure a failed AI criterion.
            valid=bool(paired['adoption_eligible'] and paired.get('candidate_implementation_hash')==change['patch_hash'])
            with S.transaction() as db:
                functional=S.put(db,'source_validation',change_id,{**functional,'real_ai_criteria_passed':valid,
                    'semantic_validation_id':experiment_id,'semantic_validation_hash':S.digest(paired),'adoption_eligible':valid})
                S.link(db,'source_validation',change_id,'semantic_validation',experiment_id)
        results.append(A.apply_change(change_id))
    elif change['adapter']=='policy':
        validation_id=change['validation'].get('validation_id')
        if not validation_id:return {'state':'validation_pending','reason':'registered_policy_validation_required'}
        import validation_spec as V
        import policy_validation as PV
        manifest=V.load_spec(validation_id);spec=manifest['spec']
        if spec.get('comparison_kind')=='prompt_pair':
            if no_ai:return {'state':'validation_pending','reason':'real_ai_comparison_pending'}
            import prompt_validation as P
            declared=spec.get('prompt',{})
            trial=P.run_prompt_validation(validation_id,with_ai=True,model=declared.get('model'),
                repetitions=spec.get('repetitions'),budget_usd=declared.get('budget_usd'))
            results.append(trial)
            if trial.get('status')=='waiting':
                return {'change_id':change_id,'state':'validation_pending','reason':'research_admission_pending','results':results}
            if trial.get('experiment_id'):results.append(PV.finalize_prompt_validation(validation_id,trial['experiment_id']))
        else:results.append(PV.run_validation(validation_id,mode=change['validation'].get('mode','replay')))
        results.append(A.apply_change(change_id))
    else:raise ValueError('UNREGISTERED_CHANGE_ADAPTER')
    return {'change_id':change_id,'state':'processed','results':results}
