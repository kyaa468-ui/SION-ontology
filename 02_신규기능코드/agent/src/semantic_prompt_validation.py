"""Bridge registered historical prompt comparisons to all four live stages."""
import copy
import common as C
import semantic_layer as SL
import semantic_experiments as E
import improvement_store as S
import validation_store as V


def cases(inputs,proposal):
    import condition_contract as CC
    import evidence_wire as W
    receipts=inputs.get('deliveries',[])
    if isinstance(receipts,dict):receipts=list(receipts.values())
    result=[];gaps=[]
    for opportunity in inputs.get('opportunities',[]):
        attempt=opportunity['run_attempt_id']
        cid='chain-'+SL.digest([attempt,opportunity['code'],opportunity['confidence_for']])[:32]
        chain=SL._read('chains',cid,attempt)
        original=inputs.get('runtime_inputs',{}).get(attempt,{})
        if 'snapshot' in original:original=original['snapshot']
        cutoff=opportunity.get('at') or opportunity.get('decision_at')
        subset=[r for r in receipts if r.get('run_attempt_id')==attempt and cutoff and S.instant(r['created_at'])<=S.instant(cutoff)]
        if not chain or not subset or not original:
            gaps.append({'target':opportunity,'reason':'original_semantic_chain_receipt_or_instructions_unavailable'});continue
        sources=original.get('source_files') or original.get('sources') or {}
        guidance={role:(sources.get('CLAUDE.md','')+'\n'+sources.get('.claude/agents/'+role+'.md','')+'\n'+original.get('cmd_prompt','')) for role in SL.ROLES}
        condition=proposal.get('scope_condition') or proposal.get('condition')
        matched=CC.evaluate(condition,opportunity.get('condition_context') or {},stage='before_judgment') if condition else True
        candidate={role:text+('\n'+proposal['effect']['text'] if matched is True else '') for role,text in guidance.items()}
        result.append({'chain':copy.deepcopy(chain),'receipts':[W.freeze_receipt(r,S.instant(cutoff)) for r in subset],
                      'guidance':guidance,'candidate_guidance':candidate,'condition_result':matched,'target':opportunity})
    return result,gaps


def run(manifest,inputs,*,with_ai,model,repetitions,budget_usd,provider=None):
    spec=manifest['spec'];proposal=manifest['proposal_snapshot']
    frozen,gaps=cases(inputs,proposal)
    if gaps or not frozen:
        return {'status':'incomplete','gaps':gaps,'adoption_eligibility':False,'expected_opportunities':len(inputs.get('opportunities',[]))}
    experiment_spec={'model':model,'model_version':spec.get('prompt',{}).get('model_version',model),
        'repetitions':repetitions,'maximum_usd':budget_usd,'criteria':spec.get('semantic_criteria',{}),
        'data_cutoff':spec['period']['end'],'tool_permissions':[]}
    # A legacy retrospective spec is explicitly not converted into a positive
    # independence proof. Prospective registrations retain their proof checks.
    if spec.get('mode')=='prospective':experiment_spec['validation_id']=manifest['validation_id']
    experiment=E.register(proposal['proposal_id'],experiment_spec,frozen)
    result=E.run(experiment['id'],provider=provider,with_ai=with_ai)
    # Seal each resumed result under its own immutable publication ID.
    publication='semantic-prompt-'+S.digest([experiment['id'],result])[:40]
    trials=[];pairs=[]
    for index,case in enumerate(frozen):
        for repeat in range(repetitions):
            pair=[]
            for arm in ('baseline','candidate'):
                descriptor=next(t for t in result['trials'] if t['role']=='lead' and t['arm']==arm and t['repetition']==repeat
                                and t['id']=='semantic-trial-'+S.digest([experiment['id'],index,repeat,arm,'lead'])[:40])
                saved=S.get('semantic_trial',descriptor['id'])
                identifier='paired-trial-'+S.digest([publication,index,repeat,arm])[:40]
                request={'experiment_id':publication,'model':model,'arm':arm,'repetition':repeat,'target':case['target'],
                         'semantic_experiment_id':experiment['id'],'stage_trial_id':descriptor['id']}
                row={'trial_id':identifier,'status':descriptor['status'],'arm':arm,'repetition':repeat,
                     'target':case['target'],'judgments':(saved or {}).get('judgments',[]),'stage_trial_id':descriptor['id']}
                V.archive('prompt_requests',identifier,request);V.archive('prompt_trials',identifier,row)
                trials.append(row);pair.append(row)
            pairs.append({'trial_ids':[x['trial_id'] for x in pair],'target':case['target'],'repetition':repeat,
                          'status':'paired' if all(x['status']=='accepted' for x in pair) else 'incomplete'})
    import prompt_validation as P
    simulations=P.simulate_trials(inputs,spec,trials,repetitions)
    output={**result,'experiment_id':publication,'semantic_experiment_id':experiment['id'],'validation_id':manifest['validation_id'],
            'model':model,'budget_usd':budget_usd,'repetitions':repetitions,'spec_hash':V.digest(manifest),
            'proposal_hash':V.digest(proposal),'input_hash':V.digest(inputs),'input_id':inputs.get('input_id'),
            'mode':'explicit_real_ai' if result['result_kind']=='real_ai' else result['result_kind'],
            'pairs':pairs,'trial_ids':[x['trial_id'] for x in trials],'simulations':simulations,
            'status':'complete' if result['state']=='completed' else 'incomplete',
            'adoption_eligibility':False,'performance_status':'requires_sequential_simulation_of_paired_outputs',
            'subagent_policy':'four_separate_frozen_roles_no_broker','gaps':gaps}
    V.archive('prompt_results',publication,output)
    V.archive('prompt_result_commits',publication,{'experiment_id':publication,
        'validation_id':manifest['validation_id'],'result_hash':V.digest(output)})
    return output
