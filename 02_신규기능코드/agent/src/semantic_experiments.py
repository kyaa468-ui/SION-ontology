"""Paired four-stage calls, using the production semantic submission checker.

The frozen runtime has no broker, production writer or mutable current input.
Local fake providers and stored-output replays can never qualify as real AI.
"""
from __future__ import annotations
import copy
import json
import math
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import common as C
import improvement_store as S
import semantic_layer as SL


class FrozenSemantic:
    def __init__(self, chain, receipts):
        self.chain=copy.deepcopy(chain)
        self.attempt_id=chain['run_attempt_id']
        self.role='interpretation'
        self.receipts={r['delivery_id']:copy.deepcopy(r) for r in receipts}
        self.records={('chains',chain['chain_id']):self.chain}
        self.judgments=[]

    def read(self, kind, identifier):
        return copy.deepcopy(self.records.get((kind,identifier)))

    def write(self, kind, identifier, value):
        if kind not in {'stages','inputs'}: raise ValueError('FROZEN_WRITE_FORBIDDEN')
        if (kind,identifier) in self.records: raise FileExistsError(identifier)
        self.records[kind,identifier]=copy.deepcopy(value)

    def assert_fresh(self, chain):
        if S.digest(chain)!=S.digest(self.chain): raise ValueError('FROZEN_CHAIN_CHANGED')

    def context(self, role):
        self.role=role
        predecessors=[]
        for prior in SL.ROLES[:SL.ROLES.index(role)]:
            row=self.read('stages',self.chain['chain_id']+'-'+prior)
            if row is None: raise ValueError('FROZEN_PREDECESSOR_MISSING')
            predecessors.append(row)
        payload={'format':SL.FORMAT,'chain':self.chain,'role':role,'predecessors':predecessors,
                 'contract':SL.contract(role),'inference_status':'qualitative_unverified'}
        iid='input-'+SL.digest(payload)[:40]
        self.records['inputs',iid]={'input_id':iid,'role':role,'run_attempt_id':self.attempt_id,
                                    'payload':payload,'payload_hash':SL.digest(payload)}
        return {'input_id':iid,**payload}

    def accept_final(self,item):
        import ai_judgment as J
        target={k:self.chain[k] for k in ('run_attempt_id','run_id','code','confidence_for')}
        row=J.validate_frozen_submission(item,self.receipts,target,self.chain['created_at'])
        if row.get('status')=='accepted':
            row['judgment_id']='experiment-judgment-'+S.digest(row)[:40]
            self.judgments.append(row)
        return row

    def submit(self,input_id,data):
        import ai_judgment as J
        import evidence_wire as W
        token=SL.FROZEN_RUNTIME.set(self)
        target={k:self.chain[k] for k in ('run_attempt_id','run_id','code','confidence_for')}
        frozen=J._FROZEN.set({'receipts':self.receipts,'at':S.instant(self.chain['created_at']),'context':target})
        try:
            with W.frozen_receipts(self.receipts,S.instant(self.chain['created_at'])):
                return SL._submit(input_id,copy.deepcopy(data))
        finally:
            J._FROZEN.reset(frozen)
            SL.FROZEN_RUNTIME.reset(token)


def cli_provider(request):
    """One explicitly budgeted call with no tools or application access."""
    import research_budget as B
    import research_capacity as K
    import research_calls
    payload=request['input']
    prompt=(request['guidance']+'\nReturn ONLY the JSON object for submit_semantic_stage data. '
            'Use this assigned role and the supplied frozen receipts and predecessors. No order tool exists.\n'+S.canonical(payload))
    with tempfile.TemporaryDirectory(prefix='sion-semantic-ai-') as work:
        settings=Path(work)/'settings.json'
        settings.write_text(S.canonical({'permissions':{'allow':[],'deny':['Read','Bash','Write','Edit','WebSearch','WebFetch','Agent','Task']},'autoMemoryEnabled':False,
                                        'claudeMdExcludes':['**/CLAUDE.md','**/AGENTS.md','**/.claude/rules/**']}))
        mcp=Path(work)/'mcp.json';mcp.write_text('{"mcpServers":{}}')
        command=[shutil.which('claude') or 'claude','-p',prompt,'--model',request['model_version'],
                 '--output-format','json','--tools','','--mcp-config',str(mcp),'--strict-mcp-config',
                 '--setting-sources','','--settings',str(settings),'--no-session-persistence',
                 '--max-budget-usd',str(request['maximum_usd'])]
        try:
            reservation=research_calls.claim(request['call_id'],request['experiment_id'],request['maximum_usd'],request)
        except K.CapacityBlocked as exc:
            return {'status':'configuration_pending' if 'CONFIGURATION_REQUIRED' in str(exc) else 'capacity_blocked',
                    'result_kind':'real_ai','reason':str(exc)}
        if not reservation['dispatch_allowed']:
            return {'status':reservation['state'],'result_kind':'real_ai','reservation':reservation}
        try:
            result=subprocess.run(command,cwd=work,env={**os.environ,'RUN_MODE':'semantic_experiment'},
                                  capture_output=True,text=True,timeout=request.get('timeout_seconds',300))
            response=json.loads(result.stdout)
            settled=B.settle(request['call_id'],response.get('total_cost_usd'),response.get('session_id') or request['call_id'])
            K.finish('call:'+request['call_id'])
            if settled['state']!='settled': return {'status':'cost_unresolved','result_kind':'real_ai'}
            reported=sorted((response.get('modelUsage') or {}).keys())
            if reported!=[request['model_version']]:
                return {'status':'model_version_unverified','result_kind':'real_ai','reported_models':reported,
                        'expected_model_version':request['model_version'],'actual_usd':response.get('total_cost_usd')}
            raw=response.get('result','').strip()
            if raw.startswith('```'): raw=raw.split('\n',1)[1].rsplit('```',1)[0]
            return {'status':'completed' if result.returncode==0 and not response.get('is_error') else 'failed',
                    'data':json.loads(raw),'provider_request_id':response.get('session_id'),
                    'reported_models':reported,'model_version_verified':True,
                    'usage':response.get('usage'),'actual_usd':response.get('total_cost_usd'),'result_kind':'real_ai'}
        except Exception:
            # mark_sent remains durable on ambiguous failure; no silent retry.
            raise


def register(change_id, spec, cases):
    if type(spec.get('repetitions')) is not int or not 1<=spec['repetitions']<=100: raise ValueError('EXPLICIT_REPETITIONS_REQUIRED')
    for field in ('model','model_version','criteria','maximum_usd','data_cutoff','tool_permissions'):
        if field not in spec: raise ValueError('EXPERIMENT_REQUIRED:'+field)
    if spec['tool_permissions']!=[]: raise ValueError('EXPERIMENT_TOOLS_MUST_BE_EMPTY')
    if not cases or len(cases)>50: raise ValueError('FINITE_EXPERIMENT_CASES_REQUIRED')
    if type(spec['maximum_usd']) not in (int,float) or not math.isfinite(spec['maximum_usd']) or spec['maximum_usd']<=0:raise ValueError('EXPLICIT_FINITE_EXPERIMENT_BUDGET_REQUIRED')
    if any(not c.get('chain') or not c.get('receipts') for c in cases): raise ValueError('FROZEN_CHAIN_AND_RECEIPTS_REQUIRED')
    change=S.get('change',change_id)
    proof={}
    if spec.get('validation_id'):
        from investigation import exposure_identity
        import exposure_checkpoint
        with S.transaction(readonly=True) as db:
            for case in cases:
                exposure=exposure_checkpoint.status(db,exposure_identity(case['chain']))
                if exposure['used']:
                    raise ValueError('EXPERIMENT_CASE_ALREADY_EXPOSED:'+exposure['basis'])
        import validation_holdout as VH
        proof=VH.status(spec['validation_id'])
        enrolled={r['run_attempt_id'] for r in proof.get('enrollments',[])}
        if any(c['chain']['run_attempt_id'] not in enrolled for c in cases):raise ValueError('EXPERIMENT_CASE_NOT_ENROLLED')
        import ai_judgment as J
        for case in cases:
            original=SL._read('chains',case['chain']['chain_id'],case['chain']['run_attempt_id'])
            if original is None or S.digest(original)!=S.digest(case['chain']):raise ValueError('FROZEN_CHAIN_PROVENANCE_MISMATCH')
            for receipt in case['receipts']:
                persisted=J._load('deliveries',receipt['delivery_id'])
                supplied={k:v for k,v in receipt.items() if not k.startswith('_wire_')}
                if persisted is None or S.digest(persisted)!=S.digest(supplied):raise ValueError('FROZEN_RECEIPT_PROVENANCE_MISMATCH')
    if change and change['adapter']=='source_patch' and change['target_id'].startswith('prompt.'):
        role=Path(change['target_path']).stem
        for case in cases:
            for assigned in (SL.ROLES if role not in SL.ROLES else (role,)):
                case.setdefault('guidance',{})[assigned]=change['before_content']
                case.setdefault('candidate_guidance',{})[assigned]=change['patch']['content']
    identifier='semantic-experiment-'+S.digest([change_id,spec,cases])[:40]
    with S.transaction() as db:
        old=S.get('experiment',identifier,conn=db)
        if old:return old
        return S.save_once('experiment',identifier,{'owner':change_id,'change_id':change_id,'spec':spec,
                          'cases':cases,'input_hash':S.digest(cases),'state':'queued','last_progress_at':S.now(),
                          'base_revision':(change or {}).get('base_revision'),'independent_proof':proof,
                          'patch_hash':(change or {}).get('patch_hash')},conn=db)


def run(experiment_id, *, provider=None, replay=False, with_ai=False):
    experiment=S.get('experiment',experiment_id)
    change=S.get('change',experiment['change_id']) if experiment else None
    if change and change.get('adapter')=='source_patch':
        import source_change_validation as W
        with W.locked(change['id']):
            return _run(experiment_id,provider=provider,replay=replay,with_ai=with_ai)
    return _run(experiment_id,provider=provider,replay=replay,with_ai=with_ai)


def _run(experiment_id, *, provider=None, replay=False, with_ai=False):
    experiment=S.get('experiment',experiment_id)
    if not experiment: raise ValueError('UNKNOWN_EXPERIMENT')
    if experiment['state'] in {'expired','cancelled'}:raise ValueError('EXPERIMENT_SUPPORT_EXPIRED')
    if provider is None and not with_ai: raise ValueError('EXPLICIT_REAL_AI_REQUIRED')
    result_kind='stored_replay' if replay else 'local_fake' if provider else 'real_ai'
    previous=S.get('experiment_result',experiment_id)
    if previous and previous['result_kind']!=result_kind:raise ValueError('EXPERIMENT_RESULT_KIND_IMMUTABLE')
    if result_kind=='real_ai':
        import retention_store as R
        for case in experiment['cases']:
            permission=R.detail_support(case['chain']['created_at'],experiment['spec'].get('validation_id'))
            if not permission['allowed']:raise ValueError('EXPERIMENT_SUPPORT_PENDING:'+permission['reason'])
    invoke=provider or cli_provider
    resumable={'budget_blocked','cost_unresolved','capacity_blocked','configuration_pending'}
    spec=experiment['spec'];trials=[]
    change=S.get('change',experiment['change_id']);workspaces=None
    if change and change['adapter']=='source_patch':
        import source_change_validation as W
        import source_validation_runner as V
        prepared=W.prepare(change['id']);candidate=Path(prepared['workspace'])
        baseline=candidate.with_name(candidate.name+'-baseline')
        if not baseline.exists():
            shutil.copytree(candidate,baseline,ignore=shutil.ignore_patterns('tmp','*.xml','__pycache__'))
            (baseline/change['target_path']).write_text(change['before_content'])
        workspaces={'baseline':baseline,'candidate':candidate}
        if S.digest((candidate/change['target_path']).read_text())!=S.digest(change['patch']['content']):raise ValueError('CANDIDATE_SOURCE_CHANGED')
        if S.digest((baseline/change['target_path']).read_text())!=change['patch']['before_hash']:raise ValueError('BASELINE_SOURCE_CHANGED')
    denominator=len(experiment['cases'])*spec['repetitions']*2*4
    for index,case in enumerate(experiment['cases']):
        for repetition in range(spec['repetitions']):
            for arm in ('baseline','candidate'):
                if workspaces:
                    from semantic_patch_runtime import SandboxSemantic
                    runtime=SandboxSemantic(case['chain'],case['receipts'],workspaces[arm])
                else:runtime=FrozenSemantic(case['chain'],case['receipts'])
                prior_failed=False
                for role in SL.ROLES:
                    trial_id='semantic-trial-'+S.digest([experiment_id,index,repetition,arm,role])[:40]
                    if prior_failed:
                        trials.append({'id':trial_id,'status':'predecessor_unavailable','role':role,'arm':arm,'repetition':repetition})
                        continue
                    try:context=runtime.context(role)
                    except Exception as exc:
                        trials.append({'id':trial_id,'status':'runtime_unavailable','error':str(exc),'role':role,'arm':arm,'repetition':repetition})
                        prior_failed=True;continue
                    request={'call_id':trial_id,'experiment_id':experiment_id,'case_index':index,'arm':arm,'role':role,
                             'repetition':repetition,'model':spec['model'],'model_version':spec['model_version'],
                             'maximum_usd':spec['maximum_usd']/denominator,
                             'guidance':case.get('candidate_guidance',{}).get(role,case.get('guidance',{}).get(role,'')) if arm=='candidate' else case.get('guidance',{}).get(role,''),
                             'input':{'context':context,'receipts':case['receipts']},'input_hash':S.digest(context)}
                    old=S.get('semantic_trial',trial_id)
                    output={}
                    try:
                        if old:
                            if old['request_hash']!=S.digest(request): raise ValueError('EXPERIMENT_INPUT_CHANGED')
                            if old['result_kind']!=result_kind:raise ValueError('EXPERIMENT_RESULT_KIND_IMMUTABLE')
                            output=old['output']
                            if old.get('error') in resumable:output=invoke(copy.deepcopy(request))
                        else:
                            output=invoke(copy.deepcopy(request))
                        if output.get('status')!='completed':
                            raise ValueError(output.get('status','provider_failed'))
                        accepted=runtime.submit(context['input_id'],output['data'])
                        stage=runtime.read('stages',runtime.chain['chain_id']+'-'+role)
                        row={'owner':experiment_id,'request':request,'request_hash':S.digest(request),'output':output,
                             'status':'accepted','result_kind':result_kind,'accepted':accepted,'stage':stage,
                             'judgments':copy.deepcopy(runtime.judgments) if role=='lead' else []}
                    except Exception as exc:
                        row={'owner':experiment_id,'request':request,'request_hash':S.digest(request),
                             'output':output,'status':'failed','error':str(exc),'result_kind':result_kind}
                    if not old or old.get('error') in resumable and any(old.get(k)!=v for k,v in row.items()):
                        with S.transaction() as db:
                            S.put(db,'semantic_trial',trial_id,row)
                            S.link(db,'experiment',experiment_id,'semantic_trial',trial_id)
                    trials.append({'id':trial_id,'status':row['status'],'role':role,'arm':arm,'repetition':repetition})
                    if row['status']!='accepted':prior_failed=True
    complete=len(trials)==denominator and all(x['status']=='accepted' for x in trials)
    result={'owner':experiment_id,'state':'completed' if complete else 'partial','result_kind':result_kind,
            'expected_stage_calls':denominator,'attempted_stage_calls':sum(t['status'] not in {'predecessor_unavailable','runtime_unavailable'} for t in trials),'trials':trials,
            'input_hash':experiment['input_hash'],'spec_hash':S.digest(spec),
            'adoption_eligible':False,'reason':'requires_independent_predeclared_business_criteria_evaluation',
            'candidate_implementation_hash':experiment.get('patch_hash') if workspaces else None,
            'actual_ai_calls':0}
    with S.transaction(readonly=True) as db:
        if db and result_kind=='real_ai':
            result['actual_ai_calls']=db.execute("SELECT count(*) FROM calls WHERE owner=? AND state IN ('sent','unknown','settled','overrun')",(experiment_id,)).fetchone()[0]
    with S.transaction() as db:
        if previous and all(previous.get(k)==v for k,v in result.items()):return previous
        saved=S.put(db,'experiment_result',experiment_id,result)
        S.link(db,'experiment',experiment_id,'experiment_result',experiment_id)
        S.put(db,'experiment',experiment_id,{**experiment,'state':result['state'],'last_progress_at':S.now(),
              **({'closed_at':S.now()} if complete else {})})
    return saved
