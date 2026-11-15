"""Four-stage semantic checker in the isolated candidate source tree.

Only structured frozen state crosses the process boundary. Model/provider calls
remain in the parent, under the shared research ledger. This process cannot
reach the broker or the operating work directory.
"""
import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys


class SandboxSemantic:
    def __init__(self,chain,receipts,workspace):
        self.chain=copy.deepcopy(chain);self.receipts=copy.deepcopy(receipts)
        self.root=Path(workspace).resolve();self.history=[];self.role=None;self.records={};self.judgments=[]

    def _request(self,operation,**extra):
        sandbox=shutil.which('sandbox-exec') if sys.platform=='darwin' else None
        if not sandbox:raise ValueError('OS_SEMANTIC_SANDBOX_UNAVAILABLE')
        from source_validation_runner import sandbox_policy
        policy=sandbox_policy(self.root,self.root/'tmp')
        (self.root/'tmp').mkdir(exist_ok=True)
        environment={k:v for k,v in os.environ.items() if k in ('PATH','LANG','LC_ALL')}
        environment.update(MODE='mock',GIT_PUSH='0',PYTHONDONTWRITEBYTECODE='1',TMPDIR=str(self.root/'tmp'),RUN_MODE='semantic_experiment')
        payload={'chain':self.chain,'receipts':self.receipts,'history':self.history,'role':self.role,'operation':operation,**extra}
        result=subprocess.run([sandbox,'-p',policy,sys.executable,str(self.root/'src/semantic_patch_runtime.py')],
            input=json.dumps(payload,ensure_ascii=False,allow_nan=False),cwd=self.root,env=environment,
            capture_output=True,text=True,timeout=30)
        if result.returncode:raise ValueError('CANDIDATE_SEMANTIC_RUNTIME_FAILED:'+result.stderr[-1000:])
        response=json.loads(result.stdout)
        self.records={(kind,key):value for kind,key,value in response['records']}
        self.judgments=response['judgments']
        return response['result']

    def context(self,role):
        self.role=role
        return self._request('context')

    def submit(self,input_id,data):
        result=self._request('submit',input_id=input_id,data=data)
        self.history.append({'role':self.role,'data':copy.deepcopy(data)})
        return result

    def read(self,kind,identifier):
        return copy.deepcopy(self.records.get((kind,identifier)))


def main():
    import common as C
    import semantic_experiments as E
    import improvement_store as S
    request=json.load(sys.stdin)
    # No producer callbacks, deployment writes or trading tools are reachable
    # through the frozen runtime. Keep incidental temp paths inside the sandbox.
    root=Path(__file__).resolve().parents[1]
    C.WORK=str(root/'tmp');C.ontology_root=lambda:root/'tmp'/'ontology'
    runtime=E.FrozenSemantic(request['chain'],request['receipts'])
    for prior in request['history']:
        incoming=runtime.context(prior['role'])
        runtime.submit(incoming['input_id'],prior['data'])
    context=runtime.context(request['role'])
    if request['operation']=='context':result=context
    elif request['operation']=='submit':
        if context['input_id']!=request['input_id']:raise ValueError('CANDIDATE_INPUT_CHANGED')
        result=runtime.submit(context['input_id'],request['data'])
    else:raise ValueError('UNSUPPORTED_SANDBOX_OPERATION')
    print(S.canonical({'result':result,'records':[[kind,key,value] for (kind,key),value in runtime.records.items()],
                       'judgments':runtime.judgments}))


if __name__=='__main__':main()
