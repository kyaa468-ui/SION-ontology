"""Registered defect reproduction and normal controls in an OS sandbox."""
from pathlib import Path
import os
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET

import improvement_store as S


def sandbox_policy(root,write_root):
    quote=lambda p:'"'+str(p).replace('\\','\\\\').replace('"','\\"')+'"'
    roots=[Path(root).resolve(),Path(sys.prefix).resolve(),Path(sys.base_prefix).resolve(),Path('/System'),Path('/usr'),Path('/Library/Developer'),Path('/private/etc')]
    # dyld/Python must resolve each parent. Literal directory permission does
    # not grant access to their children (including the operating work store).
    parents=sorted({str(parent) for path in roots for parent in path.parents})
    return ('(version 1)(deny default)(allow process*)(allow sysctl-read)(allow file-read-metadata)'
        '(allow file-read* '+ ' '.join('(subpath '+quote(p)+')' for p in roots)+')'
        '(allow file-read* '+ ' '.join('(literal '+quote(p)+')' for p in parents)+')'
        '(allow file-read* (literal "/dev/null"))(allow file-write* (literal "/dev/null") (subpath '+quote(Path(write_root).resolve())+'))(deny network*)')


def _selectors(values,root):
    if not isinstance(values,list) or not 1<=len(values)<=100:raise ValueError('EXPLICIT_DEFECT_AND_CONTROL_TESTS_REQUIRED')
    output=[]
    for value in values:
        if not isinstance(value,str) or not re.fullmatch(r'(?:tests/)?test_[A-Za-z0-9_]+\.py(?:::[A-Za-z0-9_]+){1,3}',value):
            raise ValueError('UNREGISTERED_TEST_SELECTOR')
        relative=value if value.startswith('tests/') else 'tests/'+value
        if not (root/relative.split('::')[0]).is_file():raise ValueError('TEST_SOURCE_UNAVAILABLE')
        output.append(str(root/relative))
    return output


def _run(root,selectors,label,timeout):
    sandbox=shutil.which('sandbox-exec') if sys.platform=='darwin' else None
    if not sandbox:raise ValueError('OS_SOURCE_SANDBOX_UNAVAILABLE')
    policy=sandbox_policy(root,root/'tmp')
    (root/'tmp').mkdir(exist_ok=True);report=root/'tmp'/(label+'.xml')
    environment={k:v for k,v in os.environ.items() if k in ('PATH','LANG','LC_ALL','SYSTEMROOT')}
    environment.update(MODE='mock',GIT_PUSH='0',PYTHONDONTWRITEBYTECODE='1',TMPDIR=str(root/'tmp'),PYTHONPATH=str(root/'src'))
    command=[sandbox,'-p',policy,sys.executable,'-m','pytest',*selectors,'-q','--disable-warnings','-p','no:cacheprovider','--junitxml='+str(report)]
    try:
        result=subprocess.run(command,cwd=root,env=environment,capture_output=True,text=True,timeout=timeout)
    except subprocess.TimeoutExpired:return {'state':'timeout','tests':0,'passed':0,'failed':0,'errors':1}
    summary={'state':'completed','returncode':result.returncode,'stdout_tail':result.stdout[-8000:],'stderr_tail':result.stderr[-4000:]}
    if not report.exists():return {**summary,'state':'report_missing','tests':0,'passed':0,'failed':0,'errors':1}
    cases=ET.parse(report).getroot().findall('.//testcase')
    summary.update(tests=len(cases),failed=sum(c.find('failure') is not None for c in cases),
        errors=sum(c.find('error') is not None for c in cases),skipped=sum(c.find('skipped') is not None for c in cases))
    summary['passed']=len(cases)-summary['failed']-summary['errors']-summary['skipped']
    return summary


def run(change_id,*,timeout_seconds=1800):
    import source_change_validation as W
    import research_capacity as K
    import uuid
    with W.locked(change_id):
        token='source-validation:'+uuid.uuid4().hex
        K.reserve(token,change_id)
        result=_validate(change_id,timeout_seconds=timeout_seconds)
        K.finish(token)
        return result


def _validate(change_id,*,timeout_seconds=1800):
    import change_registry as R
    import source_change_validation as W
    change=S.get('change',change_id)
    criteria=S.config().get('validation_criteria',{}).get(change['change_class'],{})
    tests=criteria.get('source_tests')
    if not tests:return {'state':'validation_pending','reason':'registered_reproduction_and_control_tests_required'}
    prepared=W.prepare(change_id);candidate=Path(prepared['workspace']).resolve()
    baseline=candidate.with_name(candidate.name+'-baseline')
    if not baseline.exists():
        shutil.copytree(candidate,baseline,ignore=shutil.ignore_patterns('tmp','*.xml','__pycache__'))
        (baseline/change['target_path']).write_text(change['before_content'])
    if S.digest((baseline/change['target_path']).read_text())!=change['patch']['before_hash']:
        raise ValueError('BASELINE_PATCH_CONFLICT')
    defects=_selectors(tests.get('defects'),baseline);controls=_selectors(tests.get('controls'),baseline)
    normal=_run(baseline,controls,'baseline-controls',timeout_seconds)
    original=_run(baseline,defects,'baseline-defects',timeout_seconds)
    fixed=_run(candidate,_selectors(tests['defects'],candidate),'candidate-defects',timeout_seconds)
    candidate_controls=_run(candidate,_selectors(tests['controls'],candidate),'candidate-controls',timeout_seconds)
    regression=_run(candidate,[str(candidate/'tests')],'candidate-suite',timeout_seconds)
    okay=lambda r:r['tests']>0 and r['passed']==r['tests'] and r.get('returncode')==0
    reproduced=original['tests']>0 and original['failed']==original['tests'] and original['errors']==0 and original.get('returncode')==1
    functional=reproduced and all(okay(r) for r in (normal,fixed,candidate_controls,regression))
    target=R.targets()[change['target_id']];ai_required=target.get('ai_impact',True)
    paired=S.get('semantic_validation',change['validation'].get('experiment_id',''))
    # A prompt-only experiment cannot validate changed Python execution. It must
    # bind the tested candidate implementation, not just contain its patch text.
    ai_pass=bool(paired and paired.get('adoption_eligible') and paired.get('patch_hash')==change['patch_hash']
                 and paired.get('candidate_implementation_hash')==change['patch_hash'])
    body={'owner':change_id,'state':'passed' if functional else 'failed','base_revision':change['base_revision'],
        'patch_hash':change['patch_hash'],'criteria_hash':S.digest(criteria),'functional_passed':functional,
        'baseline_defects':original,'baseline_controls':normal,'candidate_defects':fixed,
        'candidate_controls':candidate_controls,'candidate_regression':regression,
        'real_ai_criteria_passed':ai_pass,'ai_required':ai_required,'adoption_eligible':functional and (not ai_required or ai_pass),
        'result_kind':'isolated_functional_tests','last_progress_at':S.now()}
    with S.transaction() as db:
        row=S.put(db,'source_validation',change_id,body)
        S.link(db,'change',change_id,'source_validation',change_id)
        return row
