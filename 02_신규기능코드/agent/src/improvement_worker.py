"""One after-close worker, resumable research, no order execution capabilities."""
from __future__ import annotations
import json
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

import common as C
import improvement_store as S


def investigate(identifier):
    import investigation as Q
    import research_calls
    cfg=S.config()
    model=cfg.get('research_model')
    if not model: return {'state':'configuration_pending','reason':'research_model_unset'}
    call_id='investigation-call-'+uuid.uuid4().hex
    prompt=(f'조사 ID {identifier}. get_investigation_context부터 시작하십시오. 질문의 실제 경로와 기간·단위·계산을 조회하고 '
            '등록 검사를 실행해 경쟁 설명을 구별하십시오. submit_diagnosis에는 실제 받은 참조와 check ID만 사용하십시오. '
            '원인이 확인된 경우 수정 목표·현행 유지/대안·선택 이유·구체 패치·검증·복구를 submit_change로 제출하십시오. '
            '조회 공백을 반대 근거 없음으로 해석하지 마십시오. 결함 없음이나 판단 불가도 허용됩니다.')
    with tempfile.TemporaryDirectory(prefix='sion-investigation-') as folder:
        config=Path(folder)/'mcp.json'
        config.write_text(S.canonical({'mcpServers':{'investigation':{'command':sys.executable,
            'args':[str(Path(C.AGENT)/'src/mcp_server.py')],
            'env':{'RUN_MODE':'investigation','INVESTIGATION_ID':identifier,'MODE':'mock'}}}}))
        command=[shutil.which('claude') or 'claude','-p',prompt,'--model',model,'--output-format','json',
                 '--tools','','--setting-sources','','--mcp-config',str(config),'--strict-mcp-config',
                 '--allowedTools',*['mcp__investigation__'+name for name in Q.TOOLS],
                 '--disallowedTools','Read','Write','Edit','Bash','WebSearch','WebFetch','Agent','Task',
                 '--max-budget-usd',str(cfg.get('research_call_usd',1))]
        command=research_calls.before(call_id,identifier,command)
        result=subprocess.run(command,cwd=folder,capture_output=True,text=True,timeout=300,
                              env={**os.environ,'RUN_MODE':'investigation','INVESTIGATION_ID':identifier})
        payload=json.loads(result.stdout)
        research_calls.after(call_id,payload)
        return {'state':'finished' if result.returncode==0 else 'failed','call_id':call_id,
                'usage':payload.get('usage'),'actual_usd':payload.get('total_cost_usd')}


def run(*, no_ai=False, limit=10, close_result=None):
    import case_store
    try:
        with case_store.locked('improvement-worker',timeout=0):
            return _run(no_ai=no_ai,limit=limit,close_result=close_result)
    except case_store.StoreBusy:return {'state':'busy','actual_orders':0}


def _run(*, no_ai=False, limit=10, close_result=None):
    import improvement_issues as I
    import investigation as Q
    import opportunity_cases as O
    import change_application as A
    import observation_checks
    if not 1<=limit<=100: raise ValueError('WORKER_LIMIT')
    import assessment_observations
    assessment_observations.advance(limit=limit)
    I.expire_stalled();O.reconcile_assessments()
    worker='worker-'+uuid.uuid4().hex;results=[]
    try:
        for _ in range(limit):
            claim=I.claim(worker)
            if claim['state'] in {'empty','busy'}: break
            try:
                session=Q.start(claim['id'])
                response={'state':'prepared','investigation_id':session['id']} if no_ai else investigate(session['id'])
                if close_result and close_result.get('run_status') not in {'completed','complete'}:
                    response['close_input_status']='partial'
                results.append(response)
            except Exception as exc:
                response={'state':'blocked','issue_id':claim['id'],'reason':str(exc)};results.append(response)
            with S.transaction() as db:
                current=S.get('issue',claim['id'],conn=db)
                if current['state'] in {'claimed','investigating'}:
                    S.put(db,'issue',current['id'],{**current,'state':'queued','last_worker_result':response})
            if response['state'] in {'configuration_pending','blocked','prepared'}: break
        import improvement_validation
        for change in S.page('change',state='validation_pending',limit=limit):
            try:results.append(improvement_validation.advance(change['id'],no_ai=no_ai))
            except (ValueError,KeyError,OSError) as exc:
                results.append({'change_id':change['id'],'state':'validation_pending','reason':str(exc)})
        results.extend(observation_checks.advance(limit=limit))
        import semantic_dependencies
        results.extend(semantic_dependencies.advance(no_ai=no_ai,limit=limit))
        if not no_ai:
            import improvement_notifications as N
            N.recover();results.extend(N.dispatch(limit=limit))
    finally:
        with S.transaction() as db:
            active=S.get('worker','singleton',conn=db)
            if active and active['worker']==worker:
                S.put(db,'worker','singleton',{**active,'lease_until':S.now(),'state':'released'})
    return {'state':'processed','results':results,'actual_orders':0}


def dashboard(limit=50):
    import research_budget
    import research_capacity
    # This view contains workflow metadata only; evidence is fetched through the
    # exposure-checked investigation route, never through a convenient UI bypass.
    result={'budget':research_budget.status(),'capacity':research_capacity.status(),'kind':'current_workflow_metadata','sections':{}}
    fields={'issue':('id','state','question','occurrence_count'),
            'change':('id','state','target_id','change_class','goal'),
            'application':('id','state','change_id','after_revision'),
            'observation_job':('id','state','uses','recurrences','side_effects','missing','criteria')}
    for kind,keys in fields.items():
        result['sections'][kind]=[{k:row[k] for k in keys if k in row} for row in S.page(kind,limit=limit)]
    result['state_labels']={'applied':'적용됨','actual_use_confirmed':'실제 사용 확인','observing':'효과 관찰 중',
        'improved':'개선 확인','insufficient_data':'자료 부족','deteriorated':'악화','rolled_back':'되돌림'}
    return result
