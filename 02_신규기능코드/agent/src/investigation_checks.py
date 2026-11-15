"""Server-registered pure checks. No caller-controlled imports, shell or SQL."""
from __future__ import annotations
import improvement_store as S

VERSION='INVESTIGATION_CHECKS@v1'


def comparable(inputs, arguments):
    fields=('subject','unit','period','method','measurement_kind')
    if len(inputs)!=2: raise ValueError('TWO_MEASUREMENTS_REQUIRED')
    a,b=inputs
    missing=[key for key in fields if a.get(key) is None or b.get(key) is None]
    differences=[{'field':k,'left':a[k],'right':b[k]} for k in fields if k not in missing and a[k]!=b[k]]
    return {'verdict':'unknown' if missing else 'fail' if differences else 'pass',
            'differences':differences,'missing':missing,'contract':list(fields),
            'limitation':'Contract agreement does not prove source truth.'}


def equality(inputs, arguments):
    if len(inputs)!=2: raise ValueError('TWO_INPUTS_REQUIRED')
    return {'verdict':'pass' if inputs[0]==inputs[1] else 'fail',
            'left_hash':S.digest(inputs[0]),'right_hash':S.digest(inputs[1]),
            'limitation':'Equality is not a causal test.'}


def stage_flow(inputs, arguments):
    stages=[x for x in inputs if isinstance(x,dict) and 'stage_id' in x]
    known={x['stage_id']:x for x in stages}
    gaps=[{'stage':x['stage_id'],'missing':p} for x in stages for p in x.get('predecessor_ids',[]) if p not in known]
    return {'verdict':'unknown' if gaps else 'pass','gaps':gaps,
            'limitation':'Checks recorded predecessor links, not unrecorded internal model reasoning.'}


CHECKS={'measurement_comparability@v1':comparable,'equality@v1':equality,'stage_flow@v1':stage_flow}


def execute(function_id, inputs, arguments=None):
    if function_id not in CHECKS: raise ValueError('UNREGISTERED_CHECK')
    if len(inputs)>50: raise ValueError('CHECK_INPUT_LIMIT')
    result=CHECKS[function_id](inputs,arguments or {})
    return {'function_id':function_id,'implementation_version':VERSION,
            'input_hash':S.digest(inputs),'arguments_hash':S.digest(arguments or {}),'result':result}
