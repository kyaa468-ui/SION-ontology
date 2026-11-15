"""Typed, finite premise expressions; arbitrary Python/SQL is never evaluated.

Evidence support is separate from workflow completion and causal validity.
Observations are server-ingested immutable revisions with availability times.
"""
from __future__ import annotations

import copy
import math

import improvement_store as S

VERSION = 'ASSESSMENT@v1'
OPS = {'all', 'any', 'and', 'or', 'not', 'compare', 'sum', 'mean', 'count', 'value', 'literal', 'difference', 'ratio'}
COMPARATORS = {'gt': lambda a,b:a>b, 'ge':lambda a,b:a>=b, 'lt':lambda a,b:a<b,
               'le':lambda a,b:a<=b, 'eq':lambda a,b:a==b, 'ne':lambda a,b:a!=b}


def validate_expression(expr, depth=0):
    if depth > 12 or not isinstance(expr, dict) or expr.get('op') not in OPS:
        raise ValueError('UNSUPPORTED_EXPRESSION')
    op = expr['op']
    if op == 'literal':
        if isinstance(expr.get('value'), bool) or not isinstance(expr.get('value'), (int,float)) or not math.isfinite(expr['value']):
            raise ValueError('FINITE_LITERAL_REQUIRED')
        if not isinstance(expr.get('unit'), str) or not expr['unit']:
            raise ValueError('LITERAL_UNIT_REQUIRED')
    elif op == 'value':
        if not isinstance(expr.get('field'), str) or not expr['field']:
            raise ValueError('OBSERVATION_FIELD_REQUIRED')
    elif op in {'all','any','sum','mean','count','not'}:
        validate_expression(expr.get('arg'), depth+1)
    elif op in {'and','or'}:
        if not isinstance(expr.get('args'), list) or not 1 <= len(expr['args']) <= 20:
            raise ValueError('FINITE_ARGUMENT_LIST_REQUIRED')
        for child in expr['args']:
            validate_expression(child, depth+1)
    else:
        validate_expression(expr.get('left'), depth+1)
        validate_expression(expr.get('right'), depth+1)
        if op == 'compare' and expr.get('operator') not in COMPARATORS:
            raise ValueError('COMPARISON_OPERATOR_REQUIRED')


def normalize(spec, *, known_ids, code, origin_at, registered_at=None):
    if not isinstance(spec, dict):
        raise ValueError('ASSESSMENT_OBJECT_REQUIRED')
    for field in ('text','claim_id','path_id','kind'):
        if not isinstance(spec.get(field), str) or not spec[field].strip():
            raise ValueError('ASSESSMENT_REQUIRED:' + field)
    if spec['claim_id'] not in known_ids or spec['path_id'] not in known_ids:
        raise ValueError('ASSESSMENT_UNKNOWN_CLAIM_OR_PATH')
    value = copy.deepcopy(spec)
    registered_at=registered_at or S.now()
    value.update(version=VERSION, code=code, registered_at=registered_at, judgment_at=origin_at)
    value['registration_basis'] = 'prospective' if S.instant(registered_at) <= S.instant(spec.get('start_at', origin_at)) else 'post_hoc'
    if spec['kind'] in {'interpretation','mechanism'}:
        value.update(support='evaluation_unsupported', reason='requires_registered_independent_review_or_experiment')
        return value
    if spec['kind'] not in {'fact','future','set_condition','aggregate'}:
        value.update(support='evaluation_unsupported', reason='unregistered_claim_semantics')
        return value
    try:
        validate_expression(spec.get('expression'))
        for field in ('start_at','end_at'):
            S.instant(spec[field])
        if S.instant(spec['end_at']) < S.instant(spec['start_at']):
            raise ValueError('INVALID_PERIOD')
        expected = spec.get('expected_observations')
        if not isinstance(expected, list) or not expected or len(expected) != len(set(expected)) or len(expected) > 1000:
            raise ValueError('EXPLICIT_FINITE_DENOMINATOR_REQUIRED')
        fields = spec.get('fields')
        if not isinstance(fields, dict) or not fields:
            raise ValueError('MEASUREMENT_DEFINITIONS_REQUIRED')
        registry=S.config().get('assessment_definitions',{})
        for definition in fields.values():
            if not isinstance(definition, dict) or any(not definition.get(k) for k in ('unit','method','source','definition_id')):
                raise ValueError('MEASUREMENT_CONTRACT_REQUIRED')
            registered=registry.get(definition['definition_id'])
            if not registered or any(registered.get(k)!=definition[k] for k in ('source','unit','method')):
                raise ValueError('OBSERVATION_DEFINITION_UNREGISTERED')
        maximum = S.config().get('assessment_max_sessions')
        if spec['kind'] != 'fact' and (maximum is None or len(expected) > maximum):
            value.update(support='capacity_blocked', reason='assessment_horizon_not_configured_or_exceeded')
        else:
            value.update(support='supported', reason=None)
            if spec['kind']!='fact':
                import market_calendar as M
                start,end=(S.instant(spec[k]).date().isoformat() for k in ('start_at','end_at'))
                calendar=M.find_calendar_snapshot(start,end,registered_at)
                if calendar.get('status')!='complete':
                    value.update(support='capacity_blocked',reason='assessment_horizon_calendar_unavailable')
                elif len([d for d in M.session_dates(calendar) if start<=d<=end])>maximum:
                    value.update(support='capacity_blocked',reason='assessment_horizon_sessions_exceeded')
                else:value['calendar_id']=calendar['calendar_id']
    except (KeyError, TypeError, ValueError) as exc:
        value.update(support='evaluation_unsupported', reason=str(exc))
    return value


def register_stage(stage, chain, specs):
    """Called after acceptance. Retries reconstruct the same deterministic IDs."""
    with S.transaction() as db:
        result = []
        for index, spec in enumerate(specs):
            identifier = 'assessment-' + S.digest([stage['stage_id'], index])[:40]
            old = S.get('assessment', identifier, conn=db)
            if old:
                if old['spec'] != spec:
                    raise ValueError('ASSESSMENT_IMMUTABLE')
                result.append(old)
                continue
            body = {'spec': spec, 'stage_id':stage['stage_id'], 'chain_id':chain['chain_id'],
                    'run_attempt_id':chain['run_attempt_id'], 'code':chain['code'],
                    'confidence_for':chain['confidence_for'], 'state':'accepted',
                    'owner':chain['run_attempt_id'], 'verdict':'not_due', 'origin_at':chain['created_at'],'usage':'submitted'}
            if spec['support'] != 'supported':
                body.update(state=spec['support'], verdict='unsupported', closed_at=S.now())
                S.save_once('extension_request', identifier, {'assessment_id':identifier, 'reason':spec['reason'],
                    'text':spec['text'], 'state':'queued', 'owner':chain['run_attempt_id']}, conn=db)
                S.link(db,'assessment',identifier,'extension_request',identifier)
            result.append(S.put(db, 'assessment', identifier, body))
        return result


def mark_usage(chain, judgment_id, adopted_claims, adopted_paths):
    with S.transaction() as db:
        for item in S.page('assessment',owner=chain['run_attempt_id'],limit=200,conn=db):
            if item['chain_id']!=chain['chain_id']:continue
            usage='adopted' if item['spec']['claim_id'] in adopted_claims and item['spec']['path_id'] in adopted_paths else 'not_adopted'
            if item.get('usage')==usage and item.get('judgment_id')==judgment_id:continue
            S.put(db,'assessment',item['id'],{**item,'usage':usage,'judgment_id':judgment_id})
            if usage=='adopted' and item['spec']['support']!='supported':
                import improvement_issues as I
                from opportunity_cases import identifier
                I.record(db,'assessment_extension',identifier(chain['run_attempt_id'],chain['code'],chain['confidence_for']),
                    item['id'],'사용한 전제를 평가하려면 어떤 자료·정의·평가기 확장이 필요한가?',
                    expected=item['spec'],actual={'support':item['spec']['support'],'reason':item['spec']['reason']},
                    source_revision=S.digest(item['spec']))


def ingest(assessment_id, observation):
    """Trusted collector API; deliberately not exposed to the investigating AI."""
    assessment = S.get('assessment', assessment_id)
    if not assessment:
        raise ValueError('UNKNOWN_ASSESSMENT')
    spec = assessment['spec']
    for field in ('key','observed_at','available_at','source_identity','fields'):
        if field not in observation:
            raise ValueError('OBSERVATION_REQUIRED:' + field)
    if observation['key'] not in spec.get('expected_observations', []):
        raise ValueError('OBSERVATION_OUTSIDE_SCOPE')
    observed, available = S.instant(observation['observed_at']), S.instant(observation['available_at'])
    if not S.instant(spec['start_at']) <= observed <= S.instant(spec['end_at']) or available > S.instant(S.now()) or available<observed:
        raise ValueError('OBSERVATION_TIME_MISMATCH')
    for field, value in observation['fields'].items():
        definition = spec['fields'].get(field)
        if not definition or any(value.get(k) != definition[k] for k in ('unit','method','source','definition_id')):
            raise ValueError('OBSERVATION_MEANING_MISMATCH')
        if value.get('code', spec['code']) != spec['code']:
            raise ValueError('OBSERVATION_SUBJECT_MISMATCH')
        n = value.get('value')
        if n is not None and (isinstance(n,bool) or not isinstance(n,(int,float)) or not math.isfinite(n)):
            raise ValueError('INVALID_OBSERVATION_VALUE')
    identifier = 'observation-' + S.digest([assessment_id, observation])
    with S.transaction() as db:
        old = S.get('observation', identifier, conn=db)
        if old:
            return old
        row = S.put(db,'observation',identifier, {'assessment_id':assessment_id, 'owner':assessment_id, **observation})
        current = S.get('assessment', assessment_id, conn=db)
        if current['state'] in {'finalized','finalized_unknown'} and current.get('support_ended'):
            raise ValueError('ASSESSMENT_SUPPORT_EXPIRED')
        S.put(db, 'assessment', assessment_id, {**current,'dirty':True})
        S.link(db,'assessment',assessment_id,'observation',identifier)
        return row


def _evaluate(expr, rows, complete, current=None):
    op = expr['op']
    if op == 'literal':
        return expr['value'], expr['unit']
    if op == 'value':
        field = (current or {}).get('fields',{}).get(expr['field'], {})
        return field.get('value'), field.get('unit')
    if op in {'all','any','sum','mean','count'}:
        values = [_evaluate(expr['arg'], rows, complete, r) for r in rows]
        known = [v for v,u in values if v is not None]
        full = complete and len(known)==len(values) and bool(values)
        if op in {'all','any'}:
            if any(u!='bool' for v,u in values): raise ValueError('BOOLEAN_REQUIRED')
            if op=='all': return (False if any(v is False for v in known) else True if full else None), 'bool'
            return (True if any(v is True for v in known) else False if full else None), 'bool'
        if op=='count': return ((sum(v is True for v in known) if all(u=='bool' for v,u in values) else len(known)) if full else None), 'count'
        units = {u for v,u in values if v is not None}
        if len(units)>1: raise ValueError('AGGREGATE_UNIT_MISMATCH')
        if 'bool' in units:raise ValueError('NUMERIC_AGGREGATE_REQUIRED')
        result=(sum(known) if op=='sum' else sum(known)/len(known)) if full else None
        if result is not None and not math.isfinite(result):raise ValueError('NONFINITE_AGGREGATE')
        return result, next(iter(units),None)
    if op in {'and','or'}:
        values = [_evaluate(x, rows, complete, current) for x in expr['args']]
        if any(u!='bool' for v,u in values): raise ValueError('BOOLEAN_REQUIRED')
        vals = [v for v,u in values]
        decisive = False if op=='and' else True
        return (decisive if decisive in vals else None if None in vals else not decisive), 'bool'
    if op=='not':
        v,u = _evaluate(expr['arg'],rows,complete,current)
        if u!='bool': raise ValueError('BOOLEAN_REQUIRED')
        return (None if v is None else not v), u
    left, lu = _evaluate(expr['left'],rows,complete,current)
    right, ru = _evaluate(expr['right'],rows,complete,current)
    if left is None or right is None:
        return None, 'bool' if op=='compare' else lu
    if lu != ru: raise ValueError('COMPARISON_UNIT_MISMATCH')
    if op=='compare': return COMPARATORS[expr['operator']](left,right), 'bool'
    if op=='difference': return left-right, lu
    return (left/right if right else None), 'ratio'


def evaluate(assessment_id, *, as_of=None, calendar=None):
    cutoff = S.instant(as_of or S.now())
    if cutoff > S.instant(S.now()): raise ValueError('FUTURE_KNOWLEDGE_FORBIDDEN')
    with S.transaction() as db:
        item = S.get('assessment', assessment_id, conn=db)
        if item is None: raise ValueError('UNKNOWN_ASSESSMENT')
        if item['spec']['support'] != 'supported': return item
        spec = item['spec']
        correction_end=None
        if calendar and calendar.get('status')=='complete':
            closes=sorted(S.instant(x['close_at']) for x in calendar['sessions'] if x['date']>S.instant(spec['end_at']).date().isoformat())
            if len(closes)>=5:correction_end=closes[4]
        candidates = db.execute("SELECT body,hash FROM records WHERE kind='observation' AND owner=?", (assessment_id,))
        chosen, conflict = {}, False
        for raw in candidates:
            row = S.decode(raw)
            if S.instant(row['available_at']) > cutoff or correction_end and S.instant(row['available_at'])>correction_end: continue
            key = row['key']; previous = chosen.get(key)
            if previous and previous['available_at']==row['available_at'] and previous['fields']!=row['fields']:
                conflict = True
            if not previous or row['available_at'] > previous['available_at']:
                chosen[key] = row
        complete = len(chosen)==len(spec['expected_observations'])
        input_hash = S.digest([spec,[chosen[k]['id'] for k in sorted(chosen)],conflict])
        period_complete = cutoff >= S.instant(spec['end_at'])
        correction_over = bool(correction_end and cutoff>=correction_end)
        # Same inputs at the same phase reuse the revision. Historical reads never
        # overwrite today's verdict with an older state.
        phase = [period_complete,correction_over]
        if item.get('input_hash')==input_hash and item.get('phase')==phase:
            return {**item,'known_at':cutoff.isoformat(),'historical_projection':True} if item.get('known_at') and cutoff<S.instant(item['known_at']) else item
        rows = [chosen[k] for k in sorted(chosen)]
        try:
            value, unit = _evaluate(spec['expression'], rows, complete, rows[0] if len(rows)==1 else None)
            if unit!='bool': raise ValueError('BOOLEAN_RESULT_REQUIRED')
            verdict = 'conflicting' if conflict else 'supported' if value is True else 'refuted' if value is False else 'unknown' if period_complete else 'not_due'
            reason = 'registered_rule_evidence_only'
        except ValueError as exc:
            verdict, reason = 'unknown', str(exc)
        state = 'evaluating' if not period_complete else 'correction_pending'
        if correction_over:
            state = 'finalized' if verdict in {'supported','refuted'} else 'finalized_unknown'
        result = {**item, 'verdict':verdict, 'state':state, 'reason':reason, 'phase':phase,
                  'input_hash':input_hash, 'known_at':cutoff.isoformat(), 'period_complete':period_complete,
                  'complete':complete, 'observed_count':len(chosen), 'expected_count':len(spec['expected_observations']),
                  'observation_ids':[r['id'] for r in rows], 'dirty':False,
                  'causal_validity':'not_established', 'performance_eligible':period_complete and complete and spec['registration_basis']=='prospective'}
        if correction_over:result.update(closed_at=S.now(),support_ended=True,correction_end_at=correction_end.isoformat())
        if item.get('known_at') and cutoff < S.instant(item['known_at']):
            return {**result,'historical_projection':True}
        saved = S.put(db, 'assessment', assessment_id, result)
        if verdict=='refuted' and item.get('usage')=='adopted':
            import improvement_issues as I
            from opportunity_cases import identifier
            I.record(db, 'premise_refuted', identifier(item['run_attempt_id'],item['code'],item['confidence_for']), assessment_id,
                     '사용한 전제의 반례가 판단 경로에 어떤 영향을 주는가?',
                     expected=spec, actual={'assessment_id':assessment_id,'observation_ids':result['observation_ids']},
                     source_revision=input_hash)
        return saved


def due(*, as_of=None, calendar=None, limit=200):
    with S.transaction(readonly=True) as db:
        ids = [] if db is None else [r[0] for r in db.execute(
            "SELECT id FROM records WHERE kind='assessment' AND state IN ('accepted','evaluating','correction_pending','finalized','finalized_unknown') AND (state NOT IN ('finalized','finalized_unknown') OR json_extract(body,'$.dirty')=1) ORDER BY updated_at,id LIMIT ?", (limit,))]
    return [evaluate(identifier,as_of=as_of,calendar=calendar) for identifier in ids]
