"""Accepted-judgment condition inputs with the original fields and meanings.

This covers judgments without premises and all adopted claims, not whichever
premise happens to be first. Only the producer or explicit migration captures
inputs; ordinary reads never reconstruct missing historical evidence.
"""
import copy

import ai_judgment as J
import improvement_store as S

VERSION = 'RUNTIME_POLICY_INPUTS@v1'
FIELDS = ('features', 'signed_flows', 'stratum', 'adoption', 'source_manifest', 'comparison_context',
          'feature_eligible', 'feature_reasons', 'selection_observation_snapshot',
          'selection_definition_snapshot', 'selection_settings_snapshot', 'decision_regime_snapshot')
MAX_BYTES = 2 * 1024 * 1024


def original(judgment):
    import case_dataset as D
    application = {'application_id': None, 'judgment_id': judgment['judgment_id'],
        'code': judgment['code'], 'judgment_date': judgment['created_at'][:10],
        'claim_ids': [a['id'] for a in judgment.get('arguments', []) if a.get('adoption') == 'adopted']}
    return D.extract_case_context(judgment, application)


def capture(judgment):
    old = S.get('judgment_policy_inputs', judgment['judgment_id'])
    if old is not None:
        return read(judgment)
    with S.transaction(readonly=True) as db:
        _require_supported(db,judgment)
    context = original(judgment)
    values = {key: copy.deepcopy(context[key]) for key in FIELDS}
    body = {'schema_version': VERSION, 'judgment_id': judgment['judgment_id'],
        'judgment_hash': J._digest(judgment), 'origin_at': judgment['created_at'],
        'owner': judgment['run_attempt_id'], 'code': judgment['code'],
        'state': 'completed', 'closed_at': judgment['created_at'],
        'values': values, 'values_hash': J._digest(values),
        'consumer': 'policy_registry.judgment_inputs', 'raw_deletion_authorized': False}
    if len(J._json(body).encode()) > MAX_BYTES:
        raise ValueError('POLICY_INPUT_CAPACITY_BLOCKED')
    with S.transaction() as db:
        _require_supported(db,judgment)
        S.save_once('judgment_policy_inputs', judgment['judgment_id'], body, conn=db)
        for parent in db.execute("""SELECT r.id FROM records r WHERE r.kind='opportunity' AND r.owner=?
                AND EXISTS (SELECT 1 FROM json_each(r.body,'$.judgments') j
                    WHERE json_extract(j.value,'$.judgment_id')=?)""",(judgment['run_attempt_id'],judgment['judgment_id'])):
            S.link(db,'opportunity',parent['id'],'judgment_policy_inputs',judgment['judgment_id'])
    stored = read(judgment)
    if stored != values:
        raise ValueError('POLICY_INPUT_PARITY_FAILED')
    return stored


def read(judgment):
    with S.transaction(readonly=True) as db:
        body = S.get('judgment_policy_inputs', judgment['judgment_id'], conn=db) if db else None
        if body is None:
            _require_supported(db,judgment)
    if body is None:
        return None
    if (body.get('schema_version') != VERSION or body.get('judgment_id') != judgment['judgment_id']
            or body.get('judgment_hash') != J._digest(judgment)
            or body.get('values_hash') != J._digest(body.get('values'))
            or set(body.get('values', {})) != set(FIELDS)):
        raise ValueError('POLICY_INPUT_BINDING_INVALID')
    return copy.deepcopy(body['values'])


def _require_supported(db,judgment):
    if db is None:return
    bound=db.execute("SELECT value FROM meta WHERE key='support_lower_bound'").fetchone()
    if not bound or S.instant(judgment['created_at']).date().isoformat()>=bound[0]:return
    # An explicit old validation range may still read the original legacy
    # inputs. Missing data outside every live range must not be reconstructed.
    for index,raw in enumerate(db.execute("SELECT body,hash FROM records WHERE kind='retention_pin' AND state='active' AND json_extract(body,'$.until')>? LIMIT 2001",(S.now(),))):
        if index>=2000:raise ValueError('POLICY_INPUT_PIN_SCOPE_LIMIT')
        pin=S.decode(raw);period=pin.get('period',{});codes=pin.get('scope',{}).get('codes',['*'])
        if (period.get('start') and period.get('end')
                and S.instant(period['start'])<=S.instant(judgment['created_at'])<=S.instant(period['end'])
                and ('*' in codes or judgment['code'] in codes)):return
    raise ValueError('POLICY_INPUT_DETAIL_EXPIRED')
