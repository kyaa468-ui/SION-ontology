"""Host-verified policy evidence independent of historical trial payloads.

Only an explicit, parity-checked migration switches a legacy activation. The
active package, activation intent and rollback parameters remain authoritative.
This certificate retires one consumer, not every reader of a validation result.
"""
import copy
from contextlib import nullcontext

import common as C
import policy_registry as R

VERSION = 'POLICY_PROOF@v1'
MAX_BYTES = 2 * 1024 * 1024


def legacy_material(package, intent, params):
    import validation_store as V
    prepared = R.load('adoptions', package['adoption_id'])
    report = V.load('results', package['validation_result_id'])
    commit = V.load('result_commits', package['validation_result_id'])
    if (not prepared or not report or not commit or commit.get('result_hash') != V.digest(report)
            or V.digest(report) != package['validation_result_hash']
            or prepared.get('validation_result_hash') != package['validation_result_hash']
            or prepared.get('candidate_policy_hash') != package['candidate_policy_hash']
            or prepared.get('candidate_params') != R.semantic_params(params)
            or prepared.get('expected_revision') != R.revision(intent['before_params'])
            or report.get('adoption_eligibility', {}).get('status') != 'eligible'):
        raise ValueError('ADOPTION_PROOF_CHAIN_INVALID')
    return prepared, report, commit


def _eligible(validation):
    checks = (validation.get('criteria_complete') is True,
              validation.get('baseline_status') == 'passed',
              validation.get('effect_status') == 'complete',
              validation.get('performance_status') == 'complete',
              validation.get('criteria', {}).get('status') == 'passed',
              validation.get('independent_validation', {}).get('status') == 'verified',
              validation.get('adoption_eligibility', {}).get('status') == 'eligible')
    if not all(checks):
        raise ValueError('POLICY_PROOF_VALIDATION_INELIGIBLE')


def verify(body, package, intent, params):
    if body.get('version') != VERSION or body.get('verifier') != 'committed_adoption_and_validation@v1':
        raise ValueError('POLICY_PROOF_VERSION_UNSUPPORTED')
    identity = {'activation_id': intent['activation_id'], 'package_id': package['package_id'],
        'adoption_id': package['adoption_id'], 'validation_id': package['validation_id'],
        'validation_result_id': package['validation_result_id']}
    if body.get('identity') != identity:
        raise ValueError('POLICY_PROOF_IDENTITY_MISMATCH')
    hashes = body['source_hashes']
    if (hashes['package'] != R.digest(package) or hashes['activation_intent'] != R.digest(intent)
            or hashes['validation_result'] != package['validation_result_hash']
            or body['validation_commit'].get('result_hash') != hashes['validation_result']):
        raise ValueError('POLICY_PROOF_SOURCE_BINDING_MISMATCH')
    candidate = R.semantic_params(params)
    if (body['candidate_params'] != candidate
            or body['expected_revision'] != R.revision(intent['before_params'])
            or body['candidate_policy_hash'] != package['candidate_policy_hash']
            or body['candidate_policy_hash'] != R.digest({'params': candidate,
                'condition': package['condition'], 'effect': package['effect'], 'scope': package['scope']})):
        raise ValueError('POLICY_PROOF_CANDIDATE_MISMATCH')
    _eligible(body['validation'])
    if len(R.canonical(body).encode()) > MAX_BYTES:
        raise ValueError('POLICY_PROOF_CAPACITY_BLOCKED')
    return body


def resolve(package, intent, params):
    """Read-only dispatch; absent migration uses the exact legacy verifier."""
    binding = R.load('proof_bindings', intent['activation_id'])
    if binding is None:
        legacy_material(package, intent, params)
        return None
    if binding.get('version') != VERSION or binding.get('activation_id') != intent['activation_id']:
        raise ValueError('POLICY_PROOF_BINDING_INVALID')
    body = R.load('policy_proofs', binding['proof_id'])
    if not body or R.digest(body) != binding['proof_hash']:
        raise ValueError('POLICY_PROOF_UNAVAILABLE_OR_CHANGED')
    return verify(body, package, intent, params)


def _build(package, intent, params):
    import validation_spec as Q
    prepared, report, commit = legacy_material(package, intent, params)
    manifest = Q.load_spec(package['validation_id'])
    if (report.get('validation_id') != package['validation_id']
            or report.get('proposal') != manifest['proposal_snapshot']
            or report.get('spec') != manifest['spec']):
        raise ValueError('POLICY_PROOF_VALIDATION_SNAPSHOT_MISMATCH')
    validation = {key: copy.deepcopy(report.get(key)) for key in
                  ('baseline_status', 'effect_status', 'criteria', 'independent_validation', 'adoption_eligibility')}
    validation.update(criteria_complete=manifest.get('criteria_complete'),
        performance_status=report.get('performance', {}).get('status'),
        specification=copy.deepcopy(manifest['spec']),
        result_status=report.get('status', 'legacy_committed_result'),
        result_version=report.get('version'),
        source_snapshot_hash=R.digest(manifest['source_bundle']['base_snapshot']))
    body = {'version': VERSION, 'verifier': 'committed_adoption_and_validation@v1',
        'identity': {'activation_id': intent['activation_id'], 'package_id': package['package_id'],
            'adoption_id': package['adoption_id'], 'validation_id': package['validation_id'],
            'validation_result_id': package['validation_result_id']},
        'source_hashes': {'package': R.digest(package), 'activation_intent': R.digest(intent),
            'adoption': R.digest(prepared), 'validation_result': R.digest(report),
            'validation_spec': R.digest(manifest), 'proposal': R.digest(report['proposal'])},
        'validation_commit': copy.deepcopy(commit), 'validation': validation,
        'candidate_params': copy.deepcopy(prepared['candidate_params']),
        'candidate_policy_hash': prepared['candidate_policy_hash'],
        'expected_revision': prepared['expected_revision'],
        'source_activation_time': intent['created_at'],
        'detail_scope': 'verified_policy_evidence; historical_trial_replay_requires_originals'}
    return verify(body, package, intent, params)


def migrate(*, dry_run=False, fault=None):
    """Certify the current activation without changing numeric policy authority."""
    with nullcontext() if dry_run else R.change_lock():
        params = copy.deepcopy(C.params())
        before = R.snapshot(params)
        marker = params.get(R.MARKER)
        if not marker or marker.get('status') != 'active':
            return {'state': 'no_active_pointer', 'raw_deletion_authorized': False}
        if before['status'] == 'recording_error':
            raise ValueError('POLICY_PROOF_SOURCE_UNAVAILABLE:' + before.get('reason', 'unknown'))
        package = R.load('packages', marker['package_id'])
        intent = R.load('activation_intents', marker['activation_id'])
        binding = R.load('proof_bindings', marker['activation_id'])
        if binding:
            resolve(package, intent, params)
            return {'state': 'verified', 'proof_id': binding['proof_id'], 'replayed': True,
                    'raw_deletion_authorized': False}
        body = _build(package, intent, params)
        identifier = 'policy-proof-' + R.digest(body)[:40]
        result = {'state': 'dry_run' if dry_run else 'verified', 'proof_id': identifier,
            'activation_id': marker['activation_id'], 'proof_bytes': len(R.canonical(body).encode()),
            'source_result_hash': package['validation_result_hash'], 'raw_deletion_authorized': False,
            'retired_consumer': 'policy_registry.snapshot', 'policy_revision_changed': False}
        if dry_run:
            return result
        R.archive('policy_proofs', identifier, body)
        if fault:
            fault('proof_committed')
        R.archive('proof_bindings', marker['activation_id'], {'version': VERSION,
            'activation_id': marker['activation_id'], 'proof_id': identifier, 'proof_hash': R.digest(body)})
        if fault:
            fault('binding_committed')
        after = R.snapshot(params)
        if before != after:
            raise ValueError('POLICY_PROOF_CONSUMER_PARITY_FAILED')
        return result
