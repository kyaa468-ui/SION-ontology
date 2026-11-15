"""Independent, immutable signatures for the immediately preceding review.

This certificate retires only the weekly change-comparison consumer. It does
not authorize deleting datasets still needed by a case page or validation.
"""
import case_store as S

FIELDS=('application_id','origin_judgment_id','features','stratum','adoption',
        'decision_regime_snapshot','comparison_context','evaluation_id',
        'verdict','period_status','data_status','recording_status',
        'outcome_eligible','model_eligible','feature_reasons','evaluation_reasons',
        'conflicting_evaluation_ids','judgment_ids','reference_returns')


def signature(case):
    return S.digest({key:case.get(key) for key in FIELDS})


def signatures(cases):
    result={}
    for case in cases:
        key=case['application_id']
        if key in result:raise ValueError('DUPLICATE_COMPARISON_APPLICATION')
        result[key]=signature(case)
    return result


def publish(review_id,dataset):
    values=signatures(dataset['cases'])
    body={'schema':'REVIEW_COMPARISON@v1','review_id':review_id,
          'input_hash':dataset['input_hash'],'dataset_hash':S.digest(dataset),
          'knowledge_as_of':dataset['knowledge_as_of'],'case_signatures':values,
          'fields':list(FIELDS),'consumer':'case_review.weekly_changes'}
    S.archive('comparison_summaries',review_id,body)
    commit={'review_id':review_id,'summary_hash':S.digest(body),'dataset_hash':body['dataset_hash'],
            'consumer_contract':'exact_case_change_signature@v1'}
    S.archive('comparison_summary_commits',review_id,commit)
    return body


def load(previous):
    key=previous['review_id']
    body=S.load('comparison_summaries',key)
    commit=S.load('comparison_summary_commits',key)
    if body or commit:
        if not body or not commit or commit.get('summary_hash')!=S.digest(body) or body.get('schema')!='REVIEW_COMPARISON@v1':
            raise ValueError('COMPARISON_SUMMARY_NOT_COMMITTED')
        if body.get('input_hash')!=previous.get('input_hash') or body.get('fields')!=list(FIELDS):
            raise ValueError('COMPARISON_SUMMARY_CONTRACT_CHANGED')
        return body['case_signatures']
    # An old store stays on its verified original until explicit migration.
    dataset=S.load('datasets',key)
    if not dataset:raise ValueError('PREVIOUS_COMPARISON_EVIDENCE_UNAVAILABLE')
    return signatures(dataset['cases'])


def migrate(review_id):
    import case_review as R
    result=S.load('results',review_id)
    if not result or not R.completion_commit(review_id,result):raise ValueError('COMPLETED_REVIEW_REQUIRED')
    dataset=S.load('datasets',review_id)
    if not dataset or dataset.get('input_hash')!=result.get('input_hash'):raise ValueError('REVIEW_DATASET_UNAVAILABLE')
    before=signatures(dataset['cases'])
    publish(review_id,dataset)
    after=load(result)
    if before!=after:raise ValueError('COMPARISON_MIGRATION_PARITY_FAILED')
    return {'status':'verified','review_id':review_id,'case_count':len(before),
            'projection_hash':S.digest(before),'consumer':'case_review.weekly_changes',
            'other_consumers_retired':False,'raw_deletion_authorized':False}
