"""The prior comparison remains meaningful with the original dataset absent."""
import copy
import pytest
from test_ontology import C,isolated
import case_review as R
import case_store as S
import review_comparison as P


def fixture():
    cases=[{'application_id':'a','features':{'gap':.1},'verdict':'supported'},
           {'application_id':'b','features':{'gap':None},'verdict':'unknown'}]
    dataset={'review_id':'prior','cases':cases,'input_hash':S.digest(cases),'knowledge_as_of':C.now().isoformat()}
    result={'review_id':'prior','input_hash':dataset['input_hash'],'completion_contract':'review_result_commit@v1'}
    S.archive('datasets','prior',dataset);S.archive('results','prior',result)
    S.archive('result_commits','prior',{'result_hash':S.digest(result),'input_hash':dataset['input_hash']})
    return cases,dataset,result


def test_migration_preserves_changed_unchanged_and_new_with_dataset_masked(monkeypatch):
    cases,dataset,result=fixture()
    next_cases=copy.deepcopy(cases)+[{'application_id':'c','features':{}}]
    next_cases[1]['features']['gap']=0
    before=R._changes(next_cases,C.now(),result)
    assert before=={'previous_review_id':'prior','new':['c'],'changed':['b'],'unchanged':['a']}
    assert P.migrate('prior')['status']=='verified'
    original=S.load
    def masked(kind,key):
        if kind=='datasets':raise AssertionError('original comparison dataset reopened')
        return original(kind,key)
    monkeypatch.setattr(S,'load',masked)
    assert R._changes(next_cases,C.now(),result)==before


def test_missing_prior_and_uncommitted_projection_cannot_reset_change_history():
    with pytest.raises(ValueError,match='EVIDENCE_UNAVAILABLE'):
        R._changes([{'application_id':'a'}],C.now(),{'review_id':'missing'})
    cases,dataset,result=fixture()
    P.publish('prior',dataset)
    S._path('comparison_summary_commits','prior').unlink()
    with pytest.raises(ValueError,match='NOT_COMMITTED'):P.load(result)


def test_signature_does_not_treat_missing_as_zero_or_report_time_as_change():
    assert P.signature({'application_id':'a','features':{'gap':None}})!=P.signature({'application_id':'a','features':{'gap':0}})
    assert P.signature({'application_id':'a','report_at':'old'})==P.signature({'application_id':'a','report_at':'new'})
    with pytest.raises(ValueError,match='DUPLICATE'):P.signatures([{'application_id':'a'},{'application_id':'a'}])
