"""Receipts retain a continuation beyond the bounded assessment page."""
from datetime import timedelta
from test_ontology import isolated,C
import improvement_store as S
import assessment_observations as O


def test_more_than_one_page_is_processed_once_without_unrelated_starvation():
    key=C.now().date().isoformat()
    field={'definition_id':'institution.intraday_net@v1','source':'delivered_evidence','unit':'KRW','method':'intraday_estimate'}
    spec={'code':'000001','support':'supported','expected_observations':[key],
          'start_at':(C.now()-timedelta(days=1)).isoformat(),'end_at':(C.now()+timedelta(days=1)).isoformat(),
          'fields':{'institution':field}}
    with S.transaction() as db:
        for i in range(215):
            S.put(db,'assessment',f'assessment-{i:04}',{'state':'accepted','spec':{**spec,'code':'other' if i<10 else '000001'}})
    receipt={'delivery_id':'delivery-fixture','created_at':S.now(),'evidence_catalog':[
        {'subject':'000001','field':'institution','kind':'intraday_estimate','unit':'KRW','value':100,'evidence_id':'fixture-net'}]}
    assert len(O.from_delivery(receipt))==200
    assert S.page('observation_delivery')[0]['state']=='pending'
    assert len(O.advance())==5
    assert S.page('observation_delivery')[0]['state']=='completed'
    assert O.from_delivery(receipt)==[] and O.advance()==[]
    with S.transaction(readonly=True) as db:
        assert db.execute("SELECT count(*) FROM records WHERE kind='observation'").fetchone()[0]==205
        assert db.execute("SELECT count(*) FROM records WHERE kind='assessment' AND json_extract(body,'$.dirty')=1").fetchone()[0]==205
