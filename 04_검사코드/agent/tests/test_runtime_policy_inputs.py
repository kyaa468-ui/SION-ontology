"""Runtime and validation share frozen adopted-claim condition inputs."""
import copy
from datetime import timedelta
import pytest
from test_ontology import isolated,C,M
from test_case_dataset import traced_delivery,regime,premise_item
import ai_judgment as J
import case_dataset as D
import policy_registry as R
import runtime_policy_inputs as F
import validation_dataset as V
import improvement_store as S


def accepted(monkeypatch,premises=True):
    text,_=traced_delivery(monkeypatch);regime()
    item=premise_item(text)
    if not premises:item['premises']=[]
    result=M.record_ai_judgments([item])[0]
    assert result['status']=='accepted',result
    return J.load_judgment(result['judgment_id'])


@pytest.mark.parametrize('premises',[True,False])
def test_producer_capture_matches_runtime_and_validation_without_original_reader(monkeypatch,premises):
    judgment=accepted(monkeypatch,premises)
    original=F.original(judgment)
    expected={key:original[key] for key in F.FIELDS}
    assert F.read(judgment)==expected
    before=V._judgment_context(judgment)
    monkeypatch.setattr(D,'_read_original',lambda *args:pytest.fail('Original evidence read'))
    assert R.judgment_inputs(judgment)==expected
    assert V._judgment_context(judgment)==before
    assert F.capture(judgment)==expected


def test_changed_judgment_cannot_reuse_frozen_condition_values(monkeypatch):
    judgment=accepted(monkeypatch)
    changed=copy.deepcopy(judgment);changed['arguments']=[]
    with pytest.raises(ValueError,match='BINDING_INVALID'):R.judgment_inputs(changed)


def test_corrupt_saved_values_do_not_fall_back_to_originals(monkeypatch):
    judgment=accepted(monkeypatch);original=S.get
    def read(kind,key,**kwargs):
        value=original(kind,key,**kwargs)
        if kind=='judgment_policy_inputs':
            value=copy.deepcopy(value);value['values']['features']['gap']=100
        return value
    monkeypatch.setattr(S,'get',read)
    with pytest.raises(ValueError,match='BINDING_INVALID'):R.judgment_inputs(judgment)


def test_legacy_without_projection_remains_readable_but_is_not_certified(monkeypatch):
    judgment=accepted(monkeypatch)
    legacy=copy.deepcopy(judgment);legacy['judgment_id']='legacy-uncaptured'
    assert F.read(legacy) is None
    assert R.judgment_inputs(legacy)==F.original(legacy)
    assert S.get('judgment_policy_inputs',legacy['judgment_id']) is None
    monkeypatch.setattr(F,'MAX_BYTES',1)
    with pytest.raises(ValueError,match='CAPACITY_BLOCKED'):F.capture(legacy)
    assert S.get('judgment_policy_inputs',legacy['judgment_id']) is None


@pytest.mark.parametrize('active',[True,False])
def test_frozen_inputs_follow_parent_lifetime_and_do_not_reconstruct_after_expiry(monkeypatch,active):
    from test_improvement import calendar
    import retention_gc as G
    judgment=accepted(monkeypatch)
    with S.transaction() as db:
        # Replace the fixture's live opportunity with an explicit lifecycle.
        db.execute("DELETE FROM links WHERE owner_kind='opportunity'")
        db.execute("DELETE FROM records WHERE kind='opportunity'")
        S.put(db,'opportunity','parent',{'state':'open' if active else 'completed',
            'origin_at':S.now(),'closed_at':None if active else S.now()})
        S.link(db,'opportunity','parent','judgment_policy_inputs',judgment['judgment_id'])
    later=C.now()+timedelta(days=90);monkeypatch.setattr(C,'now',lambda:later)
    G.collect(calendar(),dry_run=False)
    monkeypatch.setattr(F,'original',lambda *_:pytest.fail('Expired raw reconstruction'))
    if active:assert R.judgment_inputs(judgment)['features']
    else:
        assert S.get('judgment_policy_inputs',judgment['judgment_id']) is None
        with pytest.raises(ValueError,match='DETAIL_EXPIRED'):R.judgment_inputs(judgment)
