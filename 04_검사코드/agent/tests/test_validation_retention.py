"""Q3 protects old validation readers as well as the new four-role runner."""
from datetime import timedelta
import pytest
from test_ontology import isolated,C
from test_improvement import calendar
import improvement_store as S
import retention_store as R
import retention_policy as P


def fixture_spec(start,end,mode='retrospective'):
    return {'mode':mode,'period':{'start':start.isoformat(),'end':end.isoformat()},
            'scope':{'codes':['000001'],'confidence_for':['매수']}}


def test_old_retrospective_registration_cannot_create_its_own_exception(monkeypatch):
    monkeypatch.setattr(R,'current_boundary',lambda *_:P.boundaries(calendar()))
    spec=fixture_spec(C.now()-timedelta(days=90),C.now()-timedelta(days=85))
    with pytest.raises(ValueError,match='detailed_replay_expired'):
        R.register_validation_support('old',spec,S.now())
    assert S.get('retention_pin','validation:old') is None


def test_preregistered_range_is_finite_and_cannot_change_spec(monkeypatch):
    from test_research_capacity import configure
    configure(monkeypatch)
    now=C.now();spec=fixture_spec(now+timedelta(days=1),now+timedelta(days=4),'prospective')
    row=R.register_validation_support('future',spec,S.now())
    assert S.instant(row['until'])==now+timedelta(days=34)
    manifest={'validation_id':'future','spec':spec}
    monkeypatch.setattr(C,'now',lambda:now+timedelta(days=25))
    assert R.require_validation_support(manifest)['basis']=='preregistered_live_validation'
    spec['scope']['codes']=['changed']
    with pytest.raises(ValueError,match='SPEC_CHANGED'):R.require_validation_support(manifest)
    monkeypatch.setattr(C,'now',lambda:now+timedelta(days=35))
    monkeypatch.setattr(R,'current_boundary',lambda *_:{'deletion_allowed':False})
    with pytest.raises(ValueError,match='calendar_unavailable'):R.require_validation_support(manifest)


def test_input_collection_and_import_stop_before_original_reads(monkeypatch):
    import validation_dataset as D
    now=C.now();spec=fixture_spec(now-timedelta(days=90),now-timedelta(days=85))
    monkeypatch.setattr(D,'load_spec',lambda _: {'validation_id':'old','spec':spec})
    monkeypatch.setattr(R,'current_boundary',lambda *_:P.boundaries(calendar()))
    monkeypatch.setattr(D,'get_inputs',lambda *_:pytest.fail('expired original read'))
    for call in (lambda:D.ensure_inputs('old'),lambda:D.build_inputs('old'),lambda:D.import_validation_market('old','tape',[])):
        with pytest.raises(ValueError,match='detailed_replay_expired'):call()


def test_live_registered_range_protects_later_created_case_and_evidence():
    import retention_gc as G
    old=(C.now()-timedelta(days=90)).isoformat()
    with S.transaction() as db:
        S.put(db,'retention_pin','validation:range',{'state':'active','until':(C.now()+timedelta(days=1)).isoformat(),
            'period':{'start':old,'end':old},'scope':{'codes':['000001']}})
        for key,code in [('wanted','000001'),('other','000002')]:
            S.put(db,'opportunity',key,{'state':'completed','origin_at':old,'closed_at':old,'code':code})
            S.put(db,'evidence',key,{'state':'recorded','origin_at':old,'content':code})
            S.link(db,'opportunity',key,'evidence',key)
    G.collect(calendar(),dry_run=False)
    assert S.get('opportunity','wanted') and S.get('evidence','wanted')
    assert S.get('opportunity','other') is None and S.get('evidence','other') is None
    with S.transaction() as db:
        pin=S.get('retention_pin','validation:range',conn=db)
        S.put(db,'retention_pin',pin['id'],{**pin,'state':'completed','closed_at':S.now()})
    G.collect(calendar(),dry_run=False)
    assert S.get('opportunity','wanted') is None and S.get('evidence','wanted') is None
