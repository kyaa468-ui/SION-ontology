"""Two distinct twenty-session windows; never guess an exchange calendar."""
from __future__ import annotations
import improvement_store as S


def boundaries(calendar, at=None):
    if calendar.get('status')!='complete' or not calendar.get('calendar_id'):
        return {'status':'calendar_unavailable','deletion_allowed':False}
    cutoff=S.instant(at or S.now())
    try:
        dates=[x['date'] for x in calendar.get('sessions',[])]
        if len(dates)!=len(set(dates)):raise ValueError('DUPLICATE_SESSION')
        sessions=sorted(x['date'] for x in calendar.get('sessions',[]) if S.instant(x['close_at'])<=cutoff)
    except (KeyError,TypeError,ValueError):return {'status':'calendar_incomplete','deletion_allowed':False}
    if len(sessions)<20:
        return {'status':'calendar_incomplete','deletion_allowed':False}
    return {'status':'ready','deletion_allowed':True,'calendar_id':calendar['calendar_id'],
            'calendar_hash':S.digest(calendar),'knowledge_as_of':cutoff.isoformat(),'timezone':'Asia/Seoul',
            'last_completed_session':sessions[-1],'case_closed_cutoff':sessions[-20],
            'detailed_origin_cutoff':sessions[-20]}


def capabilities(case, boundary):
    if not boundary.get('deletion_allowed'):
        return {'status':'calendar_unavailable','deletion_allowed':False}
    closed=case.get('closed_at')
    recent_case=not closed or S.instant(closed).date().isoformat()>=boundary['case_closed_cutoff']
    recent_detail=S.instant(case['origin_at']).date().isoformat()>=boundary['detailed_origin_cutoff']
    original=case.get('capabilities',{})
    return {'cause_investigation':recent_case and bool(original.get('cause_investigation')),
            'premise_reevaluation':recent_case and bool(original.get('premise_reevaluation')),
            'semantic_reexperiment':recent_detail and bool(original.get('semantic_reexperiment')),
            'portfolio_reexperiment':recent_detail and bool(original.get('portfolio_reexperiment')),
            'case_window_basis':'first_confirmed_completion','detail_window_basis':'original_judgment',
            'availability':'available' if recent_case else 'expired'}
