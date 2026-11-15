"""Producer-owned targets for live MA20 scoring, after explicit migration.

The acceptance intent is durable before the file commit. Publishing acceptance
and updating the target list share a SQLite writer transaction; a crash between
the two is recovered by looking up only pending IDs. Historical replay keeps its
separate legacy reader. This contract does not authorize deleting source files.
"""
from contextlib import contextmanager
from functools import wraps
import gc

import common as C
import improvement_store as I

VERSION = 'SCORING_TARGETS@v1'
MAX_PENDING = 200
MAX_MIGRATION_TARGETS = 20000
MAX_MIGRATION_BYTES = 32 * 1024 * 1024
TERMINAL = ('finalized', 'finalized_unknown')


class TargetError(RuntimeError):
    pass


def acceptance_guard(fn):
    @wraps(fn)
    def guarded(*args, **kwargs):
        import storage_leases as L
        base=C.ontology_root()
        base.mkdir(parents=True,exist_ok=True)
        pin=L.Pin(base)
        try:return fn(*args,**kwargs)
        finally:pin.close()
    return guarded


def state(db):
    if db is None:return None
    tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('scoring_target_control','scoring_target_enrollment')")}
    if not tables:return None
    if len(tables)!=2:raise TargetError('SCORING_TARGET_CONTROL_MISSING')
    row=db.execute('SELECT body,hash FROM scoring_target_control WHERE singleton=1').fetchone()
    marker=db.execute('SELECT hash FROM scoring_target_enrollment WHERE singleton=1').fetchone()
    if not row or not marker or row['hash']!=marker[0]:raise TargetError('SCORING_TARGET_MARKER_MISMATCH')
    value=I.decode(row)
    if value.get('version')!=VERSION:raise TargetError('SCORING_TARGET_VERSION_MISMATCH')
    for kind,key in [('scoring_target','target_count'),('scoring_acceptance','pending_count')]:
        count=db.execute('SELECT count(*) FROM records WHERE kind=?',(kind,)).fetchone()[0]
        if count!=value[key]:raise TargetError('SCORING_TARGET_MEMBERSHIP_MISMATCH:'+kind)
    return value


def _save(db, value):
    db.execute('CREATE TABLE IF NOT EXISTS scoring_target_control (singleton INTEGER PRIMARY KEY CHECK(singleton=1),body TEXT NOT NULL,hash TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS scoring_target_enrollment (singleton INTEGER PRIMARY KEY CHECK(singleton=1),hash TEXT NOT NULL)')
    encoded,hashed=I.canonical(value),I.digest(value)
    db.execute('INSERT OR REPLACE INTO scoring_target_control VALUES (1,?,?)',(encoded,hashed))
    db.execute('INSERT OR REPLACE INTO scoring_target_enrollment VALUES (1,?)',(hashed,))


def enabled():
    with I.transaction(readonly=True) as db:return state(db) is not None


def _drop(db,kind,identifier):
    db.execute('DELETE FROM records WHERE kind=? AND id=?',(kind,identifier))
    db.execute('DELETE FROM revisions WHERE kind=? AND id=?',(kind,identifier))
    db.execute('DELETE FROM links WHERE owner_kind=? AND owner_id=?',(kind,identifier))


def _compact_revisions(db,kind,identifier):
    # Live queue positions are not an as-of replay store.
    db.execute('DELETE FROM revisions WHERE kind=? AND id=? AND revision < (SELECT revision FROM records WHERE kind=? AND id=?)',
        (kind,identifier,kind,identifier))


def _source(kind,identifier):
    import ai_judgment as J
    import record_store as R
    from storage_objects import materialize
    path=J._path(kind,identifier)
    try:return materialize(R.load(path))
    except FileNotFoundError:
        if path.exists():raise TargetError('SCORING_ACCEPTANCE_SOURCE_INCOMPLETE')
        return None


def _binding(judgment,event):
    if (not event or event.get('status')!='accepted'
            or event.get('event_id')!=judgment['acceptance_event_id']
            or event.get('judgment_id')!=judgment['judgment_id']):
        raise TargetError('SCORING_ACCEPTANCE_COMMIT_MISMATCH')
    return {'known_at':max(I.instant(judgment['created_at']),I.instant(event['created_at'])).isoformat(),
        'judgment_hash':I.digest(judgment),'event_id':event['event_id'],'event_hash':I.digest(event)}


def _put_target(db, application, bindings, *, previous=None):
    identifier=application['application_id']
    old=previous or I.get('scoring_target',identifier,conn=db)
    if old and old['application']!=application:raise TargetError('SCORING_TARGET_APPLICATION_CHANGED')
    life=I.get('legacy_assessment_state',identifier,conn=db) or (old or {}).get('lifecycle')
    body={'owner':application['code'],'code':application['code'],'origin_at':application['created_at'],
        'application':application,'judgments':{**(old or {}).get('judgments',{}),**bindings},
        'lifecycle':life,'state':'correction_pending' if (life or {}).get('dirty') else (life or {}).get('state','open')}
    if life and life.get('closed_at'):body['closed_at']=life['closed_at']
    result=I.put(db,'scoring_target',identifier,body)
    _compact_revisions(db,'scoring_target',identifier)
    if life and I.get('legacy_assessment_state',identifier,conn=db):
        I.link(db,'scoring_target',identifier,'legacy_assessment_state',identifier,'required_state')
    return result


def prepare(judgment):
    """Called under the judgment request lock, before publishing its files."""
    if not judgment.get('premise_applications') or not enabled():return
    from storage_objects import materialize
    with I.transaction() as db:
        current=state(db)
        if not current:return
        identifier=judgment['judgment_id']
        old=I.get('scoring_acceptance',identifier,conn=db)
        body={'state':'pending','owner':judgment['run_attempt_id'],'origin_at':judgment['created_at'],
            'judgment_id':identifier,'event_id':judgment['acceptance_event_id'],
            'judgment_hash':I.digest(materialize(judgment)),
            'applications':materialize(judgment['premise_applications'])}
        if old and old['judgment_hash']==body['judgment_hash']:return
        if old and _source('submissions',old['event_id']):
            raise TargetError('SCORING_ACCEPTANCE_RECOVERY_REQUIRED')
        if not old and current['pending_count']>=MAX_PENDING:raise TargetError('SCORING_PENDING_CAPACITY')
        for app in body['applications']:
            target=I.get('scoring_target',app['application_id'],conn=db)
            if target and target['application']!=app:raise TargetError('SCORING_TARGET_APPLICATION_CHANGED')
            if target:
                prospective={**target,'judgments':{**target['judgments'],identifier:{
                    'known_at':I.now(),'judgment_hash':body['judgment_hash'],
                    'event_id':body['event_id'],'event_hash':'0'*64}}}
                if len(I.canonical(prospective).encode())>4*1024*1024-512:
                    raise TargetError('SCORING_TARGET_CAPACITY')
            if not target and (app['judgment_id']!=identifier or I.instant(app['created_at'])<I.instant(current['enrolled_at'])):
                raise TargetError('SCORING_TARGET_REUSE_UNAVAILABLE')
        I.put(db,'scoring_acceptance',identifier,body)
        _compact_revisions(db,'scoring_acceptance',identifier)
        if not old:current['pending_count']+=1
        _save(db,current)


def _finish(db, intent, current):
    event=_source('submissions',intent['event_id'])
    if event is None:return False
    judgment=_source('judgments',intent['judgment_id'])
    if not judgment or I.digest(judgment)!=intent['judgment_hash']:
        raise TargetError('SCORING_ACCEPTED_JUDGMENT_UNAVAILABLE')
    binding=_binding(judgment,event)
    if judgment['premise_applications']!=intent['applications']:
        raise TargetError('SCORING_ACCEPTANCE_TARGETS_CHANGED')
    for app in intent['applications']:
        old=I.get('scoring_target',app['application_id'],conn=db)
        if not old and app['judgment_id']!=judgment['judgment_id']:
            raise TargetError('SCORING_TARGET_REUSE_UNAVAILABLE')
        _put_target(db,app,{judgment['judgment_id']:binding},previous=old)
        if not old:current['target_count']+=1
    _drop(db,'scoring_acceptance',intent['id'])
    current['pending_count']-=1
    _save(db,current)
    return True


@contextmanager
def acceptance_transaction(judgment):
    if not judgment.get('premise_applications') or not enabled():
        yield
        return
    with I.transaction() as db:
        current=state(db)
        intent=I.get('scoring_acceptance',judgment['judgment_id'],conn=db)
        if not intent:raise TargetError('SCORING_ACCEPTANCE_INTENT_REQUIRED')
        yield
        if not _finish(db,intent,current):raise TargetError('SCORING_ACCEPTANCE_COMMIT_REQUIRED')


def recover(identifier=None):
    if not enabled():return {'recovered':0,'pending':0}
    with I.transaction() as db:
        current=state(db)
        rows=([I.get('scoring_acceptance',identifier,conn=db)] if identifier else
            I.page('scoring_acceptance',limit=MAX_PENDING,conn=db))
        recovered=sum(_finish(db,row,current) for row in rows if row)
        return {'recovered':recovered,'pending':current['pending_count']}


def lifecycle(db, application_id, value):
    """Same transaction as the evaluator's state/correction update."""
    current=state(db)
    if not current:return
    old=I.get('scoring_target',application_id,conn=db)
    if not old:return  # A historical replay must not resurrect retired targets.
    if old.get('lifecycle')==value:return
    body={**old,'lifecycle':value,'state':'correction_pending' if value.get('dirty') else value['state']}
    if value.get('closed_at'):body['closed_at']=value['closed_at']
    I.put(db,'scoring_target',application_id,body)
    _compact_revisions(db,'scoring_target',application_id)
    I.link(db,'scoring_target',application_id,'legacy_assessment_state',application_id,'required_state')


def pending(entry, knowledge_as_of):
    life=entry.get('lifecycle')
    return (not life or I.instant(knowledge_as_of)<I.instant(life['known_at'])
        or life['state'] not in TERMINAL or life.get('dirty',False))


def select(knowledge_as_of, market_as_of, *, calendar=None, limit=200, maximum_bytes=2*1024*1024, advance=False):
    """Live selection has honest counts, including a bounded incomplete page."""
    if type(limit) is not int or not 1<=limit<=200:raise TargetError('SCORING_TARGET_LIMIT')
    if type(maximum_bytes) is not int or not 1024<=maximum_bytes<=2*1024*1024:raise TargetError('SCORING_TARGET_BYTE_LIMIT')
    recover()
    import retention_policy as P
    import retention_store as RS
    boundary=P.boundaries(calendar,knowledge_as_of) if calendar is not None else RS.current_boundary(knowledge_as_of)
    with I.transaction(readonly=not advance) as db:
        current=state(db)
        if not current:raise TargetError('SCORING_TARGET_MIGRATION_REQUIRED')
        if I.instant(knowledge_as_of)<I.instant(current['supported_as_of']):
            return {'applications':[],'errors':[{'reason':'scoring_target_history_unavailable'}],
                'expected_count':None,'target_set_incomplete':True,'state':'expired'}
        where="""kind='scoring_target' AND julianday(origin_at)<=julianday(?)
            AND json_extract(body,'$.application.judgment_date')<=?
            AND EXISTS (SELECT 1 FROM json_each(records.body,'$.judgments') j
                WHERE julianday(json_extract(j.value,'$.known_at'))<=julianday(?))"""
        args=[knowledge_as_of,market_as_of,knowledge_as_of]
        # Unknown calendar keeps all retained targets; it never guesses expiry.
        if boundary.get('deletion_allowed'):
            where+=" AND (state NOT IN ('finalized','finalized_unknown') OR closed_at IS NULL OR substr(closed_at,1,10)>=?)"
            args.append(boundary['case_closed_cutoff'])
        expected=db.execute('SELECT count(*) FROM records WHERE '+where,args).fetchone()[0]
        after=current.get('selection_after','') if advance else ''
        query='SELECT id,body,hash,length(CAST(body AS BLOB)) AS bytes FROM records WHERE '+where+' AND id>? ORDER BY id LIMIT ?'
        rows=db.execute(query,(*args,after,limit)).fetchall()
        if not rows and after:rows=db.execute(query,(*args,'',limit)).fetchall()
        applications=[];used=0;errors=[]
        for raw in rows:
            if used+raw['bytes']>maximum_bytes:break
            row=I.decode(raw)
            ids=sorted(j for j,b in row['judgments'].items() if I.instant(b['known_at'])<=I.instant(knowledge_as_of))
            if not ids:
                errors.append({'reason':'scoring_target_acceptance_not_available','application_id':row['id']});continue
            applications.append({'application':row['application'],'judgment_ids':ids,'lifecycle':row['lifecycle']})
            used+=raw['bytes']
        if len(applications)!=expected:errors.append({'reason':'scoring_target_capacity_or_availability','returned':len(applications),'expected':expected})
        if advance and applications:
            current['selection_after']=applications[-1]['application']['application_id'] if len(applications)<expected else ''
            _save(db,current)
        return {'applications':applications,'errors':errors,'expected_count':expected,
            'target_set_incomplete':bool(errors),'source':'producer_scoring_targets',
            'scope':'pending_and_retained_completed','boundary':boundary,'returned_body_bytes':used}


def retire(db, candidates, at):
    """Count and support watermark commit with generic target-row deletion."""
    current=state(db)
    if not current:return
    count=sum(r['kind']=='scoring_target' for r in candidates)
    if count:
        current['target_count']-=count
        current['supported_as_of']=max(I.instant(current['supported_as_of']),I.instant(at)).isoformat()
        current['retired_count']+=count
        _save(db,current)


def _inventory(at):
    import ai_judgment as J
    from storage_objects import materialize
    selected=materialize(J.committed_premises(at,I.instant(at).date().isoformat()))
    if selected['target_set_incomplete'] or selected['unavailable_applications']:
        raise TargetError('SCORING_MIGRATION_SOURCE_INCOMPLETE')
    entries=selected['applications']
    if len(entries)>MAX_MIGRATION_TARGETS:raise TargetError('SCORING_MIGRATION_TARGET_LIMIT')
    result={};used=0
    for entry in entries:
        app=entry['application'];bindings={}
        for identifier in entry['judgment_ids']:
            row=_source('judgments',identifier)
            if not row:raise TargetError('SCORING_MIGRATION_JUDGMENT_MISSING')
            bindings[identifier]=_binding(row,_source('submissions',row['acceptance_event_id']))
        # The lifecycle/window is also copied into the live target. Include it
        # in both admission and the migration race certificate.
        item={'application':app,'judgments':bindings,
            'lifecycle':I.get('legacy_assessment_state',app['application_id'])}
        used+=len(I.canonical(item).encode())
        if used>MAX_MIGRATION_BYTES:raise TargetError('SCORING_MIGRATION_BYTES_LIMIT')
        result[app['application_id']]=item
    return result


def migrate(*,dry_run=True):
    import storage_leases as L
    with I.transaction(readonly=True) as db:
        current=state(db)
        if current:return {**current,'state':'enabled','replayed':True,'raw_deletion_authorized':False}
    at=I.now();inventory=_inventory(at);fingerprint=I.digest(inventory)
    proof={'version':VERSION,'enrolled_at':at,'supported_as_of':at,'target_count':len(inventory),
        'pending_count':0,'retired_count':0,'source_inventory_hash':fingerprint}
    if dry_run:return {**proof,'state':'dry_run','raw_deletion_authorized':False}
    base=C.ontology_root();base.mkdir(parents=True,exist_ok=True);gc.collect()
    with L.exclusive(base):
        with I.transaction() as db:
            current=state(db)
            if current:return {**current,'state':'enabled','replayed':True,'raw_deletion_authorized':False}
            if I.digest(_inventory(I.now()))!=fingerprint:raise TargetError('SCORING_MIGRATION_SOURCE_CHANGED')
            if db.execute("SELECT 1 FROM records WHERE kind IN ('scoring_target','scoring_acceptance') LIMIT 1").fetchone():
                raise TargetError('SCORING_MIGRATION_ORPHAN_TARGETS')
            for item in inventory.values():_put_target(db,item['application'],item['judgments'])
            _save(db,proof)
            actual={r['id']:{'application':r['application'],'judgments':r['judgments'],'lifecycle':r['lifecycle']}
                for r in (I.decode(raw) for raw in db.execute("SELECT body,hash FROM records WHERE kind='scoring_target'"))}
            if actual!=inventory:raise TargetError('SCORING_MIGRATION_PARITY_FAILED')
    return {**proof,'state':'enabled','raw_deletion_authorized':False}
