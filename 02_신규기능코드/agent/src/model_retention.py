"""Explicit retirement of the discontinued premise-model archive.

Enrollment closes its old producer namespace. Only verified, finished legacy
artifacts can then retire; active assessments and registered ranges still own
their inputs. Publication markers stay available to historical review readers.
"""
from pathlib import Path

import common as C
import improvement_store as I

VERSION = 'MODEL_RETENTION@v1'
CLOSED_KINDS = ('models', 'model_publications', 'publication_commits', 'predictions', 'prediction_targets')
DETAIL_KINDS = ('models', 'predictions', 'prediction_targets')
MAX_RECORDS = 20000
FORECAST_STATES = ('predicted', 'not_predicted', 'no_model', 'features_unavailable',
                   'late_callback_not_predicted', 'validation_reserved')


class DetailExpired(ValueError):
    pass


def family(base, target):
    try:
        parts = Path(target).absolute().relative_to(Path(base).absolute()).parts
    except ValueError:
        return None
    if len(parts) != 3 or parts[0] != 'case_review' or parts[1] not in CLOSED_KINDS:
        return None
    if not parts[2].endswith('.json'):
        return None
    return parts[1], parts[2][:-5]


def state(db):
    if db is None:
        return None
    tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('model_retention','model_enrollment')")}
    if not tables:
        return None
    if len(tables) != 2:
        raise ValueError('MODEL_RETENTION_IDENTITY_MISSING')
    raw = db.execute('SELECT body,hash FROM model_retention WHERE singleton=1').fetchone()
    marker = db.execute('SELECT hash FROM model_enrollment WHERE singleton=1').fetchone()
    if not raw or not marker or raw['hash'] != marker[0]:
        raise ValueError('MODEL_RETENTION_STATE_MISSING')
    body = I.decode(raw)
    if body.get('version') != VERSION or body.get('closed_writers') != list(CLOSED_KINDS):
        raise ValueError('MODEL_RETENTION_CONTRACT_CHANGED')
    return body


def _save(db, body):
    db.execute('CREATE TABLE IF NOT EXISTS model_retention (singleton INTEGER PRIMARY KEY CHECK(singleton=1),body TEXT NOT NULL,hash TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS model_expired (kind TEXT NOT NULL,id TEXT NOT NULL,source_hash TEXT NOT NULL,proof_hash TEXT NOT NULL,PRIMARY KEY(kind,id))')
    db.execute('CREATE TABLE IF NOT EXISTS model_enrollment (singleton INTEGER PRIMARY KEY CHECK(singleton=1),hash TEXT NOT NULL)')
    db.execute('INSERT OR REPLACE INTO model_retention VALUES (1,?,?)', (I.canonical(body), I.digest(body)))
    db.execute('INSERT OR REPLACE INTO model_enrollment VALUES (1,?)', (I.digest(body),))


def enabled():
    import archive_reclamation as A
    with A.database(C.ontology_root()) as db:
        return state(db) is not None


def _inventory():
    import case_store as S
    from storage_objects import materialize, fingerprint
    result = {}
    metadata_bytes = 0
    for kind in CLOSED_KINDS:
        errors = []
        rows = {}
        folder=S.root()/kind
        if folder.is_symlink():
            raise ValueError('MODEL_ARCHIVE_SYMLINK_FORBIDDEN')
        if folder.exists():
            for entry in folder.iterdir():
                if entry.is_symlink() or not entry.is_file() or not (entry.name.endswith('.json') or entry.name.endswith('.json.commit')):
                    raise ValueError('MODEL_ARCHIVE_UNKNOWN_COMPONENT')
                if entry.name.endswith('.json.commit') and not entry.with_suffix('').exists():
                    raise ValueError('MODEL_ARCHIVE_INCOMPLETE_RECORD')
        for row in S.iter_records(kind, errors=errors):
            if sum(len(v) for v in result.values()) + len(rows) >= MAX_RECORDS:
                raise ValueError('MODEL_ARCHIVE_INVENTORY_LIMIT')
            key = row.get({'models':'model_id', 'model_publications':'publication_id',
                           'publication_commits':'review_id'}.get(kind, 'prediction_id'))
            if not key or key in rows:
                raise ValueError('MODEL_ARCHIVE_IDENTITY_INVALID')
            if kind in ('predictions','prediction_targets'):
                if (row.get('prediction_version') != 'PREMISE_PREDICTION@v1'
                        or not row.get('application_id') or key != 'prediction-'+S.digest([
                            'PREMISE_PREDICTION@v1', row['application_id']])):
                    raise ValueError('MODEL_PREDICTION_NAMESPACE_UNVERIFIED')
            original = S.load(kind, key)
            if original is None or S.digest(original) != S.digest(row):
                raise ValueError('MODEL_ARCHIVE_PATH_IDENTITY_MISMATCH')
            # Keep detached small metadata, never lazy readers across sweep.
            fields = ('model_id','publication_id','review_id','prediction_id','application_id',
                      'origin_judgment_id','code','origin_at','created_at','started_at',
                      'training_cutoff','status','valid_from','valid_until','available_at',
                      'publication_ids','published_at','prediction_version','kind')
            if sum(fingerprint(row[k])[1] for k in fields if k in row) > 8*1024*1024-metadata_bytes:
                raise ValueError('MODEL_ARCHIVE_METADATA_LIMIT')
            rows[key] = {**{k:materialize(row[k]) for k in fields if k in row},
                         'source_hash':S.digest(row)}
            metadata_bytes += len(I.canonical(rows[key]).encode())
            if metadata_bytes > 8*1024*1024:
                raise ValueError('MODEL_ARCHIVE_METADATA_LIMIT')
        if errors:
            raise ValueError('MODEL_ARCHIVE_SOURCE_UNAVAILABLE')
        result[kind] = rows
    return result


def _namespace(inventory):
    return {kind:{'count':len(rows), 'hash':I.digest({key:row['source_hash'] for key,row in rows.items()})}
            for kind,rows in inventory.items()}


def enroll(*, dry_run=True):
    import archive_reclamation as A
    import record_store as R
    import storage_leases as L
    base = C.ontology_root()
    with A.database(base) as db:
        current = state(db)
        if db is not None and A._read(db, 'pending'):
            raise ValueError('ARCHIVE_RECOVERY_REQUIRED')
    if current:
        return {**current, 'state':'enabled', 'replayed':True}
    inventory = _inventory()
    counts = {kind:len(rows) for kind,rows in inventory.items()}
    body = {'version':VERSION, 'closed_writers':list(CLOSED_KINDS),
            'enrolled_at':I.now(), 'original_counts':counts, 'inventory_hash':I.digest(inventory),
            'remaining_namespace':_namespace(inventory),
            'cutoff':None, 'retired_records':0, 'history_hash':'', 'closed_details':[]}
    if dry_run:
        return {**body, 'state':'dry_run', 'raw_deletion_authorized':False}
    # Readers and old callbacks hold storage pins, so enrollment never races
    # an in-flight publication. Recheck the inventory after obtaining the lease.
    base.mkdir(parents=True, exist_ok=True)
    import gc
    gc.collect()
    with L.exclusive(base):
        with A.database(base) as db:
            current=state(db)
            if current:
                return {**current,'state':'enabled','replayed':True}
            if db is not None and A._read(db,'pending'):
                raise ValueError('ARCHIVE_RECOVERY_REQUIRED')
        if I.digest(_inventory()) != body['inventory_hash']:
            raise L.StorageBusy('MODEL_ARCHIVE_CHANGED_DURING_ENROLLMENT')
        with A.database(base, True) as db:
            if A._read(db, 'pending'):
                raise ValueError('ARCHIVE_RECOVERY_REQUIRED')
            _save(db, body)
        R.invalidate_generation(base)
    return {**body, 'state':'enabled', 'raw_deletion_authorized':False}


def require_available(db, key, pending=None):
    current = state(db)
    if not current:
        return
    kind, identifier = key
    if kind in current['closed_details']:
        raise DetailExpired('MODEL_NAMESPACE_RETIRED:' + kind)
    for row in (pending or {}).get('model_retirement', {}).get('records', []):
        if (row['kind'], row['id']) == key:
            raise DetailExpired('MODEL_DETAIL_EXPIRED:' + kind + '/' + identifier)
    raw = db.execute('SELECT source_hash,proof_hash FROM model_expired WHERE kind=? AND id=?', key).fetchone()
    if raw:
        if raw['proof_hash'] != I.digest([kind, identifier, raw['source_hash']]):
            raise ValueError('MODEL_EXPIRY_PROOF_CORRUPT')
        raise DetailExpired('MODEL_DETAIL_EXPIRED:' + kind + '/' + identifier)


def require_publication(db, key, target):
    if not state(db):
        return
    require_available(db, key)
    if not Path(target).exists():
        raise ValueError('MODEL_PRODUCER_NAMESPACE_CLOSED')


def require_origins(origins):
    import archive_reclamation as A
    with A.database(C.ontology_root()) as db:
        current = state(db)
        if not current:
            return
        pending = A._read(db, 'pending')
        cutoff = (pending or {}).get('model_retirement', {}).get('cutoff') or current['cutoff']
        for origin in origins:
            if not origin:
                raise ValueError('MODEL_QUERY_ORIGIN_REQUIRED')
            if cutoff and I.instant(origin).date().isoformat() < cutoff:
                raise DetailExpired('MODEL_DETAIL_SCOPE_EXPIRED:' + cutoff)


def _cutoff(db, cutoff, at):
    import usage_checkpoint as U
    # The same lock and range contract protects holdout and future records.
    if db is not None:
        return U._cutoff(db, cutoff, at)
    import validation_holdout as H
    return min([cutoff] + [I.instant(r['spec']['period']['start']).date().isoformat() for r in H.reservations()])


def _lifecycle(db, application_id):
    if db is None:
        return None
    rows = [I.get(kind, application_id, conn=db) for kind in ('premise_case', 'legacy_assessment_state')]
    rows = [r for r in rows if r is not None]
    if any(r.get('state') not in ('completed','finalized','finalized_unknown') or not r.get('closed_at') for r in rows):
        return None
    # Incoming required references are conservatively protected even when the
    # owner will itself be collected by a later improvement-store batch.
    if db.execute("SELECT 1 FROM links WHERE target_id=? AND target_kind IN ('premise_case','legacy_assessment_state') AND relation IN ('required_content','required_state') LIMIT 1", (application_id,)).fetchone():
        return None
    saved = I.get('model_retirement_owner', application_id, conn=db)
    if saved:
        return {'closed_at':max([saved['closed_at']]+[r['closed_at'] for r in rows]),
            'prediction_hash':saved['prediction_hash'], 'bindings':[
                ['model_retirement_owner', application_id, I.digest(saved)]]+
                [[r['kind'],r['id'],I.digest(r)] for r in rows]}
    if not rows:
        return None
    return {'closed_at':max(r['closed_at'] for r in rows),
            'bindings':[[r['kind'], r['id'], I.digest(r)] for r in rows]}


def capture_retired_lifecycles(db, retired):
    """Hand completion ownership off before the independent case disappears."""
    if not retired or not enabled():
        return
    import case_store as S
    from case_prediction import PREDICTION_VERSION
    groups = {}
    for row in retired:
        groups.setdefault(row['id'], []).append(row)
    for identifier, previous in groups.items():
        prediction_id = 'prediction-' + S.digest([PREDICTION_VERSION, identifier])
        try:
            prediction = S.load('predictions', prediction_id)
        except DetailExpired:
            continue
        if not prediction:
            continue
        if prediction.get('application_id') != identifier:
            raise ValueError('MODEL_COMPLETION_OWNER_MISMATCH')
        rows = [I.get(kind,identifier,conn=db) for kind in ('premise_case','legacy_assessment_state')]
        rows = previous + [r for r in rows if r is not None]
        if any(r.get('state') not in ('completed','finalized','finalized_unknown') or not r.get('closed_at') for r in rows):
            continue
        old = I.get('model_retirement_owner',identifier,conn=db)
        if old:
            continue
        I.put(db,'model_retirement_owner',identifier,{'state':'completed',
            'closed_at':max(r['closed_at'] for r in rows), 'prediction_id':prediction_id,
            'prediction_hash':S.digest(prediction),
            'completion_sources':[[r['kind'],r['id'],I.digest(r)] for r in rows],
            'consumer':'model_retention', 'origin_at':prediction['origin_at']})


def prune_owners(db, base, limit=200):
    import archive_reclamation as A
    with A.database(base) as archive:
        if not state(archive):
            return
        for row in I.page('model_retirement_owner',limit=limit,conn=db):
            try:
                require_available(archive,('predictions',row['prediction_id']))
            except DetailExpired:
                key = 'model_retirement_owner', row['id']
                db.execute('DELETE FROM records WHERE kind=? AND id=?', key)
                db.execute('DELETE FROM revisions WHERE kind=? AND id=?', key)
                db.execute('DELETE FROM links WHERE owner_kind=? AND owner_id=?', key)


def candidates(base, boundary, limit):
    import archive_reclamation as A
    import case_store as S
    with A.database(base) as db:
        current = state(db)
    if not current:
        return {'state':'migration_required','records':[], 'protected':[]}
    inventory = _inventory()
    if _namespace(inventory) != current.get('remaining_namespace'):
        raise ValueError('MODEL_ARCHIVE_MEMBERSHIP_CHANGED')
    selected=[]; protected=[]
    with I.transaction(readonly=True) as db:
        cutoff = _cutoff(db, boundary['detailed_origin_cutoff'], boundary['knowledge_as_of'])
        if current['cutoff'] and cutoff < current['cutoff']:
            raise ValueError('MODEL_SUPPORT_BOUNDARY_CANNOT_REVERSE')
        for identifier, prediction in sorted(inventory['predictions'].items()):
            if len(selected) + 2 > limit:
                break
            target = inventory['prediction_targets'].get(identifier)
            app = prediction.get('application_id')
            life = _lifecycle(db, app) if app else None
            if life and life.get('prediction_hash') and life['prediction_hash']!=prediction['source_hash']:
                raise ValueError('MODEL_COMPLETION_PREDICTION_CHANGED')
            if (not target or any(not prediction.get(k) or prediction.get(k) != target.get(k) for k in
                    ('prediction_id','application_id','origin_judgment_id','code','origin_at','started_at'))):
                protected.append({'kind':'predictions','id':identifier,'reason':'target_binding_unverified'}); continue
            if (not life or prediction.get('status') not in FORECAST_STATES
                    or prediction.get('prediction_version') != 'PREMISE_PREDICTION@v1'
                    or prediction.get('kind') != 'prospective'):
                protected.append({'kind':'predictions','id':identifier,'reason':'assessment_or_result_completion_unverified'}); continue
            stamps = [life['closed_at'], prediction.get('created_at'), prediction.get('origin_at'), prediction.get('started_at')]
            if any(not stamp or I.instant(stamp).date().isoformat() >= cutoff for stamp in stamps):
                continue
            for kind, row in (('predictions', prediction), ('prediction_targets', target)):
                selected.append({'kind':kind,'id':identifier,'source_hash':row['source_hash'],
                    'path':str(S._path(kind,identifier).relative_to(base)), 'application_id':app,
                    'lifecycle':life, 'consumer_contract':'ended_prediction_and_supported_origin@v1'})
    removed = {r['id'] for r in selected if r['kind']=='predictions'}
    live_models = {p.get('model_id') for key,p in inventory['predictions'].items() if key not in removed}
    unfinished = set(inventory['prediction_targets']) - set(inventory['predictions'])
    for identifier, model in sorted(inventory['models'].items()):
        if len(selected) >= limit:
            break
        pubs = [p for p in inventory['model_publications'].values() if p.get('model_id')==identifier]
        if not pubs or identifier in live_models or unfinished:
            protected.append({'kind':'models','id':identifier,'reason':'publication_or_live_prediction'}); continue
        committed = model.get('status') in ('ready','fitted','available','ok') and bool(model.get('training_cutoff'))
        for publication in pubs:
            commit=inventory['publication_commits'].get(publication.get('review_id'),{})
            stamps=[publication.get('valid_until'),publication.get('available_at'),commit.get('published_at')]
            committed = (committed and publication.get('status')=='published'
                and publication.get('valid_from') and all(stamps)
                and I.instant(model['training_cutoff']) < I.instant(publication['valid_from']) < I.instant(publication['valid_until'])
                and all(I.instant(stamp).date().isoformat()<cutoff for stamp in stamps)
                and publication.get('publication_id') in commit.get('publication_ids',[]))
        if not committed:
            protected.append({'kind':'models','id':identifier,'reason':'publication_unfinished_or_supported'}); continue
        selected.append({'kind':'models','id':identifier,'source_hash':model['source_hash'],
            'path':str(S._path('models',identifier).relative_to(base)),
            'consumer_contract':'ended_publication_outside_registered_detail@v1'})
    selected_keys={(r['kind'],r['id']) for r in selected}
    remaining={kind:{key:row for key,row in rows.items() if (kind,key) not in selected_keys}
        for kind,rows in inventory.items()}
    return {'state':'ready','cutoff':cutoff,'records':selected,'protected':protected,
            'batch_reason':'prediction_pair_requires_limit_at_least_two' if limit<2 and inventory['predictions'] else None,
            'remaining_namespace':_namespace(remaining),
            'closed_writers':list(CLOSED_KINDS), 'raw_families':list(DETAIL_KINDS)}


def revalidate(db, plan, at):
    if plan.get('state') != 'ready':
        return
    if _cutoff(db, plan['cutoff'], at) < plan['cutoff']:
        raise ValueError('MODEL_RETENTION_RANGE_CHANGED')
    for row in plan['records']:
        if row.get('application_id') and _lifecycle(db, row['application_id']) != row['lifecycle']:
            raise ValueError('MODEL_RETENTION_LIFECYCLE_CHANGED')


def commit(db, base, plan):
    if plan.get('state') != 'ready':
        return
    current = state(db)
    if not current:
        raise ValueError('MODEL_RETENTION_ENROLLMENT_MISSING')
    for row in plan['records']:
        key = row['kind'], row['id'], row['source_hash']
        db.execute('INSERT OR REPLACE INTO model_expired VALUES (?,?,?,?)', (*key, I.digest(list(key))))
    current.update(cutoff=plan['cutoff'], retired_records=current['retired_records']+len(plan['records']),
                   remaining_namespace=plan['remaining_namespace'],
                   history_hash=I.digest([current['history_hash'],plan]))
    for kind in DETAIL_KINDS:
        folder = Path(base)/'case_review'/kind
        if current['remaining_namespace'][kind]['count']==0:
            if folder.exists() and any(folder.glob('*.json')):
                raise ValueError('MODEL_ARCHIVE_NOT_EMPTY')
            if kind not in current['closed_details']:
                current['closed_details'].append(kind)
            db.execute('DELETE FROM model_expired WHERE kind=?', (kind,))
    _save(db, current)
