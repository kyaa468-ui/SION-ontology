"""Indexed reservation scopes with explicit migration and fail-closed publish.

The pre-publication intent prevents a crash between the raw commit and compact
consumer update from making reserved data appear eligible for discovery.
"""
import improvement_store as I

KINDS={'holdout_reservations','holdout_releases'}


def _root(db):
    return I.digest([list(row) for row in db.execute("SELECT id,hash FROM records WHERE kind='holdout_scope' AND state='active' ORDER BY id")])


def _seal(db):
    db.execute('INSERT OR REPLACE INTO meta VALUES (?,?)',('holdout_active_root',_root(db)))


def enabled():
    with I.transaction(readonly=True) as db:
        return bool(db and db.execute("SELECT 1 FROM meta WHERE key='holdout_scopes_ready'").fetchone())


def prepare(kind,key,body):
    if kind not in KINDS:return
    with I.transaction() as db:
        identifier=kind+':'+key
        old=I.get('holdout_sync',identifier,conn=db)
        if old and old['source_hash']!=I.digest(body):raise ValueError('HOLDOUT_SCOPE_SOURCE_CONFLICT')
        I.save_once('holdout_sync',identifier,{'state':'pending','source_kind':kind,'source_id':key,
            'source_hash':I.digest(body)},conn=db)


def commit(kind,key,body):
    import validation_store as V
    stored=V.load(kind,key)
    if not stored or V.digest(stored)!=I.digest(body):raise ValueError('HOLDOUT_SCOPE_SOURCE_NOT_COMMITTED')
    with I.transaction() as db:
        old=I.get('holdout_scope',key,conn=db) or {}
        if kind=='holdout_reservations':
            if old.get('reservation') and I.digest(old['reservation'])!=I.digest(body):raise ValueError('HOLDOUT_RESERVATION_CHANGED')
            updated={**old,'reservation':body,'reservation_hash':I.digest(body),
                'origin_at':body['spec']['period']['start'],'owner':key}
        else:
            if old.get('release') and I.digest(old['release'])!=I.digest(body):raise ValueError('HOLDOUT_RELEASE_CHANGED')
            updated={**old,'release':body,'release_hash':I.digest(body),'closed_at':body['released_at'],'owner':key}
        updated['state']='completed' if updated.get('release') else 'active'
        if kind=='holdout_releases' and not updated.get('reservation'):
            # Recover an old raw reservation explicitly; never interpret a
            # missing scope as a successful release of unprotected content.
            reservation=V.load('holdout_reservations',key)
            if not reservation:raise ValueError('RELEASED_HOLDOUT_RESERVATION_UNAVAILABLE')
            updated.update(reservation=reservation,reservation_hash=I.digest(reservation),origin_at=reservation['spec']['period']['start'])
        I.put(db,'holdout_scope',key,updated)
        _seal(db)
        db.execute("DELETE FROM revisions WHERE kind='holdout_sync' AND id=?",(kind+':'+key,))
        db.execute("DELETE FROM records WHERE kind='holdout_sync' AND id=?",(kind+':'+key,))


def reservations(include_released=False,*,limit=2000):
    with I.transaction(readonly=True) as db:
        if db is None:raise ValueError('HOLDOUT_SCOPE_STORE_UNAVAILABLE')
        if db.execute("SELECT 1 FROM records WHERE kind='holdout_sync' LIMIT 1").fetchone():
            raise ValueError('HOLDOUT_SCOPE_PUBLICATION_UNFINISHED')
        root=db.execute("SELECT value FROM meta WHERE key='holdout_active_root'").fetchone()
        if not root or root[0]!=_root(db):raise ValueError('HOLDOUT_SCOPE_MEMBERSHIP_CHANGED')
        selector="kind='holdout_scope'"+('' if include_released else " AND state='active'")
        if db.execute('SELECT count(*) FROM records WHERE '+selector).fetchone()[0]>limit:
            raise ValueError('HOLDOUT_SCOPE_CAPACITY_BLOCKED')
        rows=[]
        for raw in db.execute('SELECT body,hash FROM records WHERE '+selector+' ORDER BY id'):
            value=I.decode(raw)
            reservation=value['reservation']
            if value.get('reservation_hash')!=I.digest(reservation):raise ValueError('HOLDOUT_SCOPE_HASH_MISMATCH')
            if value.get('release') and value.get('release_hash')!=I.digest(value['release']):raise ValueError('HOLDOUT_RELEASE_HASH_MISMATCH')
            rows.append(reservation)
        return rows


def migrate():
    import validation_store as V
    # All cooperating scope writers publish a durable intent under this same
    # transaction lock before their raw commit, then clear it after projection.
    with I.transaction() as db:
        errors=[];raw=list(V.iter_records('holdout_reservations',errors=errors))
        released=list(V.iter_records('holdout_releases',errors=errors))
        if errors:raise ValueError('HOLDOUT_MIGRATION_SOURCE_UNREADABLE')
        for row in raw:commit('holdout_reservations',row['validation_id'],row)
        for row in released:commit('holdout_releases',row['validation_id'],row)
        if db.execute("SELECT 1 FROM records WHERE kind='holdout_sync' LIMIT 1").fetchone():raise ValueError('HOLDOUT_SCOPE_PUBLICATION_UNFINISHED')
        ids={r['validation_id'] for r in released}
        expected=sorted([r for r in raw if r['validation_id'] not in ids],key=lambda r:r['validation_id'])
        _seal(db)
        actual=reservations()
        if I.digest(expected)!=I.digest(actual):raise ValueError('HOLDOUT_SCOPE_PARITY_FAILED')
        proof={'state':'verified','active_scopes':len(actual),'sources':len(raw)+len(released),
            'projection_hash':I.digest(actual),'consumer':'validation_holdout.reservations','raw_deletion_authorized':False}
        I.put(db,'migration_proof','holdout_scopes',proof)
        db.execute('INSERT OR REPLACE INTO meta VALUES (?,?)',('holdout_scopes_ready',I.canonical(proof)))
        return proof
