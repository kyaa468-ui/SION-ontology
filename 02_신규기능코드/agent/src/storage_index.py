"""Rebuildable metadata index. The immutable record/commit remains authoritative."""
from __future__ import annotations
import json
import os
from pathlib import Path
import sqlite3

FIELDS = ('run_attempt_id', 'run_id', 'account_scope', 'code', 'instrument_id',
          'judgment_id', 'plan_id', 'order_attempt_id', 'application_id','position_episode_id','rule_trigger_id',
          'review_id', 'validation_id', 'proposal_id', 'scoring_job_id',
          'event_type', 'kind', 'status', 'created_at', 'recorded_at',
          'available_at', 'market_as_of', 'knowledge_as_of', 'event_id',
          'recorded_at_ns', 'confidence_for', 'recommended_action')


def path(base): return Path(base) / 'storage_index.sqlite3'


def projection(value):
    body = value.get('record', value) if isinstance(value, dict) else {}
    if not isinstance(body,dict):return {}
    if isinstance(body.get('payload'),dict):body={**body['payload'],**body}
    return {key: body[key] for key in FIELDS if key in body and
            isinstance(body[key], (str, int, float, bool, type(None)))}


def add(base, relative, manifest_sha, metadata):
    db = path(base)
    if db.is_symlink(): raise ValueError('INDEX_SYMLINK_FORBIDDEN')
    db.parent.mkdir(parents=True, exist_ok=True)
    con=sqlite3.connect(db, timeout=30)
    try:
        con.execute('PRAGMA journal_mode=DELETE')
        if not con.execute("SELECT 1 FROM sqlite_master WHERE type='table' LIMIT 1").fetchone():
            con.execute('PRAGMA auto_vacuum=INCREMENTAL')
        con.execute('CREATE TABLE IF NOT EXISTS records (path TEXT PRIMARY KEY, sha TEXT NOT NULL, metadata TEXT NOT NULL)')
        con.execute('INSERT OR REPLACE INTO records VALUES (?,?,?)',
                    (relative, manifest_sha, json.dumps(metadata, ensure_ascii=False)))
        con.commit()
    finally:con.close()


def compact(base,*,pages=128):
    """Bounded page reclamation; old non-vacuumable indexes need a rebuild."""
    target=path(base)
    if not target.exists():return {'state':'absent'}
    if target.is_symlink():raise ValueError('INDEX_SYMLINK_FORBIDDEN')
    before=target.stat().st_size
    db=sqlite3.connect(target,timeout=0)
    try:
        if db.execute('PRAGMA auto_vacuum').fetchone()[0]!=2:
            return {'state':'index_rebuild_required','database_bytes':before}
        db.execute('PRAGMA incremental_vacuum('+str(min(128,max(1,int(pages))))+')').fetchall()
        return {'state':'compacted','database_bytes_before':before,'database_bytes_after':target.stat().st_size,
                'free_pages_remaining':db.execute('PRAGMA freelist_count').fetchone()[0]}
    except sqlite3.OperationalError as exc:
        if 'locked' in str(exc):return {'state':'deferred','reason':'index_reader_or_writer_active'}
        raise
    finally:db.close()


def rebuild(base=None):
    """Build aside, then atomically replace only this derived index."""
    import common as C
    import record_store as R
    import archive_reclamation as G
    import storage_objects as O
    import storage_leases as L
    import tempfile
    base=Path(base or C.ontology_root())
    if base.resolve()!=C.ontology_root().resolve():raise ValueError('UNREGISTERED_INDEX_ROOT')
    if not base.exists():return {'state':'empty'}
    pin=L.Pin(base);temporary=None
    try:
        generation=R.generation(base)
        with G.database(base) as control:
            if control and G._read(control,'pending'):raise ValueError('ARCHIVE_RECOVERY_REQUIRED_BEFORE_INDEX_REBUILD')
        handle,temporary=tempfile.mkstemp(prefix='.index-',suffix='.sqlite3',dir=base)
        os.close(handle);count=0
        db=sqlite3.connect(temporary)
        try:
            db.execute('PRAGMA auto_vacuum=INCREMENTAL')
            db.execute('CREATE TABLE records (path TEXT PRIMARY KEY,sha TEXT NOT NULL,metadata TEXT NOT NULL)')
            for source in G._files(base):
                if source.suffix!='.json' or source.parent==G.V1.root(base):continue
                info=R.manifest(source,base=base)
                if isinstance(info,dict) and info.get('format')==R.FORMAT:
                    db.execute('INSERT INTO records VALUES (?,?,?)',(str(source.relative_to(base)),O.file_fingerprint(source)[0],json.dumps(info['projection'],ensure_ascii=False)))
                    count+=1
            db.commit()
        finally:db.close()
        pin.close()
        import gc
        gc.collect()
        with L.exclusive(base):
            if R.generation(base)!=generation:raise L.StorageBusy('INDEX_SOURCE_GENERATION_CHANGED')
            # A collector may have committed expiration and crashed before
            # advancing the generation. Never replace during that recovery.
            with G.database(base) as control:
                if control and G._read(control,'pending'):raise L.StorageBusy('ARCHIVE_RECOVERY_REQUIRED_BEFORE_INDEX_REBUILD')
            target=path(base)
            if target.is_symlink():raise ValueError('INDEX_SYMLINK_FORBIDDEN')
            if any(Path(str(target)+suffix).exists() for suffix in ('-wal','-shm','-journal')):
                raise L.StorageBusy('INDEX_SQLITE_SIDECAR_ACTIVE')
            before=target.stat().st_size if target.exists() else 0
            with open(temporary,'rb') as stream:os.fsync(stream.fileno())
            os.replace(temporary,target);temporary=None
            G.V1._sync_directory(base)
        return {'state':'rebuilt','records':count,'database_bytes_before':before,'database_bytes_after':target.stat().st_size}
    except L.StorageBusy as exc:return {'state':'deferred','reason':str(exc)}
    finally:
        pin.close()
        if temporary and os.path.exists(temporary):os.unlink(temporary)


def entries(base, prefix=''):
    """No creation on read, including the absent-index path."""
    db = path(base)
    if not db.exists(): return
    if db.is_symlink(): raise ValueError('INDEX_SYMLINK_FORBIDDEN')
    con = sqlite3.connect(db.resolve().as_uri() + '?mode=ro', uri=True)
    try:
        # Prefix is a path bound, not a caller-provided SQL fragment.
        for relative, sha, raw in con.execute('SELECT path,sha,metadata FROM records WHERE path>=? AND path<? ORDER BY path',
                                              (prefix, prefix + '\uffff')):
            yield relative, sha, json.loads(raw)
    finally: con.close()


def select(base, prefix, filters=None):
    db=path(base)
    if not db.exists():return
    if db.is_symlink():raise ValueError('INDEX_SYMLINK_FORBIDDEN')
    con=sqlite3.connect(db.resolve().as_uri()+'?mode=ro',uri=True)
    try:
        clauses=['path>=?','path<?'];params=[prefix,prefix+'\uffff']
        for key,value in (filters or {}).items():
            if key not in FIELDS:continue
            clauses.append('(json_extract(metadata,?)=? OR json_type(metadata,?) IS NULL)')
            params.extend(['$.'+key,value,'$.'+key])
        for relative,sha in con.execute('SELECT path,sha FROM records WHERE '+' AND '.join(clauses)+' ORDER BY path',params):
            yield relative,sha
    finally:con.close()


def indexed_hash(base,relative):
    db=path(base)
    if not db.exists():return None
    con=sqlite3.connect(db.resolve().as_uri()+'?mode=ro',uri=True)
    try:
        row=con.execute('SELECT sha FROM records WHERE path=?',(relative,)).fetchone()
        return row[0] if row else None
    finally:con.close()


def files(folder, *, recursive=True):
    """Bounded filesystem walk; no rglob list or sorting of the entire archive."""
    folder = Path(folder)
    if not folder.exists(): return
    if folder.is_symlink(): raise ValueError('RECORD_DIRECTORY_SYMLINK_FORBIDDEN')
    with os.scandir(folder) as stream:
        for item in stream:
            if item.is_symlink():
                if item.name.endswith('.json') or item.is_dir():
                    raise ValueError('RECORD_SYMLINK_FORBIDDEN')
            elif item.is_dir(follow_symlinks=False):
                if recursive and item.name not in ('snapshots', 'locks', 'trace_view'):
                    yield from files(item.path)
            elif item.name.endswith('.json'):
                yield Path(item.path)


def entry(base,relative):
    db=path(base)
    if not db.exists():return None
    if db.is_symlink():raise ValueError('INDEX_SYMLINK_FORBIDDEN')
    con=sqlite3.connect(db.resolve().as_uri()+'?mode=ro',uri=True)
    try:
        row=con.execute('SELECT sha,metadata FROM records WHERE path=?',(relative,)).fetchone()
        return (row[0],json.loads(row[1])) if row else None
    finally:con.close()
