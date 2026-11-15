"""Verified archive retirement, initially for completed, unused trace views.

All other logical records remain roots until their actual consumers migrate.
Physical v1/v2 dependencies are parsed by their storage codecs, never by finding
ID-like strings in user data. A compact restart journal precedes every unlink.
"""
from __future__ import annotations
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import sqlite3

import common as C
import improvement_store as I
import retention_policy as P
import snapshot_store as V1
import storage_objects as V2
import storage_leases as L

VERSION='ARCHIVE_RETENTION@v1'
_VIEW=re.compile(r'view-(?:(\d{8})-)?[a-f0-9]{32}\Z')


def path(base): return Path(base)/'archive_retention.sqlite3'


@contextmanager
def database(base,write=False):
    target=path(base)
    if target.is_symlink():raise ValueError('RETENTION_DB_SYMLINK_FORBIDDEN')
    if not write and not target.exists():yield None;return
    db=sqlite3.connect(target if write else target.resolve().as_uri()+'?mode=ro',uri=not write,timeout=5)
    db.row_factory=sqlite3.Row
    try:
        if write:
            db.execute('BEGIN IMMEDIATE')
            db.execute('CREATE TABLE IF NOT EXISTS state (singleton INTEGER PRIMARY KEY CHECK(singleton=1), body TEXT NOT NULL, hash TEXT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS pending (singleton INTEGER PRIMARY KEY CHECK(singleton=1), body TEXT NOT NULL, hash TEXT NOT NULL)')
            # This legacy set is bounded by the old namespace at first rollout.
            # New view IDs carry their creation day and need no per-ID tombstone.
            db.execute('CREATE TABLE IF NOT EXISTS legacy_expired (id TEXT PRIMARY KEY)')
        yield db
        if write:db.commit()
    except BaseException:
        if write:db.rollback()
        raise
    finally:db.close()


def _read(db,table):
    row=db.execute('SELECT body,hash FROM '+table+' WHERE singleton=1').fetchone()
    if not row:return None
    value=json.loads(row['body'])
    if I.digest(value)!=row['hash'] or value.get('version')!=VERSION:raise ValueError('RETENTION_CHECKPOINT_CORRUPT')
    return value


def _save(db,table,value):
    db.execute('INSERT OR REPLACE INTO '+table+' VALUES (1,?,?)',(I.canonical(value),I.digest(value)))


def _view(path,base):
    try:parts=Path(path).absolute().relative_to(Path(base).absolute()).parts
    except ValueError:return None
    return parts[2] if len(parts)>=4 and parts[:2]==('trace_view','snapshots') and _VIEW.fullmatch(parts[2]) else None


def require_available(base,target):
    view=_view(target,base)
    import model_retention as M
    model=M.family(base,target)
    if not view and not model:return
    with database(base) as db:
        if db is None:return
        state=_read(db,'state');pending=_read(db,'pending')
        if model:M.require_available(db,model,pending)
        if not view:return
        if pending and view in pending['views']:raise ValueError('VIEW_EXPIRED')
        if not state:return
        match=_VIEW.fullmatch(view);day=match[1]
        if day and day<state['cutoff'].replace('-',''):
            if not Path(target).exists():raise ValueError('VIEW_SUPPORT_EXPIRED')
        if not day and (state.get('legacy_namespace_closed') or db.execute('SELECT 1 FROM legacy_expired WHERE id=?',(view,)).fetchone()):
            raise ValueError('LEGACY_VIEW_SUPPORT_EXPIRED')


def require_publication(base,target):
    with database(base) as db:
        if db is None:return
        if _read(db,'pending'):raise ValueError('ARCHIVE_RECOVERY_REQUIRED_BEFORE_PUBLICATION')
        import model_retention as M
        model=M.family(base,target)
        if model:M.require_publication(db,model,target)
        state=_read(db,'state')
    view=_view(target,base)
    if not state or not view:return
    day=_VIEW.fullmatch(view)[1]
    if day and day<state['cutoff'].replace('-',''):raise ValueError('VIEW_SUPPORT_EXPIRED')
    require_available(base,target)


def _files(base,max_files=100000):
    count=0
    for folder,dirs,names in os.walk(base,followlinks=False):
        for name in dirs+names:
            if (Path(folder)/name).is_symlink():raise ValueError('ARCHIVE_SYMLINK_BLOCKS_GC')
        for name in names:
            count+=1
            if count>max_files:raise ValueError('ARCHIVE_INVENTORY_LIMIT')
            yield Path(folder)/name


def _bytes(base):
    files=list(_files(base))
    return {'bytes':sum(p.stat().st_size for p in files),'files':len(files)}


def _closure(target,base,resolver):
    import record_store as R
    # This reader is allowed to inspect a logically expired pending manifest.
    with target.open(encoding='utf-8') as stream:body=json.load(stream)
    if isinstance(body,dict) and body.get('format')==R.FORMAT:
        marker=R.commit_path(target)
        with marker.open(encoding='utf-8') as stream:commit=json.load(stream)
        if V2.file_fingerprint(target)!=(commit.get('manifest_sha256'),commit.get('manifest_bytes')) or commit.get('object_ref')!=body.get('object_ref'):
            raise ValueError('ARCHIVE_RECORD_COMMIT_MISMATCH')
        dependencies=V2.verify_closure(body['object_ref'],base=base,resolver=resolver)
        if V2.fingerprint(V2.open_object(body['object_ref'],base=base,resolver=resolver))!=(body.get('logical_sha256'),body.get('logical_bytes')):
            raise ValueError('ARCHIVE_LOGICAL_HASH_MISMATCH')
        return dependencies
    if isinstance(body,dict) and str(body.get('format','')).startswith('sigma-record@'):
        raise ValueError('ARCHIVE_STORAGE_VERSION_UNKNOWN')
    if R.commit_path(target).exists():raise ValueError('ARCHIVE_RECORD_FORMAT_CHANGED')
    if isinstance(body,dict) and str(body.get('format','')).startswith('content-addressed-snapshot@') and body['format']!=V1.VERSION:
        raise ValueError('ARCHIVE_STORAGE_VERSION_UNKNOWN')
    dependencies={}
    V1.decode(body,base=base,dependencies=dependencies)
    return dependencies


def _view_candidates(base,cutoff,limit):
    """Completion proof retires the view's own UI consumer, not its sources."""
    import record_store as R
    root=base/'trace_view/snapshots'
    selected=[];protected=[];view_pins=[];retired_usage=[];retired_locks=[]
    if not root.exists():return selected,protected,view_pins,retired_usage,retired_locks
    with os.scandir(root) as folders:
        for entry in folders:
            if len(selected)>=limit:break
            folder=Path(entry.path)
            if folder.is_symlink() or not entry.is_dir():raise ValueError('UNKNOWN_VIEW_ENTRY')
            if not _VIEW.fullmatch(entry.name):raise ValueError('UNKNOWN_VIEW_NAMESPACE')
            try:pin=L.Pin(folder,exclusive=True)
            except L.StorageBusy:protected.append({'view':entry.name,'reason':'active_view_session'});continue
            try:
                if {p.name for p in folder.iterdir()}-{'projection.json','projection.json.commit','manifest.json','commit.json'}:
                    raise ValueError('UNKNOWN_VIEW_COMPONENT')
                manifest=R.load(folder/'manifest.json',base=base)
                commit=R.load(folder/'commit.json',base=base)
                day=_VIEW.fullmatch(entry.name)[1]
                if day and day!=I.instant(manifest['created_at']).strftime('%Y%m%d'):
                    raise ValueError('VIEW_ORIGIN_NAMESPACE_MISMATCH')
                # Missing/incomplete views are not assumed abandoned writers.
                if I.instant(manifest['created_at']).date().isoformat()>=cutoff:
                    protected.append({'view':entry.name,'reason':'support_window'});pin.close();continue
                if (manifest.get('schema_version')!='TRACE_SNAPSHOT@v1' or commit.get('schema_version')!='TRACE_SNAPSHOT@v1'
                        or manifest.get('view_id')!=entry.name or commit.get('view_id')!=entry.name
                        or commit.get('manifest_hash')!=V2.fingerprint(manifest)[0]):
                    raise ValueError('VIEW_COMPLETION_PROOF_INVALID')
                physical=R.manifest(folder/'projection.json',base=base)
                projection_hash=physical.get('logical_sha256') if physical.get('format')==R.FORMAT else V2.fingerprint(R.load(folder/'projection.json',base=base))[0]
                if projection_hash!=commit.get('projection_hash') or projection_hash!=manifest.get('projection_hash'):
                    raise ValueError('VIEW_PROJECTION_PROOF_INVALID')
                # The only remaining direct reader of this owned reservation
                # is this view. Legacy usage discovery must first have its
                # separately parity-verified compact consumer activated.
                import research_usage_ledger as U
                import case_store as CS
                usage=CS.load('usage_events',manifest['exposure_reservation_id'])
                if (not usage or V2.fingerprint(usage)[0]!=manifest.get('exposure_reservation_hash')
                        or usage.get('owner_id')!=entry.name):raise ValueError('VIEW_USAGE_PROOF_INVALID')
                if U.enabled():
                    ids=usage.get('application_ids',[])
                    status=U.status(ids) if len(ids)<=2000 else {'errors':['scope_limit'],'items':[]}
                    if not status.get('errors') and all(x['status']=='used' for x in status['items']):
                        retired_usage.append(str(CS._path('usage_events',manifest['exposure_reservation_id']).relative_to(base)))
                    else:protected.append({'view':entry.name,'reason':'usage_projection_not_verified'})
                owner_lock=CS.root()/'locks'/(CS._id(manifest['exposure_reservation_id'])+'.sqlite3')
                if owner_lock.exists():
                    if owner_lock.is_symlink() or any(Path(str(owner_lock)+suffix).exists() for suffix in ('-wal','-shm','-journal')):
                        raise L.StorageBusy('VIEW_OWNER_LOCK_ACTIVE_OR_RECOVERY_REQUIRED')
                    control=sqlite3.connect(owner_lock.resolve().as_uri()+'?mode=ro',uri=True)
                    try:
                        if control.execute("SELECT 1 FROM sqlite_master WHERE type='table' LIMIT 1").fetchone():
                            raise ValueError('VIEW_OWNER_LOCK_HAS_UNKNOWN_STATE')
                    finally:control.close()
                    retired_locks.append(str(owner_lock.relative_to(base)))
                selected.append(entry.name);view_pins.append(pin)
            except FileNotFoundError:
                protected.append({'view':entry.name,'reason':'unfinished_or_missing_component'});pin.close()
            except BaseException:
                pin.close()
                for prior in view_pins:prior.close()
                raise
    return selected,protected,view_pins,retired_usage,retired_locks


def _plan(base,boundary,limit):
    import record_store as R
    pin=L.Pin(base);resolver=V2.Resolver(base);view_pins=[]
    try:
        generation=R.generation(base)
        views,protected,view_pins,retired_usage,retired_locks=_view_candidates(base,boundary['detailed_origin_cutoff'],limit)
        import model_retention
        model_plan=model_retention.candidates(base,boundary,limit)
        extra=set(retired_usage)|{x+'.commit' for x in retired_usage}
        for row in model_plan['records']:
            extra.update((row['path'],row['path']+'.commit'))
        live=set();retired=set();remove=[];blob_files={};all_files=[]
        for target in _files(base):
            relative=str(target.relative_to(base));all_files.append(target)
            if target.parent==V1.root(base):
                if not V1._SHA.fullmatch(target.stem) or target.suffix!='.json':raise ValueError('UNKNOWN_BLOB_FILE')
                blob_files[str(target.absolute())]=target;continue
            if relative in retired_locks:
                remove.append({'path':relative,'sha256':V2.file_fingerprint(target)[0],'bytes':target.stat().st_size});continue
            if target.suffix not in {'.json','.commit'}:continue
            retiring=_view(target,base) in views or relative in extra
            if retiring:
                remove.append({'path':relative,'sha256':V2.file_fingerprint(target)[0],'bytes':target.stat().st_size})
            if target.suffix=='.commit':continue
            dependencies=_closure(target,base,resolver)
            (retired if retiring else live).update(dependencies)
        if R.generation(base)!=generation:raise L.StorageBusy('STORAGE_CHANGED_DURING_MARK')
        orphans=set(blob_files)-live
        # An in-progress put_object() holds a pin, so the exclusive phase below
        # cannot take its unpublished objects. A later publish verifies closure.
        blobs=[{'path':str(blob_files[p].relative_to(base)),'sha256':blob_files[p].stem,'bytes':blob_files[p].stat().st_size} for p in sorted(orphans)[:200]]
        for item in blobs:
            if V2.file_fingerprint(base/item['path'])[0]!=item['sha256']:raise ValueError('ORPHAN_BLOB_HASH_MISMATCH')
        return {'version':VERSION,'generation':generation,'views':views,'records':remove,'blobs':blobs,
                'model_retirement':model_plan,
                'consumer_certificates':{'trace_view':'completion_and_session_exit@v1','owned_usage':retired_usage,'owned_empty_locks':retired_locks},
                'boundary':boundary,'protected':protected,'shared_blob_bytes':sum(blob_files[p].stat().st_size for p in retired&live),
                'before':{'bytes':sum(p.stat().st_size for p in all_files),'files':len(all_files)}},view_pins
    except BaseException:
        for p in view_pins:p.close()
        raise
    finally:
        # Explicit release: lazy objects used only for mark must not pin sweep.
        resolver._storage_pin.close();pin.close()


def _resume(base,plan,*,fault=None):
    import record_store as R
    # Deletion is a source mutation too. A previously prepared sweep or index
    # cannot be installed after this one merely because no writer published.
    R.invalidate_generation(base)
    # Logical expiration was committed first. No reader can observe half a view.
    for category in ('records','blobs'):
        for item in plan[category]:
            target=base/item['path']
            if target.exists():
                if V2.file_fingerprint(target)[0]!=item['sha256']:raise ValueError('RECLAIM_TARGET_CHANGED')
                target.unlink();V1._sync_directory(target.parent)
            if fault:fault(category,item['path'])
    import storage_index
    index=storage_index.path(base)
    if index.exists():
        if index.is_symlink():raise ValueError('INDEX_SYMLINK_FORBIDDEN')
        with sqlite3.connect(index) as db:
            for item in plan['records']:db.execute('DELETE FROM records WHERE path=?',(item['path'],))
    index_compaction=storage_index.compact(base)
    if fault:fault('index',None)
    for view in plan['views']:
        folder=base/'trace_view/snapshots'/view
        if folder.exists():folder.rmdir()
    with database(base,True) as db:
        state=_read(db,'state') or {'version':VERSION,'retired_views':0,'retired_file_bytes':0,'history_hash':'','legacy_namespace_closed':False}
        for view in plan['views']:
            if not _VIEW.fullmatch(view)[1]:db.execute('INSERT OR IGNORE INTO legacy_expired VALUES (?)',(view,))
        state.update(cutoff=plan['boundary']['detailed_origin_cutoff'],generation=state.get('generation',0)+1,
            retired_views=state['retired_views']+len(plan['views']),
            retired_file_bytes=state['retired_file_bytes']+sum(x['bytes'] for k in ('records','blobs') for x in plan[k]),
            history_hash=I.digest([state['history_hash'],plan]))
        root=base/'trace_view/snapshots'
        legacy_exists=root.exists() and any(_VIEW.fullmatch(p.name) and not _VIEW.fullmatch(p.name)[1] for p in root.iterdir())
        if not legacy_exists:
            state['legacy_namespace_closed']=True;db.execute('DELETE FROM legacy_expired')
        _save(db,'state',state);db.execute('DELETE FROM pending')
        import model_retention
        model_retention.commit(db,base,plan.get('model_retirement',{}))
    if fault:fault('checkpoint',None)
    # Control pages are reusable; VACUUM makes the measured final size explicit.
    db=sqlite3.connect(path(base))
    try:db.execute('VACUUM')
    finally:db.close()
    return {**state,'index_compaction':index_compaction}


def collect(calendar,*,at=None,dry_run=True,limit=50,base=None,fault=None):
    base=Path(base or C.ontology_root()).absolute()
    if base.resolve()!=Path(C.ontology_root()).resolve():raise ValueError('UNREGISTERED_ARCHIVE_ROOT')
    if type(limit) is not int or not 1<=limit<=200:raise ValueError('ARCHIVE_BATCH_LIMIT')
    if not base.exists():
        boundary=P.boundaries(calendar,at)
        return {'state':'empty','reclaimed_file_bytes':0} if boundary['deletion_allowed'] else boundary
    with database(base) as db:
        state=_read(db,'state') if db else None
        pending=_read(db,'pending') if db else None
    boundary=P.boundaries(calendar,at)
    if not pending and not boundary['deletion_allowed']:return boundary
    if not pending and state and state['cutoff']>boundary['detailed_origin_cutoff']:raise ValueError('ARCHIVE_BOUNDARY_REVERSED')
    pins=[]
    try:
        # Python's streaming encoders can leave unreachable reference cycles.
        # Collect only unreachable objects; reachable lazy readers keep pins.
        import gc
        gc.collect()
        if pending:
            if dry_run:return {'state':'recovery_pending','pending_views':len(pending['views'])}
            with I.transaction() as db:
                with L.exclusive(base):
                    finished=_resume(base,pending,fault=fault)
                import model_retention
                model_retention.prune_owners(db,base)
            return {'state':'recovered','index_compaction':finished['index_compaction'],'after':_bytes(base),'reclaimed_file_bytes':sum(x['bytes'] for k in ('records','blobs') for x in pending[k])}
        plan,pins=_plan(base,boundary,limit)
        result={'state':'dry_run','candidate_views':len(plan['views']),
            'candidate_manifest_bytes':sum(x['bytes'] for x in plan['records']),
            'candidate_blob_bytes':sum(x['bytes'] for x in plan['blobs']),
            'model_retirement':plan['model_retirement'],
            'shared_blob_bytes':plan['shared_blob_bytes'],'protected':plan['protected'],
            'other_legacy_records':'protected_pending_consumer_migration','before':plan['before']}
        if dry_run:return result
        gc.collect()
        with I.transaction() as safety_db:
            import model_retention
            model_retention.revalidate(safety_db,plan['model_retirement'],at or I.now())
            with L.exclusive(base):
                import record_store as R
                if R.generation(base)!=plan['generation']:raise L.StorageBusy('STORAGE_CHANGED_BEFORE_SWEEP')
                # Other mandatory records cannot be published while this short
                # bounded batch retires. No waiting or TTL-based reader eviction.
                with database(base,True) as db:_save(db,'pending',plan)
                if fault:fault('expired',None)
                finished=_resume(base,plan,fault=fault)
            model_retention.prune_owners(safety_db,base)
        result.update(state='reclaimed',after=_bytes(base),reclaimed_file_bytes=result['candidate_manifest_bytes']+result['candidate_blob_bytes'])
        result['net_physical_bytes_reclaimed']=result['before']['bytes']-result['after']['bytes']
        result['index_compaction']=finished['index_compaction']
        return result
    except L.StorageBusy as exc:return {'state':'deferred','reason':str(exc)}
    finally:
        for pin in pins:pin.close()
