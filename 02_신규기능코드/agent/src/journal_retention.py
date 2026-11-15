"""Atomic journal generations, with explicit recovery and reader lifetime pins.

Only the active journal is managed. The initial compaction is explicit; ordinary
reads never migrate it. A checkpoint and the retained suffix replace one file
atomically. The separate fixed-size identity marker prevents silent recreation.
"""
from contextlib import contextmanager
from datetime import datetime
import json
import os
from pathlib import Path
import shutil
import sqlite3
import uuid

import common as C
import journal_checkpoint as J
import storage_leases as L


def location():return Path(C.JOURNAL)
def control():return Path(C.WORK)/'journal_state.sqlite3'


@contextmanager
def database(write=False):
    path=control()
    if path.is_symlink():raise ValueError('JOURNAL_CONTROL_SYMLINK')
    if not path.exists() and not write:yield None;return
    db=sqlite3.connect(path if write else path.resolve().as_uri()+'?mode=ro',uri=not write,timeout=5)
    db.row_factory=sqlite3.Row
    try:
        if write:
            db.execute('BEGIN IMMEDIATE')
            db.execute('CREATE TABLE IF NOT EXISTS state (singleton INTEGER PRIMARY KEY CHECK(singleton=1),body TEXT,sha TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS pending (singleton INTEGER PRIMARY KEY CHECK(singleton=1),body TEXT,sha TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS building (singleton INTEGER PRIMARY KEY CHECK(singleton=1),body TEXT,sha TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS revision (singleton INTEGER PRIMARY KEY CHECK(singleton=1),value INTEGER NOT NULL)')
        yield db
        if write:db.commit()
    except BaseException:
        if write:db.rollback()
        raise
    finally:db.close()


def get(db,table):
    if db is None:return None
    if not db.execute('SELECT 1 FROM sqlite_master WHERE type=? AND name=?',('table',table)).fetchone():return None
    row=db.execute('SELECT body,sha FROM '+table+' WHERE singleton=1').fetchone()
    if not row:return None
    value=json.loads(row['body'])
    if J.digest(value)!=row['sha']:raise ValueError('JOURNAL_CONTROL_CORRUPT')
    return value


def put(db,table,value):
    db.execute('INSERT OR REPLACE INTO '+table+' VALUES(1,?,?)',(J.canonical(value).decode(),J.digest(value)))


def header():
    path=location()
    if path.is_symlink():raise ValueError('JOURNAL_SYMLINK')
    if not path.exists():return None
    with path.open('rb') as stream:raw=stream.readline(J.MAX_BYTES+1024)
    try:row=json.loads(raw)
    except (ValueError,UnicodeDecodeError):return None
    return row if isinstance(row,dict) and row.get('kind')=='journal_checkpoint' else None


def validate(db=None):
    if db is None:
        with database() as connection:return _validate(connection)
    return _validate(db)


def _validate(db):
    if get(db,'pending'):raise ValueError('JOURNAL_RECOVERY_REQUIRED')
    state=get(db,'state');row=header()
    if not state and not row:return None
    if not state or not row:raise ValueError('JOURNAL_IDENTITY_MARKER_MISMATCH')
    body=C.journal_checkpoint([row])
    if body['generation']!=state['generation'] or row['sha256']!=state['checkpoint_hash']:
        raise ValueError('JOURNAL_GENERATION_MISMATCH')
    return body


def read_pin(path):
    if Path(path).absolute()!=location().absolute():return None
    pin=L.Pin(C.WORK,wait=True)
    try:validate();return pin
    except BaseException:pin.close();raise


def append(row):
    root=Path(C.WORK);root.mkdir(parents=True,exist_ok=True)
    pin=L.Pin(root,wait=True)
    try:
        with database(True) as db:
            checkpoint=validate(db)
            if row.get('kind')=='journal_checkpoint':raise ValueError('JOURNAL_CHECKPOINT_HOST_ONLY')
            if checkpoint:
                if _day(row)<checkpoint['supported_after']:raise ValueError('JOURNAL_APPEND_SUPPORT_EXPIRED')
                C.journal_run_rows([J.envelope(checkpoint)],row.get('run'))
            raw=J.canonical(row)+b'\n'
            with location().open('ab') as stream:
                stream.write(raw);stream.flush();os.fsync(stream.fileno())
            db.execute('INSERT INTO revision VALUES(1,1) ON CONFLICT(singleton) DO UPDATE SET value=value+1')
    finally:pin.close()


def _day(row):
    value=datetime.fromisoformat(row['time'].replace('Z','+00:00'))
    if value.tzinfo is not None:value=value.astimezone(C.KST)
    return value.date().isoformat()


def watermark(rows):
    checkpoint=C.journal_checkpoint(rows)
    return {'version':'JOURNAL_WATERMARK@v2','checkpoint_generation':checkpoint['generation'] if checkpoint else 0,
            'checkpoint_hash':J.digest(checkpoint) if checkpoint else None,
            'retired_history_hash':checkpoint.get('history_hash') if checkpoint else None,
            'supported_after':checkpoint['supported_after'] if checkpoint else None,
            'sha256':rows.digest,'bytes':rows.size,'complete':True}


def enabled():
    with database() as db:return bool(get(db,'state') or get(db,'pending'))


def sequence():
    with database() as db:
        if db is None or not db.execute("SELECT 1 FROM sqlite_master WHERE name='revision'").fetchone():return 0
        row=db.execute('SELECT value FROM revision WHERE singleton=1').fetchone()
        return row[0] if row else 0


def stat_identity(path):
    value=Path(path).stat()
    return (value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns,value.st_ctime_ns)


def _cutoff(calendar,at):
    import retention_policy as P
    import improvement_store as S
    import validation_holdout as H
    boundary=P.boundaries(calendar,at)
    if not boundary['deletion_allowed']:return boundary,None
    cutoff=boundary['detailed_origin_cutoff'];protected=[]
    with S.transaction(readonly=True) as db:
        # Legacy as well as compact reservation writers announce their raw
        # publication under this transaction before it becomes visible.
        if db is not None and db.execute("SELECT 1 FROM records WHERE kind='holdout_sync' LIMIT 1").fetchone():
            raise L.StorageBusy('JOURNAL_HOLDOUT_PUBLICATION_PENDING')
        pins=[] if db is None else db.execute("SELECT body,hash FROM records WHERE kind='retention_pin' AND state='active' AND json_extract(body,'$.until')>? LIMIT 2001",(at or S.now(),)).fetchall()
        if len(pins)>2000:raise ValueError('JOURNAL_PIN_INVENTORY_LIMIT')
        for raw in pins:
            pin=S.decode(raw);start=(pin.get('period') or {}).get('start')
            if start:protected.append(S.instant(start).date().isoformat())
    for holdout in H.reservations():protected.append(S.instant(holdout['spec']['period']['start']).date().isoformat())
    return {**boundary,'registered_ranges':len(protected)},min([cutoff,*protected])


def _prepare(rows,cutoff):
    old=C.journal_checkpoint(rows)
    if old and cutoff<old['supported_after']:raise ValueError('JOURNAL_BOUNDARY_REVERSED')
    import logger
    fixes_hash=J.digest(logger.FIXES)
    if old and old.get('reconciliation_fixes_hash')!=fixes_hash:
        raise ValueError('JOURNAL_RECONCILIATION_DEFINITION_CHANGED')
    start=C.params()['competition']['start']
    body={'version':J.VERSION,'generation':(old or {}).get('generation',0)+1,'supported_after':cutoff,
          'competition_start':start,'all':J.state(rows),'competition':J.state(rows,'competition',start),
          'retired_runs':(old or {}).get('retired_runs','00'*32768),
          'reconciliation_fixes_hash':fixes_hash,
          'retired_rows':(old or {}).get('retired_rows',0),'history_hash':(old or {}).get('history_hash','0'*64)}
    regimes={};retired=set();retained=set();retained_codes=set();count=0;last='';ended=False
    import hashlib
    chain=hashlib.sha256();chain.update(body['history_hash'].encode())
    for index,row in enumerate(rows):
        if row['kind']=='journal_checkpoint':
            if index!=0:raise ValueError('JOURNAL_MULTIPLE_CHECKPOINTS')
            continue
        day=_day(row)
        if day<last:raise ValueError('JOURNAL_CHRONOLOGY_UNSUPPORTED')
        last=day
        if day>=cutoff:
            ended=True
            if row.get('run'):retained.add(row['run'])
            if row.get('code'):retained_codes.add(row['code'])
        elif ended:raise ValueError('JOURNAL_NONCONTIGUOUS_PREFIX')
        else:
            count+=1
            if row.get('run'):retired.add(row['run'])
            if row['kind']=='regime':regimes[row.get('run')]=row.get('regime')
        if len(retained)+len(retired)>20000:raise ValueError('JOURNAL_RUN_INVENTORY_LIMIT')
    if retired&retained:raise ValueError('JOURNAL_RUN_CROSSES_RETENTION_BOUNDARY')
    bits=bytearray.fromhex(body['retired_runs'])
    for run in retired:J.run_filter_add(bits,run)
    body['retired_runs']=bits.hex()
    body['current_names']={k:v for k,v in (old or {}).get('current_names',{}).items() if k in retained_codes}
    for row in rows:
        if row['kind']=='journal_checkpoint':continue
        if _day(row)>=cutoff:break
        if row.get('code') in retained_codes and row.get('name') and row['name']!=row['code']:
            body['current_names'][row['code']]=row['name']
        J.consume(body['all'],row,regimes=regimes)
        if _day(row)>=start:J.consume(body['competition'],row,regimes=regimes,keep_anchors=False)
        chain.update(J.canonical(row));chain.update(b'\n')
    body['history_hash']=chain.hexdigest();body['retired_rows']+=count
    body['source_watermark']={'sha256':rows.digest,'bytes':rows.size,'generation':(old or {}).get('generation',0)}
    body['consumer_contracts']=['global_fill_count','current_day_and_run','global_and_competition_book',
        'competition_counters','cooldown_ordinals','legacy_reconciliation_opening_net','frozen_replay_state']
    return J.envelope(body),count


def _file_hash(path):
    import hashlib
    h=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(256*1024),b''):h.update(block)
    return h.hexdigest()


def _sync():
    descriptor=os.open(C.WORK,os.O_RDONLY)
    try:os.fsync(descriptor)
    finally:os.close(descriptor)


def _recover(pending,*,fault=None,verified=False):
    staging=Path(C.WORK)/pending['staging']
    if staging.parent!=Path(C.WORK) or not staging.name.startswith('.journal-generation-') or staging.is_symlink():
        raise ValueError('JOURNAL_STAGING_IDENTITY')
    actual=pending['old_hash'] if verified else _file_hash(location()) if location().exists() else None
    if actual!=pending['new_hash']:
        if actual!=pending['old_hash'] or not staging.exists() or (not verified and _file_hash(staging)!=pending['new_hash']):
            raise ValueError('JOURNAL_RECOVERY_CONTENT_MISMATCH')
        os.replace(staging,location());_sync()
    if fault:fault('replaced')
    with database(True) as db:
        put(db,'state',pending['state']);db.execute('DELETE FROM pending');db.execute('DELETE FROM building')
    if staging.exists():staging.unlink()
    if fault:fault('committed')


def _discard_abandoned(building):
    """A directory lease and the live staging-file lock prove producer exit."""
    import fcntl
    target=Path(C.WORK)/building['staging']
    if target.parent!=Path(C.WORK) or not target.name.startswith('.journal-generation-') or target.is_symlink():
        raise ValueError('JOURNAL_STAGING_IDENTITY')
    with L.exclusive(C.WORK):
        with database(True) as db:
            if get(db,'building')!=building or get(db,'pending'):raise L.StorageBusy('JOURNAL_PREPARATION_CHANGED')
            if target.exists():
                with target.open('rb') as stream:
                    try:fcntl.flock(stream.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
                    except BlockingIOError as exc:raise L.StorageBusy('JOURNAL_PREPARATION_ACTIVE') from exc
                    target.unlink();_sync()
            db.execute('DELETE FROM building')


def collect(calendar,*,at=None,dry_run=True,fault=None):
    """Explicit migration/collection. Reports are read-only; originals aren't copied elsewhere."""
    import log_store
    root=Path(C.WORK);path=location()
    if path.parent.resolve()!=root.resolve():raise ValueError('UNREGISTERED_JOURNAL_ROOT')
    # Recovery completes an already committed decision. Losing access to the
    # exchange calendar must not strand mandatory readers/writers mid-swap.
    with database() as db:pending=get(db,'pending');building=get(db,'building')
    if pending:
        if dry_run:return {'state':'recovery_pending'}
        try:
            with L.exclusive(root):_recover(pending,fault=fault)
            return {'state':'recovered','generation':pending['state']['generation']}
        except L.StorageBusy as exc:return {'state':'deferred','reason':str(exc)}
    if building:
        if dry_run:return {'state':'preparation_recovery_pending'}
        try:
            _discard_abandoned(building)
            return {'state':'preparation_recovered','original_journal_preserved':True}
        except L.StorageBusy as exc:return {'state':'deferred','reason':str(exc)}
    try:boundary,cutoff=_cutoff(calendar,at)
    except L.StorageBusy as exc:return {'state':'deferred','reason':str(exc)}
    if cutoff is None:return boundary
    if not path.exists():return {'state':'empty'}
    original_sequence=sequence()
    rows=log_store.LogView(path);staging=None;pending_committed=False;stream=None
    original_identity=(*rows.identity,rows.size,*rows.stamp)
    try:
        checkpoint,count=_prepare(rows,cutoff)
        result={'state':'dry_run' if dry_run else 'unchanged','boundary':boundary,'supported_after':cutoff,
                'candidate_rows':count,'bytes_before':rows.size,'checkpoint_bytes':len(J.canonical(checkpoint)),
                'retired_rows':checkpoint['checkpoint']['retired_rows'],'operating_rules_changed':False}
        if dry_run or not count:return result
        expected={'all':J.digest(J.summarize(rows)),
                  'competition':J.digest(J.summarize(rows,scope='competition',start=C.params()['competition']['start']))}
        checkpoint['checkpoint']['parity']=expected
        checkpoint=J.envelope(checkpoint['checkpoint'])
        # A conservative temporary-space check does not delete protected data.
        if shutil.disk_usage(root).free<rows.size+len(J.canonical(checkpoint)):raise ValueError('JOURNAL_TEMP_CAPACITY_BLOCKED')
        staging=root/('.journal-generation-'+uuid.uuid4().hex+'.jsonl')
        with database(True) as db:
            if get(db,'building'):raise L.StorageBusy('JOURNAL_PREPARATION_ACTIVE')
            put(db,'building',{'staging':staging.name,'owner':'journal_retention@v1'})
        stream=staging.open('xb')
        import fcntl
        fcntl.flock(stream.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        stream.write(J.canonical(checkpoint)+b'\n')
        for row in rows:
            if row['kind']!='journal_checkpoint' and _day(row)>=cutoff:stream.write(J.canonical(row)+b'\n')
        stream.flush();os.fsync(stream.fileno())
        prepared=log_store.LogView(staging)
        actual={'all':J.digest(J.summarize(prepared)),
                'competition':J.digest(J.summarize(prepared,scope='competition',start=C.params()['competition']['start']))}
        if actual!=expected:raise ValueError('JOURNAL_CONSUMER_PARITY_FAILED')
        state={'generation':checkpoint['checkpoint']['generation'],'checkpoint_hash':checkpoint['sha256'],
               'supported_after':cutoff,'retired_rows':checkpoint['checkpoint']['retired_rows']}
        plan={'old_hash':rows.digest,'new_hash':_file_hash(staging),'staging':staging.name,'state':state}
        staging_identity=stat_identity(staging)
        if rows._journal_pin:rows._journal_pin.close()
        import improvement_store as S
        # Serialize reservation publication, recheck its boundary, and hold
        # that lock until swap completion. The WORK lock stays short: older
        # reservation history is examined before blocking journal writers.
        with S.transaction():
            _,current_cutoff=_cutoff(calendar,at)
            if current_cutoff is None or current_cutoff<cutoff:
                return {**result,'state':'deferred','reason':'JOURNAL_PROTECTION_CHANGED_DURING_PREPARATION'}
            with L.exclusive(root):
                validate()
                if sequence()!=original_sequence or stat_identity(path)!=original_identity:
                    return {**result,'state':'deferred','reason':'JOURNAL_CHANGED_DURING_PREPARATION'}
                if stat_identity(staging)!=staging_identity:raise ValueError('JOURNAL_STAGING_CHANGED')
                with database(True) as db:put(db,'pending',plan)
                pending_committed=True
                if fault:fault('prepared')
                _recover(plan,fault=fault,verified=True)
        result.update(state='reclaimed',generation=state['generation'],bytes_after=path.stat().st_size,
                      net_file_bytes_reclaimed=rows.size-path.stat().st_size,consumer_parity=actual)
        return result
    except L.StorageBusy as exc:return {'state':'deferred','reason':str(exc)}
    finally:
        try:
            if staging and not pending_committed:
                with database(True) as db:
                    owner=get(db,'building')
                    if owner and owner['staging']==staging.name:
                        if staging.exists():staging.unlink()
                        db.execute('DELETE FROM building')
        finally:
            if stream:stream.close()
            if rows._journal_pin:rows._journal_pin.close()
