"""Bounded retirement of settled call rows, with conservative replay guards.

Two fixed bitsets preserve every retired call/provider identity. They have no
false negatives; a possible collision blocks the call and is reported as such,
never treated as a confirmed duplicate or permission to retry with a new key.
Unresolved costs and active improvement owners are never compacted.
"""
import hashlib
import json
import improvement_store as S
import retention_policy as P

BYTES=1024*1024
HASHES=7
VERSION='RESEARCH_BUDGET_RETIREMENT@v1'


def _positions(value,size):
    raw=hashlib.sha256(value.encode()).digest()
    first=int.from_bytes(raw[:16],'big');step=int.from_bytes(raw[16:],'big')|1
    return [(first+n*step)%(size*8) for n in range(HASHES)]


def _set(bits,value):
    for bit in _positions(value,len(bits)):bits[bit//8]|=1<<(bit%8)


def _contains(bits,value):
    return all(bits[bit//8]&(1<<(bit%8)) for bit in _positions(value,len(bits)))


def _hash(row):
    return S.digest({key:(hashlib.sha256(row[key]).hexdigest() if key.endswith('_bits') else row[key])
        for key in ('version','cutoff','retired_count','actual_total','history_hash','call_bits','provider_bits')})


def load(db):
    if db is None or not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='budget_retirement'").fetchone():return None
    raw=db.execute('SELECT * FROM budget_retirement WHERE singleton=1').fetchone()
    if not raw:raise ValueError('BUDGET_RETIREMENT_CHECKPOINT_MISSING')
    row=dict(raw)
    if row['version']!=VERSION or not row['call_bits'] or len(row['call_bits'])!=len(row['provider_bits']) or row['hash']!=_hash(row):
        raise ValueError('BUDGET_RETIREMENT_CHECKPOINT_CORRUPT')
    return row


def check_call(db,call_id,day):
    row=load(db)
    if not row:return None
    if day<row['cutoff']:return 'research_support_expired'
    if _contains(row['call_bits'],call_id):return 'call_retired_or_checkpoint_collision'
    return None


def check_provider(db,identifier):
    row=load(db)
    return bool(row and _contains(row['provider_bits'],identifier))


def collect(calendar,*,at=None,dry_run=True,limit=1000):
    boundary=P.boundaries(calendar,at)
    if not boundary['deletion_allowed']:return boundary
    if type(limit) is not int or not 1<=limit<=1000:raise ValueError('BUDGET_COMPACTION_BATCH_LIMIT')
    cutoff=boundary['detailed_origin_cutoff']
    terminal=('completed','resolved','expired','cancelled','failed','rolled_back','improved','insufficient_data','delivered')
    with S.transaction(readonly=dry_run) as db:
        if db is None:return {'state':'empty','retired_count':0}
        old=load(db)
        if old and cutoff<old['cutoff']:raise ValueError('BUDGET_SUPPORT_BOUNDARY_CANNOT_REVERSE')
        rows=db.execute("""SELECT c.* FROM calls c WHERE c.day<? AND substr(c.updated_at,1,10)<?
            AND c.state IN ('settled','cancelled')
            AND NOT EXISTS (SELECT 1 FROM records r WHERE (r.id=c.owner OR r.owner=c.owner)
                AND r.kind IN ('issue','investigation','change','application','experiment','reassessment','retention_pin')
                AND r.state NOT IN ("""+','.join('?' for _ in terminal)+"""))
            ORDER BY c.day,c.id LIMIT ?""",(cutoff,cutoff,*terminal,limit)).fetchall()
        if not dry_run:
            checkpoint=old or {'version':VERSION,'retired_count':0,'actual_total':0,'history_hash':'0'*64,
                'call_bits':bytes(BYTES),'provider_bits':bytes(BYTES)}
            checkpoint.update(cutoff=cutoff)
            calls=bytearray(checkpoint['call_bits']);providers=bytearray(checkpoint['provider_bits'])
            for row in rows:
                _set(calls,row['id'])
                if row['provider_id']:_set(providers,row['provider_id'])
                checkpoint['history_hash']=S.digest([checkpoint['history_hash'],dict(row)])
                checkpoint['retired_count']+=1;checkpoint['actual_total']+=row['actual'] or 0
            checkpoint.update(call_bits=bytes(calls),provider_bits=bytes(providers))
            db.execute('''CREATE TABLE IF NOT EXISTS budget_retirement (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),version TEXT NOT NULL,cutoff TEXT NOT NULL,
                retired_count INTEGER NOT NULL,actual_total INTEGER NOT NULL,history_hash TEXT NOT NULL,
                call_bits BLOB NOT NULL,provider_bits BLOB NOT NULL,hash TEXT NOT NULL)''')
            db.execute('INSERT OR REPLACE INTO budget_retirement VALUES (1,?,?,?,?,?,?,?,?)',
                tuple(checkpoint[k] for k in ('version','cutoff','retired_count','actual_total','history_hash','call_bits','provider_bits'))+(_hash(checkpoint),))
            db.executemany('DELETE FROM calls WHERE id=?',((r['id'],) for r in rows))
        return {'state':'dry_run' if dry_run else 'compacted','cutoff':cutoff,'retired_count':len(rows),
            'retired_actual_usd':sum(r['actual'] or 0 for r in rows)/1e6,'more':len(rows)==limit,
            'unresolved_calls':'retained','checkpoint_bytes':2*(len(old['call_bits']) if old else BYTES),
            'identity_collision_policy':'block_as_unconfirmed; never dispatch or silently replace call ID',
            'per_call_tombstones':False}


def status(db,day):
    row=load(db)
    if not row:return {'history_scope':'uncompacted'}
    return {'history_scope':'expired' if day<row['cutoff'] else 'supported','supported_after':row['cutoff'],
        'retired_actual_usd':row['actual_total']/1e6,'retired_calls':row['retired_count'],
        'identity_checkpoint_bytes':len(row['call_bits'])+len(row['provider_bits'])}
