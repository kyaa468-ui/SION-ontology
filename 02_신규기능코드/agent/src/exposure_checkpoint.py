"""Retire ended opportunity exposure rows without restoring independence.

A fixed monotone identity filter is deliberately conservative. A match after
compaction is reported as possible prior exposure, never as proof of nonuse.
The actual reservation/enrollment proof is still required independently.
"""
import hashlib
import improvement_store as S
from research_budget_checkpoint import _set,_contains

BYTES=1024*1024
VERSION='OPPORTUNITY_EXPOSURE_RETIREMENT@v1'


def _hash(version,count,history,bits):return S.digest([version,count,history,hashlib.sha256(bits).hexdigest()])


def load(db):
    if db is None or not db.execute("SELECT 1 FROM sqlite_master WHERE name='exposure_retirement'").fetchone():return None
    row=db.execute('SELECT * FROM exposure_retirement WHERE singleton=1').fetchone()
    if not row or row['version']!=VERSION or not row['bits'] or row['hash']!=_hash(row['version'],row['count'],row['history'],row['bits']):
        raise ValueError('EXPOSURE_CHECKPOINT_CORRUPT')
    return dict(row)


def status(db,source):
    if db is not None and db.execute('SELECT 1 FROM exposure WHERE source=? LIMIT 1',(source,)).fetchone():
        return {'used':True,'basis':'recorded_exposure'}
    row=load(db)
    if row and _contains(row['bits'],source):return {'used':True,'basis':'prior_exposure_or_checkpoint_collision'}
    return {'used':False,'basis':'no_positive_evidence; not proof of independence'}


def retire(db,cases):
    from investigation import exposure_identity
    sources=set()
    for case in cases:
        if all(case.get(k) for k in ('run_attempt_id','code','confidence_for')):sources.add(exposure_identity(case))
        if all(case.get(k) for k in ('account_scope','code','confidence_for','origin_at')):
            sources.add(S.digest([case['account_scope'],case['code'],case['confidence_for'],case['origin_at']]))
    rows=[]
    for source in sorted(sources):
        rows.extend(dict(row) for row in db.execute('SELECT * FROM exposure WHERE source=? ORDER BY owner',(source,)))
    if not rows:return {'retired_rows':0}
    old=load(db) or {'version':VERSION,'count':0,'history':'','bits':bytes(BYTES)}
    bits=bytearray(old['bits']);history=old['history']
    for row in rows:
        _set(bits,row['source']);history=S.digest([history,row])
    bits=bytes(bits);count=old['count']+len(rows)
    db.execute('''CREATE TABLE IF NOT EXISTS exposure_retirement (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1), version TEXT NOT NULL,
        count INTEGER NOT NULL, history TEXT NOT NULL, bits BLOB NOT NULL, hash TEXT NOT NULL)''')
    db.execute('INSERT OR REPLACE INTO exposure_retirement VALUES (1,?,?,?,?,?)',
        (VERSION,count,history,bits,_hash(VERSION,count,history,bits)))
    db.executemany('DELETE FROM exposure WHERE source=?',((s,) for s in sources))
    return {'retired_rows':len(rows),'checkpoint_bytes':len(bits),'per_source_tombstones':False}


def collapse_duplicates(db):
    """The only current consumer tests presence by source, never owner/count."""
    before=db.execute('SELECT count(*) FROM exposure').fetchone()[0]
    db.execute('DELETE FROM exposure WHERE rowid NOT IN (SELECT min(rowid) FROM exposure GROUP BY source)')
    db.execute('CREATE UNIQUE INDEX IF NOT EXISTS exposure_source_once ON exposure(source)')
    return before-db.execute('SELECT count(*) FROM exposure').fetchone()[0]
