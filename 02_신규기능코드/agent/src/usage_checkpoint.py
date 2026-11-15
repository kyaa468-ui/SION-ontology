"""Finite research-use identities after verified consumer migration.

The fixed filter only disqualifies independence. A match may be a collision;
absence never proves nonuse. Exact historical scope queries expire below the
committed boundary. Active ranges, recent use and required state stay explicit.
"""
import hashlib
import sqlite3

import improvement_store as S
import retention_policy as P
from research_budget_checkpoint import _contains, _set

VERSION = 'RESEARCH_USAGE_RETIREMENT@v1'
BYTES = 1024 * 1024
MARKER = 'research_usage_retirement'
TERMINAL = ('completed', 'finalized', 'finalized_unknown', 'resolved', 'expired',
            'cancelled', 'failed', 'rolled_back', 'improved', 'insufficient_data',
            'delivered', 'evaluation_unsupported', 'capacity_blocked')


def _hash(row):
    fields = ('version', 'cutoff', 'retired_aliases', 'retired_sources', 'folded_events', 'history_hash')
    return S.digest({**{key: row[key] for key in fields},
                     'bits_sha256': hashlib.sha256(row['bits']).hexdigest()})


def load(db):
    if db is None:
        return None
    marker = db.execute('SELECT value FROM meta WHERE key=?', (MARKER,)).fetchone()
    exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='usage_retirement'").fetchone()
    if not exists and not marker:
        return None
    if not exists or not marker:
        raise ValueError('USAGE_CHECKPOINT_IDENTITY_MISMATCH')
    raw = db.execute('SELECT * FROM usage_retirement WHERE singleton=1').fetchone()
    if not raw:
        raise ValueError('USAGE_CHECKPOINT_MISSING')
    row = dict(raw)
    if (row['version'] != VERSION or len(row['bits']) != BYTES or
            row['hash'] != _hash(row) or marker[0] != row['hash']):
        raise ValueError('USAGE_CHECKPOINT_CORRUPT')
    return row


def _save(db, row):
    db.execute('''CREATE TABLE IF NOT EXISTS usage_retirement (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1), version TEXT NOT NULL,
        cutoff TEXT NOT NULL, retired_aliases INTEGER NOT NULL, retired_sources INTEGER NOT NULL,
        folded_events INTEGER NOT NULL, history_hash TEXT NOT NULL, bits BLOB NOT NULL, hash TEXT NOT NULL)''')
    fields = ('version', 'cutoff', 'retired_aliases', 'retired_sources', 'folded_events', 'history_hash', 'bits')
    digest = _hash(row)
    db.execute('INSERT OR REPLACE INTO usage_retirement VALUES (1,?,?,?,?,?,?,?,?)',
               tuple(row[key] for key in fields) + (digest,))
    db.execute('INSERT OR REPLACE INTO meta VALUES (?,?)', (MARKER, digest))


def possible_use(checkpoint, identifier, identity=None):
    return bool(checkpoint and (_contains(checkpoint['bits'], 'alias:' + identifier) or
                identity and _contains(checkpoint['bits'], 'source:' + identity)))


def fold_expired_use(db, checkpoint, identifier, identity, origin_at, source):
    """Re-import/replay cannot recreate an expired per-ID ledger forever."""
    if not checkpoint:
        return False
    expired = bool(origin_at and S.instant(origin_at).date().isoformat() < checkpoint['cutoff'])
    known = possible_use(checkpoint, identifier, identity)
    if not expired and not known:
        return False
    if origin_at and not expired:
        raise ValueError('USAGE_IDENTITY_RETIRED_OR_CHECKPOINT_COLLISION')
    bits = bytearray(checkpoint['bits'])
    _set(bits, 'alias:' + identifier)
    _set(bits, 'source:' + identity)
    checkpoint.update(bits=bytes(bits), folded_events=checkpoint['folded_events'] + 1,
                      history_hash=S.digest([checkpoint['history_hash'], identifier, identity, source]))
    _save(db, checkpoint)
    return True


def scope_error(checkpoint, period):
    if checkpoint and S.instant(period['start']).date().isoformat() < checkpoint['cutoff']:
        return {'reason': 'usage_detail_scope_expired', 'supported_after': checkpoint['cutoff']}
    return None


def _cutoff(db, cutoff, at):
    if db.execute("SELECT 1 FROM records WHERE kind='holdout_sync' LIMIT 1").fetchone():
        raise ValueError('USAGE_HOLDOUT_PUBLICATION_PENDING')
    pins = db.execute("SELECT body,hash FROM records WHERE kind='retention_pin' AND state='active' AND json_extract(body,'$.until')>? LIMIT 2001", (at,)).fetchall()
    if len(pins) > 2000:
        raise ValueError('USAGE_PIN_INVENTORY_LIMIT')
    starts = [cutoff]
    for raw in pins:
        start = (S.decode(raw).get('period') or {}).get('start')
        if start:
            starts.append(S.instant(start).date().isoformat())
    import validation_holdout as H
    for reservation in H.reservations():
        starts.append(S.instant(reservation['spec']['period']['start']).date().isoformat())
    return min(starts)


def _unused_by_active_owner(alias='e'):
    from retention_gc import ROOT_KINDS
    # This is a bounded reference list (at most eight entries per source).
    return f'''NOT EXISTS (SELECT 1 FROM json_each({alias}.body,'$.references') j
        JOIN records r ON r.id=json_extract(j.value,'$.owner_id') OR r.owner=json_extract(j.value,'$.owner_id')
        WHERE r.kind IN ({','.join('?' for _ in ROOT_KINDS)})
        AND r.state NOT IN ({','.join('?' for _ in TERMINAL)}))''', (*ROOT_KINDS, *TERMINAL)


def retire(db, cutoff, *, at, dry_run=True, limit=200):
    if db is None:
        return {'state': 'empty', 'retired_aliases': 0, 'retired_sources': 0}
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError('USAGE_RETIREMENT_BATCH_LIMIT')
    if not db.execute("SELECT 1 FROM meta WHERE key='research_usage_ready'").fetchone():
        return {'state': 'migration_required', 'command': 'migrate-research-usage'}
    if db.execute("SELECT 1 FROM records WHERE kind='usage_ledger_health' AND state='blocked' LIMIT 1").fetchone():
        raise ValueError('USAGE_LEDGER_HEALTH_UNRESOLVED')
    if not dry_run:
        db.execute("CREATE INDEX IF NOT EXISTS usage_source_identity ON records(json_extract(body,'$.source_identity')) WHERE kind='usage_alias'")
    checkpoint = load(db)
    cutoff = _cutoff(db, cutoff, at)
    if checkpoint and cutoff < checkpoint['cutoff']:
        raise ValueError('USAGE_SUPPORT_BOUNDARY_CANNOT_REVERSE')
    owner_condition, owner_args = _unused_by_active_owner()
    cutoff_time = cutoff + 'T00:00:00+09:00'
    last_use = "coalesce((SELECT max(julianday(json_extract(j.value,'$.created_at'))) FROM json_each(e.body,'$.references') j),julianday(e.origin_at))"
    valid_times = "NOT EXISTS (SELECT 1 FROM json_each(e.body,'$.references') j WHERE julianday(json_extract(j.value,'$.created_at')) IS NULL)"
    required = "NOT EXISTS (SELECT 1 FROM links l WHERE l.target_kind={kind} AND l.target_id={id} AND l.relation IN ('required_content','required_state'))"
    query = f'''SELECT a.body,a.hash,e.body AS exposure_body,e.hash AS exposure_hash
        FROM records a JOIN records e ON e.kind='research_exposure' AND e.id=json_extract(a.body,'$.source_identity')
        WHERE a.kind='usage_alias' AND json_extract(a.body,'$.identity_resolved')=1
        AND json_extract(a.body,'$.code') IS NOT NULL
        AND julianday(json_extract(a.body,'$.application_origin_at'))<julianday(?)
        AND {last_use}<julianday(?) AND {valid_times} AND {owner_condition}
        AND {required.format(kind="'usage_alias'", id='a.id')}
        AND {required.format(kind="'research_exposure'", id='e.id')}
        ORDER BY a.origin_at,a.id LIMIT ?'''
    rows = db.execute(query, (cutoff_time, cutoff_time, *owner_args, limit)).fetchall()
    aliases = []
    for raw in rows:
        aliases.append(S.decode(raw))
        S.decode({'body': raw['exposure_body'], 'hash': raw['exposure_hash']})
    # Project the batch's removals without changing a dry-run database.
    removed_ids = {row['id'] for row in aliases}
    sources = []
    excluded = ' AND a.id NOT IN (' + ','.join('?' for _ in removed_ids) + ')' if removed_ids else ''
    query = f'''SELECT e.body,e.hash FROM records e WHERE e.kind='research_exposure'
        AND {last_use}<julianday(?) AND {valid_times} AND {owner_condition}
        AND {required.format(kind="'research_exposure'", id='e.id')}
        AND NOT EXISTS (SELECT 1 FROM records a WHERE a.kind='usage_alias'
            AND json_extract(a.body,'$.source_identity')=e.id {excluded})
        ORDER BY e.origin_at,e.id LIMIT ?'''
    for raw in db.execute(query, (cutoff_time, *owner_args, *removed_ids, limit)):
        value = S.decode(raw)
        sources.append(value)
    result = {'state': 'dry_run' if dry_run else 'compacted', 'supported_after': cutoff,
              'retired_aliases': len(aliases), 'retired_sources': len(sources),
              'checkpoint_bytes': BYTES, 'per_identity_tombstones': False,
              'collision_policy': 'possible_prior_use; never proof of independence',
              'raw_deletion_authorized': False, 'more': len(aliases) == limit or len(sources) == limit}
    if dry_run:
        return result
    checkpoint = checkpoint or {'version': VERSION, 'cutoff': cutoff, 'retired_aliases': 0,
        'retired_sources': 0, 'folded_events': 0, 'history_hash': '0'*64, 'bits': bytes(BYTES)}
    bits = bytearray(checkpoint['bits'])
    for row in aliases:
        _set(bits, 'alias:' + row['id'])
        _set(bits, 'source:' + row['source_identity'])
    for row in sources:
        _set(bits, 'source:' + row['id'])
    checkpoint.update(bits=bytes(bits), cutoff=cutoff,
        retired_aliases=checkpoint['retired_aliases'] + len(aliases),
        retired_sources=checkpoint['retired_sources'] + len(sources),
        history_hash=S.digest([checkpoint['history_hash'], aliases, sources]))
    _save(db, checkpoint)
    for kind, selected in (('usage_alias', aliases), ('research_exposure', sources)):
        for row in selected:
            db.execute('DELETE FROM revisions WHERE kind=? AND id=?', (kind, row['id']))
            db.execute('DELETE FROM records WHERE kind=? AND id=?', (kind, row['id']))
            db.execute('DELETE FROM links WHERE owner_kind=? AND owner_id=?', (kind, row['id']))
    return result


def collect(calendar, *, at=None, dry_run=True, limit=200):
    boundary = P.boundaries(calendar, at)
    if not boundary['deletion_allowed']:
        return boundary
    with S.transaction(readonly=dry_run) as db:
        result = retire(db, boundary['detailed_origin_cutoff'], at=at or S.now(), dry_run=dry_run, limit=limit)
    if not dry_run and result['state'] == 'compacted':
        connection = sqlite3.connect(S.path(), timeout=30)
        try:
            connection.execute('VACUUM')
        finally:
            connection.close()
        result['database_bytes_after'] = S.path().stat().st_size
    return result
