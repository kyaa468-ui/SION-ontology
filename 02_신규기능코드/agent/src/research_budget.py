"""One durable KST ledger for all research calls, using integer micro-dollars.

Ambiguous dispatch/timeout is never a refund. Unsent reservations move to the
actual dispatch day atomically. Unresolved costs also block subsequent days.
"""
from __future__ import annotations
from decimal import Decimal, InvalidOperation, ROUND_CEILING

import improvement_store as S

LIMIT = 5_000_000


class DispatchBlocked(ValueError):
    def __init__(self, state, *, release_capacity=False):
        self.state = state
        self.release_capacity = release_capacity
        super().__init__('RESEARCH_' + state.upper())


def lookup(call_id):
    with S.transaction(readonly=True) as db:
        row = db.execute('SELECT * FROM calls WHERE id=?', (call_id,)).fetchone() if db else None
        return dict(row) if row else None


def money(value):
    try:
        n = Decimal(str(value))
        if not n.is_finite() or n < 0:
            raise ValueError('INVALID_COST')
        return int((n * 1_000_000).to_integral_value(rounding=ROUND_CEILING))
    except InvalidOperation as exc:
        raise ValueError('INVALID_COST') from exc


def reserve(call_id, owner, maximum_usd, payload, *, at=None):
    maximum = money(maximum_usd)
    if maximum <= 0 or maximum > LIMIT:
        raise ValueError('INVALID_RESERVATION')
    dt = S.instant(at or S.now())
    day, stamp, hashed = dt.date().isoformat(), dt.isoformat(), S.digest(payload)
    with S.transaction() as db:
        old = db.execute('SELECT * FROM calls WHERE id=?', (call_id,)).fetchone()
        if old:
            if (old['owner'], old['payload_hash'], old['reserved']) != (owner, hashed, maximum):
                raise ValueError('CALL_ID_CONFLICT')
            return {**dict(old), 'dispatch_allowed': False, 'resume_allowed': old['state']=='reserved'}
        import research_budget_checkpoint as checkpoint
        blocked=checkpoint.check_call(db,call_id,day)
        if blocked:return {'state':blocked,'dispatch_allowed':False,'call_id':call_id}
        ambiguous = db.execute("SELECT id FROM calls WHERE state IN ('sent','unknown','overrun') LIMIT 1").fetchone()
        if ambiguous:
            return {'state': 'cost_unresolved', 'blocking_call_id': ambiguous['id'], 'dispatch_allowed': False}
        used = db.execute("SELECT COALESCE(SUM(CASE WHEN actual IS NOT NULL THEN actual WHEN state='cancelled' THEN 0 ELSE reserved END),0) FROM calls WHERE day=?", (day,)).fetchone()[0]
        if used + maximum > LIMIT:
            return {'state': 'budget_blocked', 'day': day, 'available_usd': (LIMIT-used)/1e6, 'dispatch_allowed': False}
        db.execute('INSERT INTO calls (id,day,owner,payload_hash,reserved,actual,state,provider_id,created_at,updated_at) VALUES (?,?,?,?,?,?,?,NULL,?,?)',
                   (call_id, day, owner, hashed, maximum, None, 'reserved', stamp, stamp))
        return {'id': call_id, 'state': 'reserved', 'day': day, 'maximum_usd': maximum/1e6, 'dispatch_allowed': True}


def mark_sent(call_id):
    """Claim immediately before dispatch, rechecking the actual KST day budget."""
    with S.transaction() as db:
        row = db.execute('SELECT * FROM calls WHERE id=?', (call_id,)).fetchone()
        if row is None or row['state']!='reserved':
            # A competing worker may already own this call and its allowance.
            raise DispatchBlocked('call_not_dispatchable')
        if db.execute("SELECT 1 FROM calls WHERE id<>? AND state IN ('sent','unknown','overrun') LIMIT 1", (call_id,)).fetchone():
            raise DispatchBlocked('cost_unresolved', release_capacity=True)
        stamp = S.instant(S.now()).isoformat()
        day = S.instant(stamp).date().isoformat()
        used = db.execute("SELECT COALESCE(SUM(CASE WHEN actual IS NOT NULL THEN actual WHEN state='cancelled' THEN 0 ELSE reserved END),0) FROM calls WHERE day=? AND id<>?", (day,call_id)).fetchone()[0]
        if used + row['reserved'] > LIMIT:
            raise DispatchBlocked('budget_blocked', release_capacity=True)
        if 'sent_at' not in {column['name'] for column in db.execute('PRAGMA table_info(calls)')}:
            db.execute('ALTER TABLE calls ADD COLUMN sent_at TEXT')
        db.execute("UPDATE calls SET state='sent',day=?,sent_at=?,updated_at=? WHERE id=? AND state='reserved'", (day,stamp,stamp,call_id))
        return dict(db.execute('SELECT * FROM calls WHERE id=?', (call_id,)).fetchone())


def settle(call_id, actual_usd, provider_id):
    if not isinstance(provider_id, str) or not provider_id:
        raise ValueError('PROVIDER_REQUEST_ID_REQUIRED')
    actual = money(actual_usd) if actual_usd is not None else None
    with S.transaction() as db:
        row = db.execute('SELECT * FROM calls WHERE id=?', (call_id,)).fetchone()
        if row is None or row['state'] not in ('sent', 'unknown', 'settled', 'overrun'):
            raise ValueError('CALL_NOT_SENT')
        if row['state'] in ('settled', 'overrun'):
            if (row['actual'], row['provider_id']) != (actual, provider_id):
                raise ValueError('SETTLEMENT_CONFLICT')
            return dict(row)
        other = db.execute('SELECT id FROM calls WHERE provider_id=? AND id<>?', (provider_id, call_id)).fetchone()
        if other:
            raise ValueError('PROVIDER_REQUEST_ALREADY_SETTLED')
        import research_budget_checkpoint as checkpoint
        if checkpoint.check_provider(db,provider_id):
            raise ValueError('PROVIDER_ID_RETIRED_OR_CHECKPOINT_COLLISION; cost remains unresolved')
        state = 'unknown' if actual is None else 'overrun' if actual > row['reserved'] else 'settled'
        db.execute('UPDATE calls SET actual=?,provider_id=?,state=?,updated_at=? WHERE id=?',
                   (actual, provider_id, state, S.now(), call_id))
        return {'id': call_id, 'state': state, 'actual_usd': None if actual is None else actual/1e6, 'day': row['day']}


def cancel_unsent(call_id):
    with S.transaction() as db:
        if db.execute("UPDATE calls SET state='cancelled',updated_at=? WHERE id=? AND state='reserved'", (S.now(), call_id)).rowcount != 1:
            raise ValueError('SENT_CALL_CANNOT_BE_RELEASED')


def status(at=None):
    day = S.instant(at or S.now()).date().isoformat()
    with S.transaction(readonly=True) as db:
        rows = [] if db is None else db.execute("SELECT day,reserved,actual,state FROM calls WHERE day=? OR state IN ('sent','unknown','overrun')", (day,)).fetchall()
        import research_budget_checkpoint as checkpoint
        history=checkpoint.status(db,day)
    return {**history,'day': day, 'limit_usd': 5, 'actual_usd': None if history['history_scope']=='expired' else sum(r['actual'] or 0 for r in rows if r['day']==day)/1e6,
            'reserved_usd': sum(r['reserved'] for r in rows if r['day']==day and r['actual'] is None and r['state']!='cancelled')/1e6,
            'unresolved_calls': sum(r['state'] in ('sent','unknown','overrun') for r in rows)}
