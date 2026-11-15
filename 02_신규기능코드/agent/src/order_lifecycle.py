"""Finite dispatch replay support without making an old ID issuable again.

The order guard owns these tables and their transaction. Completed transport is
not completed execution: only explicit rejection, simulation, or a verified
broker execution summary can close a consumed attempt. Unknown claims stay.
"""
import json
import uuid
import common as C

VERSION='ORDER_REPLAY@v2'


def initialize(db):
    db.execute('''CREATE TABLE IF NOT EXISTS order_replay_contract (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),version TEXT NOT NULL,
        generation INTEGER NOT NULL,supported_after TEXT,
        retired_count INTEGER NOT NULL,retired_bytes INTEGER NOT NULL,history_hash TEXT NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS order_lifecycle (
        account_scope TEXT NOT NULL,order_attempt_id TEXT NOT NULL,generation INTEGER NOT NULL,
        disposition TEXT NOT NULL,closed_at TEXT,proof TEXT,proof_hash TEXT,
        PRIMARY KEY(account_scope,order_attempt_id))''')
    db.execute('''CREATE TABLE IF NOT EXISTS order_intents (
        account_scope TEXT NOT NULL,request_key TEXT NOT NULL,run_attempt_id TEXT NOT NULL,
        origin_at TEXT NOT NULL,order_attempt_id TEXT NOT NULL,
        PRIMARY KEY(account_scope,request_key))''')
    db.execute('CREATE INDEX IF NOT EXISTS order_lifecycle_closed ON order_lifecycle(disposition,closed_at)')
    db.execute('CREATE INDEX IF NOT EXISTS order_created ON order_attempts(created_at,account_scope,order_attempt_id)')
    db.execute('CREATE INDEX IF NOT EXISTS order_intent_owner ON order_intents(account_scope,order_attempt_id)')
    db.execute('CREATE TABLE IF NOT EXISTS order_gc_progress (singleton INTEGER PRIMARY KEY CHECK(singleton=1),last_key TEXT NOT NULL)')
    db.execute('INSERT OR IGNORE INTO order_replay_contract VALUES (1,?,1,NULL,0,0,?)',(VERSION,'0'*64))
    row=db.execute('SELECT * FROM order_replay_contract WHERE singleton=1').fetchone()
    if row['version']!=VERSION:raise ValueError('UNSUPPORTED_ORDER_REPLAY_CONTRACT')
    return dict(row)


def contract(db):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='order_replay_contract'").fetchone():return None
    row=db.execute('SELECT * FROM order_replay_contract WHERE singleton=1').fetchone()
    if not row or row['version']!=VERSION:raise ValueError('ORDER_REPLAY_CONTRACT_CORRUPT')
    return dict(row)


def issue(db,scope):
    state=initialize(db)
    identifier=f"order-g{state['generation']}-"+uuid.uuid4().hex
    db.execute('INSERT INTO order_lifecycle VALUES (?,?,?,\'open\',NULL,NULL,NULL)',(scope,identifier,state['generation']))
    return identifier


def require_sendable(db,row):
    import execution_trace as E
    state=contract(db)
    if row['state']=='reserved' and state and state['supported_after'] and row['created_at']<state['supported_after']:
        raise E.GuardError('ORDER_REQUEST_EXPIRED','The original request is outside dispatch support; it cannot be sent.')


def note_result(db,scope,identifier,result):
    initialize(db)
    reason=result.get('broker_acknowledgment')
    if reason not in {'rejected','simulated'}:return
    _proof(db,scope,identifier,{'basis':'explicit_broker_'+reason,'disposition':reason,
                             'result_hash':_digest(result)},closed=True)


def _digest(value):
    import execution_trace as E
    return E._hash(value)


def _proof(db,scope,identifier,proof,*,closed):
    row=db.execute('SELECT generation,closed_at FROM order_lifecycle WHERE account_scope=? AND order_attempt_id=?',(scope,identifier)).fetchone()
    generation=row['generation'] if row else 0  # Legacy issuance has no guessed date or encoded generation.
    closed_at=(row['closed_at'] if row else None) or C.now().isoformat()
    body=json.dumps(proof,sort_keys=True,separators=(',',':'),ensure_ascii=False)
    db.execute('INSERT OR REPLACE INTO order_lifecycle VALUES (?,?,?,?,?,?,?)',
        (scope,identifier,generation,'closed' if closed else 'open',closed_at if closed else None,body,_digest(proof)))


def record_confirmation(event):
    """Transfer a committed broker reconciliation into an independent proof."""
    import execution_trace as E
    import record_store as R
    if event.get('recording_status')!='recorded':return {'status':'unconfirmed'}
    identifier=E._id(event['order_attempt_id']);scope=E._id(event['account_scope'])
    run=E._id(event.get('run_attempt_id') or 'unscoped');eid=E._id(event['event_id'])
    stored=R.load(E._root()/'executions'/run/'execution_status'/(eid+'.json'))
    if stored.get('kind')!='execution_status' or stored.get('order_attempt_id')!=identifier or stored.get('account_scope')!=scope:
        raise ValueError('ORDER_RECONCILIATION_BINDING_MISMATCH')
    summary=stored['summary'];source_hash=_digest(stored);db=E._connect()
    try:
        db.execute('BEGIN IMMEDIATE');initialize(db)
        row=db.execute('SELECT * FROM order_attempts WHERE account_scope=? AND order_attempt_id=?',(scope,identifier)).fetchone()
        if not row or row['state'] not in {'claimed','complete'}:raise ValueError('CONSUMED_ORDER_REQUIRED')
        payload=json.loads(row['payload_json'])
        closed=(summary.get('confirmation_status')=='confirmed_full' and summary.get('query_complete') is True
            and summary.get('confirmed_qty')==payload['qty'] and summary.get('requested_qty')==payload['qty']
            and not summary.get('conflicts') and not summary.get('ignored_rows'))
        proof={'basis':'committed_broker_reconciliation','source_event_id':eid,'source_hash':source_hash,
            'payload_hash':row['payload_hash'],'confirmed_qty':summary.get('confirmed_qty'),
            'confirmed_average_price':summary.get('confirmed_average_price'),'query_complete':summary.get('query_complete'),
            'confirmation_status':summary.get('confirmation_status'),'order_no':summary.get('order_no')}
        _proof(db,scope,identifier,proof,closed=closed);db.commit()
        return {'status':'closed' if closed else 'unresolved','order_attempt_id':identifier}
    except BaseException:
        db.rollback();raise
    finally:db.close()


def reserve_intent(scope,request_key,run_attempt_id,*,plan_id=None,legacy_identifier=None):
    """One host-derived legacy intent per verified original run, across retries."""
    import execution_trace as E
    import judgment_coverage as J
    E._id(scope);E._id(request_key);E._id(run_attempt_id)
    source=J._manifest(run_attempt_id);origin=source['created_at']
    from improvement_store import instant
    origin=instant(origin).isoformat()
    if instant(origin)>C.now():raise ValueError('ORDER_INTENT_ORIGIN_IN_FUTURE')
    db=E._connect(create=True)
    try:
        db.execute('BEGIN IMMEDIATE');state=initialize(db)
        mapped=db.execute('SELECT * FROM order_intents WHERE account_scope=? AND request_key=?',(scope,request_key)).fetchone()
        if mapped and (mapped['run_attempt_id'],mapped['origin_at'])!=(run_attempt_id,origin):raise ValueError('ORDER_INTENT_CONFLICT')
        key=mapped['order_attempt_id'] if mapped else legacy_identifier
        row=db.execute('SELECT * FROM order_attempts WHERE account_scope=? AND order_attempt_id=?',(scope,key)).fetchone() if key else None
        if row:
            if row['plan_id']!=plan_id:raise ValueError('ORDER_INTENT_PLAN_CONFLICT')
        else:
            if mapped:raise ValueError('ORDER_INTENT_LEDGER_MISSING')
            if state['supported_after'] and origin<state['supported_after']:raise ValueError('ORDER_INTENT_EXPIRED')
            identifier=issue(db,scope)
            db.execute("INSERT INTO order_attempts(account_scope,order_attempt_id,plan_id,run_id,run_attempt_id,created_at,state) VALUES(?,?,?,?,?,?,'reserved')",
                (scope,identifier,plan_id,source.get('run_id'),run_attempt_id,E._now()))
            row=db.execute('SELECT * FROM order_attempts WHERE account_scope=? AND order_attempt_id=?',(scope,identifier)).fetchone()
        if not mapped:
            db.execute('INSERT INTO order_intents VALUES (?,?,?,?,?)',(scope,request_key,run_attempt_id,origin,row['order_attempt_id']))
        db.commit();return {**E._decode(row),'recording_status':'recorded'}
    except BaseException:
        db.rollback();raise
    finally:db.close()


def _position_protected(db,row):
    """Keep a confirmed buy while any current holding or unknown account exists."""
    import execution_trace as E
    if row['disposition']=='expired_unsent':return False
    proof=json.loads(row['proof']) if row['proof'] else {}
    if _digest(proof)!=row['proof_hash']:raise ValueError('ORDER_TERMINAL_PROOF_CORRUPT')
    payload=json.loads(row['payload_json'])
    if E.intent_fingerprint(payload)!=row['payload_hash']:raise ValueError('ORDER_PAYLOAD_HASH_MISMATCH')
    if proof.get('basis') in {'explicit_broker_rejected','explicit_broker_simulated'}:
        if _digest(json.loads(row['result_json']))!=proof['result_hash']:raise ValueError('ORDER_RESULT_HASH_MISMATCH')
        return False
    if proof.get('payload_hash')!=row['payload_hash']:raise ValueError('ORDER_PROOF_PAYLOAD_MISMATCH')
    from pathlib import Path
    path=Path(C.WORK)/'ontology'/'position_episodes.sqlite3'
    if not path.exists():return True
    import sqlite3
    with sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True) as positions:
        if not positions.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='scope_checkpoints'").fetchone():return True
        state=positions.execute('SELECT complete_at FROM scope_checkpoints WHERE scope=?',(row['account_scope'],)).fetchone()
        if not state or state[0]<row['closed_at']:return True
        return bool(positions.execute('SELECT 1 FROM episodes WHERE scope=? AND code=? AND closed IS NULL',
            (row['account_scope'],payload['code'])).fetchone())


def collect(calendar,*,dry_run=True,limit=200,at=None):
    """Rotate support and retire only independently closed requests in a batch.

    This never deletes the ledger file or its identity marker. Unknown claims
    remain even across generations. Expired unknown IDs can never be reserved.
    """
    import execution_trace as E
    import retention_policy as P
    import improvement_store as I
    boundary=P.boundaries(calendar,at)
    if not boundary['deletion_allowed']:return boundary
    if type(limit) is not int or not 1<=limit<=200:raise ValueError('ORDER_GC_BATCH_LIMIT')
    cutoff=boundary['detailed_origin_cutoff']+'T00:00:00+09:00'
    if not E._db_path().exists():
        if E._db_path().with_suffix('.identity.json').exists():raise ValueError('ORDER_LEDGER_MISSING')
        return {'state':'empty','retired_count':0}
    # Registration outside Q3 is already forbidden; a new pin cannot acquire
    # these old requests between this read and the ledger transaction.
    with I.transaction(readonly=True) as research:
        pins=[] if research is None else [I.decode(r) for r in research.execute("SELECT body,hash FROM records WHERE kind='retention_pin' AND state='active' AND json_extract(body,'$.until')>?",(at or I.now(),))]
    db=E._connect(readonly=dry_run)
    try:
        db.execute('BEGIN' if dry_run else 'BEGIN IMMEDIATE')
        state=contract(db) if dry_run else initialize(db)
        if not state:return {'state':'migration_pending','retired_count':0}
        if state['supported_after'] and cutoff<state['supported_after']:raise ValueError('ORDER_SUPPORT_BOUNDARY_CANNOT_REVERSE')
        cursor=db.execute('SELECT last_key FROM order_gc_progress WHERE singleton=1').fetchone()
        after=json.loads(cursor[0]) if cursor and cutoff==state['supported_after'] else ['','','']
        rows=db.execute('''SELECT a.*,l.disposition,l.closed_at,l.proof,l.proof_hash,coalesce(l.generation,0) AS generation
            FROM order_attempts a LEFT JOIN order_lifecycle l USING(account_scope,order_attempt_id)
            WHERE a.created_at<? AND (l.disposition='closed' OR a.state='reserved')
            AND (a.created_at,a.account_scope,a.order_attempt_id)>(?,?,?)
            ORDER BY a.created_at,a.account_scope,a.order_attempt_id LIMIT ?''',(cutoff,*after,limit)).fetchall()
        candidates=[];protected=0
        for raw in rows:
            row=dict(raw)
            payload=json.loads(row['payload_json']) if row['payload_json'] else {}
            if any(p.get('period',{}).get('start','9999')<=row['created_at']<=p.get('period',{}).get('end','0000')
                and ('*' in p.get('scope',{}).get('codes',[]) or payload.get('code') in p.get('scope',{}).get('codes',[])) for p in pins):
                protected+=1;continue
            if row['state']=='reserved':row['disposition']='expired_unsent'
            elif not row['closed_at'] or row['closed_at']>=cutoff or _position_protected(db,row):
                protected+=1;continue
            candidates.append(row)
        total_bytes=sum(len((r['payload_json'] or '').encode())+len((r['result_json'] or '').encode()) for r in candidates)
        if not dry_run:
            generation=state['generation']+(cutoff!=state['supported_after'])
            history=state['history_hash']
            for row in candidates:
                key=(row['account_scope'],row['order_attempt_id'])
                history=_digest([history,key,row['payload_hash'],row['proof_hash'],row['disposition']])
                db.execute('DELETE FROM order_intents WHERE account_scope=? AND order_attempt_id=?',key)
                db.execute('DELETE FROM order_lifecycle WHERE account_scope=? AND order_attempt_id=?',key)
                db.execute('DELETE FROM order_attempts WHERE account_scope=? AND order_attempt_id=?',key)
            db.execute('UPDATE order_replay_contract SET generation=?,supported_after=?,retired_count=retired_count+?,retired_bytes=retired_bytes+?,history_hash=? WHERE singleton=1',
                (generation,cutoff,len(candidates),total_bytes,history))
            last=[rows[-1][key] for key in ('created_at','account_scope','order_attempt_id')] if len(rows)==limit else ['','','']
            db.execute('INSERT OR REPLACE INTO order_gc_progress VALUES (1,?)',(json.dumps(last),))
            db.commit()
        return {'state':'dry_run' if dry_run else 'retired','contract':VERSION,'cutoff':cutoff,
                'retired_count':len(candidates),'logical_body_bytes':total_bytes,'protected_count':protected,
                'examined_count':len(rows),'selection':'bounded_batch','more':len(rows)==limit,
                'unconfirmed_orders':'retained','generation':state['generation'] if dry_run else generation,
                'permanent_id_tombstones':False,'ledger_file_deleted':False}
    except BaseException:
        db.rollback();raise
    finally:db.close()
