"""Transactional, bounded records for investigation work (never an order store).

SQLite commits the body, revision, work cursor and dependency edges together.
Read APIs do not create or repair storage. Every body is hash checked on read.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from contextvars import ContextVar
import uuid
from datetime import datetime
from pathlib import Path

import common as C

VERSION = 'IMPROVEMENT@v1'
_TRANSACTION = ContextVar('improvement_transaction',default=None)
SCHEMA = '''
CREATE TABLE IF NOT EXISTS records (
 kind TEXT NOT NULL, id TEXT NOT NULL, revision INTEGER NOT NULL,
 state TEXT NOT NULL, owner TEXT NOT NULL, origin_at TEXT NOT NULL,
 updated_at TEXT NOT NULL, closed_at TEXT, body TEXT NOT NULL, hash TEXT NOT NULL,
 PRIMARY KEY(kind,id));
CREATE INDEX IF NOT EXISTS records_queue ON records(kind,state,updated_at,id);
CREATE INDEX IF NOT EXISTS records_owner ON records(kind,owner,id);
CREATE INDEX IF NOT EXISTS records_expiry ON records(kind,closed_at,id);
CREATE TABLE IF NOT EXISTS revisions (
 kind TEXT NOT NULL,id TEXT NOT NULL,revision INTEGER NOT NULL,
 known_at TEXT NOT NULL,body TEXT NOT NULL,hash TEXT NOT NULL,
 PRIMARY KEY(kind,id,revision));
CREATE TABLE IF NOT EXISTS links (
 owner_kind TEXT NOT NULL,owner_id TEXT NOT NULL,target_kind TEXT NOT NULL,
 target_id TEXT NOT NULL,relation TEXT NOT NULL,
 PRIMARY KEY(owner_kind,owner_id,target_kind,target_id,relation));
CREATE INDEX IF NOT EXISTS links_target ON links(target_kind,target_id);
CREATE TABLE IF NOT EXISTS calls (
 id TEXT PRIMARY KEY,day TEXT NOT NULL,owner TEXT NOT NULL,payload_hash TEXT NOT NULL,
 reserved INTEGER NOT NULL,actual INTEGER, state TEXT NOT NULL,
 provider_id TEXT UNIQUE,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS exposure (
 source TEXT NOT NULL,owner TEXT NOT NULL,known_at TEXT NOT NULL,
 PRIMARY KEY(source,owner));
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
'''


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def now():
    return C.now().astimezone(C.KST).isoformat()


def instant(value):
    dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if dt.tzinfo is None:
        raise ValueError('TIMEZONE_REQUIRED')
    return dt.astimezone(C.KST)


def path():
    return C.ontology_root() / 'improvement' / 'state.sqlite3'


@contextmanager
def transaction(*, readonly=False):
    target = path()
    active=_TRANSACTION.get()
    if active and active[0]==str(target.absolute()):
        _,connection,parent_readonly=active
        if parent_readonly and not readonly:raise ValueError('READONLY_TRANSACTION_SCOPE')
        if readonly:
            yield connection
        else:
            savepoint='nested_'+uuid.uuid4().hex
            connection.execute('SAVEPOINT '+savepoint)
            try:
                yield connection
                connection.execute('RELEASE SAVEPOINT '+savepoint)
            except BaseException:
                connection.execute('ROLLBACK TO SAVEPOINT '+savepoint)
                connection.execute('RELEASE SAVEPOINT '+savepoint)
                raise
        return
    if readonly and not target.exists():
        token=_TRANSACTION.set((str(target.absolute()),None,True))
        try:yield None
        finally:_TRANSACTION.reset(token)
        return
    if target.is_symlink() or target.parent.is_symlink():
        raise ValueError('IMPROVEMENT_SYMLINK_FORBIDDEN')
    if not readonly:
        target.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect('file:' + str(target) + '?mode=ro', uri=True, timeout=30) if readonly else sqlite3.connect(target, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        if not readonly:
            conn.execute('PRAGMA synchronous=FULL')
            conn.executescript(SCHEMA)
        conn.execute('BEGIN' if readonly else 'BEGIN IMMEDIATE')
        token=_TRANSACTION.set((str(target.absolute()),conn,readonly))
        try:yield conn
        finally:_TRANSACTION.reset(token)
        if not readonly:
            conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def decode(row):
    if row is None:
        return None
    value = json.loads(row['body'])
    if digest(value) != row['hash']:
        raise ValueError('IMPROVEMENT_RECORD_CORRUPT')
    return value


def get(kind, identifier, *, conn=None):
    if conn is None:
        with transaction(readonly=True) as db:
            return get(kind, identifier, conn=db) if db else None
    return decode(conn.execute('SELECT body,hash FROM records WHERE kind=? AND id=?', (kind, identifier)).fetchone())


def put(conn, kind, identifier, body, *, expected_revision=None, state=None, owner=None):
    old = get(kind, identifier, conn=conn)
    if expected_revision is not None and (old or {}).get('revision', 0) != expected_revision:
        raise ValueError('REVISION_CONFLICT')
    value = dict(body)
    value.update(id=identifier, kind=kind, version=VERSION,
                 revision=(old or {}).get('revision', 0) + 1,
                 origin_at=(old or {}).get('origin_at', body.get('origin_at', now())),
                 updated_at=now(), state=state or body.get('state', 'recorded'),
                 owner=owner or body.get('owner', identifier))
    # Completion is the first confirmed completion, never the last read/correction.
    if old and old.get('closed_at'):
        value['closed_at'] = old['closed_at']
    encoded, hashed = canonical(value), digest(value)
    if len(encoded.encode()) > 4 * 1024 * 1024:
        raise ValueError('RECORD_BYTES_EXCEEDED')
    conn.execute('INSERT OR REPLACE INTO records VALUES (?,?,?,?,?,?,?,?,?,?)',
                 (kind, identifier, value['revision'], value['state'], value['owner'],
                  value['origin_at'], value['updated_at'], value.get('closed_at'), encoded, hashed))
    conn.execute('INSERT INTO revisions VALUES (?,?,?,?,?,?)',
                 (kind, identifier, value['revision'], value['updated_at'], encoded, hashed))
    return value


def page(kind, *, state=None, owner=None, after='', limit=50, conn=None):
    if not isinstance(limit, int) or not 1 <= limit <= 200:
        raise ValueError('PAGE_LIMIT_1_TO_200')
    if conn is None:
        with transaction(readonly=True) as db:
            return page(kind, state=state, owner=owner, after=after, limit=limit, conn=db) if db else []
    sql, args = 'SELECT body,hash FROM records WHERE kind=? AND id>?', [kind, after]
    for key, value in (('state', state), ('owner', owner)):
        if value is not None:
            sql += ' AND ' + key + '=?'
            args.append(value)
    return [decode(row) for row in conn.execute(sql + ' ORDER BY id LIMIT ?', [*args, limit])]


def link(conn, owner_kind, owner_id, target_kind, target_id, relation='required_content'):
    if relation not in {'required_content', 'required_state', 'historical_provenance', 'reassesses', 'supersedes'}:
        raise ValueError('INVALID_REFERENCE_KIND')
    conn.execute('INSERT OR IGNORE INTO links VALUES (?,?,?,?,?)',
                 (owner_kind, owner_id, target_kind, target_id, relation))


def config():
    value = C.read_json(Path(C.CONFIG) / 'ontology' / 'improvement.json')
    if not isinstance(value, dict) or value.get('version') != VERSION:
        raise ValueError('IMPROVEMENT_CONFIGURATION_UNAVAILABLE')
    return value


def save_once(kind, identifier, body, *, conn):
    old = get(kind, identifier, conn=conn)
    request_hash = digest(body)
    if old:
        if old.get('request_hash') != request_hash:
            raise ValueError('REQUEST_CONTENT_CONFLICT')
        return old
    return put(conn, kind, identifier, {**body, 'request_hash': request_hash})
