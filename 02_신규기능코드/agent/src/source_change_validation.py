"""Isolated source patch rehearsal. Never copies credentials or runtime stores.

The OS sandbox is mandatory for execution. Unsupported hosts retain a reviewable
patch and explicitly block automated source validation/application.
"""
from __future__ import annotations
import ast
import os
import shutil
import subprocess
import sys
import tempfile
import re
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path

import common as C
import improvement_store as S

_LOCKED=ContextVar('source_workspace_locks',default=frozenset())


@contextmanager
def locked(change_id):
    import case_store
    if change_id in _LOCKED.get():
        yield
        return
    with case_store.locked('source-workspace-'+change_id,timeout=0):
        token=_LOCKED.set(_LOCKED.get()|{change_id})
        try:yield
        finally:_LOCKED.reset(token)


def _files():
    """A finite source allowlist; symlinks cannot import credentials or work."""
    base=Path(C.AGENT);result=[]
    for name in ('src','tests','config','.claude/agents'):
        source=base/name
        if source.is_symlink():raise ValueError('SOURCE_SYMLINK_FORBIDDEN')
        if not source.exists():continue
        for folder,dirs,files in os.walk(source,followlinks=False):
            dirs[:]=[d for d in dirs if d not in {'__pycache__','work','.git'}]
            for part in dirs+files:
                if (Path(folder)/part).is_symlink():raise ValueError('SOURCE_SYMLINK_FORBIDDEN')
            result.extend(Path(folder)/f for f in files if not f.endswith(('.pyc','.sqlite3','.pem','.key')) and not f.startswith('.env'))
    result.extend(base/f for f in ('CLAUDE.md','run.py'))
    if any(f.is_symlink() for f in result):raise ValueError('SOURCE_SYMLINK_FORBIDDEN')
    return result


def prepare(change_id):
    with locked(change_id):return _prepare(change_id)


def _prepare(change_id):
    import research_capacity as K
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,200}',change_id):raise ValueError('INVALID_WORKSPACE_OWNER')
    change=S.get('change',change_id)
    if not change or change['adapter']!='source_patch': raise ValueError('SOURCE_CHANGE_REQUIRED')
    if change['state'] in {'expired','cancelled'}:raise ValueError('SOURCE_CHANGE_EXPIRED')
    target=Path(change['target_path'])
    if target.is_absolute() or '..' in target.parts: raise ValueError('SOURCE_PATH_FORBIDDEN')
    if target.suffix=='.py': ast.parse(change['patch']['content'])
    root=C.ontology_root()/'improvement'/'workspaces'/change_id
    staging=root.with_name('.prepare-'+change_id)
    manifest=S.get('patch_workspace',change_id)
    if root.is_symlink() or staging.is_symlink() or root.parent.is_symlink():raise ValueError('WORKSPACE_SYMLINK_FORBIDDEN')
    if root.exists():
        if not manifest or manifest['patch_hash']!=change['patch_hash']: raise ValueError('PATCH_WORKSPACE_CONFLICT')
        for name,hashed in manifest.get('source_hashes',{}).items():
            path=root/name
            value=path.read_bytes().hex() if path.is_file() and manifest.get('hash_encoding')=='bytes_hex' else path.read_text() if path.is_file() else None
            if not path.is_file() or path.is_symlink() or S.digest(value)!=hashed:raise ValueError('PATCH_WORKSPACE_CHANGED')
        if manifest['state']=='prepared':return manifest
        # A crash after rename is completed from the durable file manifest.
        if not manifest.get('source_hashes'):raise ValueError('PATCH_WORKSPACE_UNVERIFIED')
        with S.transaction() as db:manifest=S.put(db,'patch_workspace',change_id,{**manifest,'state':'prepared'})
        K.finish('workspace:'+change_id)
        return manifest
    files=_files()
    # Text/binary source bytes plus the baseline copy and finite patch expansion.
    minimum=2*(sum(f.stat().st_size for f in files)+len(change['patch']['content'].encode()))
    K.reserve('workspace:'+change_id,change_id,minimum_bytes=minimum)
    if manifest and manifest['patch_hash']!=change['patch_hash']:raise ValueError('PATCH_WORKSPACE_CONFLICT')
    if staging.exists():
        if not manifest or manifest.get('state')!='preparing':raise ValueError('PATCH_STAGING_UNOWNED')
        shutil.rmtree(staging)
    body={'owner':change_id,'state':'preparing','workspace':str(root),'patch_hash':change['patch_hash'],
          'target_path':str(target),'last_progress_at':S.now()}
    with S.transaction() as db:
        S.put(db,'patch_workspace',change_id,body)
        S.link(db,'change',change_id,'patch_workspace',change_id)
    staging.mkdir(parents=True)
    for source in files:
        dest=staging/source.relative_to(C.AGENT);dest.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(source,dest)
    target_path=staging/target
    if not target_path.is_file():raise ValueError('PATCH_TARGET_MISSING')
    if S.digest(target_path.read_text())!=change['patch']['before_hash']:raise ValueError('PATCH_BASE_CHANGED')
    target_path.write_text(change['patch']['content'])
    # Binary files are included in the digest, without locale-dependent decoding.
    body['source_hashes']={str(f.relative_to(C.AGENT)):S.digest((staging/f.relative_to(C.AGENT)).read_bytes().hex()) for f in files}
    body['hash_encoding']='bytes_hex'
    with S.transaction() as db:S.put(db,'patch_workspace',change_id,body)
    staging.rename(root)
    body['state']='prepared'
    with S.transaction() as db:
        row=S.put(db,'patch_workspace',change_id,body)
        S.link(db,'change',change_id,'patch_workspace',change_id)
    K.finish('workspace:'+change_id)
    return row


def run(change_id, *, timeout_seconds=1800):
    from source_validation_runner import run as validate
    return validate(change_id,timeout_seconds=timeout_seconds)
