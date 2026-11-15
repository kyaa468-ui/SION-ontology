"""Recoverable physical reclamation of expired, program-owned patch copies."""
from pathlib import Path
import os
import re
import shutil
import common as C
import improvement_store as S


def root():return C.ontology_root()/'improvement'/'workspaces'


def enqueue(db,workspace,*,cutoff):
    key=workspace['id']
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,200}',key):raise ValueError('INVALID_WORKSPACE_OWNER')
    expected=root()/key
    if Path(workspace['workspace']).absolute()!=expected.absolute():raise ValueError('WORKSPACE_OWNERSHIP_MISMATCH')
    old=S.get('workspace_reclamation',key,conn=db)
    if old:return old
    owner=S.get('change',key,conn=db)
    if not owner or owner['state'] not in {'completed','expired','cancelled','failed','rolled_back','resolved'} or not owner.get('closed_at') or S.instant(owner['closed_at']).date().isoformat()>=cutoff:
        raise ValueError('WORKSPACE_OWNER_NOT_EXPIRED')
    return S.put(db,'workspace_reclamation',key,{'state':'pending','owner':key,
        'owner_closed_at':owner['closed_at'],'workspace_hash':S.digest(workspace),
        'workspace_state':workspace.get('state'),'names':[key,key+'-baseline','.prepare-'+key],'phases':{},'reclaimed_bytes':0})


def _safe(path):
    boundary=root().absolute();path=path.absolute()
    if path.parent!=boundary:raise ValueError('WORKSPACE_OUTSIDE_OWNED_ROOT')
    current=path
    while current!=C.ontology_root().absolute().parent:
        if current.is_symlink():raise ValueError('WORKSPACE_SYMLINK_FORBIDDEN')
        current=current.parent
    return path


def _bytes(path):
    total=0
    for folder,dirs,files in os.walk(path,followlinks=False):
        dirs[:]=[d for d in dirs if not (Path(folder)/d).is_symlink()]
        for name in files:
            item=Path(folder)/name
            if not item.is_symlink():total+=item.stat().st_size
    return total


def resume(*,limit=20):
    import case_store
    import source_change_validation as W
    if not isinstance(limit,int) or not 1<=limit<=200:raise ValueError('RECLAMATION_BATCH_LIMIT')
    results=[]
    with S.transaction() as db:
        cursor=db.execute("SELECT value FROM meta WHERE key='workspace_reclamation_cursor'").fetchone()
        rows=S.page('workspace_reclamation',state='pending',after=cursor[0] if cursor else '',limit=limit,conn=db)
        if not rows:rows=S.page('workspace_reclamation',state='pending',limit=limit,conn=db)
        if rows:db.execute('INSERT OR REPLACE INTO meta VALUES (?,?)',('workspace_reclamation_cursor',rows[-1]['id']))
    # A persistently damaged first workspace cannot starve later owned copies.
    for row in rows:
        try:
            with W.locked(row['id']):
                row=S.get('workspace_reclamation',row['id'])
                if row['state']!='pending':continue
                owner=S.get('change',row['id'])
                if owner and owner['state'] not in {'completed','expired','cancelled','failed','rolled_back','resolved'}:
                    raise ValueError('WORKSPACE_OWNER_ACTIVE')
                for name in row['names']:
                    source=_safe(root()/name);trash=_safe(root()/('.reclaim-'+name))
                    phase=row['phases'].get(name)
                    if phase=='reclaimed':continue
                    if phase is None:
                        if source.exists():
                            if trash.exists():raise ValueError('WORKSPACE_QUARANTINE_CONFLICT')
                            size=_bytes(source);source.rename(trash)
                        elif trash.exists():size=_bytes(trash)
                        elif name.endswith('-baseline') or name.startswith('.prepare-') or row.get('workspace_state')=='preparing':
                            size=0
                        else:raise ValueError('WORKSPACE_SOURCE_UNAVAILABLE')
                        with S.transaction() as db:
                            row=S.put(db,'workspace_reclamation',row['id'],{**row,
                                'phases':{**row['phases'],name:'quarantined'},
                                'reclaimed_bytes':row['reclaimed_bytes']+size})
                    if trash.exists():shutil.rmtree(trash)
                    with S.transaction() as db:
                        row=S.put(db,'workspace_reclamation',row['id'],{**row,'phases':{**row['phases'],name:'reclaimed'}})
                with S.transaction() as db:row=S.put(db,'workspace_reclamation',row['id'],{**row,'state':'completed','closed_at':S.now()})
                import research_capacity as K
                K.finish('workspace:'+row['id'])
                results.append(row)
        except (OSError,ValueError,case_store.StoreBusy) as exc:
            results.append({'id':row['id'],'state':'reclamation_pending','reason':str(exc)})
    return results
