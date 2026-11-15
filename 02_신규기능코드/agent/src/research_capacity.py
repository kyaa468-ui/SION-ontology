"""Admission for optional research; required trading records are never dropped.

The physical inventory is an explicit admission audit, not a case-reader scan.
It includes the active work directory only, never developer/rehearsal siblings.
Reservations are durable and conservative: unfinished work keeps its allowance
until its producer confirms completion. This is not a filesystem quota; required
writers can exceed the budget, which blocks the next optional admission.
"""
from pathlib import Path
import os
import improvement_store as S
import common as C


class CapacityBlocked(ValueError):
    pass


def _positive(value):return type(value) is int and value>0


def inventory():
    root=Path(C.WORK).absolute()
    if root.is_symlink():raise CapacityBlocked('CAPACITY_ROOT_SYMLINK')
    size=count=0
    if root.exists():
        def error(exc):raise exc
        for folder,dirs,files in os.walk(root,followlinks=False,onerror=error):
            for name in dirs+files:
                path=Path(folder)/name
                if path.is_symlink():raise CapacityBlocked('CAPACITY_UNMANAGED_SYMLINK')
            for name in files:
                try:size+=(Path(folder)/name).stat().st_size;count+=1
                except FileNotFoundError:pass  # A committed removal can only free capacity.
    return {'root':str(root),'physical_bytes':size,'files':count,'at':S.now(),
            'scope':'active_work_directory','kind':'physical_metadata_audit'}


def reserve(identifier,owner,*,minimum_bytes=0,at=None):
    cfg=S.config();maximum=cfg.get('storage_bytes');cases=cfg.get('case_count')
    allowance=cfg.get('research_job_bytes')
    if not all(_positive(x) for x in (maximum,cases,allowance)):
        raise CapacityBlocked('CAPACITY_CONFIGURATION_REQUIRED:storage_bytes,case_count,research_job_bytes')
    if type(minimum_bytes) is not int or minimum_bytes<0:raise ValueError('INVALID_CAPACITY_REQUEST')
    if minimum_bytes>allowance:raise CapacityBlocked('CAPACITY_JOB_BYTES_EXCEEDED')
    with S.transaction() as db:
        old=S.get('capacity_reservation',identifier,conn=db)
        if old:
            if old['owner']!=owner or old['minimum_bytes']!=minimum_bytes:raise ValueError('CAPACITY_REQUEST_CONFLICT')
            if old['state']=='reserved':return old
            if old['state']!='cancelled_before_dispatch':raise CapacityBlocked('CAPACITY_REQUEST_ALREADY_FINISHED')
        # Physical files plus all outstanding maximums deliberately overcount
        # partial writes. No guessed refund or optimistic space credit.
        audit=inventory()
        held=db.execute("SELECT coalesce(sum(json_extract(body,'$.reserved_bytes')),0) FROM records WHERE kind='capacity_reservation' AND state='reserved'").fetchone()[0]
        count=db.execute("SELECT count(*) FROM records WHERE kind='opportunity'").fetchone()[0]
        if count>cases:raise CapacityBlocked('CAPACITY_CASE_COUNT_EXCEEDED')
        if audit['physical_bytes']+held+allowance>maximum:raise CapacityBlocked('CAPACITY_STORAGE_BYTES_EXCEEDED')
        return S.put(db,'capacity_reservation',identifier,{'owner':owner,'state':'reserved',
            'reserved_bytes':allowance,'minimum_bytes':minimum_bytes,'inventory':audit,
            'global_bytes_limit':maximum,'case_count_limit':cases,'case_count':count,
            'last_progress_at':at or S.now()})


def finish(identifier,*,outcome='completed'):
    if outcome not in {'completed','cancelled_before_dispatch'}:raise ValueError('CAPACITY_COMPLETION_REQUIRED')
    with S.transaction() as db:
        row=S.get('capacity_reservation',identifier,conn=db)
        if not row:return None
        if row['state']!='reserved':return row
        audit=inventory()
        growth=max(0,audit['physical_bytes']-row['inventory']['physical_bytes'])
        return S.put(db,'capacity_reservation',identifier,{**row,'state':outcome,'closed_at':S.now(),
            'final_inventory':audit,'physical_growth_bytes':growth,
            'capacity_exceeded':growth>row['reserved_bytes'] or audit['physical_bytes']>row['global_bytes_limit']})


def status():
    cfg=S.config()
    with S.transaction(readonly=True) as db:
        held=0 if db is None else db.execute("SELECT coalesce(sum(json_extract(body,'$.reserved_bytes')),0) FROM records WHERE kind='capacity_reservation' AND state='reserved'").fetchone()[0]
        count=0 if db is None else db.execute("SELECT count(*) FROM records WHERE kind='opportunity'").fetchone()[0]
    return {'state':'configured' if all(_positive(cfg.get(k)) for k in ('storage_bytes','case_count','research_job_bytes')) else 'configuration_pending',
            'storage_bytes':cfg.get('storage_bytes'),'case_count_limit':cfg.get('case_count'),
            'research_job_bytes':cfg.get('research_job_bytes'),'reserved_bytes':held,'case_count':count,
            'scope':'optional_research_admission','required_record_writes':'preserved',
            'physical_inventory_on_admission':True,'hard_filesystem_quota':False}
