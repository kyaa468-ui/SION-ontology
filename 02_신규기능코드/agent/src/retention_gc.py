"""Transactional mark/sweep of migrated improvement records.

Required-content reachability, not arbitrary ID strings, controls retention.
Legacy stores stay protected until a consumer-specific migration certificate is
available. This collector never traverses development/rehearsal copies.
"""
from __future__ import annotations
import sqlite3
import improvement_store as S
import retention_policy as P

TERMINAL=('completed','finalized','finalized_unknown','resolved','expired','cancelled','failed',
          'improved','insufficient_data','rolled_back','delivered','evaluation_unsupported','capacity_blocked','cancelled_before_dispatch')
ROOT_KINDS=('opportunity','issue','investigation','change','application','observation_job','assessment','experiment',
            'tuning_request','application_hold','legacy_assessment_state','premise_case','reassessment','semantic_dependency_owner','notification','workspace_reclamation','capacity_reservation','observation_delivery','judgment_policy_inputs','scoring_target')
SWEEP_KINDS=ROOT_KINDS+('evidence','occurrence','query','check','diagnosis','query_cursor','extension_request',
                       'observation','observation_event','change_consumption','experiment_result','semantic_trial','notification','producer_cursor',
                       'semantic_dependency','change_target','source_validation','semantic_validation','patch_workspace','case_cursor',
                       'reassessment_inputs','reassessment_trial','retention_pin')


def pin(owner, targets, *, until, reason, operations=()):
    if S.instant(until)<=S.instant(S.now()): raise ValueError('FINITE_FUTURE_PIN_REQUIRED')
    if len(targets)>200: raise ValueError('PIN_TARGET_LIMIT')
    with S.transaction() as db:
        row=S.put(db,'retention_pin',owner,{'owner':owner,'state':'active','until':until,'reason':reason,'operations':list(operations)})
        for kind,identifier in targets:
            if not S.get(kind,identifier,conn=db): raise ValueError('PIN_TARGET_UNAVAILABLE')
            S.link(db,'retention_pin',owner,kind,identifier)
        return row


def collect(calendar, *, at=None, dry_run=True, limit=200):
    boundary=P.boundaries(calendar,at)
    if not boundary['deletion_allowed']: return boundary
    if not isinstance(limit,int) or not 1<=limit<=1000: raise ValueError('GC_BATCH_LIMIT')
    now=at or S.now();cutoff=boundary['case_closed_cutoff']
    with S.transaction(readonly=dry_run) as db:
        if db is None:return {'state':'empty','boundary':boundary,'candidate_count':0,'candidate_bytes':0,'protected_count':0,'protected_bytes':0}
        # All writers/readers of this store share this transaction contract.
        # Roots and edges are snapshotted after the writer lock, including pins.
        db.execute('CREATE TEMP TABLE marked (kind TEXT,id TEXT,PRIMARY KEY(kind,id))')
        roots=','.join('?' for _ in ROOT_KINDS); terminal=','.join('?' for _ in TERMINAL)
        db.execute(f"INSERT OR IGNORE INTO marked SELECT kind,id FROM records WHERE kind IN ({roots}) AND (state NOT IN ({terminal}) OR closed_at IS NULL OR substr(closed_at,1,10)>=?)",(*ROOT_KINDS,*TERMINAL,cutoff))
        db.execute("INSERT OR IGNORE INTO marked SELECT kind,id FROM records WHERE kind='retention_pin' AND state='active' AND json_extract(body,'$.until')>?",(now,))
        # A registered range also owns future records: it is not limited to
        # IDs which happened to exist when the validation was registered.
        db.execute("""INSERT OR IGNORE INTO marked SELECT r.kind,r.id FROM records r
            JOIN records p ON p.kind='retention_pin' AND p.state='active'
            WHERE json_extract(p.body,'$.until')>?
            AND r.kind IN ('opportunity','premise_case','semantic_dependency_owner','judgment_policy_inputs','scoring_target')
            AND julianday(r.origin_at)>=julianday(json_extract(p.body,'$.period.start'))
            AND julianday(r.origin_at)<=julianday(json_extract(p.body,'$.period.end'))
            AND (json_type(p.body,'$.scope.codes') IS NULL
                OR EXISTS (SELECT 1 FROM json_each(p.body,'$.scope.codes') c WHERE c.value='*'
                    OR c.value=coalesce(json_extract(r.body,'$.code'),json_extract(r.body,'$.context.code'),json_extract(r.body,'$.context.application_snapshot.code')))
                OR coalesce(json_extract(r.body,'$.code'),json_extract(r.body,'$.context.code'),json_extract(r.body,'$.context.application_snapshot.code')) IS NULL)""",(now,))
        # Active policy/application evidence remains a root even after research ends.
        db.execute("INSERT OR IGNORE INTO marked SELECT kind,id FROM records WHERE kind='application' AND state IN ('intent','applied')")
        db.execute("""WITH RECURSIVE reach(kind,id) AS (
            SELECT kind,id FROM marked UNION
            SELECT l.target_kind,l.target_id FROM links l JOIN reach r
            ON l.owner_kind=r.kind AND l.owner_id=r.id
            WHERE l.relation IN ('required_content','required_state'))
            INSERT OR IGNORE INTO marked SELECT kind,id FROM reach""")
        kinds=','.join('?' for _ in SWEEP_KINDS)
        candidates=db.execute(f"SELECT r.kind,r.id,length(CAST(r.body AS BLOB)) AS bytes FROM records r LEFT JOIN marked m ON r.kind=m.kind AND r.id=m.id WHERE m.id IS NULL AND r.kind IN ({kinds}) ORDER BY r.origin_at,r.id LIMIT ?",(*SWEEP_KINDS,limit)).fetchall()
        protected=db.execute('SELECT count(*),coalesce(sum(length(CAST(r.body AS BLOB))),0) FROM records r JOIN marked m ON r.kind=m.kind AND r.id=m.id').fetchone()
        # Integrity failures abort the whole sweep, including inaccessible children.
        missing=db.execute("SELECT l.target_kind,l.target_id FROM links l JOIN marked m ON l.owner_kind=m.kind AND l.owner_id=m.id LEFT JOIN records r ON r.kind=l.target_kind AND r.id=l.target_id WHERE l.relation IN ('required_content','required_state') AND r.id IS NULL LIMIT 1").fetchone()
        if missing: raise ValueError('REQUIRED_CONTENT_UNAVAILABLE_GC_BLOCKED')
        for raw in db.execute('SELECT r.body,r.hash FROM records r JOIN marked m ON r.kind=m.kind AND r.id=m.id'):
            S.decode(raw)
        before=S.path().stat().st_size
        if not dry_run:
            import scoring_targets
            scoring_targets.retire(db,candidates,now)
            retired_model_lifecycles=[S.get(r['kind'],r['id'],conn=db) for r in candidates
                if r['kind'] in ('premise_case','legacy_assessment_state')]
            import exposure_checkpoint
            exposure_retirement=exposure_checkpoint.retire(db,[S.get(r['kind'],r['id'],conn=db) for r in candidates if r['kind']=='opportunity'])
            exposure_retirement['duplicate_rows_collapsed']=exposure_checkpoint.collapse_duplicates(db)
            # Logical expiration and body removal commit atomically. A global
            # support lower bound replaces one permanent tombstone per old ID.
            db.execute('INSERT OR REPLACE INTO meta VALUES (?,?)',('support_lower_bound',cutoff))
            # Queue physical copies before deleting their ownership proof. A
            # small GC batch may remove the change before its workspace row.
            import workspace_reclamation
            for row in candidates:
                if row['kind'] in {'change','patch_workspace'}:
                    workspace=S.get('patch_workspace',row['id'],conn=db)
                    if workspace:workspace_reclamation.enqueue(db,workspace,cutoff=cutoff)
            for row in candidates:
                key=(row['kind'],row['id'])
                db.execute('DELETE FROM revisions WHERE kind=? AND id=?',key)
                db.execute('DELETE FROM links WHERE owner_kind=? AND owner_id=?',key)
                db.execute('DELETE FROM records WHERE kind=? AND id=?',key)
            import model_retention
            model_retention.capture_retired_lifecycles(db,retired_model_lifecycles)
            # These are monotone safety checkpoints or producer positions, not
            # historical evidence. Keep the latest value, not every heartbeat.
            db.execute("DELETE FROM revisions WHERE kind IN ('worker','application_quota','producer_cursor','research_exposure','usage_alias','usage_ledger_health') AND revision < (SELECT r.revision FROM records r WHERE r.kind=revisions.kind AND r.id=revisions.id)")
        import usage_checkpoint
        usage_retirement=usage_checkpoint.retire(db,boundary['detailed_origin_cutoff'],at=now,dry_run=dry_run,limit=limit)
        budget=S.config().get('storage_bytes')
        output={'state':'dry_run' if dry_run else 'expired','boundary':boundary,'candidate_count':len(candidates),
                'candidate_bytes':sum(r['bytes'] for r in candidates),'protected_count':protected[0],
                'protected_bytes':protected[1],'database_bytes_before':before,
                'capacity_status':'unconfigured' if budget is None else 'capacity_blocked' if protected[1]>budget else 'within_budget',
                'legacy_stores':'protected_pending_consumer_migration_certificate',
                'candidates':[{'kind':r['kind'],'id':r['id']} for r in candidates]}
        output['research_usage']=usage_retirement
        if not dry_run:output['opportunity_exposure']=exposure_retirement
    if not dry_run:
        import workspace_reclamation
        output['workspace_reclamation']=workspace_reclamation.resume(limit=min(limit,200))
        db=sqlite3.connect(S.path(),timeout=30)
        try: db.execute('VACUUM')
        finally: db.close()
        output.update(state='reclaimed',database_bytes_after=S.path().stat().st_size)
    return output
