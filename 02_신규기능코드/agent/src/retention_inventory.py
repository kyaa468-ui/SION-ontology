"""Explicit ownership inventory; audit metadata only, never enroll copied stores."""
from pathlib import Path
import common as C

# The IDs are stable references into plan 4. A migration is a consumer-specific
# proof, not a boolean permission to delete an entire directory.
ENTRIES = (
 ('R01','runtime_inputs,data_prep,intraday','ai_judgment,semantic_layer','inputs','current_input'),
 ('R02','ontology','ai_judgment,case_dataset','selection','active_decision_or_detail'),
 ('R03','ontology','ai_judgment,case_dataset,policy_validation','deliveries','active_decision_or_detail'),
 ('R04','evidence_wire','ai_judgment,semantic_layer,prompt_validation','wire','active_scope_or_detail'),
 ('R05','judgment_coverage,ai_judgment,runtime_policy_inputs,scoring_targets','opportunity_cases,execution_trace,policy_registry,validation_dataset,scoring_targets','judgments','pending_order_or_assessment'),
 ('R06','case_prediction [namespace closed after explicit migration],model_retention','case_prediction [scoped historical only],model_retention','predictions','finished_assessment_and_registered_detail'),
 ('R07','audit_inputs,execution_trace,risk,sizing','execution_reconcile,simulation','executions','pending_order_or_detail'),
 ('R08','execution_reconcile,position_episodes','broker,monitor','execution','order_safety_ledger'),
 ('R09','monitor','portfolio_capture,simulation','monitor','detail_or_registered_validation'),
 ('R10','portfolio_capture','policy_validation,simulation','portfolio','registered_validation'),
 ('R11','close_evaluation,premise_evaluation,scoring_targets,legacy_assessments','independent_cases,case_review,scoring_targets [live target list after explicit migration]','evaluations','pending_assessment_or_correction'),
 ('R12','daily_observations,market_calendar','assessment_contracts,close_evaluation','observations','pending_assessment_or_correction'),
 ('R13','independent_cases,opportunity_cases','case_dataset,investigation','improvement','case_completion_20_sessions'),
 ('R14','case_review','learning_proposals','case_review','result_commit_or_active_change'),
 ('R15','case_statistics [scheduled producer retired],model_retention','case_prediction,trace_query,model_retention','models','ended_publication_and_finished_predictions'),
 ('R16','learning_proposals,change_registry','policy_validation,change_application','proposals','active_change'),
 ('R17','validation_spec,validation_dataset','policy_validation,prompt_validation,policy_proof','validation','registered_validation'),
 ('R18','policy_validation,prompt_validation,semantic_experiments','policy_adoption,change_application,policy_proof','validation','registered_validation_or_active_proof'),
 ('R19','policy_registry,policy_adoption,policy_proof','runtime_inputs,change_observation,policy_registry','policies','active_policy_or_rollback'),
 ('R20','research_evidence,validation_holdout,research_usage_ledger,usage_checkpoint','investigation,validation_dataset,research_usage_ledger,validation_holdout','research','monotone_exposure_ledger'),
 ('R21','runtime_inputs,process_spool','prompt_validation,semantic_runner','runtime_inputs','detail_or_unsettled_cost'),
 ('R22','trace_query,trace_snapshot,trace_view','trace_pages,research_pages','views','active_view'),
 ('R23','trace_export,logger','public_review','exports','current_export'),
 ('R24','common,log_store,logger,tuning','risk,sizing,monitor','journal','order_safety_checkpoint'),
 ('R25','record_store,storage_index','record_store','locks','live_reader_writer'),
 ('R26','snapshot_store,storage_objects,record_store','all_record_consumers','blobs','required_content_closure'),
 ('R27','semantic_layer,semantic_runner','investigation,semantic_dependencies','semantic','active_decision_or_detail'),
 ('R28','opportunity_cases,improvement_issues','investigation','improvement','case_completion_20_sessions'),
 ('R29','investigation,investigation_checks','change_registry','improvement','research_30_days_or_registered_end'),
 ('R30','change_registry,source_change_validation,change_application','change_observation','improvement','active_change_or_rollback'),
 ('R31','semantic_dependencies,change_observation','improvement_worker','improvement','registered_observation_end'),
 ('R32','research_budget,research_calls','improvement_worker','improvement','unsettled_cost_or_day_summary'),
 ('R33','change_registry,change_application [host configuration]','change_application,improvement_validation,change_observation','improvement','active_authority_or_registered_criteria'),
 ('R34','assessment_contracts,assessment_observations','opportunity_cases,investigation','improvement','assessment_end_plus_5_sessions'),
)


def audit():
    root=C.ontology_root().resolve()
    # Directory sizes are an explicit audit, not the ordinary case read path.
    sizes={}; files={}; symlinks=[]
    if root.exists():
        import os
        for folder, dirs, names in os.walk(root, followlinks=False):
            dirs[:]=[d for d in dirs if not (Path(folder)/d).is_symlink()]
            for name in names:
                path=Path(folder)/name
                if path.is_symlink(): symlinks.append(str(path.relative_to(root))); continue
                try: size=path.stat().st_size
                except FileNotFoundError: continue
                part=path.relative_to(root).parts[0]
                sizes[part]=sizes.get(part,0)+size;files[part]=files.get(part,0)+1
    return {'managed_root':str(root),'development_copies_enrolled':False,
            'physical_bytes':sum(sizes.values()),'file_count':sum(files.values()),
            'directories':{k:{'bytes':v,'files':files[k]} for k,v in sorted(sizes.items())},
            'symlinks_excluded':symlinks,
            'inventory':[{'id':r,'writers':w.split(','),'readers':c.split(','),'region_hint':p,
                         'root_contract':end,'delete_authority':'improvement_gc_only' if p=='improvement'
                         else 'protected_until_consumer_migration_certificate'} for r,w,c,p,end in ENTRIES]}
