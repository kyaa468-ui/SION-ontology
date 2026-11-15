"""Small adapter used by every existing research CLI provider."""
import improvement_store as S
import research_budget as B
import research_capacity as K


def claim(call_id,owner,maximum,payload):
    """Capacity and money are claimed together; losers cannot release a winner."""
    with S.transaction():
        reservation=B.reserve(call_id,owner,maximum,payload)
        if not (reservation['dispatch_allowed'] or reservation.get('resume_allowed')):
            if reservation['state'] in {'budget_blocked','cost_unresolved','research_support_expired','call_retired_or_checkpoint_collision'}:
                K.finish('call:'+call_id,outcome='cancelled_before_dispatch')
            return reservation
        K.reserve('call:'+call_id,owner)
        try:
            sent=B.mark_sent(call_id)
        except B.DispatchBlocked as exc:
            if exc.release_capacity:K.finish('call:'+call_id,outcome='cancelled_before_dispatch')
            return {'state':exc.state,'dispatch_allowed':False,'id':call_id}
        return {**sent,'dispatch_allowed':True}


def before(call_id,owner,command,*,paired=False):
    command=list(command)
    index=command.index('--max-budget-usd')+1
    maximum=float(command[index])
    old=B.lookup(call_id)
    if old:
        if B.money(maximum)<old['reserved']:raise ValueError('CALL_RESERVATION_EXCEEDS_REQUESTED_MAXIMUM')
        # A crash before dispatch must not shrink its own persisted cap and
        # thereby change the command hash on retry.
        maximum=old['reserved']/1e6
    elif not paired:
        status=B.status()
        maximum=min(maximum,max(0,5-status['actual_usd']-status['reserved_usd']))
    if maximum<=0: raise ValueError('RESEARCH_BUDGET_BLOCKED')
    command[index]=str(maximum)
    reservation=claim(call_id,owner,maximum,{'command':command})
    if not reservation['dispatch_allowed']:
        raise ValueError('RESEARCH_'+reservation['state'].upper())
    return command


def after(call_id,result):
    if not isinstance(result,dict): return {'state':'cost_unresolved'}
    settled=B.settle(call_id,result.get('total_cost_usd'),result.get('session_id') or call_id)
    K.finish('call:'+call_id)
    return settled
