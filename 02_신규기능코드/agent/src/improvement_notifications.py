"""Bounded, idempotency-keyed delivery queue, independent of policy writes."""
from datetime import timedelta
import urllib.request
import urllib.parse

import improvement_store as S


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):raise ValueError('NOTIFICATION_REDIRECT_FORBIDDEN')


def webhook(channel,body,key):
    url=channel.get('url','');parsed=urllib.parse.urlparse(url)
    if parsed.scheme!='https' or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError('HTTPS_NOTIFICATION_ENDPOINT_REQUIRED')
    payload=S.canonical(body).encode()
    request=urllib.request.Request(url,data=payload,headers={'Content-Type':'application/json','Idempotency-Key':key},method='POST')
    with urllib.request.build_opener(NoRedirect).open(request,timeout=10) as response:
        if not 200<=response.status<300:raise ValueError('NOTIFICATION_HTTP_FAILED')
    return {'state':'delivered'}


def dispatch(*, transport=None, limit=20):
    # A configured recipient is necessary but insufficient: the recipient must
    # support durable deduplication, because a timeout can follow acceptance.
    channels={c['id']:c for c in S.config().get('notification_channels',[])}
    results=[]
    with S.transaction(readonly=True) as db:
        pending=[] if db is None else [S.decode(r) for r in db.execute("SELECT body,hash FROM records WHERE kind='notification' AND state IN ('queued','retrying') AND coalesce(json_extract(body,'$.retry_at'),updated_at)<=? ORDER BY updated_at,id LIMIT ?",(S.now(),limit))]
    for row in pending:
        channel=channels.get(row['channel_id'])
        if not channel:continue
        if channel.get('kind')!='webhook' or not channel.get('supports_idempotency_key'):
            results.append({'id':row['id'],'state':'notification_pending','reason':'idempotent_channel_adapter_required'});continue
        maximum=channel.get('max_attempts');delay=channel.get('retry_seconds')
        if type(maximum) is not int or not 1<=maximum<=10 or type(delay) is not int or delay<1:
            results.append({'id':row['id'],'state':'notification_pending','reason':'finite_retry_policy_required'});continue
        with S.transaction() as db:
            current=S.get('notification',row['id'],conn=db)
            if current['state'] not in {'queued','retrying'}:continue
            if current['attempts']>=maximum:
                results.append(S.put(db,'notification',row['id'],{**current,'state':'failed','closed_at':S.now(),'error':'delivery_unconfirmed_retry_limit'}))
                continue
            current=S.put(db,'notification',row['id'],{**current,'state':'sending','attempts':current['attempts']+1})
        try:
            response=(transport or webhook)(channel,{'event':row['event'],'application_id':row['application_id']},row['id'])
            if response.get('state')!='delivered':raise ValueError('DELIVERY_NOT_CONFIRMED')
            update={'state':'delivered','closed_at':S.now()}
        except Exception as exc:
            update={'state':'failed' if current['attempts']>=maximum else 'retrying','error':str(exc)[:500],
                    'retry_at':(S.instant(S.now())+timedelta(seconds=delay)).isoformat()}
            if update['state']=='failed':update['closed_at']=S.now()
        with S.transaction() as db:results.append(S.put(db,'notification',row['id'],{**current,**update}))
    return results


def recover():
    # Use the SAME idempotency key after an ambiguous process exit.
    with S.transaction() as db:
        for row in S.page('notification',state='sending',limit=200,conn=db):
            S.put(db,'notification',row['id'],{**row,'state':'retrying','retry_at':S.now()})
