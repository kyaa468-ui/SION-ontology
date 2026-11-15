"""Pure journal reducers. Global counts and competition counts never mix."""
import copy
import hashlib
import json
import math
import common as C

VERSION='JOURNAL_CHECKPOINT@v1'
MAX_BYTES=2*1024*1024


def canonical(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def digest(value):return hashlib.sha256(canonical(value)).hexdigest()


def blank():
    return {'fills':0,'book':{},'net':{},'reconcile_anchors':[],
            'closed':{'count':0,'positive':0,'negative':0,'sum_price_returns':0.0},
            'decisions':{},'blocks':{},'tunes':0,'last_tuned':{},'tuning_tail':[],
            'last_review':None,'confidence':{}}


def state(rows,scope='all',start=None):
    checkpoint=C.journal_checkpoint(rows)
    if not checkpoint:return blank()
    if scope=='all':return copy.deepcopy(checkpoint['all'])
    if scope!='competition':raise ValueError('JOURNAL_SCOPE_REQUIRED')
    start=start or C.params()['competition']['start']
    if start==checkpoint['competition_start']:return copy.deepcopy(checkpoint['competition'])
    if start>=checkpoint['supported_after']:return blank()
    raise ValueError('JOURNAL_COMPETITION_SCOPE_EXPIRED')


def consume(out,row,*,regimes=None,keep_anchors=True):
    kind=row['kind']
    if kind=='fill':
        code=row['code'];qty=row['qty'];price=row['price']
        if type(qty) is not int or qty<0 or type(price) not in (int,float) or not math.isfinite(price) or price<=0:
            raise ValueError('JOURNAL_FILL_UNSUPPORTED')
        side=row['side']
        if side not in ('매수','매도'):raise ValueError('JOURNAL_FILL_SIDE_UNSUPPORTED')
        out['fills']+=1
        out['net'][code]=out['net'].get(code,0)+(qty if side=='매수' else -qty)
        if not out['net'][code]:del out['net'][code]
        book=out['book'].setdefault(code,{'qty':0,'cost':0.0})
        if side=='매수':
            if book['qty']==0:book.update(entry=row['time'],conf=row.get('confidence'),regime=row.get('regime'),name=row['name'])
            book['qty']+=qty;book['cost']+=qty*price
        elif book['qty']>0:
            avg=book['cost']/book['qty'];ret=round(price/avg-1,4)
            closed=out['closed'];closed['count']+=1;closed['positive']+=ret>0;closed['negative']+=ret<0
            closed['sum_price_returns']+=ret
            book['qty']=max(0,book['qty']-qty);book['cost']=avg*book['qty']
        if book['qty']==0:del out['book'][code]
        if keep_anchors:
            import logger
            ordered=row.get('ordered') or logger.FIXES.get(f"{row['time'][:16]}|{code}")
            if ordered and qty<ordered:
                out['reconcile_anchors'].append({'ordinal':out['fills'],'row':copy.deepcopy(row)})
    elif kind=='decision' and row.get('mode')!='view':
        action=row['action'];out['decisions'][action]=out['decisions'].get(action,0)+1
        if action=='관망':
            value=row.get('confidence_adj')
            key=['adjusted',value] if value is not None else ['raw',float(row['confidence']),(regimes or {}).get(row.get('run'))]
            # Preserve the raw fallback and regime. A later market_boost may
            # change, so a previously adjusted number cannot replace them.
            encoded=canonical(key).decode();out['confidence'][encoded]=out['confidence'].get(encoded,0)+1
    elif kind=='block':
        key=row['rule'];out['blocks'][key]=out['blocks'].get(key,0)+1
    elif kind=='tune':
        out['tunes']+=1
        for change in row.get('applied',[]):out['last_tuned'][change['key']]=out['tunes']
        out['tuning_tail']=(out['tuning_tail']+[copy.deepcopy(row)])[-12:]
    elif kind=='review':out['last_review']=copy.deepcopy(row)
    if len(canonical(out))>MAX_BYTES:raise ValueError('JOURNAL_CHECKPOINT_CAPACITY_BLOCKED')
    return out


def run_filter_add(bits,identifier):
    if not identifier:return
    hashed=hashlib.sha256(str(identifier).encode()).digest()
    for i in range(0,28,4):
        p=int.from_bytes(hashed[i:i+4],'big')%(len(bits)*8);bits[p//8]|=1<<(p%8)


def envelope(body):
    if len(canonical(body))>MAX_BYTES:raise ValueError('JOURNAL_CHECKPOINT_CAPACITY_BLOCKED')
    return {'kind':'journal_checkpoint','time':body['supported_after']+' 00:00',
            'checkpoint':body,'sha256':digest(body)}


def summarize(rows,*,scope='all',start=None):
    out=state(rows,scope,start)
    regimes={}
    for r in rows:
        if r['kind']=='regime' and (not start or r['time'][:10]>=start):regimes[r.get('run')]=r.get('regime')
        if len(regimes)>20000:raise ValueError('JOURNAL_RUN_INVENTORY_LIMIT')
    for row in rows:
        if row['kind']=='journal_checkpoint' or start and row['time'][:10]<start:continue
        consume(out,row,regimes=regimes,keep_anchors=scope=='all')
    return out


class Since:
    """Replayable scoped stream; checkpoint state remains explicit."""
    def __init__(self,rows,start):self.rows,self.start=rows,start
    def __iter__(self):
        return (r for r in self.rows if r['kind']=='journal_checkpoint' or r['time'][:10]>=self.start)
    def __reversed__(self):
        return (r for r in reversed(self.rows) if r['kind']=='journal_checkpoint' or r['time'][:10]>=self.start)


def confidence_histogram(summary,params):
    boost=params.get('market_boost') or {};hist={}
    for encoded,count in summary['confidence'].items():
        key=json.loads(encoded);value=float(key[1])
        if key[0]=='raw' and key[2] in boost.get('regimes',[]):value+=boost.get('add',0)
        value=round(value,3);hist[value]=hist.get(value,0)+count
    return [{'confidence':value,'count':hist[value]} for value in sorted(hist,reverse=True)]
