"""Audit completed case caches without rerunning model inference."""
import json,sys,collections
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
run=ROOT/'runs'/(sys.argv[1] if len(sys.argv)>1 else 'full')
for method in ['prompt','mcts']:
    statuses=collections.Counter();reasons=collections.Counter();roles=collections.Counter();models=set();nodes=[]
    for path in (run/method/'cases').glob('*.json'):
        row=json.loads(path.read_text());statuses[row['status']]+=1
        reasons[row.get('evaluation',{}).get('reason',row.get('error',''))]+=1
        for call in row.get('calls',[]):
            roles[call['role']]+=1
            if call.get('returned_model'):models.add(call['returned_model'])
        if row.get('stats'):nodes.append(row['stats'].get('tree_nodes',1))
    print(json.dumps({'method':method,'statuses':dict(statuses),'reasons':dict(reasons),'roles':dict(roles),'models':sorted(models),'tree_nodes':nodes},ensure_ascii=False))
