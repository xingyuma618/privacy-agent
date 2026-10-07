"""Reuse completed cases after ONLY concurrency/request-timeout changes, with provenance."""
import json,sys,copy
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from run_synthesis import fingerprint
from common import atomic_json
source=ROOT/'runs/full';dest=ROOT/'runs/full_parallel100'
old=json.loads((source/'manifest.json').read_text())
old_config=old['config'];new_config=json.loads((ROOT/'configs/synthesis_parallel100.json').read_text())
allowed={'workers','timeout_seconds'}
assert {k:v for k,v in old_config.items() if k not in allowed}=={k:v for k,v in new_config.items() if k not in allowed}, 'Generation-affecting configuration changed'
assert fingerprint(old_config)==old['fingerprint'], 'Source changed; reuse needs a separate semantic audit'
new_fingerprint=fingerprint(new_config)
counts={}
for sub in ['labels','prompt/cases','mcts/cases']:
    counts[sub]=0
    for path in (source/sub).glob('*.json'):
        row=json.loads(path.read_text())
        assert row['fingerprint']==old['fingerprint']
        if row.get('status')=='infrastructure_error':continue
        row['original_fingerprint']=row['fingerprint'];row['fingerprint']=new_fingerprint
        row['reused_from']=str(path)
        target=dest/sub/path.name
        if target.exists():raise RuntimeError('Destination already exists: '+str(target))
        atomic_json(target,row);counts[sub]+=1
atomic_json(dest/'cache_provenance.json',{'source_run':str(source),'source_fingerprint':old['fingerprint'],'new_fingerprint':new_fingerprint,'runtime_changes':{k:{'before':old_config[k],'after':new_config[k]} for k in sorted(allowed)},'reused':counts,'note':'Original caches untouched; incomplete searches restart; seed, prompts, acceptance and model unchanged.'})
print(json.dumps(counts))
