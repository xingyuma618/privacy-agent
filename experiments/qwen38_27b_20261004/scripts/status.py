import json,collections
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
for run in ['full','full_parallel100']:
    print(run)
    for method in ['prompt','mcts']:
        counts=collections.Counter()
        for path in (ROOT/'runs'/run/method/'cases').glob('*.json'):
            counts[json.loads(path.read_text())['status']]+=1
        print(' ',method,dict(counts))
    print('  labels:',len(list((ROOT/'runs'/run/'labels').glob('*.json'))))
