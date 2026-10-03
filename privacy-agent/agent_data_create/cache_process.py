
# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[2]))

import os
import json  
training_data_path = str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/mcts/train_data/bird_train_sc_filtered_training_data.jsonl')
files = os.listdir(str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/mcts/output/bird_train_sc_filtered/agent_synthesis/cache'))
used_data = set()
with open(training_data_path, 'w', encoding='utf-8') as f_train:
    for fi in files:
        res = json.load(open(f"/mnt/wh_intern/MXY/paper/privacy-agent/agent_data_create/mcts/output/bird_train_sc_filtered/agent_synthesis/cache/{fi}", 'r'))
        if res.get("training_messages"):
            if res['db_id'] + res['question'] + res['gold_sql'] in used_data:
                continue
            train_record = {"messages": res["training_messages"]}
            f_train.write(json.dumps(train_record, ensure_ascii=False) + '\n')
            used_data.add(res['db_id'] + res['question'] + res['gold_sql'])
