from __future__ import annotations
import json
import hashlib
from pathlib import Path
from common import ROOT, atomic_json, stable_hash
from database import schema_from_sqlite, render_schema

def main():
    config=json.loads((ROOT/'configs/synthesis.json').read_text())
    source=Path(config['raw_train_path'])
    rows=json.loads(source.read_text());schemas={};seen=set()
    target=ROOT/'data/bird_train.jsonl'
    with target.open('w') as output:
        for index,row in enumerate(rows):
            db_id=row['db_id'];key=f"{db_id}__{row.get('question_id',index)}"
            if key in seen:raise ValueError('Duplicate input key: '+key)
            seen.add(key)
            path=Path(config['database_root'])/db_id/(db_id+'.sqlite')
            if not path.is_file():raise FileNotFoundError(path)
            if db_id not in schemas:schemas[db_id]=schema_from_sqlite(path)
            item={'index':index,'case_key':key,'question_id':row.get('question_id',index),'db_id':db_id,'question':row['question'],'evidence':row.get('evidence',''),'gold_sql':row['SQL'],'db_path':str(path),'structured_db_schema':schemas[db_id],'db_schema':render_schema(schemas[db_id])}
            output.write(json.dumps(item,ensure_ascii=False)+'\n')
    manifests={'source_path':str(source),'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'input_sha256':hashlib.sha256(target.read_bytes()).hexdigest(),'rows':len(rows),'databases':len(schemas),'schema_source':'deterministic full SQLite schema; no historical teacher filtering','teacher_model_path':config['teacher_model_path'],'student_model_path':config['student_model_path']}
    model=Path(config['teacher_model_path'])
    manifests['teacher_config_sha256']=hashlib.sha256((model/'config.json').read_bytes()).hexdigest()
    manifests['teacher_weights_inventory']=[{'file':p.name,'bytes':p.stat().st_size} for p in sorted(model.glob('*.safetensors'))]
    atomic_json(ROOT/'data/input_manifest.json',manifests)
    print(json.dumps({k:v for k,v in manifests.items() if k!='teacher_weights_inventory'},ensure_ascii=False,indent=2))

if __name__=='__main__':main()
