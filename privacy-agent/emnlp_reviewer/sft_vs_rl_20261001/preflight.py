#!/usr/bin/env python3
"""Fail-closed validation and provenance inventory (stdlib only)."""
import argparse, hashlib, json
from pathlib import Path
HERE=Path(__file__).resolve().parent
def sha256(path):
 h=hashlib.sha256()
 with Path(path).open('rb') as f:
  for chunk in iter(lambda:f.read(1048576),b''): h.update(chunk)
 return h.hexdigest()
def inventory_model(path):
 path=Path(path); out={'path':str(path),'exists':path.is_dir(),'metadata':[],'weights':[]}
 if not path.is_dir(): return out
 meta={'config.json','generation_config.json','model.safetensors.index.json','trainer_state.json','training_args.bin','tokenizer_config.json','merge_info.json'}
 for p in sorted(path.rglob('*')):
  if not p.is_file(): continue
  s=p.stat(); row={'path':str(p.relative_to(path)),'size':s.st_size,'mtime_ns':s.st_mtime_ns}
  if p.name in meta or p.suffix in {'.yaml','.yml'}: row['sha256']=sha256(p); out['metadata'].append(row)
  elif p.suffix in {'.safetensors','.bin','.pt','.pth'}: out['weights'].append(row)
 return out
def build_report(manifest_path):
 manifest_path=Path(manifest_path); m=json.loads(manifest_path.read_text()); errors=[]; pending=[]; art={'manifest_sha256':sha256(manifest_path)}; ds=m['dataset']
 for key in ('source','masksql_annotations'):
  p=Path(ds[key])
  if not p.is_file(): errors.append(f'missing dataset file: {p}')
  else: art[key]={'path':str(p),'sha256':sha256(p),'size':p.stat().st_size}
 source,ann=Path(ds['source']),Path(ds['masksql_annotations'])
 if source.is_file() and ann.is_file():
  try:
   src=[json.loads(x) for x in source.read_text().splitlines() if x.strip()]; rows=json.loads(ann.read_text())
   if not isinstance(rows,list): raise TypeError('annotations must be a list')
   ids=[r.get('idx') for r in rows if isinstance(r,dict)]
   if len(rows)!=ds['expected_cases']: errors.append(f'annotation count {len(rows)} != expected {ds["expected_cases"]}')
   if len(ids)!=len(rows) or any(not isinstance(x,str) or not x for x in ids): errors.append('every annotation requires nonempty string idx')
   if len(set(ids))!=len(ids): errors.append('duplicate annotation idx')
   src_keys={(str(r.get('question_id')),str(r.get('db_id'))) for r in src if isinstance(r,dict)}; unmatched=[]; missing=[]; root=Path(ds['db_root'])
   for r in rows:
    if not isinstance(r,dict): errors.append('annotation row must be an object'); continue
    idx,dbid=r.get('idx'),r.get('orig_db_id'); qid=idx[5:] if isinstance(idx,str) and idx.startswith('bird_') else str(idx)
    if (qid,str(dbid)) not in src_keys: unmatched.append(idx)
    if not isinstance(dbid,str) or not any((root/dbid/f'{dbid}{ext}').is_file() for ext in ('.sqlite','.db')): missing.append(idx)
    for field in ('orig_gold_schema_links','orig_gold_value_links'):
     v=r.get(field)
     if not isinstance(v,dict) or any(not isinstance(k,str) or not k for k in v): errors.append(f'{idx}: invalid {field}')
   art.update(annotation_count=len(rows),canonical_case_ids=ids,db_root={'path':str(root),'required_unique_dbs':len({r.get('orig_db_id') for r in rows if isinstance(r,dict)})})
   if unmatched: errors.append(f'annotations not found in source ({len(unmatched)}): {unmatched[:10]}')
   if missing: errors.append(f'missing per-case SQLite databases ({len(missing)}): {missing[:10]}')
   runtime_root=Path(ds.get('runtime_db_root','')); hash_manifest=Path(ds.get('runtime_db_hash_manifest',''))
   if not runtime_root.is_dir() or not hash_manifest.is_file(): errors.append('runtime DB root/hash manifest is missing')
   else:
    hashes=json.loads(hash_manifest.read_text()); art['runtime_db_hash_manifest']={'path':str(hash_manifest),'sha256':sha256(hash_manifest),'status':hashes.get('status')}
    if hashes.get('status')!='verified' or Path(hashes.get('runtime_root',''))!=runtime_root: errors.append('runtime DB hash manifest is not verified for configured root')
    by_db={x.get('db_id'):x for x in hashes.get('databases',[])}
    for dbid in {r.get('orig_db_id') for r in rows if isinstance(r,dict)}:
     dbfile=next((runtime_root/dbid/f'{dbid}{ext}' for ext in ('.sqlite','.db') if (runtime_root/dbid/f'{dbid}{ext}').is_file()),None)
     record=next((f for f in by_db.get(dbid,{}).get('files',[]) if dbfile and f.get('name')==dbfile.name),None)
     if not dbfile or not record or not record.get('match') or sha256(dbfile)!=record.get('sha256'): errors.append(f'runtime DB hash mismatch or missing: {dbid}')
  except Exception as e: errors.append(f'invalid dataset JSON: {e}')
 evaluator=Path(m['evaluator'])
 if not evaluator.is_file(): errors.append(f'missing evaluator: {evaluator}')
 else: art['evaluator']={'path':str(evaluator),'sha256':sha256(evaluator)}
 models={name:inventory_model(spec['path']) for name,spec in m['models'].items()}
 for name,inv in models.items():
  if not inv['exists']: errors.append(f'missing {name} model: {inv["path"]}')
  elif not inv['weights']: errors.append(f'{name} model has no weight files: {inv["path"]}')
  if m['models'][name].get('parent_chain_status')!='verified': pending.append(f'{name} parent chain not verified')
 generator=m.get('inference',{}).get('cloud_generator',{}); generator_path=generator.get('checkpoint_path') or generator.get('path')
 if not generator_path: errors.append('local sql generator checkpoint path is missing')
 else:
  generator_inventory=inventory_model(generator_path); art['sql_generator_model']=generator_inventory
  if not generator_inventory['exists']: errors.append(f'missing sql generator model: {generator_path}')
  elif not generator_inventory['weights']: errors.append(f'sql generator model has no weight files: {generator_path}')
  expected_hashes=generator.get('checkpoint_metadata',{}); hash_files={'config_sha256':'config.json','model_index_sha256':'model.safetensors.index.json','tokenizer_config_sha256':'tokenizer_config.json'}
  for key,filename in hash_files.items():
   p=Path(generator_path)/filename
   if not p.is_file(): errors.append(f'sql generator missing locked metadata file: {filename}')
   elif expected_hashes.get(key)!=sha256(p): errors.append(f'sql generator {key} does not match manifest')
 if generator.get('base_url')!='http://127.0.0.1:18342/v1' or generator.get('deployment')!='local-only': errors.append('sql generator must be locked to local-only 127.0.0.1:18342')
 def scan(o,p=''):
  if isinstance(o,dict):
   for k,v in o.items(): scan(v,f'{p}.{k}' if p else k)
  elif o=='pending': pending.append(p)
 scan(m.get('inference',{}))
 if m['policy'].get('require_verified_rl_parent_chain') and m['models']['rl'].get('parent_chain_status')!='verified': errors.append('RL parent chain is not verified')
 return {'status':'blocked' if errors or pending else 'ready','errors':errors,'pending':sorted(set(pending)),'artifacts':art,'models':models}
def main():
 ap=argparse.ArgumentParser(); ap.add_argument('--manifest',default=str(HERE/'manifest.json')); ap.add_argument('--output',default=str(HERE/'preflight_report.json')); ap.add_argument('--allow-blocked',action='store_true'); a=ap.parse_args(); report=build_report(a.manifest); Path(a.output).write_text(json.dumps(report,indent=2)+'\n'); print(json.dumps(report,indent=2)); return 0 if report['status']=='ready' or a.allow_blocked else 2
if __name__=='__main__': raise SystemExit(main())
