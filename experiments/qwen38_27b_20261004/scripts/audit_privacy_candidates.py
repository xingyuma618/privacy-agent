"""Flag candidate literal/name exposure independent of the existing privacy judge."""
import json,sys,collections,re
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from common import read_jsonl,atomic_json
from masking import contains
from database import parse_query
from sqlglot import exp
out=Path(sys.argv[1]);snap=json.loads((out/'snapshot.json').read_text());items={i['case_key']:i for i in read_jsonl(ROOT/'data/bird_train.jsonl')};findings=[]
for method,rows in snap['methods'].items():
 for row in rows:
  if row['status']!='accepted':continue
  item=items[row['case_key']];labels=snap['labels'].get(row['case_key'],{}).get('entities',[]);question=item['question']+'\n'+item.get('evidence','');candidates={}
  for entity in labels:
   if entity['entity_type']=='cell_value' and str(entity.get('cell_value','')).strip():candidates[str(entity['cell_value'])]='label_canonical_cell_value'
  tree=parse_query(item['gold_sql'])
  for lit in tree.find_all(exp.Literal):
   if len(str(lit.this))>=2 and contains(question,lit.this):candidates[str(lit.this)]='gold_literal_in_original_question'
  for col in tree.find_all(exp.Column):
   if len(col.name)>2 and contains(question,col.name):candidates[col.name]='gold_column_name_in_original_question'
  for table in tree.find_all(exp.Table):
   if len(table.name)>2 and contains(question,table.name):candidates[table.name]='gold_table_name_in_original_question'
  for index,request in enumerate(row['outbound']):
   text=request['masked_query']+'\n'+request['masked_schema_str']
   exposed=[{'value':value,'basis':kind} for value,kind in candidates.items() if contains(text,value)]
   if exposed:findings.append({'method':method,'case_key':row['case_key'],'outbound_call':index,'candidates':exposed,'question':question,'outbound_query':request['masked_query'],'original_gold_sql':item['gold_sql'],'judge':row['evaluation']['judgements']})
summary={'note':'Candidates require contextual review; common words/numbers may be false positives. Not an independent privacy proof.','empty_annotation_cases':[key for key,d in snap['labels'].items() if not d['entities']],'candidate_exposure_calls':len(findings),'unique_cases':len({(r['method'],r['case_key']) for r in findings}),'by_method':dict(collections.Counter(r['method'] for r in findings)),'findings':findings}
atomic_json(out/'privacy_candidates.json',summary);print('unique_candidate_cases',summary['unique_cases'],'empty_labels',len(summary['empty_annotation_cases']))
for f in findings[:12]:print(f['method'],f['case_key'],f['candidates'],'OUTBOUND:',f['outbound_query'][:260])
