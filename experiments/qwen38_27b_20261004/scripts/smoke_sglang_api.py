import json,time,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from teacher import Teacher
from search import Environment
config=json.loads((ROOT/'configs/synthesis_parallel100.json').read_text());config['endpoints']=['http://127.0.0.1:18280/v1']
item=json.loads((ROOT/'data/bird_train.jsonl').open().readline());model=Teacher(config,'sglang-api-smoke')
result=model.json('annotation','Return exactly {"ok": true}.');assert result=={'ok':True}
env=Environment(item,[],model,config)
actions=model.call('policy',env.initial().messages,tools=__import__('prompts').TOOLS,temperature=0.8,n=3)
assert len(actions)==3
for action in actions:
 assert action.get('tool_calls'), 'Policy did not emit tool calls'
 for call in action['tool_calls']:assert isinstance(json.loads(call['function']['arguments']),dict)
summary={'json_ok':True,'n3_tool_calls_ok':True,'actions':[{'name':a['tool_calls'][0]['function']['name'],'argument_chars':len(a['tool_calls'][0]['function']['arguments'])} for a in actions],'calls':model.calls}
(ROOT/'runs/sglang_api_smoke.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary))
