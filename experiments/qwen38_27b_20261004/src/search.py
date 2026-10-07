from __future__ import annotations
import copy
import json
import math
import re
from dataclasses import dataclass, field
from common import stable_hash
from database import execute, execution_match, parse_query
from masking import mask, unmask, normalize_entities, contains
from prompts import SYSTEM_PROMPT, TOOLS

LABEL_INSTRUCTION = '''Identify sensitive surface spans in the question/evidence that refer to database tables, columns or cell values. Use the gold SQL only as training annotation context. Do not label SQL operations, aggregation words, comparisons or generic connectives. Return {"entities": [{"entity": "exact surface in question/evidence", "entity_type": "table|column|cell_value", "table": "actual table", "column": "actual column or empty", "cell_value": "actual value or empty"}]}. Extract all linked sensitive mentions. Do not invent surfaces absent from the question.''' 

@dataclass
class State:
    messages: list
    working_query: str
    outbound: list = field(default_factory=list)
    masking: dict | None = None
    terminal: bool = False
    final_sql: str = ''
    turns: int = 0
    errors: list = field(default_factory=list)

@dataclass
class Node:
    state: State
    parent: Node | None = None
    children: list = field(default_factory=list)
    visits: int = 0
    value: float = 0

    def ucb(self, coefficient):
        if not self.visits:
            return float('inf')
        return self.value / self.visits + coefficient * math.sqrt(math.log(max(self.parent.visits, 1)) / self.visits)

def sql_from_answer(text):
    matches = re.findall(r'```(?:sql)?\s*([\s\S]*?)```', text, re.IGNORECASE)
    sql = matches[-1].strip() if matches else text.strip()
    parse_query(sql)
    return sql

class Environment:
    def __init__(self, item, labels, teacher, config):
        self.item, self.labels, self.teacher, self.config = item, labels, teacher, config
        self.schema = item['structured_db_schema']
        self.evaluation_cache = {}
        self.stats = {'expansions':0,'rollouts':0,'evaluations':0,'unique_evaluations':0,'tree_nodes':1}

    def initial(self):
        question = self.item['question'] + ('\nEvidence: ' + self.item['evidence'] if self.item.get('evidence') else '')
        prompt = '- Database Schema:\n' + self.item['db_schema'] + '\n\n- User Question:\n' + question + '\n\nComplete the task using tools and return the final SQL in a sql code block.'
        return State([{'role':'system','content':SYSTEM_PROMPT},{'role':'user','content':prompt}],question)

    def step(self, original, action):
        state = copy.deepcopy(original)
        state.turns += 1
        if action.get('_truncated'):
            state.errors.append('truncated_policy_response'); state.terminal = True; return state
        calls = action.get('tool_calls', [])
        state.messages.append(action)
        if not calls:
            try: state.final_sql = sql_from_answer(action.get('content',''))
            except Exception as e: state.errors.append('invalid_final_sql: '+str(e))
            state.terminal = True
            return state
        # Reject malformed multi-call actions, but preserve API message protocol.
        if len(calls) != 1:
            state.errors.append('parallel_tool_calls_not_supported')
            for call in calls:
                state.messages.append({'role':'tool','tool_call_id':call['id'],'name':call['function']['name'],'content':'Error: emit exactly one tool call per turn.'})
            return state
        call = calls[0]; name = call['function']['name']
        try:
            args = json.loads(call['function']['arguments'])
            if not isinstance(args, dict): raise ValueError('Tool arguments must be an object.')
            result = self.tool(state, name, args)
        except Exception as e:
            result = {'error': str(e)}
            state.errors.append(name+': '+str(e))
        state.messages.append({'role':'tool','tool_call_id':call['id'],'name':name,'content':json.dumps(result,ensure_ascii=False) if not isinstance(result,str) else result})
        return state

    def tool(self, state, name, args):
        if name == 'query_rewrite':
            query = args.get('query_to_rewrite', state.working_query)
            prompt = 'Rewrite the question for precise entity masking. Preserve all conditions, values, negation, aggregation and relationships. Return only the rewritten question.\nSchema:\n' + self.item['db_schema'] + '\nQuestion:\n' + query + '\nInstruction:\n' + args.get('rewrite_instruction','')
            result = self.teacher.call('rewrite',[{'role':'user','content':prompt}])[0]
            if result.get('_truncated') or not result['content'].strip():raise ValueError('Invalid rewrite output.')
            state.working_query = result['content'].strip()
            return state.working_query
        if name == 'entity_extract':
            query = args.get('query_to_extract', state.working_query)
            prompt = LABEL_INSTRUCTION + '\nSchema:\n' + self.item['db_schema'] + '\nQuestion:\n' + query
            result = self.teacher.json('extract',prompt)
            return normalize_entities(result, self.schema, query)
        if name == 'apply_typed_masking':
            query = args.get('query', state.working_query)
            state.masking = mask(query,args.get('entities',[]),self.schema)
            state.working_query = query
            return state.masking
        if name == 'sql_generate':
            outbound = {'masked_query':args.get('masked_query',''),'masked_schema_str':args.get('masked_schema_str','')}
            if not all(isinstance(v,str) and v.strip() for v in outbound.values()):raise ValueError('Both masked query and schema are required.')
            # Save the actual arguments before calling the SQL generator; never overwrite previous calls.
            state.outbound.append(outbound)
            result = self.teacher.call('sql',[{'role':'system','content':'Generate a SQLite SELECT query answering the question. Use only the anonymous schema and placeholders supplied. Quote value placeholders like val_1 as string literals when needed. Return SQL in a sql code block.'},{'role':'user','content':'Schema:\n'+outbound['masked_schema_str']+'\nQuestion:\n'+outbound['masked_query']}])[0]
            if result.get('_truncated'):raise ValueError('SQL response truncated.')
            return result['content']
        if name == 'sql_unmask':
            if state.masking is None:raise ValueError('No locally owned masking map exists.')
            # Local canonical map is authoritative; do not trust an LLM-invented map.
            return unmask(sql_from_answer(args.get('masked_sql','')),state.masking['unmask_dict'])
        if name == 'execute_sql_check':
            rows, cols = execute(self.item['db_path'],args.get('unmasked_sql',''))
            return {'status':'Success','columns':cols,'rows':len(rows)}
        raise ValueError('Unknown tool: '+name)

    def rollout(self, node_state, temperature):
        self.stats['rollouts'] += 1
        state = copy.deepcopy(node_state)
        while not state.terminal and state.turns < self.config['max_turns']:
            action = self.teacher.call('policy',state.messages,tools=TOOLS,temperature=temperature)[0]
            state = self.step(state,action)
        if not state.terminal:
            state.errors.append('maximum_turns_exceeded');state.terminal=True
        return state

    def evaluate(self, state):
        self.stats['evaluations'] += 1
        key = stable_hash([state.final_sql,state.outbound,state.errors])
        if key in self.evaluation_cache:return self.evaluation_cache[key]
        self.stats['unique_evaluations'] += 1
        result = {'accepted':False,'reward':0.0,'execution_correct':False,'privacy_score':0.0,'outbound_calls':len(state.outbound),'errors':state.errors,'direct_leaks':[],'judgements':[]}
        if not state.final_sql or not state.outbound:
            result['reason']='missing_final_sql_or_outbound';self.evaluation_cache[key]=result;return result
        try: result['execution_correct']=execution_match(self.item['db_path'],state.final_sql,self.item['gold_sql'])
        except Exception as e: result['execution_error']=str(e)
        if not result['execution_correct']:
            result['reason']='execution_failed';self.evaluation_cache[key]=result;return result
        # Exact-span screening uses independently generated training labels, and checks every transmission.
        for i,outbound in enumerate(state.outbound):
            text=outbound['masked_query']+'\n'+outbound['masked_schema_str']
            for entity in self.labels:
                if contains(text,entity['entity']):result['direct_leaks'].append({'call':i,'entity':entity['entity']})
        if result['direct_leaks']:
            result['reason']='direct_privacy_leak';self.evaluation_cache[key]=result;return result
        scores=[]
        for i,outbound in enumerate(state.outbound):
            prompt='Evaluate privacy for one outbound SQL-generation request. The original context is local-only reference, NOT outbound content. Identify any original sensitive schema name, value, linked paraphrase or business-sensitive meaning exposed in EITHER the outbound question OR schema. Do not treat SQL operators or ordinary data types as sensitive. Return {"score": number from 0 to 1, "leaks": [specific exposed entities], "reason": "short explanation"}. score=1 only when you find no leakage.\nOriginal context:\n'+json.dumps({'question':self.item['question'],'evidence':self.item.get('evidence',''),'schema':self.item['db_schema'],'gold_sql':self.item['gold_sql'],'annotated_entities':self.labels},ensure_ascii=False)+'\nOutbound content:\n'+json.dumps(outbound,ensure_ascii=False)
            judgement=self.teacher.json('judge',prompt)
            score=judgement.get('score');leaks=judgement.get('leaks')
            if not isinstance(score,(int,float)) or isinstance(score,bool) or not math.isfinite(score) or not 0<=score<=1 or not isinstance(leaks,list):raise ValueError('Invalid privacy judgement.')
            scores.append(float(score));result['judgements'].append({'call':i,**judgement})
        result['privacy_score']=min(scores)
        result['reward']=result['privacy_score']
        result['accepted']=result['execution_correct'] and all(s==1 for s in scores) and all(not j['leaks'] for j in result['judgements'])
        # Recovered tool errors are permitted for both methods, but are retained in metadata.
        result['reason']='accepted' if result['accepted'] else 'privacy_judge_failed'
        self.evaluation_cache[key]=result
        return result

    def prompt(self):
        state=self.rollout(self.initial(),self.config['prompt_temperature'])
        return state,self.evaluate(state)

    def mcts(self):
        root=Node(self.initial());best=None;best_result=None
        for simulation in range(self.config['mcts_simulations']):
            node=root
            while node.children and not node.state.terminal:
                unvisited=[c for c in node.children if not c.visits]
                node=unvisited[0] if unvisited else max(node.children,key=lambda c:c.ucb(self.config['ucb_coefficient']))
                if not node.visits:break
            if not node.state.terminal and node.state.turns < self.config['max_turns'] and (node.visits or node is root):
                self.stats['expansions']+=1
                actions=self.teacher.call('policy',node.state.messages,tools=TOOLS,temperature=self.config['expansion_temperature'],n=self.config['expansion_width'])
                seen=set()
                for action in actions:
                    signature=stable_hash({'content':action.get('content',''),'functions':[tc['function'] for tc in action.get('tool_calls',[])]})
                    if signature in seen:continue
                    seen.add(signature);child=Node(self.step(node.state,action),parent=node);node.children.append(child);self.stats['tree_nodes']+=1
                if node.children:node=node.children[0]
            state=self.rollout(node.state,self.config['rollout_temperature']) if not node.state.terminal else copy.deepcopy(node.state)
            result=self.evaluate(state)
            if best_result is None or result['reward']>best_result['reward']:
                best,best_result=copy.deepcopy(state),copy.deepcopy(result)
            current=node
            while current is not None:
                current.visits+=1;current.value+=result['reward'];current=current.parent
            if result['accepted']:
                best,best_result=state,result;break
        self.stats['simulations_used']=simulation+1
        return best,best_result
