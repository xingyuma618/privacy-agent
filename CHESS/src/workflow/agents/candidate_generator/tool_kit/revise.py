from typing import Dict

from llm.models import async_llm_chain_call, get_llm_chain
from llm.prompts import get_prompt
from llm.parsers import get_parser
from database_utils.execution import ExecutionStatus
from workflow.system_state import SystemState
from workflow.sql_meta_info import SQLMetaInfo
from workflow.agents.tool import Tool
import json 
import re
from openai import OpenAI


def clean_space(sql):
    sql = re.sub(r'\s*\)', ')', sql)
    #聚合函数括号需要连着
    sql = re.sub(r'min\s*\(\s*', 'min(', sql)
    sql = re.sub(r'max\s*\(\s*', 'max(', sql)
    sql = re.sub(r'sum\s*\(\s*', 'sum(', sql)
    sql = re.sub(r'avg\s*\(\s*', 'avg(', sql)
    sql = re.sub(r'count\s*\(\s*', 'count(', sql)
    sql = re.sub(r'MIN\s*\(\s*', 'MIN(', sql)
    sql = re.sub(r'MAX\s*\(\s*', 'MAX(', sql)
    sql = re.sub(r'SUM\s*\(\s*', 'SUM(', sql)
    sql = re.sub(r'AVG\s*\(\s*', 'AVG(', sql)
    sql = re.sub(r'COUNT\s*\(\s*', 'COUNT(', sql)
    #非聚合函数，括号只删除左括号右边空格
    sql = re.sub(r'\(\s*', '(', sql)
    #删除多余空格
    sql = re.sub(r' +', ' ', sql)
    sql = re.sub(r'\s*,', ',', sql) 

    return sql


def find_sql(text):
    sql = text
    if '```sql' in text:
        pattern = r"```sql(.*?)```"
        matches = re.findall(pattern, text, re.DOTALL)
        if len(matches) > 0:
            sql = matches[0].replace('\n', ' ').strip()
            sql = clean_space(sql)
            return sql
    
    if '```' in text:
        pattern = r"```(.*?)```"
        matches = re.findall(pattern, text, re.DOTALL)
        if len(matches) > 0:
            sql = matches[0].replace('\n', ' ').strip()
            sql = clean_space(sql)
            return sql
        
    sql = sql.replace('\n', ' ').strip()
    sql = clean_space(sql)
    return sql

def api_fix(error_reason, question, evidence, db_schema, sql):
    client = OpenAI(api_key="sk-533b918073e141ee9857ebf7d78598ec", base_url="https://api.deepseek.com/v1")
    input_text = f'''【User's Question】
-- {question} {evidence}
【Database Schema】
{db_schema}
【old SQL】
```sql
{sql}
```
【Error Analysis】
{error_reason}
'''
    print(input_text)
    response = client.chat.completions.create(
        model="deepseek-chat",
        messages = [
            {"role": "system", "content": "The old SQL below may not answer the user's question. The reasons for the errors are as follows. Now, please refer to the error analysis and generate a new SQL query. you must put the correct SQL in ```sql```tags"},
            {"role": "user", "content": input_text}
        ],
            stream=False,
            temperature=0.1,
            max_tokens=4096
    )
    text = response.choices[0].message.content.strip()
    return text


def api_crr_check(input_text, base_url, model, system_prompt=''):
    client = OpenAI(api_key='YOUR_API_KEY', base_url=base_url)

    response = client.chat.completions.create(
    model=model,

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": input_text}
    ],
        stream=False,
        temperature=0.1,
        max_tokens=4096
    )
    text = response.choices[0].message.content.strip()
    return text

system_prompt = '''
You are an expert in SQLite databases. Given the following SQL tables, a user's question about these tables, and a generated SQL query based on the user's question, your task is to evaluate whether the SQL query accurately answers the user's question. To do this, you should verify the correctness of each SQL clause step-by-step.

Please follow the response format below:

### Analysis:
Provide a detailed clause-by-clause analysis of the SQL query here.

### Summary:
Summarize your analysis and state whether the SQL query meets the user's requirements.

### Answer:
If all clauses are correct, please respond with:
```answer
YES
```
If any clause is incorrect, please respond with:
```answer
NO
```
'''
##bird
def correct_check(question, gen_sql, evidence=''):

    gen_sql = find_sql(gen_sql.replace('`', ''))
    prompt = get_check_prompt(question, gen_sql, evidence)
    print(prompt)
    check_res = api_crr_check(prompt,base_url="http://172.16.10.2:2333/v1",
                              model='/home/maxingyu/work/docker/paper/erro_detection/output/models/chatml/Qwen2.5-Coder-14B-Instruct/bird_llm/v0_cot_2/only_check/checkpoint-600',
                              system_prompt=system_prompt)
    
    erro_reason = check_res.split('### Answer:', )[0].replace('## Task 1:', '')
    
    if 'YES' in check_res: return True, erro_reason
    else:
        print(check_res)
        return False, erro_reason
#bird
def get_check_prompt(question, gen_sql, evidence):
    data = json.load(open('/home/maxingyu/work/docker/DB-GPT-Hub/dbgpt_hub/data/bird_codes_value_evidence/bird_codes_value_evidence_dev.json', 'r'))
    input_str = ''
    for item in data:
        if question in item['input']:
            db_schema = item['instruction'].replace("Given the following SQL tables, your job is to generate the Sqlite SQL query given the user's question.\nPut your answer inside the ```sql and ``` tags.\n\"\n##Instruction:\n", "")
            if evidence is None or evidence == '':
                input_str = db_schema + "## User's Question:\n" + question + "\n\n## Generated SQL:\n" + gen_sql
            else:
                input_str = db_schema + "## User's Question:\n" + question + " " + evidence + "\n\n## Generated SQL:\n" + gen_sql 
            # item['input'] = item['input'].split('\n\n##Generated SQL: ')[0] + '\n\n##Generated SQL: '+ gen_sql +'\n\n### Response:\n'
            # input_str = item['instruction']+ item['input']
            return input_str
    return input_str


class Revise(Tool):
    """
    Tool for correcting a SQL query that returns empty set or has a syntax error.
    """

    def __init__(self, template_name: str = None, engine_config: str = None, parser_name: str = None):
        super().__init__()
        self.template_name = template_name
        self.engine_config = engine_config
        self.parser_name = parser_name
        

    def _run(self, state: SystemState):
        """
        Executes the SQL revision process.
        
        Args:
            state (SystemState): The current system state.
        """
        try:
            key_to_refine = list(state.SQL_meta_infos.keys())[-1]
            target_SQL_meta_infos = state.SQL_meta_infos[key_to_refine]
        except Exception as e:
            print(f"Error in Checker: {e}")
            return
        if key_to_refine.startswith(self.tool_name):
            id = int(key_to_refine[len(self.tool_name)+1:])
            SQL_id = self.tool_name + "_" + str(id+1)
        else:
            SQL_id = self.tool_name + "_1" 
        ## 这里先进行sql-checker 语义检测，最多修复三次    
        # for target_SQL_meta_info in target_SQL_meta_infos:     
        #     max_fix = 3
        #     fix_sql = target_SQL_meta_info.SQL
        #     while max_fix > 0:
        #         passed, erro_reason =correct_check(question=state.task.question, gen_sql=fix_sql, evidence=state.task.evidence)  
        #         if passed: 
        #             target_SQL_meta_info.SQL = fix_sql
        #             break
        #         else:
        #             fix_response = api_fix(error_reason=erro_reason, question=state.task.question, evidence=state.task.evidence, db_schema=state.get_schema_string(schema_type="complete"), sql=fix_sql)
        #             fix_sql = find_sql(fix_response)
        #             max_fix -= 1
        #             if max_fix == 0:
        #                 passed, erro_reason =correct_check(question=state.task.question, gen_sql=fix_sql, evidence=state.task.evidence) 
        #                 if passed:
        #                     target_SQL_meta_info.SQL = fix_sql

        state.SQL_meta_infos[SQL_id] = []
        request_list = []
        for SQL_meta_info in target_SQL_meta_infos:
            try:
                execution_status = SQL_meta_info.execution_status
                if execution_status != ExecutionStatus.SYNTACTICALLY_CORRECT:
                    SQL_meta_info.need_fixing = True
            except Exception:
                SQL_meta_info.need_fixing = True
        need_fixing_SQL_meta_infos = [(index, target_SQL_meta_info) for index, target_SQL_meta_info in enumerate(target_SQL_meta_infos) if target_SQL_meta_info.need_fixing]
        for index, target_SQL_meta_info in need_fixing_SQL_meta_infos:   
            try:            
                request_kwargs = {
                    "DATABASE_SCHEMA": state.get_schema_string(schema_type="complete"),
                    "QUESTION": state.task.question,
                    "HINT": state.task.evidence,
                    "QUERY": target_SQL_meta_info.SQL,
                    "RESULT": self.get_formatted_execution_result(target_SQL_meta_info)
                }
                request_list.append(request_kwargs)
            except Exception as e:
                print(f"Error in Checker while creating request list: {e}")
                continue
                
        try:
            response = async_llm_chain_call(
                prompt=get_prompt(template_name=self.template_name),
                engine=get_llm_chain(**self.engine_config),
                parser=get_parser(self.parser_name),
                request_list=request_list,
                step=self.tool_name
            )
            response = [r[0] for r in response]
        except Exception as e:
            print(f"Error in Checker while getting response: {e}")
            response = []
        index = 0
        for target_SQL_meta_info in target_SQL_meta_infos:
            try:
                if target_SQL_meta_info.need_fixing:
                    refinement_response = response[index]
                    index += 1
                    if "SELECT" not in refinement_response["refined_sql_query"]:
                        refinement_response = {
                            "refined_sql_query": target_SQL_meta_info.SQL
                        }
                else:
                    refinement_response = {
                        "refined_sql_query": target_SQL_meta_info.SQL
                    }
            except Exception as e:
                print(f"Error in Checker while updating SQL meta info: {e}")
                refinement_response = {
                    "refined_sql_query": target_SQL_meta_info.SQL
                }
            if "refined_sql_query" in refinement_response:
                if refinement_response["refined_sql_query"]:
                    state.SQL_meta_infos[SQL_id].append(SQLMetaInfo(**{
                        "SQL": refinement_response["refined_sql_query"]
                    })) 

    def get_formatted_execution_result(self, target_SQL_meta_info: SQLMetaInfo) -> str:
        try:
            execution_result = target_SQL_meta_info.execution_result
            return {
                "execution_result": execution_result
            }
        except Exception as e:
            return {
                "execution_result": str(e)
            }
        
    def need_to_fix(self, state: SystemState) -> bool:  
        key_to_check = list(state.SQL_meta_infos.keys())[-1]
        SQL_meta_infos = state.SQL_meta_infos[key_to_check]
        needs_fixing = False
        for SQL_meta_info in SQL_meta_infos:
            try:
                execution_status = SQL_meta_info.execution_status
                if execution_status != ExecutionStatus.SYNTACTICALLY_CORRECT:
                    SQL_meta_info.need_fixing = True
                    needs_fixing = True
            except Exception:
                SQL_meta_info.need_fixing = True
                needs_fixing = True
                
        if self.fixing == self.max_fixing:
            return False
        self.fixing += 1

        return needs_fixing    
        
    def _get_updates(self, state: SystemState) -> Dict:
        original_SQL_id = list(state.SQL_meta_infos.keys())[-2]
        refined_SQL_id = list(state.SQL_meta_infos.keys())[-1]
        target_SQL_meta_infos = state.SQL_meta_infos[refined_SQL_id]
        candidates = []
        for target_SQL_meta_info in target_SQL_meta_infos:
            candidates.append({
                "refined_query": target_SQL_meta_info.SQL
            })
        return {
            "original_SQL_id": original_SQL_id,
            "refined_SQL_id": refined_SQL_id,
            "candidates": candidates
        }
            
    