import copy
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from database import schema_from_sqlite, execution_match, execute
from masking import mask, unmask
from search import Environment

class RegressionTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.path=Path(self.temp.name)/'test.sqlite'
        with sqlite3.connect(self.path) as conn:
            conn.executescript('CREATE TABLE a (id INTEGER PRIMARY KEY, name TEXT); CREATE TABLE b (id INTEGER, a_id INTEGER REFERENCES a(id)); INSERT INTO a VALUES (1, "Alice"), (2, "Alice"); INSERT INTO b VALUES (10,1);')
        self.schema=schema_from_sqlite(self.path)
    def tearDown(self):self.temp.cleanup()
    def test_duplicate_rows_matter(self):
        self.assertFalse(execution_match(self.path,'SELECT DISTINCT name FROM a','SELECT name FROM a'))
    def test_write_queries_rejected(self):
        with self.assertRaises(ValueError):execute(self.path,'DELETE FROM a')
        with self.assertRaises(ValueError):execute(self.path,'SELECT 1; SELECT 2')
    def test_same_named_columns_and_foreign_keys(self):
        result=mask('Find Alice', [{'entity':'Alice','entity_type':'cell_value','table':'a','column':'name','cell_value':'Alice'}],self.schema)
        self.assertIn('"col_1"',result['masked_schema_str']);self.assertIn('"col_3"',result['masked_schema_str'])
        self.assertIn('REFERENCES "table_1"("col_1")',result['masked_schema_str'])
        sql=unmask('SELECT table_1.col_1 FROM table_1 JOIN table_2 ON table_1.col_1=table_2.col_4 WHERE table_1.col_2=\'val_1\'',result['unmask_dict'])
        self.assertTrue(execution_match(self.path,sql,"SELECT a.id FROM a JOIN b ON a.id=b.a_id WHERE a.name='Alice'"))
    def test_apostrophe_value(self):
        result=mask("Find O'Brien", [{'entity':"O'Brien",'entity_type':'cell_value','table':'a','column':'name','cell_value':"O'Brien"}],self.schema)
        sql=unmask("SELECT col_1 FROM table_1 WHERE col_2='val_1'",result['unmask_dict'])
        self.assertIn("O''Brien",sql);execute(self.path,sql)
    def test_value_types_preserved(self):
        number=mask('Find 2', [{'entity':'2','entity_type':'cell_value','table':'a','column':'id','cell_value':'2'}],self.schema)
        sql=unmask("SELECT col_1 FROM table_1 WHERE col_1='val_1'",number['unmask_dict'])
        self.assertIn('= 2',sql)
        text=mask('Find 002', [{'entity':'002','entity_type':'cell_value','table':'a','column':'name','cell_value':'002'}],self.schema)
        sql=unmask("SELECT col_1 FROM table_1 WHERE col_2=val_1",text['unmask_dict'])
        self.assertIn("'002'",sql)
    def test_rollout_does_not_mutate_tree_and_search_grows(self):
        class FakeTeacher:
            def __init__(self):self.expansions=0
            def call(self,role,messages,**kw):
                if 'n' in kw:
                    self.expansions+=1
                    return [{'role':'assistant','content':'','tool_calls':[{'id':str(i),'function':{'name':'unknown','arguments':json.dumps({'branch':i})}}]} for i in range(kw['n'])]
                return [{'role':'assistant','content':'```sql\nSELECT 1\n```'}]
        teacher=FakeTeacher();config={'max_turns':20,'mcts_simulations':24,'expansion_width':3,'ucb_coefficient':1.414,'expansion_temperature':0.8,'rollout_temperature':0.0,'prompt_temperature':0.8}
        item={'question':'q','db_schema':'s','structured_db_schema':self.schema,'gold_sql':'SELECT 1','db_path':str(self.path)}
        env=Environment(item,[],teacher,config);initial=env.initial();before=copy.deepcopy(initial)
        env.rollout(initial,0);self.assertEqual(initial,before)
        env.evaluate=lambda state:{'reward':0,'accepted':False}
        env.mcts();self.assertGreater(teacher.expansions,1);self.assertGreater(env.stats['tree_nodes'],4)
    def test_every_outbound_call_is_checked(self):
        config={'max_turns':20};item={'question':'Find Alice','db_schema':'s','structured_db_schema':self.schema,'gold_sql':'SELECT 1','db_path':str(self.path)}
        env=Environment(item,[{'entity':'Alice'}],None,config);state=env.initial();state.final_sql='SELECT 1';state.outbound=[{'masked_query':'Find Alice','masked_schema_str':'s'},{'masked_query':'Find val_1','masked_schema_str':'s'}]
        result=env.evaluate(state);self.assertFalse(result['accepted']);self.assertEqual(result['direct_leaks'][0]['call'],0)

if __name__=='__main__':unittest.main()
