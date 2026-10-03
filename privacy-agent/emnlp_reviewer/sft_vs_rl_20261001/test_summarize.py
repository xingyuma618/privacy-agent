import contextlib, hashlib, io, json, tempfile, unittest
from pathlib import Path
from unittest import mock
import summarize

def request(role="agent",endpoint="local",model="m"):
 return {"role":role,"endpoint":endpoint,"request_model":model,"response_model":model,"ok":True,"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2},"messages":[]}
def row(cid="bird_1",status="valid",requests=None):
 requests=requests if requests is not None else [request()]; cloud=[x for x in requests if x["role"]=="sql_generator"]
 return {"case_id":cid,"question_id":cid,"execution_correct":0 if status!="valid" else 1,"annotated_terms":["name"],"masked_terms":["name"],"cost_tokens":2,"cloud_requests":cloud,"all_model_requests":requests,"trace_complete":True,"task_status":status}

class MetricsTest(unittest.TestCase):
 def test_legacy_preserves_duplicate_gold_bounded_deduplicates(self):
  r=row(); r["annotated_terms"]=["name"," name ","full name"]; r["masked_terms"]=["full name"]
  m=summarize.case_metrics(r); self.assertEqual(m["legacy_MR"],1); self.assertEqual(m["bounded_MR"],.5); self.assertEqual(m["bounded_MP"],1)
 def test_bad_gold_rejected(self):
  r=row(); r["annotated_terms"]="name"
  with self.assertRaisesRegex(ValueError,"annotated_terms"): summarize.case_metrics(r)
 def test_loader_rejects_bad_id_and_request(self):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/"x"; r=row(); r["question_id"]="wrong"; p.write_text(json.dumps(r)+"\n")
   with self.assertRaisesRegex(ValueError,"canonical ID"): summarize.load_run(p,["bird_1"],"http://127.0.0.1:18342/v1")
   r=row(); r["all_model_requests"]=[{"ok":True}]; p.write_text(json.dumps(r)+"\n")
   with self.assertRaisesRegex(ValueError,"model request"): summarize.load_run(p,["bird_1"],"http://127.0.0.1:18342/v1")
 def test_infrastructure_error_retained(self):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/"x"; p.write_text(json.dumps(row(status="infrastructure_error"))+"\n")
   self.assertEqual(summarize.load_run(p,["bird_1"],"http://127.0.0.1:18342/v1")["bird_1"]["EX"],0)
 def test_maximum_matching_does_not_reuse_prediction(self): self.assertEqual(summarize.maximum_matching(["a","a b"],["a b"]),1)
 def test_role_usage_metrics(self):
  r=row(); r["all_model_requests"]=[request("agent"),request("sql_generator")]; m=summarize.case_metrics(r)
  self.assertEqual(m["calls_agent"],1); self.assertEqual(m["total_tokens_sql_generator"],2); self.assertEqual(m["calls_entity_extract"],0)

class RecoveryTest(unittest.TestCase):
 def build(self,root):
  source=root/"source"; source.mkdir(); driver=root/"run_missing_recovery.py"; driver.write_text("frozen driver\n")
  retained=[f"bird_x{i}" for i in range(299)]; expected=retained+["bird_122"]
  source_rows=[{"case_id":cid,"question_id":cid,"payload":i} for i,cid in enumerate(retained)]
  (source/"cases.jsonl").write_text("".join(json.dumps(x)+"\n" for x in source_rows))
  frozen={"model":"sft","run":1,"config_hash":"cfg","manifest_sha256":"manifest","model_identity":{"path":"s"},"fresh_output_dir":True,"workers":8,"limit":0,"runner_sha256":"runner","legacy_evaluator_sha256":"legacy","prompt_template_sha256":{"a":"b"},"question_ids":expected}
  source_meta={**frozen,"status":"running","completed_cases":299,"started_unix":10.0}; (source/"metadata.json").write_text(json.dumps(source_meta))
  sha=lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
  provenance={"source_directory":str(source.resolve()),"source_cases_sha256":sha(source/"cases.jsonl"),"source_metadata_sha256":sha(source/"metadata.json"),"retained_question_ids":retained,"missing_question_ids":["bird_122"],"driver_sha256":sha(driver),"source_model":"sft","source_run":1}
  meta={**frozen,"status":"complete","completed_cases":300,"started_unix":10.0,"recovery_started_unix":20.0,"inference_origin":"segmented_recovery","recovery_provenance":provenance}
  return source_rows+[{"case_id":"bird_122","question_id":"bird_122","payload":"new"}],meta,expected,driver,source
 def test_rejects_tampered_retained_row_and_cross_run_source(self):
  with tempfile.TemporaryDirectory() as d:
   rows,meta,ids,driver,source=self.build(Path(d)); self.assertEqual(summarize.validate_recovery(rows,meta,ids,"sft",1,driver,source)["retained_cases"],299)
   tampered=[dict(x) for x in rows]; tampered[0]["payload"]="changed"
   with self.assertRaisesRegex(ValueError,"retained recovery row differs"): summarize.validate_recovery(tampered,meta,ids,"sft",1,driver,source)
   crossed=json.loads(json.dumps(meta)); crossed["recovery_provenance"]["source_run"]=2
   with self.assertRaisesRegex(ValueError,"cross-repetition"): summarize.validate_recovery(rows,crossed,ids,"sft",1,driver,source)

class EndToEndTest(unittest.TestCase):
 def build(self,root,bad_meta=None,bad_id=False,infra=False):
  ann=root/"ann.json"; ann.write_text(json.dumps([{"idx":"bird_1"},{"idx":"bird_2"}]))
  models={"sft":{"path":"s","base_url":"http://127.0.0.1:18340/v1","served_model_name":"sft-alias"},"rl":{"path":"r","base_url":"http://127.0.0.1:18341/v1","served_model_name":"rl-alias"}}
  inference={"cloud_generator":{"base_url":"http://127.0.0.1:18342/v1","model_name":"sql-alias"},"tools_checkpoint":{"query_rewrite_base_url":"http://127.0.0.1:18336/v1","query_rewrite_model":"rewrite-alias","entity_extract_base_url":"http://127.0.0.1:18337/v1","entity_extract_model":"extract-alias"}}
  manifest=root/"manifest.json"; manifest.write_text(json.dumps({"dataset":{"masksql_annotations":str(ann),"expected_cases":2},"models":models,"inference":inference})); mh=hashlib.sha256(manifest.read_bytes()).hexdigest()
  identities={"sft":{"path":"s"},"rl":{"path":"r"}}; pf=root/"pf.json"; pf.write_text(json.dumps({"status":"ready","artifacts":{"manifest_sha256":mh},"models":identities}))
  results=root/"results"
  for model in ("sft","rl"):
   for run in range(1,4):
    d=results/model/f"run-{run}"; d.mkdir(parents=True); ids=["bird_1","bird_2"]
    meta={"status":"complete","completed_cases":2,"config_hash":"same","manifest_sha256":mh,"model_identity":identities[model],"fresh_output_dir":True,"model":model,"run":run,"question_ids":ids,"workers":(8 if run==1 else 16),"limit":0,"runner_sha256":"runner","legacy_evaluator_sha256":"legacy","prompt_template_sha256":{"query_rewrite_template.py":"q","entity_extract_template.py":"e","sql_generate_template.py":"s"}}
    if run==2: meta["agent_routing"]={"logical_endpoint":models[model]["base_url"],"actual_endpoint":summarize.REPLICA_ENDPOINTS[model],"checkpoint_path":models[model]["path"],"wrapper_sha256":hashlib.sha256((Path(summarize.__file__).parent/"run_replica_eval.py").read_bytes()).hexdigest(),"verified_served_model":models[model]["served_model_name"],"serving_origin":"approved_local_replica_same_checkpoint"}
    if bad_meta and model=="sft" and run==1: meta.update(bad_meta)
    (d/"metadata.json").write_text(json.dumps(meta)); endpoint=summarize.REPLICA_ENDPOINTS[model] if run==2 else models[model]["base_url"]
    requests=[request("agent",endpoint,models[model]["served_model_name"]),request("query_rewrite",inference["tools_checkpoint"]["query_rewrite_base_url"],inference["tools_checkpoint"]["query_rewrite_model"]),request("entity_extract",inference["tools_checkpoint"]["entity_extract_base_url"],inference["tools_checkpoint"]["entity_extract_model"]),request("sql_generator",inference["cloud_generator"]["base_url"],inference["cloud_generator"]["model_name"])]
    rows=[row(x,"infrastructure_error" if infra and x=="bird_1" else "valid",requests) for x in ids]
    if bad_id and model=="sft" and run==1: rows[0]["case_id"]=rows[0]["question_id"]="bird_999"
    (d/"cases.jsonl").write_text("".join(json.dumps(x)+"\n" for x in rows))
  return results,manifest,pf
 def invoke(self,results,manifest,pf):
  argv=["summarize.py",str(results),"--manifest",str(manifest),"--preflight",str(pf),"--bootstrap","20"]
  with mock.patch("sys.argv",argv),contextlib.redirect_stdout(io.StringIO()) as out: summarize.main()
  return json.loads(out.getvalue())
 def test_synthetic_end_to_end_and_blocked_infra(self):
  with tempfile.TemporaryDirectory() as d:
   args=self.build(Path(d)); report=self.invoke(*args); self.assertEqual(report["status"],"complete"); self.assertEqual(report["n_cases"],2); self.assertEqual(report["worker_schedule"],[8,16,16]); self.assertIn("sample_sd",report["overall_across_run_means"]["sft"]["EX"])
  with tempfile.TemporaryDirectory() as d:
   args=self.build(Path(d),infra=True); report=self.invoke(*args); self.assertEqual(report["status"],"blocked_infrastructure_errors"); self.assertFalse(report["formal_conclusion_allowed"])
 def test_rejects_bad_metadata_and_ids(self):
  with tempfile.TemporaryDirectory() as d:
   args=self.build(Path(d),bad_meta={"model_identity":{"path":"wrong"}})
   with self.assertRaisesRegex(ValueError,"identity"): self.invoke(*args)
  with tempfile.TemporaryDirectory() as d:
   args=self.build(Path(d),bad_id=True)
   with self.assertRaisesRegex(ValueError,"canonical IDs"): self.invoke(*args)
 def test_rejects_paired_worker_mismatch(self):
  with tempfile.TemporaryDirectory() as d:
   args=self.build(Path(d)); meta_path=args[0]/"rl"/"run-2"/"metadata.json"; meta=json.loads(meta_path.read_text()); meta["workers"]=8; meta_path.write_text(json.dumps(meta))
   with self.assertRaisesRegex(ValueError,"paired SFT/RL workers mismatch"): self.invoke(*args)
 def test_rejects_replica_provenance_and_hidden_routing(self):
  with tempfile.TemporaryDirectory() as d:
   args=self.build(Path(d)); meta_path=args[0]/"sft"/"run-2"/"metadata.json"; meta=json.loads(meta_path.read_text()); meta["agent_routing"]["checkpoint_path"]="different-weights"; meta_path.write_text(json.dumps(meta))
   with self.assertRaisesRegex(ValueError,"provenance mismatch"): self.invoke(*args)
  with tempfile.TemporaryDirectory() as d:
   args=self.build(Path(d)); cases=args[0]/"rl"/"run-2"/"cases.jsonl"; rows=[json.loads(x) for x in cases.read_text().splitlines()]; rows[0]["all_model_requests"][0]["endpoint"]="https://external.invalid/v1"; cases.write_text("".join(json.dumps(x)+"\n" for x in rows))
   with self.assertRaisesRegex(ValueError,"agent routing/model identity"): self.invoke(*args)
  with tempfile.TemporaryDirectory() as d:
   args=self.build(Path(d)); cases=args[0]/"sft"/"run-3"/"cases.jsonl"; rows=[json.loads(x) for x in cases.read_text().splitlines()]; rows[0]["all_model_requests"].append(request("hidden_role","https://external.invalid/v1","hidden")); cases.write_text("".join(json.dumps(x)+"\n" for x in rows))
   with self.assertRaisesRegex(ValueError,"unknown or hidden"): self.invoke(*args)
 def test_failed_budget_call_allows_no_response_model_but_success_rejects_wrong_alias(self):
  with tempfile.TemporaryDirectory() as d:
   args=self.build(Path(d)); cases=args[0]/"sft"/"run-1"/"cases.jsonl"; rows=[json.loads(x) for x in cases.read_text().splitlines()]
   rows[0]["execution_correct"]=0; rows[0]["all_model_requests"][0].update(ok=False,response_model=None,error_type="BadRequestError",status_code=400,error_summary="model budget exceeded")
   cases.write_text("".join(json.dumps(x)+"\n" for x in rows)); report=self.invoke(*args); self.assertEqual(report["status"],"complete")
  with tempfile.TemporaryDirectory() as d:
   args=self.build(Path(d)); cases=args[0]/"rl"/"run-3"/"cases.jsonl"; rows=[json.loads(x) for x in cases.read_text().splitlines()]
   rows[0]["all_model_requests"][0]["response_model"]="wrong-success-alias"; cases.write_text("".join(json.dumps(x)+"\n" for x in rows))
   with self.assertRaisesRegex(ValueError,"agent routing/model identity"): self.invoke(*args)
if __name__=="__main__": unittest.main()
