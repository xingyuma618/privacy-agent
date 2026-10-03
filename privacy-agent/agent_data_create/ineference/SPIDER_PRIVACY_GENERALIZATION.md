# Spider Privacy Generalization Experiment

This experiment addresses the reviewer concern that detailed privacy metrics were
reported only on the 300 MaskSQL-annotated BIRD-dev examples.

## 1. Annotation Audit and Repair

The Spider MaskSQL-style files keep the real fields under `orig_*` and use
placeholders in the outer fields.  They should be used as annotation files, not
as direct inference data.

```bash
cd /mnt/wh_intern/MXY/paper/privacy-agent

python agent_data_create/ineference/spider_privacy_generalization.py audit \
  --annotations agent_data_create/data/spider_dev_sc_masksql.json \
  --output agent_data_create/ineference/output/spider_dev_privacy_annotation_audit.json

python agent_data_create/ineference/spider_privacy_generalization.py prepare \
  --data agent_data_create/data/spider_dev_sc.jsonl \
  --annotations agent_data_create/data/spider_dev_sc_masksql.json \
  --output-jsonl agent_data_create/data/spider_dev_sc_masksql_matched.jsonl \
  --report agent_data_create/ineference/output/spider_dev_masksql_match_report.json
```

The same commands can be run for `spider_test_sc`.

Current audit results:

| Split | Rows | Strict matched | Schema links | Value links | Avg. sensitive terms | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Spider-dev | 1,034 | 1,034 | 2,537 | 534 | 2.97 | 2 duplicate-surface rows |
| Spider-test | 2,147 | 2,147 | 5,176 | 1,060 | 2.90 | 18 rows with no sensitive terms |

## 2. Run Agent Inference with Spider Privacy Annotations

Start the agent, query-rewrite, entity-extraction, and SQL-generation services
first.  In the current environment, `127.0.0.1:2337`, `8.130.67.139:8036`,
`8.130.67.139:8037`, and `api.deepseek.com` were not reachable, so full Spider
inference was not executed here.

Spider-dev:

```bash
cd /mnt/wh_intern/MXY/paper/privacy-agent/agent_data_create/ineference

INFERENCE_DATA_NAMES=spider_dev_sc \
MASKSQL_TEST_PATH=/mnt/wh_intern/MXY/paper/privacy-agent/agent_data_create/data/spider_dev_sc_masksql.json \
MASKSQL_MATCH_MODE=strict \
DB_BASE_DIR=/mnt/wh_intern/MXY/paper/privacy-agent/CHESS/spider/dev/dev_databases \
SPIDER_DB_ROOT_PATH=/mnt/wh_intern/MXY/paper/privacy-agent/CHESS/spider/dev/dev_databases \
SPIDER_GOLD_PATH=/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/dev_gold.sql \
SPIDER_TABLE_PATH=/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/tables.json \
MAX_WORKERS=20 \
python agent_inference_with_privacy_eval_v4.py
```

Spider-test:

```bash
cd /mnt/wh_intern/MXY/paper/privacy-agent/agent_data_create/ineference

INFERENCE_DATA_NAMES=spider_test_sc \
MASKSQL_TEST_PATH=/mnt/wh_intern/MXY/paper/privacy-agent/agent_data_create/data/spider_test_sc_masksql.json \
MASKSQL_MATCH_MODE=strict \
DB_BASE_DIR=/mnt/wh_intern/MXY/paper/privacy-agent/CHESS/spider/test/test_databases \
SPIDER_DB_ROOT_PATH=/mnt/wh_intern/MXY/paper/privacy-agent/CHESS/spider/test/test_databases \
SPIDER_GOLD_PATH=/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/test_gold.sql \
SPIDER_TABLE_PATH=/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/test_tables.json \
MAX_WORKERS=20 \
python agent_inference_with_privacy_eval_v4.py
```

## 3. Recompute Detailed Privacy Metrics

The inference script writes `privacy_eval.json` automatically when
`MASKSQL_TEST_PATH` is set.  To recompute detailed M-P/M-R/M-F1 from an
`all_res.json` file:

```bash
python /mnt/wh_intern/MXY/paper/privacy-agent/agent_data_create/ineference/spider_privacy_generalization.py score \
  --all-res /mnt/wh_intern/MXY/paper/privacy-agent/agent_data_create/ineference/output/spider_dev_sc/agent_eval/all_res.json \
  --output /mnt/wh_intern/MXY/paper/privacy-agent/agent_data_create/ineference/output/spider_dev_sc/agent_eval/privacy_eval_with_precision.json
```

The same scorer was checked on BIRD-300 and reproduces the existing M-R exactly:
M-P 96.17, M-R 80.12, M-F1 85.81, prompt privacy score 88.21.
