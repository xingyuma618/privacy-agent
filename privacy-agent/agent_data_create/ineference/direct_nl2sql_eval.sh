RELEASE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# python direct_nl2sql_eval_schema_source.py \
#   --eval_mode direct_nl2sql \
#   --checkpoint_root ${RELEASE_ROOT}/privacy-agent/models \
#   --checkpoint_pattern "checkpoint-*" \
#   --gpus 0,1,2,3 \
#   --max_parallel 4 \
#   --base_port 2337 \
#   --server_backend lmdeploy \
#   --eval_data_base_dir ${RELEASE_ROOT}/privacy-agent/agent_data_create/data \
#   --inference_data_names bird_dev_sc \
#   --db_base_dir /root/data/nl2sql/bird/databases \
#   --ground_truth_path /mnt/wh_intern/MXY/paper/rl/ours/CHESS/data/dev/dev.sql \
#   --diff_json_path /mnt/wh_intern/MXY/paper/rl/ours/open-r1/data/bird/diff.json \
#   --schema_source_path /mnt/wh_intern/MXY/paper/rl/ours/verl/data/oiginal_data/bird/dev.json \
#   --masksql_test_path ${RELEASE_ROOT}/privacy-agent/agent_data_create/data/masksql_test.json 


# python direct_nl2sql_eval_masksql_subset.py \
#   --eval_mode direct_nl2sql \
#   --checkpoint_root ${RELEASE_ROOT}/privacy-agent/models \
#   --inference_data_names bird_dev_sc \
#   --eval_data_base_dir ${RELEASE_ROOT}/privacy-agent/agent_data_create/data \
#   --masksql_test_path ${RELEASE_ROOT}/privacy-agent/agent_data_create/data/masksql_test.json \
#   --db_base_dir /root/data/nl2sql/bird/databases \
#   --ground_truth_path /mnt/wh_intern/MXY/paper/rl/ours/CHESS/data/dev/dev.sql \
#   --diff_json_path /mnt/wh_intern/MXY/paper/rl/ours/open-r1/data/bird/diff.json

export SQL_GENERATE_API_KEY="EMPTY"
export SQL_GENERATE_MODEL_NAME="deepseek-v4-flash"
# python agent_eval_bird_full_sql_only.py \
#   --checkpoint_root ${RELEASE_ROOT}/LlamaFactory/saves/deepseek_pro \
#   --inference_data_names bird_dev_sc \
#   --eval_data_base_dir ${RELEASE_ROOT}/privacy-agent/agent_data_create/data \
#   --db_base_dir /root/data/nl2sql/bird/databases \
#   --ground_truth_path /mnt/wh_intern/MXY/paper/rl/ours/CHESS/data/dev/dev.sql

#bird
# python eval_multi_ckpt_gpu_replicas_metrics.py \
#   --gpus 0,1,2,3,4,5,6,7 \
#   --replica_schedule sequential_full_gpu \
#   --replicas_per_checkpoint 0 \
#   --per_checkpoint_workers 0 \
#   --workers_per_replica 8 \
#   --server_backend lmdeploy \
#   --checkpoint_root ${RELEASE_ROOT}/LlamaFactory/saves/deepseek_pro \
#   --eval_data_base_dir ${RELEASE_ROOT}/privacy-agent/agent_data_create/data \
#   --db_base_dir /root/data/nl2sql/bird/databases \
#   --inference_data_names bird_dev_sc \
#   --ground_truth_path /mnt/wh_intern/MXY/paper/rl/ours/CHESS/data/dev/dev.sql \
#   --base_port 2337

## spider-test
# python eval_multi_ckpt_gpu_replicas_metrics.py \
#   --gpus 0,1,2,3,4,5,6,7 \
#   --replica_schedule sequential_full_gpu \
#   --replicas_per_checkpoint 0 \
#   --per_checkpoint_workers 0 \
#   --workers_per_replica 8 \
#   --server_backend lmdeploy \
#   --checkpoint_root ${RELEASE_ROOT}/LlamaFactory/saves/deepseek_pro \
#   --eval_data_base_dir ${RELEASE_ROOT}/privacy-agent/agent_data_create/data \
#   --db_base_dir /root/data/nl2sql/spider/database \
#   --inference_data_names spider_test_sc \
#   --ground_truth_path /mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/test_gold.sql \
#   --base_port 2337

## spider-dev
python eval_multi_ckpt_gpu_replicas_metrics.py \
  --gpus 0,1 \
  --replica_schedule sequential_full_gpu \
  --replicas_per_checkpoint 0 \
  --per_checkpoint_workers 0 \
  --workers_per_replica 8 \
  --server_backend lmdeploy \
  --checkpoint_root ${RELEASE_ROOT}/LlamaFactory/saves/deepseek_pro \
  --eval_data_base_dir ${RELEASE_ROOT}/privacy-agent/agent_data_create/data \
  --db_base_dir /root/data/nl2sql/spider/database \
  --inference_data_names spider_dev_sc \
  --ground_truth_path /mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/dev_gold.sql \
  --base_port 2337