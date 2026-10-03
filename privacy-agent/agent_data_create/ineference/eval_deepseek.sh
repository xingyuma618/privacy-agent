RELEASE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export SQL_GENERATE_API_KEY="EMPTY"
export SQL_GENERATE_BASE_URL="https://api.deepseek.com/v1"
export SQL_GENERATE_MODEL_NAME="deepseek-chat"



# python eval_direct_deepseek_sql_jsonlist.py \
#   --eval_data_base_dir /mnt/wh_intern/MXY/paper/rl/ours/verl/data/oiginal_data/bird \
#   --db_base_dir /root/data/nl2sql/bird/databases \
#   --inference_data_names dev.json \
#   --ground_truth_path /mnt/wh_intern/MXY/paper/rl/ours/CHESS/data/dev/dev.sql \
#   --output_dir ${RELEASE_ROOT}/privacy-agent/agent_data_create/ineference/output_direct_deepseek \
#   --max_workers 50

# python eval_direct_deepseek_sql_jsonlist.py \
#   --eval_data_base_dir ${RELEASE_ROOT}/privacy-agent/agent_data_create/data \
#   --db_base_dir /root/data/nl2sql/spider/database \
#   --inference_data_names spider_test_sc \
#   --ground_truth_path /mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/test_gold.sql \
#   --output_dir ${RELEASE_ROOT}/privacy-agent/agent_data_create/ineference/output_direct_deepseek \
#   --max_workers 50

python eval_direct_deepseek_sql_jsonlist.py \
  --eval_data_base_dir /mnt/wh_intern/MXY/paper/rl/ours/verl/data/oiginal_data/spider \
  --db_base_dir /root/data/nl2sql/spider/database \
  --inference_data_names spider_full_schema_dev \
  --ground_truth_path /mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/dev_gold.sql \
  --output_dir ${RELEASE_ROOT}/privacy-agent/agent_data_create/ineference/output_direct_deepseek \
  --max_workers 50