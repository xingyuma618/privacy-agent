RELEASE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
python spider_eval/evaluation.py \
    --gold /mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/dev_gold.sql \
    --pred ${RELEASE_ROOT}/privacy-agent/agent_data_create/ineference/output_multi_ckpt/checkpoint-40/spider_dev_sc/agent_eval/predict.sql \
    --etype all \
    --db /root/data/nl2sql/spider/database \
    --table /mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/tables.json