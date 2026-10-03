RELEASE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# sh set_env.sh
export DATA_MODE="train"
export DATA_PATH="./data/train/train.json"
export DB_ROOT_DIRECTORY="./data/train/"
export DB_ROOT_PATH="./data/train/"
export DATA_TABLES_PATH="./data/train/train_tables.json"
export INDEX_SERVER_HOST='localhost'
export INDEX_SERVER_PORT=12345

export OPENAI_API_KEY='EMPTY'
export GCP_PROJECT=''
export GCP_REGION='us-central1'
export GCP_CREDENTIALS=''
export GOOGLE_CLOUD_PROJECT=''

data_mode=$DATA_MODE # Options: 'dev', 'train' 
data_path=$DATA_PATH # UPDATE THIS WITH THE PATH TO THE TARGET DATASET

# config="./run/configs/CHESS_IR_SS.yaml"
config="./run/configs/CHESS_IR_SS_CG.yaml"
#这个路径没有用，需要直接在${RELEASE_ROOT}/privacy-agent/CHESS/src/workflow/agents/candidate_generator/tool_kit/generate_candidate.py 里面设置
sc_data_save_path='output_data/bird_train_sc.jsonl'

num_workers=5 # Number of workers to use for parallel processing, set to 1 for no parallel processing
# sleep 7200
python3 -u ./src/main.py --data_mode ${data_mode} --data_path ${data_path} --config "$config" \
        --num_workers ${num_workers} --pick_final_sql true --sc_data_save_path $sc_data_save_path



