# sh set_env.sh
# export DATA_MODE="dev"
# export DATA_PATH="./spider/dev/dev.json"
# export DB_ROOT_DIRECTORY="./spider/dev"
# export DB_ROOT_PATH="./spider/dev"
# export DATA_TABLES_PATH="./spider/tables.json"
# export INDEX_SERVER_HOST='localhost'
# export INDEX_SERVER_PORT=12345

export DATA_MODE="test"
export DATA_PATH="./spider/test/test.json"
export DB_ROOT_DIRECTORY="./spider/test/"
export DB_ROOT_PATH="./spider/test/"
export DATA_TABLES_PATH="./spider/test_tables.json"
export INDEX_SERVER_HOST='localhost'
export INDEX_SERVER_PORT=12345



export OPENAI_API_KEY='sk-c695642fcd37430c94d76c9d4ab7c10d'
export GCP_PROJECT=''
export GCP_REGION='us-central1'
export GCP_CREDENTIALS=''
export GOOGLE_CLOUD_PROJECT=''

data_mode=$DATA_MODE # Options: 'dev', 'train' 
data_path=$DATA_PATH # UPDATE THIS WITH THE PATH TO THE TARGET DATASET

# config="./run/configs/CHESS_IR_SS.yaml"
config="./run/configs/CHESS_IR_SS_CG.yaml"
sc_data_save_path='output_data/spider_chess_sc_test.jsonl'

num_workers=50 # Number of workers to use for parallel processing, set to 1 for no parallel processing
# sleep 7200
python3 -u ./src/main.py --data_mode ${data_mode} --data_path ${data_path} --config "$config" \
        --num_workers ${num_workers} --pick_final_sql true --sc_data_save_path $sc_data_save_path

