# sh set_env.sh
export DATA_MODE="dev"
export DATA_PATH="./data/dev/dev.json"
export DB_ROOT_DIRECTORY="./data/dev/dev_databases"
export DB_ROOT_PATH="./data/dev/"
export DATA_TABLES_PATH="./data/dev/dev_tables.json"
export INDEX_SERVER_HOST='localhost'
export INDEX_SERVER_PORT=12345

export OPENAI_API_KEY='sk-000cba2a211c4808a75106dda086aefd'
export GCP_PROJECT=''
export GCP_REGION='us-central1'
export GCP_CREDENTIALS=''
export GOOGLE_CLOUD_PROJECT=''

data_mode=$DATA_MODE # Options: 'dev', 'train' 
data_path=$DATA_PATH # UPDATE THIS WITH THE PATH TO THE TARGET DATASET

config="./run/configs/CHESS_IR_CG_UT.yaml"

num_workers=1 # Number of workers to use for parallel processing, set to 1 for no parallel processing

python3 -u ./src/main.py --data_mode ${data_mode} --data_path ${data_path} --config "$config" \
        --num_workers ${num_workers} --pick_final_sql true 

