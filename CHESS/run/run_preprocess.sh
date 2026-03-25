# Define variables
# source .env
# sh set_env.sh
# export DATA_MODE="dev"
# export DATA_PATH="./data/dev/dev.json"
# export DB_ROOT_DIRECTORY="./data/dev/dev_databases"
# export DATA_TABLES_PATH="./data/dev/dev_tables.json"
# export INDEX_SERVER_HOST='localhost'
# export INDEX_SERVER_PORT=12345

# export DATA_MODE="dev"
# export DATA_PATH="./spider/dev.json"
# export DB_ROOT_DIRECTORY="./spider/database/"
# export DATA_TABLES_PATH="./spider/tables.json"
# export INDEX_SERVER_HOST='localhost'
# export INDEX_SERVER_PORT=12345

## spider train
export DATA_MODE="train"
export DATA_PATH="./spider/train/train.json"
export DB_ROOT_DIRECTORY="./spider/database"
export DATA_TABLES_PATH="./spider/tables.json"
export INDEX_SERVER_HOST='localhost'
export INDEX_SERVER_PORT=12345

## spider dev
# export DATA_MODE="dev"
# export DATA_PATH="./spider/dev/dev.json"
# export DB_ROOT_DIRECTORY="./spider/dev/dev_databases"
# export DATA_TABLES_PATH="./spider/tables.json"
# export INDEX_SERVER_HOST='localhost'
# export INDEX_SERVER_PORT=12345

## spider test
# export DATA_MODE="test"
# export DATA_PATH="./spider/test/test.json"
# export DB_ROOT_DIRECTORY="./spider/test/test_databases"
# export DATA_TABLES_PATH="./spider/test_tables.json"
# export INDEX_SERVER_HOST='localhost'
# export INDEX_SERVER_PORT=12345


# spider2 
# export DATA_MODE="dev"
# export DATA_PATH="./spider2/dev/dev.json"
# export DB_ROOT_DIRECTORY="./spider2/dev/dev_databases"
# export DATA_TABLES_PATH="./spider2/tables.json"
# export INDEX_SERVER_HOST='localhost'
# export INDEX_SERVER_PORT=12345

# export OPENAI_API_KEY='sk-000cba2a211c4808a75106dda086aefd'
# export GCP_PROJECT=''
# export GCP_REGION='us-central1'
# export GCP_CREDENTIALS=''
# export GOOGLE_CLOUD_PROJECT=''

db_root_directory=$DB_ROOT_DIRECTORY # UPDATE THIS WITH THE PATH TO THE PARENT DIRECTORY OF THE DATABASES
db_id="all" # Options: all or a specific db_id
verbose=true
signature_size=100
n_gram=3
threshold=0.01

# Run the Python script with the defined variables
python3 -u ./src/preprocess.py --db_root_directory "${db_root_directory}" \
                              --signature_size "${signature_size}" \
                              --n_gram "${n_gram}" \
                              --threshold "${threshold}" \
                              --db_id "${db_id}" \
                              --verbose "${verbose}"
