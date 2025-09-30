set dotenv-load

# Extract the data from Dynamo DB into files
extract:
    uv run -- python main.py extract

# Normalize the Data into a DuckDB file
transform:
    uv run -- python main.py transform

# Load the data into Postgresql
load:
    uv run -- python main.py load


duckdb:
    duckdb ./data/all.duckdb -cmd "INSTALL postgres; LOAD postgres; ATTACH '' AS postgres_db (TYPE postgres);"

psql:
    psql

fish:
    fish

pg_schema:
    pg_dump  -t task_rep_images --schema-only -d $PGDATABASE
    pg_dump  -t tasks           --schema-only -d $PGDATABASE
    pg_dump  -t task_questions  --schema-only -d $PGDATABASE

docker_run:
  docker-compose run --rm dynamo-sync python main.py watch

# Use AWS CLI to dump DynamoDB tables
aws_extract:
    mkdir -p data/raw && \
    aws dynamodb scan \
        --table-name GforceTasks-notow4pikzczbpjg42gytvbuci-production \
        --region ap-southeast-2 \
        --output json > data/raw/GforceTasks-aws.json && \
    aws dynamodb scan \
        --table-name GforceStore-notow4pikzczbpjg42gytvbuci-production \
        --region ap-southeast-2 \
        --output json > data/raw/GforceStore-aws.json && \
    aws dynamodb scan \
        --table-name GforceCallCycle-notow4pikzczbpjg42gytvbuci-production \
        --region ap-southeast-2 \
        --output json > data/raw/GforceCallCycle-aws.json





############################################################
## Get Schemas #############################################
############################################################
# Generate schema for Tasks (42 Seconds on Vale)
schema-tasks:
    mkdir -p models && \
    uv run --with datamodel-code-generator \
        datamodel-codegen \
        --input ./data/raw/GforceTasks-notow4pikzczbpjg42gytvbuci-production.json \
        --input-file-type json \
        --output models/tasks.py

# Generate schema for stores
schema-stores:
    mkdir -p models && \
    uv run --with datamodel-code-generator \
        datamodel-codegen \
        --input ./data/raw/GforceCallCycle-notow4pikzczbpjg42gytvbuci-production.json \
        --input-file-type json \
        --output models/stores.py

# Generate schema for call cycles
schema-call-cycles:
    mkdir -p models && \
    uv run --with datamodel-code-generator \
        datamodel-codegen \
        --input ./data/raw/GforceStore-notow4pikzczbpjg42gytvbuci-production.json \
        --input-file-type json \
        --output models/call_cycles.py

# Generate all schemas
schema-all: schema-tasks schema-stores schema-call-cycles


