

# Extract the data from Dynamo DB into files
extract:
    uv run -- python main.py extract

# Normalize the Data into a DuckDB file
transform:
    uv run -- python main.py transform

# Load the data into Postgresql
transform:
    uv run -- python main.py transform








############################################################
## Get Schemas #############################################
############################################################
# Generate schema for Tasks
schema-tasks:
    uv run --with datamodel-code-generator \
        datamodel-codegen \
        --input fixtures/tasks/1.json \
        --input-file-type json \
        --output tasks.py

# Generate schema for stores
schema-stores:
    uv run --with datamodel-code-generator \
        datamodel-codegen \
        --input fixtures/stores/1.json \
        --input-file-type json \
        --output stores.py

# Generate schema for call cycles
schema-call-cycles:
    uv run --with datamodel-code-generator \
        datamodel-codegen \
        --input fixtures/call_cycles/1.json \
        --input-file-type json \
        --output call_cycles.py

# Generate all schemas
schema-all: schema-tasks schema-stores schema-call-cycles
