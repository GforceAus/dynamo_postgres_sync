import os
from pathlib import Path
import shutil
from time import sleep
import typer
from dotenv import load_dotenv
from dynamo_utils import print_table_indexes, dump_table_data, save_table_data, run_sql_transforms, sync_tables_to_postgres

load_dotenv()

app = typer.Typer()

# Directory constants
DATA_DIR = "data"
DATA_DIR_BAK = f"{DATA_DIR}_bak"
RAW_DATA_DIR = f"{DATA_DIR}/raw"
DUCKDB_PATH = f"{DATA_DIR}/all.duckdb"

TABLE_DYNAMO_TASKS = "GforceTasks-notow4pikzczbpjg42gytvbuci-production"
TABLE_DYNAMO_STORE = "GforceStore-notow4pikzczbpjg42gytvbuci-production"
TABLE_DYNAMO_CALLS = "GforceCallCycle-notow4pikzczbpjg42gytvbuci-production"
dynamo_tables = [TABLE_DYNAMO_STORE, TABLE_DYNAMO_CALLS, TABLE_DYNAMO_TASKS]

def cleanup():
    try:
        shutil.rmtree(DATA_DIR_BAK)
        shutil.move(DATA_DIR, DATA_DIR_BAK)
    except Exception:
        pass # Could be initial run


@app.command()
def extract():
    """Extract data from source."""
    # Remove any old data
    cleanup()
    for table_name in dynamo_tables:
        print(f"Extracting data from {table_name}...")
        total_items = dump_table_data(table_name, max_workers=2)
        print(f"Completed extraction for {table_name}: {total_items} items saved in batches\n")


@app.command()
def transform():
    """Transform the extracted data."""
    run_sql_transforms(raw_data_dir=RAW_DATA_DIR, duckdb_path=DUCKDB_PATH)


@app.command()
def load():
    """Load data into destination."""
    import duckdb

    # Connect to DuckDB with PostgreSQL setup
    conn = duckdb.connect(DUCKDB_PATH)

    # Setup PostgreSQL connection
    # Requires environment variables: PGPASSWORD, PGHOST, PGPORT, PGUSER, PGDATABASE
    conn.execute("INSTALL postgres")
    conn.execute("LOAD postgres")
    conn.execute("ATTACH '' AS postgres_db (TYPE postgres)")

    try:
        # Execute load SQL file
        load_sql_path = "load/load_tables.sql"
        if os.path.exists(load_sql_path):
            print("Loading tables to PostgreSQL...")
            with open(load_sql_path, "r") as f:
                sql_content = f.read()
            conn.execute(sql_content)
            print("Load completed successfully")
        else:
            print(f"Load SQL file not found: {load_sql_path}")
    except Exception as e:
        print(f"Load failed: {e}")
        raise
    finally:
        conn.close()

@app.command()
def etl():
    extract()
    transform()
    load()

@app.command()
def watch():
    WAIT_TIME = 1 * 60 * 60
    while True:
        etl()
        sleep(WAIT_TIME)


@app.command()
def indexes():
    """Print indexed fields for each DynamoDB table."""
    print_table_indexes(dynamo_tables)


if __name__ == "__main__":
    app()
