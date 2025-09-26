import os
import typer
from dotenv import load_dotenv
from dynamo_utils import print_table_indexes, dump_table_data, save_table_data

load_dotenv()

app = typer.Typer()

# Directory constants
RAW_DATA_DIR = "data/raw"

TABLE_DYNAMO_TASKS = "GforceTasks-notow4pikzczbpjg42gytvbuci-production"
TABLE_DYNAMO_STORE = "GforceStore-notow4pikzczbpjg42gytvbuci-production"
TABLE_DYNAMO_CALLS = "GforceCallCycle-notow4pikzczbpjg42gytvbuci-production"
dynamo_tables = [TABLE_DYNAMO_STORE, TABLE_DYNAMO_CALLS, TABLE_DYNAMO_TASKS]


@app.command()
def extract():
    """Extract data from source."""
    for table_name in dynamo_tables:
        print(f"Extracting data from {table_name}...")
        items = dump_table_data(table_name, max_workers=24)
        output_file = f"{RAW_DATA_DIR}/{table_name}.json"
        save_table_data(table_name, items, output_file)
        print(f"Completed extraction for {table_name}\n")


@app.command()
def transform():
    """Transform the extracted data."""
    print("Transforming data...")


@app.command()
def load():
    """Load data into destination."""
    print("Loading data...")


@app.command()
def indexes():
    """Print indexed fields for each DynamoDB table."""
    print_table_indexes(dynamo_tables)


if __name__ == "__main__":
    app()
