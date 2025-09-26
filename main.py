import os
import typer
from dotenv import load_dotenv
from dynamo_utils import print_table_indexes

load_dotenv()

app = typer.Typer()


TABLE_DYNAMO_TASKS = "GforceTasks-notow4pikzczbpjg42gytvbuci-production"
TABLE_DYNAMO_STORE = "GforceStore-notow4pikzczbpjg42gytvbuci-production"
TABLE_DYNAMO_CALLS = "GforceCallCycle-notow4pikzczbpjg42gytvbuci-production"
dynamo_tables = [TABLE_DYNAMO_TASKS, TABLE_DYNAMO_STORE, TABLE_DYNAMO_CALLS]


@app.command()
def extract():
    """Extract data from source."""
    for table in dynamo_tables:
        print(f"Extracting data from {table}...")


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
