import os
import typer
import boto3
from dotenv import load_dotenv

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
    dynamodb = boto3.client('dynamodb')
    
    for table_name in dynamo_tables:
        print(f"\n=== {table_name} ===")
        try:
            response = dynamodb.describe_table(TableName=table_name)
            table = response['Table']
            
            # Print hash key and range key
            key_schema = table['KeySchema']
            for key in key_schema:
                key_type = key['KeyType']
                attr_name = key['AttributeName']
                print(f"{key_type}: {attr_name}")
            
            # Print Global Secondary Indexes
            if 'GlobalSecondaryIndexes' in table:
                print("Global Secondary Indexes:")
                for gsi in table['GlobalSecondaryIndexes']:
                    print(f"  Index: {gsi['IndexName']}")
                    for key in gsi['KeySchema']:
                        key_type = key['KeyType']
                        attr_name = key['AttributeName']
                        print(f"    {key_type}: {attr_name}")
            
            # Print Local Secondary Indexes
            if 'LocalSecondaryIndexes' in table:
                print("Local Secondary Indexes:")
                for lsi in table['LocalSecondaryIndexes']:
                    print(f"  Index: {lsi['IndexName']}")
                    for key in lsi['KeySchema']:
                        key_type = key['KeyType']
                        attr_name = key['AttributeName']
                        print(f"    {key_type}: {attr_name}")
                        
        except Exception as e:
            print(f"Error describing table {table_name}: {e}")


if __name__ == "__main__":
    app()
