from pathlib import Path
import boto3
import duckdb
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any


def print_table_indexes(table_names):
    """Print indexed fields for each DynamoDB table."""
    dynamodb = boto3.client("dynamodb")

    for table_name in table_names:
        print(f"\n=== {table_name} ===")
        try:
            response = dynamodb.describe_table(TableName=table_name)
            table = response["Table"]

            # Print hash key and range key
            key_schema = table["KeySchema"]
            for key in key_schema:
                key_type = key["KeyType"]
                attr_name = key["AttributeName"]
                print(f"{key_type}: {attr_name}")

            # Print Global Secondary Indexes
            if "GlobalSecondaryIndexes" in table:
                print("Global Secondary Indexes:")
                for gsi in table["GlobalSecondaryIndexes"]:
                    print(f"  Index: {gsi['IndexName']}")
                    for key in gsi["KeySchema"]:
                        key_type = key["KeyType"]
                        attr_name = key["AttributeName"]
                        print(f"    {key_type}: {attr_name}")

            # Print Local Secondary Indexes
            if "LocalSecondaryIndexes" in table:
                print("Local Secondary Indexes:")
                for lsi in table["LocalSecondaryIndexes"]:
                    print(f"  Index: {lsi['IndexName']}")
                    for key in lsi["KeySchema"]:
                        key_type = key["KeyType"]
                        attr_name = key["AttributeName"]
                        print(f"    {key_type}: {attr_name}")

        except Exception as e:
            print(f"Error describing table {table_name}: {e}")


def scan_table_segment(
    table_name: str, segment: int, total_segments: int
) -> List[Dict[str, Any]]:
    """Scan a segment of a DynamoDB table."""
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    items = []
    scan_count = 0
    scan_kwargs = {"Segment": segment, "TotalSegments": total_segments}

    try:
        while True:
            response = table.scan(**scan_kwargs)
            batch_items = response["Items"]
            items.extend(batch_items)
            scan_count += 1

            # Progress indicator every 10 scans (roughly every 10MB of data)
            if scan_count % 10 == 0:
                print(f"  Segment {segment}: {len(items):,} items ({scan_count} scans)")

            if "LastEvaluatedKey" not in response:
                break

            scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    except Exception as e:
        print(f"Error scanning segment {segment} of table {table_name}: {e}")

    return items


def dump_table_data(table_name: str, max_workers: int = 4) -> List[Dict[str, Any]]:
    """Efficiently dump all data from a DynamoDB table using parallel scanning."""
    start_time = time.time()
    print(f"Starting parallel scan of table: {table_name} ({max_workers} segments)")

    # Use parallel scanning for better performance
    total_segments = max_workers
    all_items = []
    completed_segments = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit scan tasks for each segment
        future_to_segment = {
            executor.submit(
                scan_table_segment, table_name, segment, total_segments
            ): segment
            for segment in range(total_segments)
        }

        # Collect results as they complete
        for future in as_completed(future_to_segment):
            segment = future_to_segment[future]
            try:
                items = future.result()
                all_items.extend(items)
                completed_segments += 1

                elapsed = time.time() - start_time
                items_per_sec = len(all_items) / elapsed if elapsed > 0 else 0

                print(
                    f"✓ Segment {segment} complete: {len(items):,} items | "
                    f"Progress: {completed_segments}/{total_segments} | "
                    f"Total: {len(all_items):,} items | "
                    f"Rate: {items_per_sec:.0f} items/sec"
                )

            except Exception as e:
                print(f"✗ Segment {segment} failed: {e}")

    elapsed_total = time.time() - start_time
    avg_rate = len(all_items) / elapsed_total if elapsed_total > 0 else 0

    print(
        f"📊 Scan completed in {elapsed_total:.1f}s | "
        f"Total: {len(all_items):,} items | "
        f"Average rate: {avg_rate:.0f} items/sec"
    )

    return all_items


def save_table_data(
    table_name: str, items: List[Dict[str, Any]], output_file: str = None
):
    """Save table data to a JSON file."""
    if output_file is None:
        output_file = f"{table_name}_data.json"

    # Create directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # Convert Decimal types to float for JSON serialization
    def decimal_default(obj):
        if hasattr(obj, "__float__"):
            return float(obj)
        raise TypeError

    with open(output_file, "w") as f:
        json.dump(items, f, default=decimal_default, indent=2)

    print(f"Saved {len(items)} items to {output_file}")


def load_json_data(conn, raw_data_dir: str):
    """Load raw JSON files into DuckDB tables."""
    json_files = [f for f in os.listdir(raw_data_dir) if f.endswith(".json")]

    for json_file in json_files:
        table_name = os.path.splitext(json_file)[0].replace("-", "_")
        json_path = os.path.join(raw_data_dir, json_file)

        # Load JSON file as a table
        conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_json_auto('{json_path}')
        """)

        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Loaded {row_count:,} rows from {json_file}")

        # Create simplified aliases for transform queries
        if "GforceStore" in table_name:
            conn.execute(f"CREATE OR REPLACE VIEW stores_raw AS SELECT * FROM {table_name}")
        elif "GforceTasks" in table_name:
            conn.execute(f"CREATE OR REPLACE VIEW tasks_raw AS SELECT * FROM {table_name}")
        elif "GforceCallCycle" in table_name:
            conn.execute(f"CREATE OR REPLACE VIEW call_cycles_raw AS SELECT * FROM {table_name}")


def execute_sql_models(conn, models_dir: str, transform_dir: str = "transform"):
    """Execute SQL model files and transform files in order."""

    # Run model files (create tables)
    model_files = ["stores.sql", "call_cycles.sql", "tasks.sql"]
    print("Creating tables...")
    for sql_file in model_files:
        sql_path = os.path.join(models_dir, sql_file)
        if os.path.exists(sql_path):
            with open(sql_path, "r") as f:
                sql_content = f.read()
            try:
                conn.execute(sql_content)
                print(f"Created tables from {sql_file}")
            except Exception as e:
                print(f"Error in {sql_file}: {e}")

    # Run transform files (normalize data)
    transform_files = [
        "normalize_stores.sql",
        "normalize_call_cycles.sql",
        "normalize_tasks.sql",
    ]
    print("Normalizing data...")
    for sql_file in transform_files:
        sql_path = os.path.join(transform_dir, sql_file)
        if os.path.exists(sql_path):
            with open(sql_path, "r") as f:
                sql_content = f.read()
            try:
                conn.execute(sql_content)
                print(f"Processed {sql_file}")
            except Exception as e:
                print(f"Error in {sql_file}: {e}")


def export_transformed_tables(conn, output_dir: str):
    """Export transformed tables to Parquet files."""
    tables = conn.execute("SHOW TABLES").fetchall()
    exported_count = 0

    for table_row in tables:
        table_name = table_row[0]

        # Skip raw data tables and views
        if (not any(raw_table in table_name for raw_table in ["GforceTasks", "GforceStore", "GforceCallCycle"])
            and not table_name.endswith("_raw")):

            output_path = os.path.join(output_dir, f"{table_name}.parquet")
            conn.execute(f"COPY {table_name} TO '{output_path}' (FORMAT PARQUET)")

            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            if row_count > 0:
                exported_count += 1
                print(f"Exported {table_name}: {row_count:,} rows")

    print(f"Export completed: {exported_count} tables")


def sync_tables_to_postgres(conn):
    """
    Sync DuckDB tables to PostgreSQL.

    This is not used, instead I wrote ./load/load_tables.sql
    because I wanted to append the tasks and photo table, just in case the data upstream was dropped.
    """
    tables = conn.execute("SHOW TABLES").fetchall()
    synced_count = 0

    print("Syncing tables to PostgreSQL...")
    for table_row in tables:
        table_name = table_row[0]

        # Skip raw data tables, views, and temporary tables
        if (not table_name.endswith("_raw")
            and not any(raw_table in table_name for raw_table in ["GforceTasks", "GforceStore", "GforceCallCycle"])
            and table_name != "tmp"):

            try:
                # Create table structure in PostgreSQL if it doesn't exist
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS postgres_db.{table_name} AS
                    (SELECT * FROM {table_name} LIMIT 0)
                """)

                # Replace table data in PostgreSQL
                conn.execute(f"""
                    CREATE OR REPLACE TABLE postgres_db.tmp AS
                    (SELECT * FROM {table_name})
                """)

                row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                if row_count > 0:
                    synced_count += 1
                    print(f"Synced {table_name}: {row_count:,} rows")

            except Exception as e:
                print(f"Error syncing {table_name}: {e}")

    print(f"Sync completed: {synced_count} tables")


def run_sql_transforms(
    raw_data_dir: str = "data/raw",
    models_dir: str = "models",
    output_dir: str | None = "data/transformed",
    duckdb_path: str = "data/all.duckdb",
):
    """Run SQL transformations on raw JSON data using DuckDB."""
    print("Starting data transformation...")

    # Create directories
    duckdb_dir = os.path.dirname(duckdb_path)
    if duckdb_dir:
        os.makedirs(duckdb_dir, exist_ok=True)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # Connect to DuckDB
    conn = duckdb.connect(duckdb_path)

    # Setup PostgreSQL connection
    # Requires environment variables: PGPASSWORD, PGHOST, PGPORT, PGUSER, PGDATABASE
    conn.execute("INSTALL postgres")
    conn.execute("LOAD postgres")
    conn.execute("ATTACH '' AS postgres_db (TYPE postgres)")

    try:
        load_json_data(conn, raw_data_dir)
        execute_sql_models(conn, models_dir)
        if output_dir:
            export_transformed_tables(conn, output_dir)
        print("Transformation completed successfully")

    except Exception as e:
        print(f"Transformation failed: {e}")
        raise
    finally:
        conn.close()

