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
    table_name: str,
    segment: int,
    total_segments: int,
    batch_size: int,
    save_callback,
    batch_lock,
    shared_state,
) -> int:
    """Scan a segment of a DynamoDB table and save batches immediately."""
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    segment_items = 0
    scan_count = 0
    scan_kwargs = {"Segment": segment, "TotalSegments": total_segments}

    try:
        while True:
            response = table.scan(**scan_kwargs)
            batch_items = response["Items"]

            # Process items immediately and save batches as we go
            with batch_lock:
                for item in batch_items:
                    shared_state["current_batch"].append(item)
                    shared_state["total_items"] += 1
                    segment_items += 1

                    if len(shared_state["current_batch"]) >= batch_size:
                        save_callback()

            scan_count += 1

            # Progress indicator every 10 scans (roughly every 10MB of data)
            if scan_count % 10 == 0:
                print(
                    f"  Segment {segment}: {segment_items:,} items ({scan_count} scans)"
                )

            if "LastEvaluatedKey" not in response:
                break

            scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    except Exception as e:
        print(f"Error scanning segment {segment} of table {table_name}: {e}")

    return segment_items


# TODO this should be 1000 ish, set to 10 for testing
def dump_table_data(
    table_name: str,
    max_workers: int = 4,
    batch_size: int = 1000,
    output_dir: str = None,
) -> int:
    """Efficiently dump all data from a DynamoDB table using parallel scanning and batch saving."""
    start_time = time.time()
    print(
        f"Starting parallel scan of table: {table_name} ({max_workers} segments, batch size: {batch_size})"
    )

    # Use parallel scanning for better performance
    total_segments = max_workers
    completed_segments = 0
    batch_num = 0
    batch_lock = threading.Lock()

    # Shared state for all segments
    shared_state = {"current_batch": [], "total_items": 0}

    def save_current_batch():
        nonlocal batch_num
        if shared_state["current_batch"]:
            save_table_data_batch(
                table_name, shared_state["current_batch"].copy(), batch_num, output_dir
            )
            batch_num += 1
            shared_state["current_batch"].clear()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit scan tasks for each segment
        future_to_segment = {
            executor.submit(
                scan_table_segment,
                table_name,
                segment,
                total_segments,
                batch_size,
                save_current_batch,
                batch_lock,
                shared_state,
            ): segment
            for segment in range(total_segments)
        }

        # Collect results as they complete
        for future in as_completed(future_to_segment):
            segment = future_to_segment[future]
            try:
                segment_items = future.result()
                completed_segments += 1
                elapsed = time.time() - start_time
                items_per_sec = (
                    shared_state["total_items"] / elapsed if elapsed > 0 else 0
                )

                print(
                    f"✓ Segment {segment} complete: {segment_items:,} items | "
                    f"Progress: {completed_segments}/{total_segments} | "
                    f"Total: {shared_state['total_items']:,} items | "
                    f"Rate: {items_per_sec:.0f} items/sec"
                )

            except Exception as e:
                print(f"✗ Segment {segment} failed: {e}")

        # Save any remaining items in the final batch
        with batch_lock:
            save_current_batch()

    elapsed_total = time.time() - start_time
    avg_rate = shared_state["total_items"] / elapsed_total if elapsed_total > 0 else 0

    print(
        f"📊 Scan completed in {elapsed_total:.1f}s | "
        f"Total: {shared_state['total_items']:,} items | "
        f"Batches saved: {batch_num} | "
        f"Average rate: {avg_rate:.0f} items/sec"
    )

    return shared_state["total_items"]


def save_table_data_batch(
    table_name: str, items: List[Dict[str, Any]], batch_num: int, output_dir: str = None
):
    """Save a batch of table data to a JSON file."""
    if output_dir is None:
        output_dir = "data/raw"

    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{table_name}_batch_{batch_num:04d}.json")

    # Convert Decimal types to float for JSON serialization
    def decimal_default(obj):
        if hasattr(obj, "__float__"):
            return float(obj)
        raise TypeError

    with open(output_file, "w") as f:
        json.dump(items, f, default=decimal_default, indent=2)

    print(f"Saved batch {batch_num}: {len(items)} items to {output_file}")


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
    """Load raw JSON files into DuckDB tables using glob patterns."""
    # Define table patterns for the three main tables (with correct hyphens)
    table_patterns = {
        "GforceStore-notow4pikzczbpjg42gytvbuci-production": "stores_raw",
        "GforceTasks-notow4pikzczbpjg42gytvbuci-production": "tasks_raw",
        "GforceCallCycle-notow4pikzczbpjg42gytvbuci-production": "call_cycles_raw",
    }

    for table_pattern, view_name in table_patterns.items():
        # Use glob pattern to match both single files and batch files
        glob_pattern = os.path.join(raw_data_dir, f"{table_pattern}*.json")

        print(f"Loading {view_name} from pattern: {glob_pattern}")

        try:
            # Create table using glob pattern (replace hyphens with underscores for SQL table name)
            table_name = table_pattern.replace("-", "_")
            conn.execute(
                f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_json_auto('{glob_pattern}', union_by_name=true, ignore_errors=true)
            """
            )

            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"Loaded {row_count:,} rows into {view_name}")

            # Create simplified alias view
            conn.execute(
                f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {table_name}"
            )

        except Exception as e:
            print(f"Warning: Could not load {view_name} - {e}")
            # Create empty view as fallback
            conn.execute(
                f"CREATE OR REPLACE VIEW {view_name} AS SELECT NULL as placeholder WHERE FALSE"
            )


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
        if not any(
            raw_table in table_name
            for raw_table in ["GforceTasks", "GforceStore", "GforceCallCycle"]
        ) and not table_name.endswith("_raw"):

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
        if (
            not table_name.endswith("_raw")
            and not any(
                raw_table in table_name
                for raw_table in ["GforceTasks", "GforceStore", "GforceCallCycle"]
            )
            and table_name != "tmp"
        ):

            try:
                # Create table structure in PostgreSQL if it doesn't exist
                conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS postgres_db.{table_name} AS
                    (SELECT * FROM {table_name} LIMIT 0)
                """
                )

                # Replace table data in PostgreSQL
                conn.execute(
                    f"""
                    CREATE OR REPLACE TABLE postgres_db.tmp AS
                    (SELECT * FROM {table_name})
                """
                )

                row_count = conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
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
