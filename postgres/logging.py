import os
from datetime import datetime, timezone
import psycopg2



def add_log_entry(
    application_name: str,
    application_version: str | None = os.getenv("APP_VERSION"),
    status: str = "completed",
    records_processed: int = 0,
    error_message: str | None = None
):
    """Add a sample log entry to demonstrate how applications should use the logging table."""

    # Generate timestamps to simulate a sync operation
    sync_start_time = datetime.now(timezone.utc).replace(microsecond=0)
    sync_end_time = datetime.now(timezone.utc).replace(microsecond=0)

    insert_query = """
    INSERT INTO logging.data_sync_log
    (application_name, application_version, sync_start_time, sync_end_time,
     status, records_processed, error_message)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    RETURNING id;
    """

    try:
        conn = psycopg2.connect(
            host=os.getenv('PGHOST', 'localhost'),
            database=os.getenv('PGDATABASE', 'postgres'),
            user=os.getenv('PGUSER', 'postgres'),
            password=os.getenv('PGPASSWORD'),
            port=os.getenv('PGPORT', '5432')
        )

        with conn:
            with conn.cursor() as cursor:
                cursor.execute(insert_query, (
                    application_name,
                    application_version,
                    sync_start_time,
                    sync_end_time,
                    status,
                    records_processed,
                    error_message
                ))

    except psycopg2.Error as e:
        print(f"Error adding log entry: {e}")
        raise SystemExit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise SystemExit(1)
    finally:
        if 'conn' in locals():
            conn.close()
