import os
from datetime import datetime, timezone
import psycopg2
from typing import Optional


class SyncLogger:
    """
    Logger for tracking sync operations with start/end times.
    
    Note: When creating the logging.data_sync_log table, partition by sync_start_time
    rather than sync_end_time since start time is always known when the log entry
    is created, while end time may be null for failed/incomplete syncs.
    """
    
    def __init__(self, application_name: str, application_version: str | None = None):
        self.application_name = application_name
        self.application_version = application_version or os.getenv("APP_VERSION", "1.0.0")
        self.sync_start_time = None
        self.log_id = None
    
    def start_sync(self) -> Optional[int]:
        """Start a new sync operation and return the log ID. Returns None if logging fails."""
        self.sync_start_time = datetime.now(timezone.utc).replace(microsecond=0)
        
        insert_query = """
        INSERT INTO logging.data_sync_log
        (application_name, application_version, sync_start_time, status)
        VALUES (%s, %s, %s, %s)
        RETURNING id;
        -- Note: Table should be partitioned by sync_start_time for optimal performance
        """
        
        try:
            conn = self._get_connection()
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute(insert_query, (
                        self.application_name,
                        self.application_version,
                        self.sync_start_time,
                        "running"
                    ))
                    self.log_id = cursor.fetchone()[0]
                    print(f"Started sync operation (log_id: {self.log_id})")
                    return self.log_id
        except psycopg2.Error as e:
            print(f"⚠️  WARNING: Failed to log sync start to database: {e}")
            print(f"⚠️  PostgreSQL Error Details: {e.pgcode} - {e.pgerror}" if hasattr(e, 'pgcode') else "")
            print(f"⚠️  Continuing without database logging...")
            return None
        except Exception as e:
            print(f"⚠️  WARNING: Unexpected error starting sync log: {e}")
            print(f"⚠️  Connection details: host={os.getenv('PGHOST', 'localhost')}, db={os.getenv('PGDATABASE', 'postgres')}, user={os.getenv('PGUSER', 'postgres')}")
            print(f"⚠️  Continuing without database logging...")
            return None
        finally:
            if 'conn' in locals():
                conn.close()
    
    def end_sync(self, status: str = "completed", records_processed: int = 0, error_message: str | None = None):
        """End the sync operation and update the log entry."""
        if not self.log_id:
            print("⚠️  WARNING: No sync was logged to database, cannot update end time")
            print(f"📊 Sync completed locally: status={status}, records={records_processed}")
            if error_message:
                print(f"❌ Error details: {error_message}")
            return
        
        sync_end_time = datetime.now(timezone.utc).replace(microsecond=0)
        
        update_query = """
        UPDATE logging.data_sync_log
        SET sync_end_time = %s, status = %s, records_processed = %s, error_message = %s
        WHERE id = %s;
        """
        
        try:
            conn = self._get_connection()
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute(update_query, (
                        sync_end_time,
                        status,
                        records_processed,
                        error_message,
                        self.log_id
                    ))
                    duration = (sync_end_time - self.sync_start_time).total_seconds()
                    print(f"Ended sync operation (log_id: {self.log_id}, duration: {duration:.1f}s, status: {status})")
        except psycopg2.Error as e:
            print(f"⚠️  WARNING: Failed to log sync end to database: {e}")
            print(f"⚠️  PostgreSQL Error Details: {e.pgcode} - {e.pgerror}" if hasattr(e, 'pgcode') else "")
            duration = (sync_end_time - self.sync_start_time).total_seconds() if self.sync_start_time else 0
            print(f"📊 Sync completed locally: log_id={self.log_id}, duration={duration:.1f}s, status={status}")
            if error_message:
                print(f"❌ Error details: {error_message}")
        except Exception as e:
            print(f"⚠️  WARNING: Unexpected error ending sync log: {e}")
            duration = (sync_end_time - self.sync_start_time).total_seconds() if self.sync_start_time else 0
            print(f"📊 Sync completed locally: log_id={self.log_id}, duration={duration:.1f}s, status={status}")
            if error_message:
                print(f"❌ Error details: {error_message}")
        finally:
            if 'conn' in locals():
                conn.close()
    
    def _get_connection(self):
        """Get a PostgreSQL connection."""
        return psycopg2.connect(
            host=os.getenv('PGHOST', 'localhost'),
            database=os.getenv('PGDATABASE', 'postgres'),
            user=os.getenv('PGUSER', 'postgres'),
            password=os.getenv('PGPASSWORD'),
            port=os.getenv('PGPORT', '5432')
        )


def add_log_entry(
    application_name: str,
    application_version: str | None = os.getenv("APP_VERSION"),
    status: str = "completed",
    records_processed: int = 0,
    error_message: str | None = None
):
    """Add a complete log entry (legacy function for backward compatibility)."""
    logger = SyncLogger(application_name, application_version)
    logger.start_sync()
    logger.end_sync(status, records_processed, error_message)
