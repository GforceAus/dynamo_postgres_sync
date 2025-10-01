import os
from datetime import datetime, timezone
import psycopg2
from typing import Optional


class SyncLogger:
    """Logger for tracking sync operations with start/end times."""
    
    def __init__(self, application_name: str, application_version: str | None = None):
        self.application_name = application_name
        self.application_version = application_version or os.getenv("APP_VERSION", "1.0.0")
        self.sync_start_time = None
        self.log_id = None
    
    def start_sync(self) -> int:
        """Start a new sync operation and return the log ID."""
        self.sync_start_time = datetime.now(timezone.utc).replace(microsecond=0)
        
        insert_query = """
        INSERT INTO logging.data_sync_log
        (application_name, application_version, sync_start_time, status)
        VALUES (%s, %s, %s, %s)
        RETURNING id;
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
            print(f"Error starting sync log: {e}")
            raise
        finally:
            if 'conn' in locals():
                conn.close()
    
    def end_sync(self, status: str = "completed", records_processed: int = 0, error_message: str | None = None):
        """End the sync operation and update the log entry."""
        if not self.log_id:
            print("Warning: No sync started, cannot end sync")
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
            print(f"Error ending sync log: {e}")
            raise
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
