import csv
import logging
import psycopg2
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Tuple

from exceptions.custom_exception import MorphusAirflowException

from custom_logger.db_config import get_db_config

log = logging.getLogger("airflow.task")


# METRICS_DB_CONFIG =  {
#     "host": "34.136.54.82",
#     "port": 5432,
#     "database": "file_transfer",
#     "user": "airflow",
#     "password": "airflow",
# }

METRICS_DB_CONFIG = get_db_config()

# ------------------------------------------------------------------
# DB CONNECTION (SAME PATTERN AS execution_logger.py)
# ------------------------------------------------------------------
@contextmanager
def get_metrics_db_conn():
    conn = psycopg2.connect(**METRICS_DB_CONFIG)
    try:
        log.info(
            "[METRICS][DB][CONNECT] host=%s db=%s",
            METRICS_DB_CONFIG["host"],
            METRICS_DB_CONFIG["database"],
        )
        yield conn
    finally:
        try:
            conn.close()
        finally:
            log.info("[METRICS][DB][CLOSE]")


# ------------------------------------------------------------------
# METRICS CONTEXT (IN-MEMORY COUNTERS)
# ------------------------------------------------------------------
@dataclass
class MetricsContext:
    dag_id: str
    run_id: str
    task_id: str

    # timing
    start_time: datetime = field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None

    # source metrics
    rows_read: int = 0
    cols_read: Optional[int] = None

    rows_after_transform: int = 0
    cols_after_transform: Optional[int] = None

    rows_written: int = 0
    file_path: Optional[str] = None

    # destination metrics
    rows_loaded: Optional[int] = None
    cols_loaded: Optional[int] = None
    source_file_path: Optional[str] = None

    # transform metadata
    transform_count: Optional[int] = None
    has_expression: Optional[bool] = None

    def finish(self):
        self.end_time = datetime.utcnow()

    @property
    def duration_ms(self) -> int:
        end = self.end_time or datetime.utcnow()
        return int((end - self.start_time).total_seconds() * 1000)


# ------------------------------------------------------------------
# CSV PROFILER (STREAM SAFE)
# ------------------------------------------------------------------
def csv_profile(file_path: str, delimiter: str = ",") -> Tuple[int, int]:
    """
    Returns:
        rows (int): number of data rows (excluding header)
        cols (int): number of columns (from header)
    """
    rows = 0
    cols = 0

    try:
        with open(file_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f, delimiter=delimiter)

            header = next(reader, None)
            if header:
                clean_header = [h.strip() for h in header if h.strip()]
                cols = len(clean_header)

            for _ in reader:
                rows += 1

        return rows, cols

    except Exception as e:
        log.exception("[METRICS][CSV_PROFILE][FAILED] file=%s err=%s", file_path, repr(e))
        # best-effort: do not raise
        return 0, 0


# ------------------------------------------------------------------
# SOURCE METRICS LOGGER
# ------------------------------------------------------------------
def log_source_metrics(
    *,
    dag_id: str,
    run_id: str,
    task_id: str,
    source_type: str,
    source_location: Optional[str],
    source_object: Optional[str],
    header_less_file: Optional[bool],
    delimiter: Optional[str],
    cols_read: Optional[int],
    rows_read: int,
    cols_after_transform: Optional[int],
    rows_after_transform: int,
    rows_written: int,
    file_path: Optional[str],
    transform_count: Optional[int],
    has_expression: Optional[bool],
    status: str,
    start_time: datetime,
    end_time: datetime,
    duration_ms: int,
    error_source: Optional[str] = None,
    error_message: Optional[str] = None,
):
    """
    Best-effort insert into ft_source_metrics.
    Never raises.
    """

    # Ensure optional error fields are explicitly None if missing (Option A requirement)
    error_source = error_source if error_source else None
    error_message = error_message if error_message else None

    log.info(
        "[METRICS][DB][SOURCE][PRE_INSERT] dag_id=%s task_id=%s rows_read=%s rows_written=%s status=%s",
        dag_id, task_id, rows_read, rows_written, status
    )

    try:
        with get_metrics_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ft_source_metrics (
                        event_id,
                        dag_id,
                        run_id,
                        task_id,

                        source_type,
                        source_location,
                        source_object,

                        header_less_file,
                        delimiter,

                        cols_read,
                        rows_read,

                        cols_after_transform,
                        rows_after_transform,

                        rows_written,
                        file_path,

                        transform_count,
                        has_expression,

                        status,
                        started_at,
                        ended_at,
                        duration_ms,

                        error_source,
                        error_message
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        str(uuid.uuid4()),
                        dag_id,
                        run_id,
                        task_id,

                        source_type,
                        source_location,
                        source_object,

                        header_less_file,
                        delimiter,

                        cols_read,
                        rows_read,

                        cols_after_transform,
                        rows_after_transform,

                        rows_written,
                        file_path,

                        transform_count,
                        has_expression,

                        status,
                        start_time,
                        end_time,
                        duration_ms,

                        error_source,
                        error_message,
                    )
                )
                conn.commit()

        log.info("[METRICS][DB][SOURCE][SUCCESS] inserted dag_id=%s task_id=%s", dag_id, task_id)

    except Exception as e:
        # IMPORTANT: metrics logging must never fail the pipeline
        log.exception(
            "[METRICS][DB][SOURCE][FAILED] insert failed dag_id=%s task_id=%s err=%s",
            dag_id, task_id, repr(e)
        )
        # best-effort: do not raise


# ------------------------------------------------------------------
# DESTINATION METRICS LOGGER
# ------------------------------------------------------------------
def log_destination_metrics(
    *,
    dag_id: str,
    run_id: str,
    task_id: str,

    destination_type: str,
    destination_location: Optional[str],
    destination_object: Optional[str],

    write_mode: Optional[str],

    cols_loaded: Optional[int],
    rows_loaded: Optional[int],

    source_file_path: Optional[str],

    status: str,

    start_time: datetime,
    end_time: datetime,
    duration_ms: int,

    error_source: Optional[str] = None,
    error_message: Optional[str] = None,
):
    """
    Best-effort insert into ft_destination_metrics.
    Never raises.
    """

    # Ensure optional error fields are explicitly None if missing (Option A requirement)
    error_source = error_source if error_source else None
    error_message = error_message if error_message else None

    log.info(
        "[METRICS][DB][DESTINATION][PRE_INSERT] dag_id=%s task_id=%s rows_loaded=%s status=%s",
        dag_id, task_id, rows_loaded, status
    )

    try:
        with get_metrics_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ft_destination_metrics (
                        event_id,
                        dag_id,
                        run_id,
                        task_id,

                        destination_type,
                        destination_location,
                        destination_object,
                        write_mode,

                        cols_loaded,
                        rows_loaded,
                        source_file_path,

                        status,
                        started_at,
                        ended_at,
                        duration_ms,

                        error_source,
                        error_message
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        str(uuid.uuid4()),
                        dag_id,
                        run_id,
                        task_id,

                        destination_type,
                        destination_location,
                        destination_object,
                        write_mode,

                        cols_loaded,
                        rows_loaded,
                        source_file_path,

                        status,
                        start_time,
                        end_time,
                        duration_ms,

                        error_source,
                        error_message,
                    )
                )
                conn.commit()

        log.info("[METRICS][DB][DESTINATION][SUCCESS] inserted dag_id=%s task_id=%s", dag_id, task_id)

    except Exception as e:
        log.exception(
            "[METRICS][DB][DESTINATION][FAILED] insert failed dag_id=%s task_id=%s err=%s",
            dag_id, task_id, repr(e)
        )
        # best-effort: do not raise
