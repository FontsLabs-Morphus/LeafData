import psycopg2
from contextlib import contextmanager
from datetime import datetime
from custom_logger.db_config import get_db_config

DB_CONFIG = get_db_config()


# DB_CONFIG = {
#     "host": "34.136.54.82",
#     "port": 5432,
#     "database": "file_transfer",
#     "user": "airflow",
#     "password": "airflow",
# }

@contextmanager
def get_db_conn():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
    finally:
        conn.close()


def log_execution(
    *,
    dag_id: str,
    task_id: str = None,
    run_id: str = None,
    status: str,
    error_source: str = None,
    error_message: str = None,
    root_cause: str = None,
):
    if execution_already_logged(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        status=status,
        error_source=error_source,
        error_message=error_message,
        root_cause=root_cause,
    ):
        # Same failure already logged → skip insert
        return

    try:
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pipeline_execution_log (
                        dag_id,
                        task_id,
                        run_id,
                        status,
                        error_source,
                        error_message,
                        root_cause,
                        end_time
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        dag_id,
                        task_id,
                        run_id,
                        status,
                        error_source,
                        error_message,
                        root_cause,
                        datetime.utcnow(),
                    ),
                )
                conn.commit()
    except psycopg2.errors.UndefinedTable:
        return
    except Exception:
        return


def execution_already_logged(
    *,
    dag_id: str,
    task_id: str,
    run_id: str,
    status: str,
    error_source: str,
    error_message: str,
    root_cause: str,
) -> bool:
    try:
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1
                    FROM pipeline_execution_log
                    WHERE dag_id = %s
                      AND task_id IS NOT DISTINCT FROM %s
                      AND run_id IS NOT DISTINCT FROM %s
                      AND status = %s
                      AND error_source IS NOT DISTINCT FROM %s
                      AND error_message IS NOT DISTINCT FROM %s
                      AND root_cause IS NOT DISTINCT FROM %s
                    LIMIT 1
                    """,
                    (
                        dag_id,
                        task_id,
                        run_id,
                        status,
                        error_source,
                        error_message,
                        root_cause,
                    ),
                )
                return cur.fetchone() is not None
    except psycopg2.errors.UndefinedTable:
        return False
    except Exception:
        return False

def log_pipeline_run_status(
    *,
    dag_id: str,
    run_id: str,
    status: str,
    execution_date=None,
    start_time=None,
    end_time=None,
    trigger_type=None,
):
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_run_status (
                    dag_id,
                    run_id,
                    status,
                    execution_date,
                    start_time,
                    end_time,
                    trigger_type,
                    last_updated
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s, NOW())
                ON CONFLICT (dag_id, run_id)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    end_time = EXCLUDED.end_time,
                    last_updated = NOW();
                """,
                (
                    dag_id,
                    run_id,
                    status,
                    execution_date,
                    start_time,
                    end_time,
                    trigger_type,
                )
            )
            conn.commit()

