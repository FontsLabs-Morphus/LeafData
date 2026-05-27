import csv
from typing import Iterable

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from custom_operators.constants import (
    NT_TRANSFORMATIONS,
    PROCESS,
    DEFAULT_STOREDPROCEDURE_QUERY_TEMPLATE,
    DEFAULT_SQL_QUERY_TEMPLATE,
    SQL_TABLE_EXISTS,
)
from custom_operators.metaclass import Source, Destination

from custom_logger.execution_logger import log_execution
from exceptions.custom_exception import MorphusAirflowException
from custom_logger.metrics_logger import (
    MetricsContext,
    log_source_metrics,
    csv_profile,
    log_destination_metrics
)

from custom_logger.logger_utils import log_dag_state


class LoadPostgresOperator(BaseOperator, Source):
    """
    Streaming pull from Postgres.

    - SELECT jobs stream rows using fetchmany() via itersize
    - PROCESS jobs CALL stored procedures (no resultset)
    - Uses constants-based SQL formatting
    - Uses _fetch_data_stream / _transform_data_stream / _save_data_stream
    """

    def __init__(
        self,
        postgres_conn_id: str,
        schema_name: str,
        table_name: str,
        header_less_file: str,
        headers: list[str],
        tx_list: list[list],
        custom_query: str = None,
        source_job_type: str = None,
        fetch_size: int = 1000,
        ext: str = "csv",
        delimiter: str = ",",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.task_id = kwargs.get("task_id")
        self.postgres_conn_id = postgres_conn_id
        self.schema_name = schema_name
        self.table_name = table_name
        self.custom_query = custom_query
        self.source_job_type = source_job_type
        self.header_less_file = header_less_file
        self.headers = headers
        self.fetch_size = fetch_size
        self.ext = ext
        self.delimiter = delimiter

        self.tx_list = [NT_TRANSFORMATIONS(*tx) for tx in tx_list]

        if self.source_job_type == PROCESS:
            self.sql = custom_query or DEFAULT_STOREDPROCEDURE_QUERY_TEMPLATE.format(
                schema_name=self.schema_name,
                table_name=self.table_name,
            )
        else:
            self.sql = custom_query or DEFAULT_SQL_QUERY_TEMPLATE.format(
                schema_name=self.schema_name,
                table_name=self.table_name,
            )

        if not self.sql.strip():
            raise MorphusAirflowException(
                message="Postgres SQL query cannot be empty.",
                task_id=self.task_id,
                error_source="LoadPostgresOperator.__init__"
            )

    def _source_descriptor(self) -> dict:
        return {
            "source_type": "POSTGRES",
            "source_location": self.schema_name,
            "source_object": self.table_name,
            "header_less_file": str(
                self.header_less_file).lower() == "yes" if self.header_less_file is not None else False,
            "delimiter": self.delimiter,
        }

    def _setup_connection(self):
        """
        Establish connection to PostgreSQL and execute query.
        Extract column headers from cursor metadata.
        """
        self.log.info("Connecting to PostgreSQL...")

        try:
            hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            conn = hook.get_conn()
            cursor = conn.cursor()

            self.log.info("Executing SQL query")
            cursor.execute(self.sql)

            if not cursor.description:
                raise MorphusAirflowException(
                    message="PostgreSQL query returned no result set",
                    task_id=self.task_id,
                    error_source="LoadPostgresOperator._setup_connection"
                )

            self._db_headers = [col.name.lower() for col in cursor.description]

            self.log.info("PostgreSQL connection and query execution successful.")
            return conn, cursor

        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed while setting up PostgreSQL connection or executing query",
                task_id=self.task_id,
                error_source="LoadPostgresOperator._setup_connection",
                original_exception=e
            ) from e

    # -----------------------------------------------------------------
    def _fetch_data_stream(self, cursor):
        try:
            while True:
                rows = cursor.fetchmany(self.fetch_size)
                if not rows:
                    break
                for row in rows:
                    yield list(row)

        except Exception as e:
            raise MorphusAirflowException(
                message="Error while fetching data stream from PostgreSQL cursor",
                task_id=self.task_id,
                error_source="LoadPostgresOperator._fetch_data_stream",
                original_exception=e
            ) from e

    def execute(self, context: Context) -> None:
        dag_id = context["dag"].dag_id
        run_id = context["run_id"]

        self._metrics = MetricsContext(
            dag_id=dag_id,
            run_id=run_id,
            task_id=self.task_id
        )

        self.log.info(
            "[METRICS][INIT] metrics_context_created=True | dag_id=%s | run_id=%s | task_id=%s",
            dag_id, run_id, self.task_id
        )

        try:
            conn, cursor = self._setup_connection()

            if self.source_job_type == PROCESS:
                self.log.info("PROCESS job detected. Stored procedure executed successfully.")

                def process_iter():
                    yield ["status"]
                    yield ["SUCCESS"]

                self._save_data_stream(
                    context=context,
                    row_iter=process_iter(),
                    ext=self.ext,
                    delimiter=self.delimiter,
                )
                return

            raw_iter = self._fetch_data_stream(cursor)

            transformed_iter = self._transform_data_stream(
                raw_iter=raw_iter,
                header_less_file=self.header_less_file,
                headers=self._db_headers,
            )

            self._save_data_stream(
                context=context,
                row_iter=transformed_iter,
                ext=self.ext,
                delimiter=self.delimiter,
            )

            self._metrics.finish()
            d = self._source_descriptor()

            self.log.info(
                "[METRICS][PRE_DB_INSERT] "
                "status=SUCCESS | rows_read=%s | rows_after_transform=%s | rows_written=%s | "
                "cols_read=%s | cols_after_transform=%s | file_path=%s",
                self._metrics.rows_read,
                self._metrics.rows_after_transform,
                self._metrics.rows_written,
                self._metrics.cols_read,
                self._metrics.cols_after_transform,
                self._metrics.file_path,
            )

            log_source_metrics(
                dag_id=self._metrics.dag_id,
                run_id=self._metrics.run_id,
                task_id=self._metrics.task_id,

                source_type=d["source_type"],
                source_location=d["source_location"],
                source_object=d["source_object"],

                header_less_file=d["header_less_file"],
                delimiter=d["delimiter"],

                cols_read=self._metrics.cols_read,
                rows_read=self._metrics.rows_read,

                cols_after_transform=self._metrics.cols_after_transform,
                rows_after_transform=self._metrics.rows_after_transform,

                rows_written=self._metrics.rows_written,
                file_path=self._metrics.file_path,

                transform_count=self._metrics.transform_count,
                has_expression=self._metrics.has_expression,

                status="SUCCESS",

                start_time=self._metrics.start_time,
                end_time=self._metrics.end_time,
                duration_ms=self._metrics.duration_ms,
            )


        except MorphusAirflowException as e:
            log_execution(
                dag_id=dag_id,
                task_id=self.task_id,
                run_id=run_id,
                status="FAILED",
                error_source=e.error_source,
                error_message=str(e),
                root_cause=repr(e.original_exception)
            )

            self._metrics.finish()
            d = self._source_descriptor()

            log_source_metrics(
                dag_id=self._metrics.dag_id,
                run_id=self._metrics.run_id,
                task_id=self._metrics.task_id,

                source_type=d["source_type"],
                source_location=d["source_location"],
                source_object=d["source_object"],

                header_less_file=d["header_less_file"],
                delimiter=d["delimiter"],

                cols_read=self._metrics.cols_read,
                rows_read=self._metrics.rows_read,

                cols_after_transform=self._metrics.cols_after_transform,
                rows_after_transform=self._metrics.rows_after_transform,

                rows_written=self._metrics.rows_written,
                file_path=self._metrics.file_path,

                transform_count=self._metrics.transform_count,
                has_expression=self._metrics.has_expression,

                status="FAILED",

                start_time=self._metrics.start_time,
                end_time=self._metrics.end_time,
                duration_ms=self._metrics.duration_ms,

                error_source=e.error_source,
                error_message=str(e),
            )

            raise

        except Exception as e:
            log_execution(
                dag_id=dag_id,
                task_id=self.task_id,
                run_id=run_id,
                status="FAILED",
                error_source="LoadPostgresOperator.execute",
                error_message=str(e),
                root_cause=repr(e)
            )

            self._metrics.finish()
            d = self._source_descriptor()

            log_source_metrics(
                dag_id=self._metrics.dag_id,
                run_id=self._metrics.run_id,
                task_id=self._metrics.task_id,

                source_type=d["source_type"],
                source_location=d["source_location"],
                source_object=d["source_object"],

                header_less_file=d["header_less_file"],
                delimiter=d["delimiter"],

                cols_read=self._metrics.cols_read,
                rows_read=self._metrics.rows_read,

                cols_after_transform=self._metrics.cols_after_transform,
                rows_after_transform=self._metrics.rows_after_transform,

                rows_written=self._metrics.rows_written,
                file_path=self._metrics.file_path,

                transform_count=self._metrics.transform_count,
                has_expression=self._metrics.has_expression,

                status="FAILED",

                start_time=self._metrics.start_time,
                end_time=self._metrics.end_time,
                duration_ms=self._metrics.duration_ms,

                error_source=e.error_source,
                error_message=str(e),
            )

            raise
        finally:
            try:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
                self.log.info("[POSTGRES_CLEANUP] Connection closed successfully.")
            except Exception as cleanup_error:
                self.log.warning(
                    f"[POSTGRES_CLEANUP] Failed to close connection: {cleanup_error}"
                )


class PushPostgresOperator(BaseOperator, Destination):
    """
    Push CSV (from XCom path) into Postgres.

    Supports 'overwrite' and 'append' modes.
    Dynamically creates table based on cleaned CSV header.
    """

    def __init__(
            self,
            postgres_conn_id: str,
            schema_name: str,
            data_write_mode: str,
            table_name: str,
            *args,
            **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.task_id = kwargs.get("task_id")
        self.postgres_conn_id = postgres_conn_id
        self.table_name = table_name
        self.schema_name = schema_name
        self.data_write_mode = data_write_mode.lower().strip()

        # Will be initialized in _setup_connection()
        self._db_headers: list[str] = []

    def _destination_descriptor(self) -> dict:
        return {
            "destination_type": "POSTGRES",
            "destination_location": self.schema_name,
            "destination_object": self.table_name,
            "write_mode": self.data_write_mode,
        }

    def _setup_connection(self) -> PostgresHook:
        """Connect to Postgres."""
        self.log.info("Connecting to Postgres Database")
        try:
            hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            conn = hook.get_conn()
            self.log.info("Connection Successful")
            return hook, conn

        except MorphusAirflowException:
            raise

        except Exception as e:
            self.log.error("Error occurred while connecting to Database")
            raise MorphusAirflowException(
                message="Failed to connect to Postgres database",
                task_id=self.task_id,
                error_source="PushPostgresOperator._setup_connection",
                original_exception=e
            ) from e

    def _table_exists(self, hook: PostgresHook) -> bool:
        """Check if table exists."""
        try:
            result = hook.get_first(SQL_TABLE_EXISTS, parameters=(self.schema_name, self.table_name))
            return bool(result and result[0])

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed while checking if Postgres table exists",
                task_id=self.task_id,
                error_source="PushPostgresOperator._table_exists",
                original_exception=e
            ) from e

    def _create_table_from_header(self, hook: PostgresHook, header: list[str]) -> None:
        """Create table dynamically from cleaned CSV header."""
        try:
            if not header:
                raise MorphusAirflowException(
                    message="CSV header is missing, cannot create table.",
                    task_id=self.task_id,
                    error_source="PushPostgresOperator._create_table_from_header"
                )

            clean_header = [col.strip().replace('"', "") for col in header if col.strip()]
            if not clean_header:
                raise MorphusAirflowException(
                    message="CSV header has no valid column names.",
                    task_id=self.task_id,
                    error_source="PushPostgresOperator._create_table_from_header"
                )

            dropped = [col for col in header if not col.strip()]
            if dropped:
                self.log.warning("Dropped empty column names from header: %s", dropped)

            cols_sql = ", ".join(f'"{col}" TEXT' for col in clean_header)
            ddl = f'CREATE TABLE IF NOT EXISTS "{self.schema_name}"."{self.table_name}" ({cols_sql})'
            self.log.info("Creating table %s.%s with columns: %s", self.schema_name, self.table_name, clean_header)
            hook.run(ddl)

        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed while creating Postgres table from CSV header",
                task_id=self.task_id,
                error_source="PushPostgresOperator._create_table_from_header",
                original_exception=e
            ) from e

    def _truncate_table(self, hook: PostgresHook) -> None:
        """Truncate table for overwrite mode."""
        try:
            self.log.info(f"Truncating table {self.schema_name}.{self.table_name} before overwrite...")
            truncate_sql = f'TRUNCATE TABLE "{self.schema_name}"."{self.table_name}"'
            hook.run(truncate_sql)

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed while truncating Postgres table",
                task_id=self.task_id,
                error_source="PushPostgresOperator._truncate_table",
                original_exception=e
            ) from e

    def _read_file(self, file_path, delimiter=","):
        """Read CSV and clean whitespace."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f, delimiter=delimiter)
                rows = [[cell.strip() for cell in row] for row in reader]
            return rows

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed while reading CSV file",
                task_id=self.task_id,
                error_source="PushPostgresOperator._read_file",
                original_exception=e
            ) from e

    def _upload_data(self, hook: PostgresHook, data_filename: str) -> None:
        """Upload CSV data to Postgres."""
        self.log.info("Uploading Data to Postgres")

        try:
            rows = self._read_file(data_filename, delimiter=",")
            if not rows:
                raise MorphusAirflowException(
                    message="CSV file is empty.",
                    task_id=self.task_id,
                    error_source="PushPostgresOperator._upload_data"
                )

            csv_header = rows[0]

            if not self._table_exists(hook):
                self._create_table_from_header(hook, csv_header)

            if self.data_write_mode == "overwrite":
                self.log.info("Overwrite mode detected.")
                self._truncate_table(hook)
            elif self.data_write_mode == "append":
                self.log.info("Append mode detected.")
            else:
                raise MorphusAirflowException(
                    message=f"Invalid data_write_mode '{self.data_write_mode}'. Must be 'overwrite' or 'append'.",
                    task_id=self.task_id,
                    error_source="PushPostgresOperator._upload_data"
                )

            clean_columns = [col.strip().replace('"', "") for col in csv_header if col.strip()]
            copy_sql = f"""
            COPY "{self.schema_name}"."{self.table_name}" ({', '.join(f'"{c}"' for c in clean_columns)})
            FROM STDIN
            WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', QUOTE '"');
            """

            self.log.info(f"Prepared COPY SQL for {self.data_write_mode} mode.")
            hook.copy_expert(sql=copy_sql, filename=data_filename)
            self.log.info("Data uploaded successfully using COPY command.")

        except MorphusAirflowException:
            raise

        except Exception as e:
            self.log.error("Error occurred while uploading data to PostgreSQL.")
            raise MorphusAirflowException(
                message="Failed while uploading data to PostgreSQL",
                task_id=self.task_id,
                error_source="PushPostgresOperator._upload_data",
                original_exception=e
            ) from e

    def execute(self, context: Context) -> None:
        """Execute task logic."""
        dag_id = context["dag"].dag_id
        run_id = context["run_id"]

        self._metrics = MetricsContext(
            dag_id=dag_id,
            run_id=run_id,
            task_id=self.task_id
        )

        self.log.info(
            "[METRICS][INIT] metrics_context_created=True | dag_id=%s | run_id=%s | task_id=%s",
            dag_id, run_id, self.task_id
        )

        try:
            hook, conn = self._setup_connection()
            data_filename = self._fetch_data(context)
            rows, cols = csv_profile(data_filename, delimiter=",")
            self._metrics.rows_loaded = rows
            self._metrics.cols_loaded = cols
            self._metrics.source_file_path = data_filename

            self.log.info(
                "[METRICS][DEST][CSV_PROFILE] rows_loaded=%s | cols_loaded=%s | file_path=%s",
                rows, cols, data_filename
            )

            self._upload_data(hook, data_filename)

            self._metrics.finish()
            d = self._destination_descriptor()

            self.log.info(
                "[METRICS][PRE_DB_INSERT][DEST] status=SUCCESS | rows_loaded=%s | cols_loaded=%s | file_path=%s",
                self._metrics.rows_loaded,
                self._metrics.cols_loaded,
                self._metrics.source_file_path,
            )

            # log_dag_state(context, "SUCCESS")

            log_destination_metrics(
                dag_id=self._metrics.dag_id,
                run_id=self._metrics.run_id,
                task_id=self._metrics.task_id,

                destination_type=d["destination_type"],
                destination_location=d["destination_location"],
                destination_object=d["destination_object"],
                write_mode=d["write_mode"],

                cols_loaded=self._metrics.cols_loaded,
                rows_loaded=self._metrics.rows_loaded,
                source_file_path=self._metrics.source_file_path,

                status="SUCCESS",

                start_time=self._metrics.start_time,
                end_time=self._metrics.end_time,
                duration_ms=self._metrics.duration_ms,
            )
            self._cleanup_temp_file(data_filename)


        except MorphusAirflowException as e:
            log_execution(
                dag_id=dag_id,
                task_id=self.task_id,
                run_id=run_id,
                status="FAILED",
                error_source=e.error_source,
                error_message=str(e),
                root_cause=repr(e.original_exception)
            )

            self._metrics.finish()
            d = self._destination_descriptor()

            log_destination_metrics(
                dag_id=self._metrics.dag_id,
                run_id=self._metrics.run_id,
                task_id=self._metrics.task_id,

                destination_type=d["destination_type"],
                destination_location=d["destination_location"],
                destination_object=d["destination_object"],
                write_mode=d["write_mode"],

                cols_loaded=self._metrics.cols_loaded,
                rows_loaded=self._metrics.rows_loaded,
                source_file_path=self._metrics.source_file_path,

                status="FAILED",

                start_time=self._metrics.start_time,
                end_time=self._metrics.end_time,
                duration_ms=self._metrics.duration_ms,

                error_source=e.error_source,
                error_message=str(e),
            )

            raise

        except Exception as e:
            log_execution(
                dag_id=dag_id,
                task_id=self.task_id,
                run_id=run_id,
                status="FAILED",
                error_source="PushPostgresOperator.execute",
                error_message=str(e),
                root_cause=repr(e)
            )

            self._metrics.finish()
            d = self._destination_descriptor()

            log_destination_metrics(
                dag_id=self._metrics.dag_id,
                run_id=self._metrics.run_id,
                task_id=self._metrics.task_id,

                destination_type=d["destination_type"],
                destination_location=d["destination_location"],
                destination_object=d["destination_object"],
                write_mode=d["write_mode"],

                cols_loaded=self._metrics.cols_loaded,
                rows_loaded=self._metrics.rows_loaded,
                source_file_path=self._metrics.source_file_path,

                status="FAILED",

                start_time=self._metrics.start_time,
                end_time=self._metrics.end_time,
                duration_ms=self._metrics.duration_ms,

                error_source=e.error_source,
                error_message=str(e),
            )

            raise
        finally:
            try:
                if conn:
                    conn.close()
                    self.log.info("[POSTGRES_CLEANUP] Connection closed successfully.")
            except Exception as cleanup_error:
                self.log.warning(
                    f"[POSTGRES_CLEANUP] Failed to close connection: {cleanup_error}"
                )


