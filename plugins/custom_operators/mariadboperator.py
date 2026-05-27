import csv
from typing import Iterable

from airflow.models import BaseOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.hooks.base import BaseHook
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
    log_destination_metrics,
)


class LoadMariaDbOperator(BaseOperator, Source):
    """
    Streaming pull from MariaDB.

    - SELECT jobs stream rows using fetchmany()
    - PROCESS jobs CALL stored procedures (no resultset expected)
    - Uses constants-based SQL formatting
    - Uses _fetch_data_stream / _transform_data_stream / _save_data_stream
    """

    def __init__(
        self,
        mariadb_conn_id: str,
        table_name: str,
        schema_name: str,
        tx_list: list[list],
        custom_query: str = None,
        source_job_type: str = None,
        header_less_file: str = None,
        headers: list[str] = None,
        fetch_size: int = 1000,
        ext: str = "csv",
        delimiter: str = ",",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.task_id = kwargs.get("task_id")
        self.mariadb_conn_id = mariadb_conn_id
        self.table_name = table_name
        self.schema_name = schema_name

        if not self.schema_name or str(self.schema_name).lower() == "none":
            conn = BaseHook.get_connection(self.mariadb_conn_id)
            if conn.schema:
                self.schema_name = conn.schema

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

        if not self.sql or not self.sql.strip():
            raise MorphusAirflowException(
                message="MariaDB SQL query cannot be empty",
                task_id=self.task_id,
                error_source="LoadMariaDbOperator.__init__",
            )

        self._db_headers: list[str] = []

    def _source_descriptor(self) -> dict:
        return {
            "source_type": "MARIADB",
            "source_location": self.schema_name,
            "source_object": self.table_name,
            "header_less_file": str(self.header_less_file).strip().lower() in ("yes", "true", "1")
            if self.header_less_file is not None
            else True,
            "delimiter": self.delimiter,
        }

    def _setup_connection(self):
        """
        Establish MariaDB streaming cursor and initialize headers eagerly.
        """
        self.log.info("Connecting to MariaDB (streaming cursor)...")

        try:
            # MariaDB is MySQL-protocol compatible, but this is a separate operator file
            hook = MySqlHook(mysql_conn_id=self.mariadb_conn_id, local_infile=True)
            conn = hook.get_conn()
            cursor = conn.cursor()
            cursor.execute(self.sql)

            if cursor.description:
                self._db_headers = [desc[0].lower() for desc in cursor.description]
            else:
                self._db_headers = []

            self.log.info(f"MariaDB headers detected: {self._db_headers}")
            return cursor, conn

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed to connect to MariaDB or execute query",
                task_id=self.task_id,
                error_source="LoadMariaDbOperator._setup_connection",
                original_exception=e,
            ) from e

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
                message="Error while fetching data from MariaDB cursor",
                task_id=self.task_id,
                error_source="LoadMariaDbOperator._fetch_data_stream",
                original_exception=e,
            ) from e

    def execute(self, context: Context) -> None:
        dag_id = context["dag"].dag_id
        run_id = context["run_id"]

        self._metrics = MetricsContext(
            dag_id=dag_id,
            run_id=run_id,
            task_id=self.task_id,
        )

        self.log.info(
            "[METRICS][INIT] metrics_context_created=True | dag_id=%s | run_id=%s | task_id=%s",
            dag_id, run_id, self.task_id
        )

        conn = None
        try:
            cursor, conn = self._setup_connection()

            if self.source_job_type == PROCESS:
                self.log.info("PROCESS job detected. Stored procedure executed successfully.")

                def row_iter():
                    yield ["status"]
                    yield ["SUCCESS"]

                self._save_data_stream(
                    context=context,
                    row_iter=row_iter(),
                    ext=self.ext,
                    delimiter=self.delimiter,
                )

                # self._metrics.finish()
                # d = self._source_descriptor()
                #
                # log_source_metrics(
                #     dag_id=self._metrics.dag_id,
                #     run_id=self._metrics.run_id,
                #     task_id=self._metrics.task_id,
                #     source_type=d["source_type"],
                #     source_location=d["source_location"],
                #     source_object=d["source_object"],
                #     header_less_file=True,
                #     delimiter=self.delimiter,
                #     cols_read=1,
                #     rows_read=1,
                #     cols_after_transform=1,
                #     rows_after_transform=1,
                #     rows_written=2,
                #     file_path=self._metrics.file_path,
                #     transform_count=0,
                #     has_expression=False,
                #     status="SUCCESS",
                #     start_time=self._metrics.start_time,
                #     end_time=self._metrics.end_time,
                #     duration_ms=self._metrics.duration_ms,
                # )

                return

            if not self._db_headers:
                raise MorphusAirflowException(
                    message="No headers detected from MariaDB cursor. Cannot transform data.",
                    task_id=self.task_id,
                    error_source="LoadMariaDbOperator.execute",
                )

            raw_iter = self._fetch_data_stream(cursor)

            transformed_iter = self._transform_data_stream(
                raw_iter=raw_iter,
                header_less_file="yes",
                headers=self._db_headers,
            )

            self._save_data_stream(
                context=context,
                row_iter=transformed_iter,
                ext=self.ext,
                delimiter=self.delimiter,
            )

            # self._metrics.finish()
            # d = self._source_descriptor()
            #
            # self.log.info(
            #     "[METRICS][PRE_DB_INSERT] "
            #     "status=SUCCESS | rows_read=%s | rows_after_transform=%s | rows_written=%s | "
            #     "cols_read=%s | cols_after_transform=%s | file_path=%s",
            #     self._metrics.rows_read,
            #     self._metrics.rows_after_transform,
            #     self._metrics.rows_written,
            #     self._metrics.cols_read,
            #     self._metrics.cols_after_transform,
            #     self._metrics.file_path,
            # )
            #
            # log_source_metrics(
            #     dag_id=self._metrics.dag_id,
            #     run_id=self._metrics.run_id,
            #     task_id=self._metrics.task_id,
            #     source_type=d["source_type"],
            #     source_location=d["source_location"],
            #     source_object=d["source_object"],
            #     header_less_file=d["header_less_file"],
            #     delimiter=d["delimiter"],
            #     cols_read=self._metrics.cols_read,
            #     rows_read=self._metrics.rows_read,
            #     cols_after_transform=self._metrics.cols_after_transform,
            #     rows_after_transform=self._metrics.rows_after_transform,
            #     rows_written=self._metrics.rows_written,
            #     file_path=self._metrics.file_path,
            #     transform_count=self._metrics.transform_count,
            #     has_expression=self._metrics.has_expression,
            #     status="SUCCESS",
            #     start_time=self._metrics.start_time,
            #     end_time=self._metrics.end_time,
            #     duration_ms=self._metrics.duration_ms,
            # )

        except MorphusAirflowException as e:
            log_execution(
                dag_id=dag_id,
                task_id=self.task_id,
                run_id=run_id,
                status="FAILED",
                error_source=e.error_source,
                error_message=str(e),
                root_cause=repr(e.original_exception),
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
                error_source="LoadMariaDbOperator.execute",
                error_message=str(e),
                root_cause=repr(e),
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
                error_source="LoadMariaDbOperator.execute",
                error_message=str(e),
            )

            raise

        finally:
            try:
                if conn:
                    conn.close()
                    self.log.info("MariaDB connection closed successfully.")
            except Exception as cleanup_error:
                self.log.warning(f"Failed to close MariaDB connection: {cleanup_error}")


class PushMariaDbOperator(BaseOperator, Destination):
    """
    Push CSV into MariaDB.

    Modes:
      - append: appends rows if table exists
      - overwrite: drops table, recreates, and loads data
    """

    def __init__(
        self,
        mariadb_conn_id: str,
        schema_name: str,
        table_name: str,
        data_write_mode: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.task_id = kwargs.get("task_id")
        self.mariadb_conn_id = mariadb_conn_id
        self.schema_name = schema_name
        self.table_name = table_name

        if not self.schema_name or str(self.schema_name).lower() == "none":
            conn = BaseHook.get_connection(self.mariadb_conn_id)
            if conn.schema:
                self.schema_name = conn.schema

        data_write_mode = data_write_mode or "append"
        self.data_write_mode = data_write_mode.lower().strip()

    def _destination_descriptor(self) -> dict:
        return {
            "destination_type": "MARIADB",
            "destination_location": self.schema_name,
            "destination_object": self.table_name,
            "write_mode": self.data_write_mode,
        }

    def _setup_connection(self):
        self.log.info("Connecting to MariaDB...")
        try:
            hook = MySqlHook(mysql_conn_id=self.mariadb_conn_id, local_infile=True)
            conn = hook.get_conn()
            cur = conn.cursor()
            cur.execute("SET GLOBAL local_infile = 1;")
            conn.commit()
            self.log.info("MariaDB connection successful.")
            return hook, conn
        except Exception as e:
            raise MorphusAirflowException(
                message="Failed to connect to MariaDB database",
                task_id=self.task_id,
                error_source="PushMariaDbOperator._setup_connection",
                original_exception=e,
            ) from e

    def _table_exists(self, hook) -> bool:
        try:
            return bool(
                hook.get_first(SQL_TABLE_EXISTS, parameters=(self.schema_name, self.table_name))
            )
        except Exception as e:
            raise MorphusAirflowException(
                message="Failed to check if MariaDB table exists",
                task_id=self.task_id,
                error_source="PushMariaDbOperator._table_exists",
                original_exception=e,
            ) from e

    def _get_table_columns(self, hook) -> list:
        try:
            sql = f"SHOW COLUMNS FROM `{self.schema_name}`.`{self.table_name}`"
            return [row[0] for row in hook.get_records(sql)]
        except Exception as e:
            raise MorphusAirflowException(
                message="Failed to fetch MariaDB table columns",
                task_id=self.task_id,
                error_source="PushMariaDbOperator._get_table_columns",
                original_exception=e,
            ) from e

    def _create_table_from_header(self, hook, header: list[str]) -> None:
        if not header:
            raise MorphusAirflowException(
                message="CSV header missing. Cannot create MariaDB table.",
                task_id=self.task_id,
                error_source="PushMariaDbOperator._create_table_from_header",
            )

        clean_header = [col.strip() for col in header if col.strip()]
        if not clean_header:
            raise MorphusAirflowException(
                message="CSV header has no valid columns.",
                task_id=self.task_id,
                error_source="PushMariaDbOperator._create_table_from_header",
            )

        cols_sql = ", ".join(f"`{col}` TEXT" for col in clean_header)

        ddl = f"""
        CREATE TABLE IF NOT EXISTS `{self.schema_name}`.`{self.table_name}` (
            {cols_sql}
        ) ENGINE=InnoDB
        ROW_FORMAT=DYNAMIC;
        """

        try:
            self.log.info(
                "Creating MariaDB table %s.%s dynamically from CSV header.",
                self.schema_name,
                self.table_name,
            )
            hook.run(ddl)
        except Exception as e:
            raise MorphusAirflowException(
                message="Failed to create MariaDB table from CSV header",
                task_id=self.task_id,
                error_source="PushMariaDbOperator._create_table_from_header",
                original_exception=e,
            ) from e

    def _drop_table(self, hook):
        try:
            sql = f"DROP TABLE IF EXISTS `{self.schema_name}`.`{self.table_name}`"
            self.log.warning(
                "Dropping MariaDB table %s.%s (overwrite mode).",
                self.schema_name,
                self.table_name,
            )
            hook.run(sql)
        except Exception as e:
            raise MorphusAirflowException(
                message="Failed to drop MariaDB table",
                task_id=self.task_id,
                error_source="PushMariaDbOperator._drop_table",
                original_exception=e,
            ) from e

    def _read_file(self, file_path, delimiter=","):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f, delimiter=delimiter)
                return [[cell.strip() for cell in row] for row in reader]
        except Exception as e:
            raise MorphusAirflowException(
                message="Failed to read CSV file for MariaDB load",
                task_id=self.task_id,
                error_source="PushMariaDbOperator._read_file",
                original_exception=e,
            ) from e

    def _upload_data(self, hook, data_filename):
        try:
            rows = self._read_file(data_filename, delimiter=",")
            if not rows:
                raise MorphusAirflowException(
                    message="CSV file is empty.",
                    task_id=self.task_id,
                    error_source="PushMariaDbOperator._upload_data",
                )

            csv_header = rows[0]

            if self.data_write_mode == "overwrite":
                self._drop_table(hook)
                self._create_table_from_header(hook, csv_header)

            elif self.data_write_mode == "append":
                if not self._table_exists(hook):
                    raise MorphusAirflowException(
                        message=(
                            f"Table `{self.schema_name}`.`{self.table_name}` "
                            f"does not exist for append mode."
                        ),
                        task_id=self.task_id,
                        error_source="PushMariaDbOperator._upload_data",
                    )
            else:
                raise MorphusAirflowException(
                    message="data_write_mode must be 'append' or 'overwrite'.",
                    task_id=self.task_id,
                    error_source="PushMariaDbOperator._upload_data",
                )

            table_columns = self._get_table_columns(hook)
            mapped_columns = [c for c in csv_header if c in table_columns]

            if not mapped_columns:
                raise MorphusAirflowException(
                    message="No matching columns between CSV and destination table.",
                    task_id=self.task_id,
                    error_source="PushMariaDbOperator._upload_data",
                )

            columns_str = ", ".join(f"`{c}`" for c in mapped_columns)

            sql = f"""
            LOAD DATA LOCAL INFILE '{data_filename}'
            INTO TABLE `{self.schema_name}`.`{self.table_name}`
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            ({columns_str});
            """

            self.log.info("Executing LOAD DATA LOCAL INFILE into MariaDB")
            hook.run(sql)
            self.log.info("Data uploaded successfully into MariaDB.")

        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed while uploading data to MariaDB",
                task_id=self.task_id,
                error_source="PushMariaDbOperator._upload_data",
                original_exception=e,
            ) from e

    def execute(self, context: Context):
        dag_id = context["dag"].dag_id
        run_id = context["run_id"]

        self._metrics = MetricsContext(
            dag_id=dag_id,
            run_id=run_id,
            task_id=self.task_id,
        )

        self.log.info(
            "[METRICS][INIT] metrics_context_created=True | dag_id=%s | run_id=%s | task_id=%s",
            dag_id, run_id, self.task_id
        )

        conn = None
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
                root_cause=repr(e.original_exception),
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
                error_source="PushMariaDbOperator.execute",
                error_message=str(e),
                root_cause=repr(e),
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
                error_source="PushMariaDbOperator.execute",
                error_message=str(e),
            )

            raise

        finally:
            try:
                if conn:
                    conn.close()
                    self.log.info("MariaDB connection closed successfully.")
            except Exception as cleanup_error:
                self.log.warning(f"Failed to close MariaDB connection: {cleanup_error}")


# Optional alias safety
LoadMariaDBOperator = LoadMariaDbOperator
PushMariaDBOperator = PushMariaDbOperator