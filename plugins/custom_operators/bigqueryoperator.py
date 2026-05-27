import csv
import json
import os
from typing import TYPE_CHECKING, Iterable

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.context import Context
from custom_operators.constants import NT_TRANSFORMATIONS
from custom_operators.constants import (
    NT_TRANSFORMATIONS,
    PROCESS,
    BIGQUERY_DEFAULT_SQL_QUERY_TEMPLATE,
    BIGQUERY_DEFAULT_STOREDPROCEDURE_QUERY_TEMPLATE,
)
from custom_operators.metaclass import Source, Destination
from google.cloud import bigquery

from exceptions.custom_exception import MorphusAirflowException

from custom_logger.execution_logger import log_execution

from custom_logger.metrics_logger import (
    MetricsContext,
    log_source_metrics,
    csv_profile,
    log_destination_metrics
)

if TYPE_CHECKING:
    from google.cloud.bigquery.table import RowIterator


class PullBigQueryOperator(BaseOperator, Source):
    """
    Streaming pull from BigQuery.

    - SELECT jobs stream rows row-by-row
    - PROCESS jobs CALL stored procedures (no resultset)
    - Uses constants-based SQL formatting (MSSQL-style)
    """

    def __init__(
            self,
            bigquery_conn_id: str,
            dataset: str,
            table: str,
            header_less_file: str,
            headers: list[str],
            tx_list: list[list],
            custom_query: str = None,
            source_job_type: str = None,
            ext: str = "csv",
            delimiter: str = ",",
            *args,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.task_id = kwargs.get("task_id")
        self.bigquery_conn_id = bigquery_conn_id
        self.dataset = dataset
        self.table = table
        self.header_less_file = header_less_file
        self.headers = headers
        self.source_job_type = source_job_type
        self.ext = ext
        self.delimiter = delimiter
        self.custom_query = custom_query

        self.tx_list = [NT_TRANSFORMATIONS(*tx) for tx in tx_list]

        if self.source_job_type == PROCESS:
            self.sql = custom_query or BIGQUERY_DEFAULT_STOREDPROCEDURE_QUERY_TEMPLATE.format(
                schema_name=self.dataset,
                table_name=self.table,
            )
        else:
            self.sql = custom_query or BIGQUERY_DEFAULT_SQL_QUERY_TEMPLATE.format(
                schema_name=self.dataset,
                table_name=self.table,
            )

        if not self.sql.strip():
            raise ValueError("BigQuery SQL query cannot be empty.")

        # Will be initialized in _setup_connection()
        self._db_headers: list[str] = []

    def _source_descriptor(self) -> dict:
        return {
            "source_type": "BIGQUERY",
            "source_location": self.dataset,
            "source_object": self.table,
            "header_less_file": True,
            "delimiter": None,
        }

    def _setup_connection(self) -> "Client":
        self.log.info("Connecting to BigQuery...")
        try:
            hook = BigQueryHook(gcp_conn_id=self.bigquery_conn_id)
            client = hook.get_client()
            self.log.info("BigQuery connection successful.")
            return client

        except MorphusAirflowException:
            raise

        except Exception as e:
            self.log.error("Failed to connect to BigQuery")
            raise MorphusAirflowException(
                message="Failed to connect to BigQuery",
                task_id=self.task_id,
                error_source="PullBigQueryOperator._setup_connection",
                original_exception=e,
            ) from e

    def _prepare_query(self, client):
        self.log.info(f"Executing BigQuery SQL: {self.sql}")
        try:
            query_job = client.query(self.sql)
            results = query_job.result()

            if not results.schema:
                raise MorphusAirflowException(
                    message="BigQuery query returned no schema",
                    task_id=self.task_id,
                    error_source="PullBigQueryOperator._prepare_query",
                )

            self._db_headers = [field.name for field in results.schema]
            return results

        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed while executing BigQuery query",
                task_id=self.task_id,
                error_source="PullBigQueryOperator._prepare_query",
                original_exception=e,
            ) from e

    def _fetch_data_stream(self, results):
        try:
            for row in results:
                out_row = []
                for col in self._db_headers:
                    val = row[col]
                    if isinstance(val, list):
                        val = ",".join(map(str, val))
                    elif isinstance(val, dict):
                        val = json.dumps(val)
                    out_row.append(val)
                yield out_row

        except Exception as e:
            raise MorphusAirflowException(
                message="Error while streaming data from BigQuery results",
                task_id=self.task_id,
                error_source="PullBigQueryOperator._fetch_data_stream",
                original_exception=e,
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
            "[METRICS][INIT] dag_id=%s run_id=%s task_id=%s",
            dag_id, run_id, self.task_id
        )

        try:
            client = self._setup_connection()

            # ---------------- PROCESS JOB ----------------
            if self.source_job_type == PROCESS:
                self.log.info("PROCESS job detected. Executing BigQuery stored procedure.")
                job = client.query(self.sql)
                job.result()

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

            # ---------------- SELECT JOB ----------------
            results = self._prepare_query(client)

            raw_iter = self._fetch_data_stream(results)

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

            self._metrics.finish()
            d = self._source_descriptor()

            log_source_metrics(
                dag_id=self._metrics.dag_id,
                run_id=self._metrics.run_id,
                task_id=self._metrics.task_id,

                source_type=d["source_type"],  # BIGQUERY
                source_location=d["source_location"],  # dataset
                source_object=d["source_object"],  # table

                header_less_file=d["header_less_file"],  # True
                delimiter=d["delimiter"],  # None

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
                root_cause=repr(e.original_exception),
            )
            raise

        except Exception as e:
            log_execution(
                dag_id=dag_id,
                task_id=self.task_id,
                run_id=run_id,
                status="FAILED",
                error_source="PullBigQueryOperator.execute",
                error_message=str(e),
                root_cause=repr(e),
            )
            raise
        finally:
            try:
                if client:
                    client.close()
                    self.log.info(" Connection closed successfully.")
            except Exception as cleanup_error:
                self.log.warning(
                    f" Failed to close connection: {cleanup_error}"
                )


class PushBigQueryOperator(BaseOperator, Destination):
    """
    Loads CSV data into BigQuery.

    Supports:
    - overwrite (truncate table)
    - append (add rows)
    """

    def __init__(
            self,
            bigquery_conn_id: str,
            dataset: str,
            table: str,
            data_write_mode: str,
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.task_id = kwargs.get("task_id")
        self.bigquery_conn_id = bigquery_conn_id
        self.dataset = dataset
        self.table = table
        self.data_write_mode = data_write_mode.lower().strip()

    def _destination_descriptor(self) -> dict:
        return {
            "destination_type": "BIGQUERY",
            "destination_location": self.dataset,
            "destination_object": self.table,
            "write_mode": self.data_write_mode,
        }

    # ------------------------------------------------------------------
    def _setup_connection(self) -> bigquery.Client:
        self.log.info("Connecting to BigQuery...")
        try:
            hook = BigQueryHook(gcp_conn_id=self.bigquery_conn_id)
            client = hook.get_client()
            self.log.info(f"Connected to BigQuery project: {client.project}")
            return client

        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed to connect to BigQuery",
                task_id=self.task_id,
                error_source="PushBigQueryOperator._setup_connection",
                original_exception=e,
            ) from e

    # ------------------------------------------------------------------
    def _rewrite_csv_name_based(
            self, client: bigquery.Client, file_path: str
    ) -> str:
        try:
            table_id = f"{client.project}.{self.dataset}.{self.table}"
            table = client.get_table(table_id)

            bq_columns = [field.name for field in table.schema]
            rewritten_file = f"{file_path}.bq_aligned.csv"

            with open(file_path, "r", encoding="utf-8") as src, open(
                    rewritten_file, "w", newline="", encoding="utf-8"
            ) as dst:
                reader = csv.DictReader(src)
                writer = csv.writer(dst)

                writer.writerow(bq_columns)
                for row in reader:
                    writer.writerow([row.get(col, "") for col in bq_columns])

            self.log.info("CSV rewritten to match BigQuery schema order")
            return rewritten_file

        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed while rewriting CSV to match BigQuery schema",
                task_id=self.task_id,
                error_source="PushBigQueryOperator._rewrite_csv_name_based",
                original_exception=e,
            ) from e

    # ------------------------------------------------------------------
    def _upload_data(self, client: bigquery.Client, file_path: str) -> None:
        table_id = f"{client.project}.{self.dataset}.{self.table}"

        try:
            write_disposition = (
                bigquery.WriteDisposition.WRITE_TRUNCATE
                if self.data_write_mode == "overwrite"
                else bigquery.WriteDisposition.WRITE_APPEND
            )

            dataset_ref = bigquery.Dataset(f"{client.project}.{self.dataset}")
            try:
                client.get_dataset(dataset_ref)
            except Exception:
                self.log.info("Dataset not found, creating dataset")
                client.create_dataset(dataset_ref)

            table_exists = True
            try:
                client.get_table(table_id)
            except Exception:
                table_exists = False

            schema = None
            if not table_exists:
                self.log.info("Table does not exist, inferring schema from CSV")
                with open(file_path, "r", encoding="utf-8") as f:
                    reader = csv.reader(f)
                    headers = next(reader)
                    schema = [
                        bigquery.SchemaField(h.strip(), "STRING")
                        for h in headers if h.strip()
                    ]

            job_config = bigquery.LoadJobConfig(
                schema=schema,
                autodetect=False,
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                field_delimiter=",",
                quote_character='"',
                allow_quoted_newlines=True,
                encoding="UTF-8",
                write_disposition=write_disposition,
            )

            aligned_file = (
                self._rewrite_csv_name_based(client, file_path)
                if table_exists
                else file_path
            )

            try:
                with open(aligned_file, "rb") as source_file:
                    self.log.info("Submitting BigQuery load job")
                    job = client.load_table_from_file(
                        source_file, table_id, job_config=job_config
                    )
                    job.result()

                self.log.info("BigQuery load completed successfully")

            finally:
                if aligned_file != file_path and os.path.exists(aligned_file):
                    os.remove(aligned_file)

        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed while uploading data to BigQuery",
                task_id=self.task_id,
                error_source="PushBigQueryOperator._upload_data",
                original_exception=e,
            ) from e

    # ------------------------------------------------------------------
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
            self.log.info("Starting PushBigQueryOperator")

            client = self._setup_connection()
            file_path = self._fetch_data(context)
            rows, cols = csv_profile(file_path)

            self._metrics.rows_loaded = rows
            self._metrics.cols_loaded = cols
            self._metrics.source_file_path = file_path

            self.log.info(
                "[METRICS][DEST][CSV_PROFILE] rows_loaded=%s | cols_loaded=%s | file_path=%s",
                rows, cols, file_path
            )

            self._upload_data(client, file_path)

            self._metrics.finish()
            d = self._destination_descriptor()

            self.log.info(
                "[METRICS][PRE_DB_INSERT][DEST] "
                "status=SUCCESS | rows_loaded=%s | cols_loaded=%s | file_path=%s",
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

            self.log.info("PushBigQueryOperator completed successfully")
            self._cleanup_temp_file(file_path)


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
                error_source="PushBigQueryOperator.execute",
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

                error_source=e.error_source,
                error_message=str(e),
            )

            raise
        finally:
            try:
                if client:
                    client.close()
                    self.log.info(" Connection closed successfully.")
            except Exception as cleanup_error:
                self.log.warning(
                    f" Failed to close connection: {cleanup_error}"
                )

