import csv
import os
import tempfile
from typing import List, Optional

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.context import Context
from custom_operators.constants import NT_TRANSFORMATIONS
from custom_operators.metaclass import Source, Destination

from custom_logger.execution_logger import log_execution
from exceptions.custom_exception import MorphusAirflowException
from custom_logger.metrics_logger import (
    MetricsContext,
    log_source_metrics,
    csv_profile,
    log_destination_metrics
)



class PullGCPOperator(BaseOperator, Source):
    """
    Pull data from Google Cloud Storage and apply transformation.
    """

    def __init__(
            self,
            gcs_conn_id: str,
            bucket_name: str,
            object_name: str,
            header_less_file: str,
            headers: list[str],
            tx_list: List[NT_TRANSFORMATIONS],
            delimiter: str = ",",
            *args,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.task_id = kwargs.get("task_id")
        self.gcs_conn_id = gcs_conn_id
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.header_less_file = header_less_file
        self.headers = headers
        self.tx_list = [NT_TRANSFORMATIONS(*tx) for tx in tx_list]
        self.delimiter = delimiter

    def _source_descriptor(self) -> dict:
        return {
            "source_type": "GCS",
            "source_location": self.bucket_name,
            "source_object": self.object_name,
            "header_less_file": str(self.header_less_file).lower() == "yes",
            "delimiter": self.delimiter,
        }

    def _setup_connection(self) -> GCSHook:
        self.log.info(f"Connecting to GCS bucket: {self.bucket_name}")
        try:
            hook = GCSHook(gcp_conn_id=self.gcs_conn_id)
            conn = hook.get_conn()
            self.log.info("Connection Successful.")
            return hook, conn

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed to connect to Google Cloud Storage",
                task_id=self.task_id,
                error_source="PullGCPOperator._setup_connection",
                original_exception=e,
            ) from e

    def _fetch_data(self, hook: GCSHook):
        self.log.info(f"Streaming data from GCS object: {self.object_name}")

        try:
            client = hook.get_conn()
            bucket = client.bucket(self.bucket_name)
            blob = bucket.blob(self.object_name)

            with blob.open("r") as f:
                reader = csv.reader(f, delimiter=self.delimiter)

                for row in reader:
                    cleaned = [cell.strip() for cell in row]
                    if any(cleaned):
                        yield cleaned

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed while streaming data from GCS",
                task_id=self.task_id,
                error_source="PullGCPOperator._fetch_data",
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
            "[METRICS][INIT] metrics_context_created=True | dag_id=%s | run_id=%s | task_id=%s",
            dag_id, run_id, self.task_id
        )

        try:
            hook, conn = self._setup_connection()
            raw_iter = self._fetch_data(hook)

            self.log.info(
                "[METRICS][BEFORE_TRANSFORM] rows_read=%s | rows_after_transform=%s | rows_written=%s",
                self._metrics.rows_read,
                self._metrics.rows_after_transform,
                self._metrics.rows_written,
            )

            transformed_iter = self._transform_data_stream(
                raw_iter,
                self.header_less_file,
                self.headers,
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
                error_source=f"{self.__class__.__name__}.execute",
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
                if conn:
                    conn.close()
                    self.log.info(" Connection closed successfully.")
            except Exception as cleanup_error:
                self.log.warning(
                    f" Failed to close connection: {cleanup_error}"
                )


class PushGCPOperator(BaseOperator, Destination):
    """
    Push CSV data into Google Cloud Storage.
    """

    def __init__(
            self,
            gcs_conn_id: str,
            bucket_name: str,
            object_name: str,
            data_write_mode: str,
            *args,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.task_id: Optional[str] = kwargs.get("task_id")
        self.gcs_conn_id = gcs_conn_id
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.data_write_mode = data_write_mode.lower().strip()

    def _destination_descriptor(self) -> dict:
        return {
            "destination_type": "GCS",
            "destination_location": self.bucket_name,
            "destination_object": self.object_name,
            "write_mode": self.data_write_mode,
        }

    def _setup_connection(self) -> GCSHook:
        self.log.info(f"Connecting to GCS bucket: {self.bucket_name}")
        try:
            hook = GCSHook(gcp_conn_id=self.gcs_conn_id)
            conn = hook.get_conn()
            self.log.info("Connection Successful.")
            return hook, conn

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed to connect to Google Cloud Storage",
                task_id=self.task_id,
                error_source="PushGCPOperator._setup_connection",
                original_exception=e,
            ) from e

    def _upload_data(self, hook: GCSHook, data_filename: str) -> None:
        self.log.info(f"Uploading data to GCS: {self.bucket_name}/{self.object_name}")

        try:
            client = hook.get_conn()
            bucket = client.bucket(self.bucket_name)
            blob = bucket.blob(self.object_name)

            cleaned_file = self._clean_file(data_filename)

            if self.data_write_mode == "overwrite":
                blob.upload_from_filename(cleaned_file, content_type="text/csv")

            elif self.data_write_mode == "append":
                if self._object_exists(hook):
                    existing_data = blob.download_as_text().strip()
                    with open(cleaned_file, "r", encoding="utf-8") as f:
                        new_data = f.read().strip()

                    existing_lines = existing_data.splitlines()
                    new_lines = new_data.splitlines()
                    if existing_lines and new_lines and existing_lines[0] == new_lines[0]:
                        new_lines = new_lines[1:]

                    combined_data = "\n".join(existing_lines + new_lines)

                    with tempfile.NamedTemporaryFile(
                        delete=False, suffix=".csv", mode="w", encoding="utf-8"
                    ) as tmp:
                        tmp.write(combined_data)
                        tmp_path = tmp.name

                    blob.upload_from_filename(tmp_path, content_type="text/csv")
                    os.remove(tmp_path)
                else:
                    blob.upload_from_filename(cleaned_file, content_type="text/csv")

            else:
                raise MorphusAirflowException(
                    message=f"Invalid data_write_mode '{self.data_write_mode}'",
                    task_id=self.task_id,
                    error_source="PushGCPOperator._upload_data",
                )

            os.remove(cleaned_file)

        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed while uploading data to Google Cloud Storage",
                task_id=self.task_id,
                error_source="PushGCPOperator._upload_data",
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
            "[METRICS][INIT] metrics_context_created=True | dag_id=%s | run_id=%s | task_id=%s",
            dag_id, run_id, self.task_id
        )

        try:
            hook, conn = self._setup_connection()
            data_filename = self._fetch_data(context)
            rows, cols = csv_profile(data_filename)

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
            raise

        except Exception as e:
            log_execution(
                dag_id=dag_id,
                task_id=self.task_id,
                run_id=run_id,
                status="FAILED",
                error_source=f"{self.__class__.__name__}.execute",
                error_message=str(e),
                root_cause=repr(e)
            )
            raise
        finally:
            try:
                if conn:
                    conn.close()
                    self.log.info(" Connection closed successfully.")
            except Exception as cleanup_error:
                self.log.warning(
                    f" Failed to close connection: {cleanup_error}"
                )


