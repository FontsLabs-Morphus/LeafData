import csv
import io
import os
import tempfile

from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.context import Context
from botocore.exceptions import ClientError
from custom_logger.execution_logger import log_execution
from custom_logger.metrics_logger import (
    MetricsContext,
    log_source_metrics,
    csv_profile,
    log_destination_metrics
)
from custom_operators.constants import NT_TRANSFORMATIONS
from custom_operators.metaclass import Source, Destination
from exceptions.custom_exception import MorphusAirflowException


class LoadS3Operator(BaseOperator, Source):
    """
    Pull data from AWS S3 and apply transformation.
    Cleans empty headers and trims whitespace.
    """

    def __init__(
            self,
            tx_list: list,
            aws_conn_id: str,
            bucket_name: str,
            object_name: str,
            header_less_file: str,
            headers: list[str],
            *args,
            **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.task_id = kwargs.get("task_id")
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.header_less_file = header_less_file
        self.headers = headers
        self.tx_list = [NT_TRANSFORMATIONS(*tx) for tx in tx_list]

    def _source_descriptor(self) -> dict:
        return {
            "source_type": "S3",
            "source_location": self.bucket_name,
            "source_object": self.object_name,
            "header_less_file": str(self.header_less_file).lower() == "yes",
            "delimiter": ",",
        }

    def _setup_connection(self) -> S3Hook:
        """Set up connection to S3."""
        self.log.info("Connecting to AWS S3...")
        try:
            hook = S3Hook(aws_conn_id=self.aws_conn_id)
            conn = hook.get_credentials()
            self.log.info("Connection Successful.")
            return hook, conn

        except MorphusAirflowException:
            raise

        except Exception as e:
            self.log.error(f"Error connecting to S3: {e}")
            raise MorphusAirflowException(
                message="Failed to connect to AWS S3",
                task_id=self.task_id,
                error_source="LoadS3Operator._setup_connection",
                original_exception=e
            ) from e

    def _fetch_data(self, hook: S3Hook):
        """
        Pure streaming read from S3.
        - No download_file()
        - No ListBucket / HeadObject checks (works with strict read-only policies)
        - Constant memory
        """
        self.log.info(f"Streaming data from S3: s3://{self.bucket_name}/{self.object_name}")

        try:
            s3 = hook.get_conn()  # boto3 client

            # This is the only call you truly need for read-only: s3:GetObject
            resp = s3.get_object(Bucket=self.bucket_name, Key=self.object_name)
            body = resp["Body"]  # botocore.response.StreamingBody (file-like)

            # Wrap bytes stream -> text stream -> csv.reader
            # newline="" is important for correct CSV parsing
            text_stream = io.TextIOWrapper(body, encoding="utf-8", newline="")

            reader = csv.reader(text_stream, delimiter=self.delimiter)

            for row in reader:
                # keep same behavior as your other operators: trim cells, skip empty rows
                cleaned = [cell.strip() for cell in row]
                if any(cleaned):
                    yield cleaned

        except ClientError as e:
            # Useful AWS error details
            code = (e.response.get("Error") or {}).get("Code")
            msg = (e.response.get("Error") or {}).get("Message")

            raise MorphusAirflowException(
                message=f"Failed to stream from S3 (aws_error={code}, message={msg})",
                task_id=self.task_id,
                error_source="LoadS3Operator._fetch_data_stream",
                original_exception=e,
            ) from e

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed while streaming data from AWS S3",
                task_id=self.task_id,
                error_source="LoadS3Operator._fetch_data_stream",
                original_exception=e,
            ) from e

    def execute(self, context: Context) -> None:
        """Execute operator."""
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
            raw_data = self._fetch_data(hook)
            transformed_data = self._transform_data_stream(raw_data, self.header_less_file, self.headers)
            self._save_data_stream(context, transformed_data, self.ext, self.delimiter)

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
                error_source="LoadS3Operator.execute",
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

                error_source="LoadS3Operator.execute",
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


class PushS3Operator(BaseOperator, Destination):
    """
    Push CSV (from XCom path) into AWS S3.
    Supports 'overwrite' and 'append' modes.
    Cleans whitespace and removes empty lines before upload.
    """

    def __init__(
            self,
            aws_conn_id: str,
            bucket_name: str,
            object_name: str,
            data_write_mode: str,
            *args,
            **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.task_id = kwargs.get("task_id")
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.data_write_mode = data_write_mode.lower().strip()

    def _destination_descriptor(self) -> dict:
        return {
            "destination_type": "S3",
            "destination_location": self.bucket_name,
            "destination_object": self.object_name,
            "write_mode": self.data_write_mode,
        }

    def _setup_connection(self) -> S3Hook:
        """Connect to AWS S3."""
        self.log.info(f"Connecting to AWS S3 bucket: {self.bucket_name}")
        try:
            hook = S3Hook(aws_conn_id=self.aws_conn_id)
            conn = hook.get_credentials()
            self.log.info("Connection to S3 successful.")
            return hook, conn

        except MorphusAirflowException:
            raise

        except Exception as e:
            self.log.error(f"Error occurred while connecting to S3: {e}")
            raise MorphusAirflowException(
                message="Failed to connect to AWS S3",
                task_id=self.task_id,
                error_source="PushS3Operator._setup_connection",
                original_exception=e
            ) from e

    def _object_exists(self, hook: S3Hook) -> bool:
        """Check if object exists in S3."""
        try:
            return hook.check_for_key(self.object_name, bucket_name=self.bucket_name)
        except Exception:
            return False

    def _clean_file(self, file_path: str, delimiter: str = ",") -> str:
        """Clean whitespace and remove empty lines."""
        cleaned_rows = []
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=delimiter)
            for row in reader:
                cleaned = [cell.strip() for cell in row if cell.strip() != ""]
                if cleaned:
                    cleaned_rows.append(cleaned)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="w", encoding="utf-8") as tmp:
            writer = csv.writer(tmp, delimiter=delimiter)
            writer.writerows(cleaned_rows)
            tmp_path = tmp.name

        return tmp_path

    def _upload_data(self, hook: S3Hook, data_filename: str) -> None:
        """Upload file to S3 with overwrite or append behavior."""
        self.log.info(f"Uploading data to S3: s3://{self.bucket_name}/{self.object_name}")
        self.log.info(f"Write mode: {self.data_write_mode}")

        try:
            s3_client = hook.get_conn()

            if not hook.check_for_bucket(bucket_name=self.bucket_name):
                raise MorphusAirflowException(
                    message=f"Invalid Bucket Name: {self.bucket_name}",
                    task_id=self.task_id,
                    error_source="PushS3Operator._upload_data"
                )

            # ✅ Clean file before upload
            cleaned_file = self._clean_file(data_filename)

            # --- Overwrite mode ---
            if self.data_write_mode == "overwrite":
                self.log.info("Overwrite mode detected: replacing S3 object.")
                hook.load_file(
                    filename=cleaned_file,
                    key=self.object_name,
                    bucket_name=self.bucket_name,
                    replace=True,
                )
                self.log.info("File overwritten successfully in S3.")

            # --- Append mode ---
            elif self.data_write_mode == "append":
                self.log.info("Append mode detected: merging new data with existing S3 object.")
                if self._object_exists(hook):
                    self.log.info("Existing object found, downloading current data...")
                    obj = s3_client.get_object(Bucket=self.bucket_name, Key=self.object_name)
                    existing_data = obj["Body"].read().decode("utf-8").strip()

                    with open(cleaned_file, "r", encoding="utf-8") as new_file:
                        new_data = new_file.read().strip()

                    # Avoid duplicate headers
                    existing_lines = existing_data.splitlines()
                    new_lines = new_data.splitlines()
                    if existing_lines and new_lines and existing_lines[0] == new_lines[0]:
                        new_lines = new_lines[1:]

                    combined_data = "\n".join(existing_lines + new_lines)

                    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="w", encoding="utf-8") as tmp:
                        tmp.write(combined_data)
                        tmp_path = tmp.name

                    hook.load_file(
                        filename=tmp_path,
                        key=self.object_name,
                        bucket_name=self.bucket_name,
                        replace=True,
                    )
                    os.remove(tmp_path)
                    self.log.info("S3 object updated successfully with appended data.")
                else:
                    self.log.info("No existing object found — uploading as new file.")
                    hook.load_file(
                        filename=cleaned_file,
                        key=self.object_name,
                        bucket_name=self.bucket_name,
                        replace=True,
                    )
                    self.log.info("File uploaded successfully as new S3 object.")

            else:
                raise MorphusAirflowException(
                    message=f"Invalid data_write_mode '{self.data_write_mode}'. Must be 'overwrite' or 'append'.",
                    task_id=self.task_id,
                    error_source="PushS3Operator._upload_data"
                )

            os.remove(cleaned_file)
            self.log.info("Upload completed successfully.")

        except MorphusAirflowException:
            raise

        except Exception as e:
            self.log.error(f"Error occurred while uploading data to S3: {e}")
            raise MorphusAirflowException(
                message="Failed while uploading data to AWS S3",
                task_id=self.task_id,
                error_source="PushS3Operator._upload_data",
                original_exception=e
            ) from e

    def execute(self, context: Context) -> None:
        """Execute operator."""
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
                error_source="PushS3Operator.execute",
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
                    self.log.info(" Connection closed successfully.")
            except Exception as cleanup_error:
                self.log.warning(
                    f" Failed to close connection: {cleanup_error}"
                )

