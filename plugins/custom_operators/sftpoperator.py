import os
import tempfile
from pathlib import Path

from airflow.models import BaseOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.utils.context import Context

from custom_operators.metaclass import Source
from exceptions.custom_exception import MorphusAirflowException
from custom_logger.execution_logger import log_execution
from custom_logger.metrics_logger import (
    MetricsContext,
    log_source_metrics,
    csv_profile,
)


class LoadSFTPOperator(BaseOperator, Source):
    """
    SFTP SOURCE operator

    Purpose:
    - Connect to an SFTP server using an Airflow connection
    - Download a remote file to a local temp file inside the worker
    - Push that file path into XCom for downstream destination operators
    """

    def __init__(
        self,
        sftp_conn_id: str,
        remote_path: str,
        tx_list: list[list],
        delimiter: str = ",",
        header_less_file: str = "N",
        headers: list[str] = None,
        ext: str = "csv",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.task_id = kwargs.get("task_id")
        self.sftp_conn_id = sftp_conn_id
        self.remote_path = remote_path
        self.delimiter = delimiter or ","
        self.header_less_file = header_less_file
        self.headers = headers or []
        self.ext = ext
        self.tx_list = tx_list or []

    def _source_descriptor(self) -> dict:
        return {
            "source_type": "SFTP",
            "source_location": self.sftp_conn_id,
            "source_object": self.remote_path,
            "header_less_file": str(self.header_less_file).strip().lower() in ("yes", "true", "1", "y"),
            "delimiter": self.delimiter,
        }

    def _download_file(self) -> str:
        if not self.remote_path or not str(self.remote_path).strip():
            raise MorphusAirflowException(
                message="SFTP remote_path is empty",
                task_id=self.task_id,
                error_source="LoadSFTPOperator._download_file",
            )

        try:
            hook = SFTPHook(ssh_conn_id=self.sftp_conn_id)

            suffix = Path(self.remote_path).suffix or f".{self.ext}"
            fd, local_path = tempfile.mkstemp(prefix="sftp_src_", suffix=suffix)
            os.close(fd)

            self.log.info(
                "Downloading SFTP file from '%s' to local temp '%s'",
                self.remote_path,
                local_path,
            )

            hook.retrieve_file(
                remote_full_path=self.remote_path,
                local_full_path=local_path,
            )

            if not os.path.exists(local_path):
                raise MorphusAirflowException(
                    message=f"SFTP download failed; local file not found: {local_path}",
                    task_id=self.task_id,
                    error_source="LoadSFTPOperator._download_file",
                )

            return local_path

        except MorphusAirflowException:
            raise
        except Exception as e:
            raise MorphusAirflowException(
                message="Failed to download file from SFTP",
                task_id=self.task_id,
                error_source="LoadSFTPOperator._download_file",
                original_exception=e,
            ) from e

    def execute(self, context: Context) -> str:
        dag_id = context["dag"].dag_id
        run_id = context["run_id"]

        self._metrics = MetricsContext(
            dag_id=dag_id,
            run_id=run_id,
            task_id=self.task_id,
        )

        local_file = None

        try:
            local_file = self._download_file()

            rows, cols = csv_profile(local_file, delimiter=self.delimiter)
            self._metrics.rows_written = rows
            self._metrics.cols_after_transform = cols
            self._metrics.cols_read = cols
            self._metrics.rows_read = max(rows - 1, 0) if rows else 0
            self._metrics.file_path = local_file

            self.log.info(
                "[SFTP] Download complete. local_file=%s | rows=%s | cols=%s",
                local_file,
                rows,
                cols,
            )

            ti = context["ti"]
            xcom_key = f"{self.task_id}_data"
            self.log.info(
                "[SFTP] Pushing XCom key=%s value=%s",
                xcom_key,
                local_file,
            )
            ti.xcom_push(key=xcom_key, value=local_file)

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
                rows_after_transform=self._metrics.rows_read,
                rows_written=self._metrics.rows_written,
                file_path=self._metrics.file_path,
                transform_count=0,
                has_expression=False,
                status="SUCCESS",
                start_time=self._metrics.start_time,
                end_time=self._metrics.end_time,
                duration_ms=self._metrics.duration_ms,
            )

            return local_file

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
                rows_after_transform=self._metrics.rows_read,
                rows_written=self._metrics.rows_written,
                file_path=self._metrics.file_path,
                transform_count=0,
                has_expression=False,
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
                error_source="LoadSFTPOperator.execute",
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
                rows_after_transform=self._metrics.rows_read,
                rows_written=self._metrics.rows_written,
                file_path=self._metrics.file_path,
                transform_count=0,
                has_expression=False,
                status="FAILED",
                start_time=self._metrics.start_time,
                end_time=self._metrics.end_time,
                duration_ms=self._metrics.duration_ms,
                error_source="LoadSFTPOperator.execute",
                error_message=str(e),
            )
            raise