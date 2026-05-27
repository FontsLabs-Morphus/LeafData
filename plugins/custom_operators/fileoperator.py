import os

from airflow.models import BaseOperator
from airflow.utils.context import Context
from custom_operators.constants import (
    TEMP_FILE_PATH,
    INPUT_FILES_PATH,
    TRANSFORMED_FILES_PATH,
    NT_TRANSFORMATIONS,
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



def merge_headers(
        dest_header: list[str],
        temp_header: list[str],
        dest_rows: list[list[str]]
) -> list[str]:
    """
    Merge new headers into destination header and pad existing rows if needed.
    """

    try:
        if not isinstance(dest_header, list):
            raise MorphusAirflowException(
                message="dest_header must be a list",
                error_source="merge_headers"
            )

        if not isinstance(temp_header, list):
            raise MorphusAirflowException(
                message="temp_header must be a list",
                error_source="merge_headers"
            )

        if not isinstance(dest_rows, list):
            raise MorphusAirflowException(
                message="dest_rows must be a list of rows",
                error_source="merge_headers"
            )

        existing = set(dest_header)
        new_cols = [col for col in temp_header if col not in existing]

        if new_cols:
            dest_header.extend(new_cols)
            pad_len = len(new_cols)

            # Skip header row if present in dest_rows
            for row in dest_rows[1:]:
                try:
                    if not isinstance(row, list):
                        raise MorphusAirflowException(
                            message="Each row in dest_rows must be a list",
                            error_source="merge_headers:row_validation"
                        )
                    row.extend([""] * pad_len)

                except MorphusAirflowException:
                    raise

                except Exception as e:
                    raise MorphusAirflowException(
                        message="Failed while padding destination rows",
                        error_source="merge_headers:row_padding",
                        original_exception=e
                    ) from e

        return dest_header

    except MorphusAirflowException:
        raise

    except Exception as e:
        raise MorphusAirflowException(
            message="Unexpected error while merging headers",
            error_source="merge_headers",
            original_exception=e
        ) from e


class PullFileOperator(BaseOperator, Source):
    def __init__(
            self,
            file_path: str,
            tx_list: list[list],
            header_less_file: str,
            headers: list[str],
            delimiter: str = None,
            *args,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)

        try:
            if not file_path:
                raise MorphusAirflowException(
                    message="Source property 'file_path' is required",
                    error_source="PullFileOperator.__init__"
                )

            if not delimiter:
                raise MorphusAirflowException(
                    message="Delimiter must be provided for PullFileOperator",
                    error_source="PullFileOperator.__init__"
                )

            self.delimiter = delimiter
            self.header_less_file = header_less_file
            self.headers = headers
            self.file_path = os.path.join(INPUT_FILES_PATH, file_path)
            self.tx_list = [NT_TRANSFORMATIONS(*tx) for tx in tx_list]
            self.task_id = kwargs.get("task_id")

            if not os.path.exists(self.file_path):
                raise MorphusAirflowException(
                    message=f"Input file not found: {self.file_path}",
                    task_id=self.task_id,
                    error_source="PullFileOperator.__init__"
                )

        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed to initialize PullFileOperator",
                task_id=kwargs.get("task_id"),
                error_source="PullFileOperator.__init__",
                original_exception=e
            ) from e

    def execute(self, context: Context) -> str:
        dag_id = context["dag"].dag_id
        run_id = context["run_id"]

        try:
            self.log.info(f"[Pull] Reading file {self.file_path} with delimiter '{self.delimiter}'")

            raw_data = self._read_file(self.file_path, self.delimiter)
            data = self._transform_data(raw_data, self.header_less_file, self.headers)

            self._save_data(context, data, delimiter=",")
            temp_file_path = os.path.join(TEMP_FILE_PATH, f"{self.task_id}.csv")
            return temp_file_path

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
                error_source="PullFileOperator.execute",
                error_message=str(e),
                root_cause=repr(e),
            )
            raise


class PushFileOperator(BaseOperator, Destination):
    def __init__(
            self,
            file_path: str,
            data_write_mode: str,
            delimiter: str = None,
            *args,
            **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)

        try:
            if not file_path:
                raise MorphusAirflowException(
                    message="Destination property 'file_path' is required",
                    error_source="PushFileOperator.__init__"
                )

            if not delimiter:
                raise MorphusAirflowException(
                    message="Delimiter must be provided for PushFileOperator",
                    error_source="PushFileOperator.__init__"
                )

            self.delimiter = delimiter
            self.file_path = os.path.join(TRANSFORMED_FILES_PATH, file_path)
            self.task_id = kwargs.get("task_id")
            self.data_write_mode = data_write_mode.lower().strip()

        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed to initialize PushFileOperator",
                task_id=kwargs.get("task_id"),
                error_source="PushFileOperator.__init__",
                original_exception=e
            ) from e

    def execute(self, context: Context) -> str:
        dag_id = context["dag"].dag_id
        run_id = context["run_id"]

        try:
            self.log.info("[Push] Starting PushFileOperator")

            temp_file = self._fetch_data(context)
            if not temp_file or not os.path.exists(temp_file):
                raise MorphusAirflowException(
                    message=f"Temp file not found: {temp_file}",
                    task_id=self.task_id,
                    error_source="PushFileOperator.execute"
                )

            self.log.info(f"[Push] Upstream temp file fetched: {temp_file}")

            temp_data = self._read_file(temp_file, ",")
            if not temp_data:
                raise MorphusAirflowException(
                    message="Temp file is empty",
                    task_id=self.task_id,
                    error_source="PushFileOperator.execute"
                )

            temp_header, temp_rows = temp_data[0], temp_data[1:]

            if self.data_write_mode == "overwrite":
                self.log.info("[Push] Overwrite mode: replacing destination file.")
                self._save_file(
                    os.path.dirname(self.file_path) or ".",
                    os.path.basename(self.file_path),
                    temp_data,
                    delimiter=self.delimiter,
                )
                return self.file_path

            self.log.info("[Push] Append mode: merging into existing file.")

            if os.path.exists(self.file_path):
                dest_data = self._read_file(self.file_path, self.delimiter)
                dest_header = dest_data[0]
                dest_rows = dest_data[1:]
            else:
                dest_header, dest_rows = [], []

            if dest_header:
                dest_header = merge_headers(dest_header, temp_header, [dest_header] + dest_rows)
            else:
                dest_header = temp_header.copy()

            col_index = {col: i for i, col in enumerate(temp_header)}

            new_rows_aligned = [
                [
                    (row[col_index[col]] if col in col_index and col_index[col] < len(row) else "")
                    for col in dest_header
                ]
                for row in temp_rows
            ]

            final_rows = dest_rows + new_rows_aligned

            self._save_file(
                os.path.dirname(self.file_path) or ".",
                os.path.basename(self.file_path),
                [dest_header] + final_rows,
                delimiter=self.delimiter,
            )

            self.log.info(f"File updated successfully with {len(new_rows_aligned)} appended rows.")
            return self.file_path

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
                error_source="PushFileOperator.execute",
                error_message=str(e),
                root_cause=repr(e),
            )
            raise


