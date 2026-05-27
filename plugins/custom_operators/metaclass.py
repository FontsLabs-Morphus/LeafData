import csv
import os
from os import path
import re

from airflow.utils.context import Context
from transformation.transformations import TransformData

from exceptions.custom_exception import MorphusAirflowException
from custom_logger.metrics_logger import MetricsContext


TEMP_FILE_PATH = '/opt/airflow/test/data/result'


class Source:
    def _setup_connection(self):
        """
        Placeholder method for setting up connection to the data source.
        """
        pass

    def _fetch_data(self, hook=None):
        """
        Placeholder method for fetching data from the source.
        """
        pass

    def _transform_data_stream(self, raw_iter, header_less_file: str, headers: list):
        """
        Streaming transformation generator.
        Accepts an iterator of rows and yields transformed rows.
        """
        try:
            if not headers:
                raise MorphusAirflowException(
                    message="Headers cannot be empty for transformation",
                    task_id=self.task_id,
                    error_source="Source._transform_data_stream"
                )

            norm_header = [h.lower() for h in headers]
            self.log.info(f"Normalized header (stream): {norm_header}")
            header_less = str(header_less_file).lower() == "yes"
            if not self.tx_list:
                if hasattr(self, "_metrics") and self._metrics:
                    self._metrics.cols_read = len(norm_header)
                    self._metrics.cols_after_transform = len(norm_header)
                    self._metrics.transform_count = 0
                    self._metrics.has_expression = False

                yield norm_header

                data_iter = raw_iter if header_less else raw_iter
                for row in data_iter:
                    if hasattr(self, "_metrics") and self._metrics:
                        self._metrics.rows_read += 1
                        self._metrics.rows_after_transform += 1
                    yield row
                return

            header_mapping = {h: i for i, h in enumerate(norm_header)}

            #self.log.info(f"Normalized header (stream): {norm_header}")

            tx_obj = TransformData(self.tx_list, header_mapping)

            # --------------------------------------------------
            # METRICS: input/output schema + transform metadata
            # --------------------------------------------------
            if hasattr(self, "_metrics") and self._metrics:
                self._metrics.cols_read = len(headers)
                self._metrics.cols_after_transform = len(self.tx_list)
                self._metrics.transform_count = len(self.tx_list)
                self._metrics.has_expression = any(
                    getattr(tx, "is_expr", False) for tx in self.tx_list
                )

            # Output destination header
            yield [tx.destination_name for tx in self.tx_list]

            data_iter = raw_iter if header_less else raw_iter
            for row in data_iter:
                try:
                    if hasattr(self, "_metrics") and self._metrics:
                        self._metrics.rows_read += 1

                    transformed = tx_obj.apply_all_transformations(row)

                    if hasattr(self, "_metrics") and self._metrics:
                        self._metrics.rows_after_transform += 1

                    yield transformed
                except MorphusAirflowException:
                    raise
                except Exception as e:
                    raise MorphusAirflowException(
                        message="Error while applying transformations on row",
                        task_id=self.task_id,
                        error_source="Source._transform_data_stream:row_transform",
                        original_exception=e
                    ) from e
        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed during streaming transformation setup",
                task_id=self.task_id,
                error_source="Source._transform_data_stream",
                original_exception=e
            ) from e

    def _transform_data(self, raw_data: list, header_less_file: str,
                        headers: list) -> list:  # TODO: Need to Remove this Function once _transform_data_stream works
        mod_data = []

        norm_header = [h.lower() for h in headers]

        if header_less_file == "yes":
            header = norm_header
            data_rows = raw_data
        else:
            header = norm_header
            data_rows = raw_data[1:]  # skip source header

        self.log.info(f"Normalized header: {header}")

        header_mapping = {h: i for i, h in enumerate(header)}
        tx_obj = TransformData(self.tx_list, header_mapping)

        destination_headers = [tx.destination_name for tx in self.tx_list]
        mod_data.append(destination_headers)

        for row in data_rows:
            mod_data.append(tx_obj.apply_all_transformations(row))

        return mod_data

    def _save_data(self, context: Context, transformed_data: list, ext: str = 'csv', delimiter: str = ",") -> None:  #TODO: Need to remove when _save_data_stream works
        """
        Save transformed data to a CSV file and push file path to XCom.

        Parameters:
            context (Context): Context object for the task.
            transformed_data (list): Transformed data to be saved.
            delimiter (str): Delimiter for the CSV file.
        """
        self.log.info('Saving Data to CSV')

        try:
            data_file_name = f"{self.task_id}.{ext}"
            data_file_path_name = self._save_file(TEMP_FILE_PATH, data_file_name, transformed_data, delimiter)
            context['ti'].xcom_push(key=f'{self.task_id}_data', value=data_file_path_name)

        except Exception as e:
            self.log.error('Error Occurred in Saving Data')
            print(e.args)
            raise e

        self.log.info(f'Data Saved to {data_file_path_name}')

    def _save_data_stream(
            self,
            context: Context,
            row_iter,
            ext: str = "csv",
            delimiter: str = ",",
    ) -> None:

        os.makedirs(TEMP_FILE_PATH, exist_ok=True)

        file_path = os.path.join(
            TEMP_FILE_PATH,
            f"{self.task_id}_{re.sub(r'[^A-Za-z0-9_-]', '_', context['run_id'])}_try{context['ti'].try_number}.{ext}"
        )

        mode ="w"
        self.log.info(f"[STREAM_WRITE] Saving CSV using mode='{mode}'")

        rows_written = 0
        batch_size = 10000  # Tune this (5k–50k works great)

        try:
            with open(
                    file_path,
                    mode,
                    newline="",
                    encoding="utf-8",
                    buffering=1024 * 1024  #  1MB OS buffer (huge improvement)
            ) as f:

                writer = csv.writer(f, delimiter=delimiter)
                first = True

                batch = []

                for row in row_iter:
                    if first:
                        writer.writerow(row)
                        first=False
                        continue
                    batch.append(row)
                    rows_written += 1

                    if self._metrics:
                        self._metrics.rows_written += 1

                    if len(batch) >= batch_size:
                        writer.writerows(batch)  #  MUCH faster
                        batch.clear()

                # flush remaining
                if batch:
                    writer.writerows(batch)

            context["ti"].xcom_push(
                key=f"{self.task_id}_data",
                value=file_path,
            )

            if self._metrics:
                self._metrics.file_path = file_path

            self.log.info(
                f"[STREAM_WRITE_COMPLETE] "
                f"Task={self.task_id} | "
                f"Rows Written={rows_written} | "
                f"File={file_path}"
            )

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed while saving streamed data to CSV",
                task_id=self.task_id,
                error_source="Source._save_data_stream",
                original_exception=e,
            ) from e

    def _read_file(self, file_path: str, delimiter: str = ",") -> list:
        """
        Read data from a CSV file.
        """
        data_list = []

        try:
            try:
                with open(file_path, newline="", encoding="utf-8") as csvfile:
                    csv_reader = csv.reader(csvfile, delimiter=delimiter)
                    for row in csv_reader:
                        data_list.append(
                            [cell.replace("\xa0", " ").strip() for cell in row]
                        )

            except UnicodeDecodeError:
                with open(file_path, newline="", encoding="latin-1") as csvfile:
                    csv_reader = csv.reader(csvfile, delimiter=delimiter)
                    for row in csv_reader:
                        data_list.append(
                            [cell.replace("\xa0", " ").strip() for cell in row]
                        )

            return data_list

        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message=f"Failed while reading CSV file '{file_path}'",
                task_id=self.task_id,
                error_source="Destination._read_file",
                original_exception=e
            ) from e

    def _save_file(
            self,
            csv_file_path: str,
            csv_file_name: str,
            data: list,
            delimiter: str = ","
    ) -> str:
        """
        Save data to a CSV file.
        """
        try:
            if not path.exists(csv_file_path):
                os.makedirs(csv_file_path)

            csv_file_path_name = path.join(csv_file_path, csv_file_name)

            with open(csv_file_path_name, "w", newline="", encoding="utf-8") as csv_file:
                csv_writer = csv.writer(csv_file, delimiter=delimiter)
                csv_writer.writerows(data)

            return csv_file_path_name

        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message=f"Failed while saving CSV file '{csv_file_name}'",
                task_id=self.task_id,
                error_source="Destination._save_file",
                original_exception=e
            ) from e


class Destination:
    def _setup_connection(self):
        """Placeholder method for setting up connection to the destination."""
        pass

    def _fetch_data(self, context: Context) -> str:
        """
        Fetch data file path from XCom.
        """
        self.log.info("Fetching data from XCom")

        try:
            task = context["task"]
            ti = context["ti"]

            upstream_tasks = task.get_direct_relatives(upstream=True)

            if upstream_tasks:
                upstream_task_id = upstream_tasks[0].task_id
                data_filename = ti.xcom_pull(
                    task_ids=upstream_task_id,
                    key=f"{upstream_task_id}_data"
                )

                if not data_filename:
                    raise MorphusAirflowException(
                        message=f"No data found in XCom for upstream task '{upstream_task_id}'",
                        task_id=self.task_id,
                        error_source="Destination._fetch_data"
                    )
            else:
                data_filename = ti.xcom_pull(
                    task_ids=task.task_id,
                    key=f"{task.task_id}_data"
                )

                if not data_filename:
                    raise MorphusAirflowException(
                        message=f"No data found in XCom for task '{task.task_id}'",
                        task_id=self.task_id,
                        error_source="Destination._fetch_data"
                    )

            # --------------------------
            # METRICS: source file path
            # --------------------------
            if hasattr(self, "_metrics") and self._metrics:
                self._metrics.source_file_path = data_filename

            self.log.info("Data fetched successfully from XCom")
            return data_filename

        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed while fetching data from XCom",
                task_id=self.task_id,
                error_source="Destination._fetch_data",
                original_exception=e
            ) from e

    def _upload_data(self):
        """Placeholder method for uploading data to the destination."""
        pass

    def _read_file(self, file_path: str, delimiter: str = ",") -> list:
        """
        Read data from a CSV file.
        """
        try:
            data_list = []
            with open(file_path, newline="", encoding="utf-8") as csvfile:
                csv_reader = csv.reader(csvfile, delimiter=delimiter)
                for row in csv_reader:
                    data_list.append(row)
            return data_list

        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message=f"Failed while reading CSV file '{file_path}'",
                task_id=self.task_id,
                error_source="Destination._read_file",
                original_exception=e
            ) from e

    def _save_file(
        self,
        csv_file_path: str,
        csv_file_name: str,
        data: list,
        delimiter: str = ","
    ) -> str:
        """
        Save data to a CSV file.
        """
        try:
            if not path.exists(csv_file_path):
                os.makedirs(csv_file_path, exist_ok=True)

            csv_file_path_name = path.join(csv_file_path, csv_file_name)

            with open(csv_file_path_name, "w", newline="", encoding="utf-8") as csv_file:
                csv_writer = csv.writer(csv_file, delimiter=delimiter)
                csv_writer.writerows(data)

            self.log.info(f"Data saved to {csv_file_path_name}")
            return csv_file_path_name

        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message=f"Failed while saving CSV file '{csv_file_name}'",
                task_id=self.task_id,
                error_source="Destination._save_file",
                original_exception=e
            ) from e

    def _cleanup_temp_file(self, file_path: str) -> None:
        """
        Safely delete temp file after successful load.
        Deletes only the exact file passed.
        """
        try:
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
                self.log.info(
                    f"[TEMP_CLEANUP] Deleted temp file: {file_path}"
                )
        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message=f"[TEMP_CLEANUP] Failed to delete temp file {file_path}",
                task_id=self.task_id,
                error_source="Destination._save_file",
                original_exception=e
            ) from e

