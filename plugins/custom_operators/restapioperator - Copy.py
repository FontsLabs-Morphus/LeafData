import csv
import json
import os
import tempfile
from pathlib import Path
from urllib.parse import urljoin

import requests
from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.context import Context

from custom_operators.metaclass import Source
from exceptions.custom_exception import MorphusAirflowException
from custom_logger.execution_logger import log_execution
from custom_logger.metrics_logger import (
    MetricsContext,
    log_source_metrics,
    csv_profile,
)


class LoadRestAPIOperator(BaseOperator, Source):
    """
    REST API SOURCE operator (version 1)

    Supported:
    - GET only
    - JSON response
    - response can be:
        1) a list of objects
        2) a dict containing a list under a key such as "data"
    - endpoint is passed through sourceProperties.path
    - auth value is read from Airflow connection.password or extra/securityToken fallback

    Connection expectation:
    - conn_type: REST_API
    - host: base URL OR connection_string/base_url in extras
    - securityToken / password may contain token
    """

    def __init__(
        self,
        rest_conn_id: str,
        endpoint: str,
        tx_list: list[list],
        header_less_file: str = "N",
        headers: list[str] = None,
        delimiter: str = ",",
        response_path: str = "",
        method: str = "GET",
        timeout: int = 30,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.task_id = kwargs.get("task_id")
        self.rest_conn_id = rest_conn_id
        self.endpoint = endpoint
        self.tx_list = tx_list or []
        self.header_less_file = header_less_file
        self.headers = headers or []
        self.delimiter = delimiter or ","
        self.response_path = response_path or ""
        self.method = (method or "GET").upper().strip()
        self.timeout = int(timeout or 30)

    def _source_descriptor(self) -> dict:
        return {
            "source_type": "REST_API",
            "source_location": self.rest_conn_id,
            "source_object": self.endpoint,
            "header_less_file": False,
            "delimiter": self.delimiter,
        }

    def _get_connection_details(self) -> tuple[str, dict, dict]:
        try:
            conn = BaseHook.get_connection(self.rest_conn_id)
            extra = conn.extra_dejson or {}

            base_url = (
                (extra.get("base_url") or "").strip()
                or (extra.get("connection_string") or "").strip()
            )

            if not base_url:
                host = (conn.host or "").strip()
                port = conn.port
                schema = (conn.schema or "http").strip()

                if host:
                    if host.startswith("http://") or host.startswith("https://"):
                        base_url = host
                    else:
                        if port:
                            base_url = f"{schema}://{host}:{port}"
                        else:
                            base_url = f"{schema}://{host}"

            

            auth_type = (
                extra.get("auth_type")
                or extra.get("authType")
                or "NONE"
            )
            auth_type = str(auth_type).upper().strip()

            auth_value = (
                conn.password
                or extra.get("securityToken")
                or extra.get("auth_value")
                or extra.get("authValue")
                or ""
            )

            headers = {}
            params = {}

            if auth_type == "BEARER" and auth_value:
                headers["Authorization"] = f"Bearer {auth_value}"
            elif auth_type == "API_KEY" and auth_value:
                headers["x-api-key"] = auth_value
            elif auth_type == "BASIC" and auth_value:
                # For version 1, caller may store username:password in auth_value
                # requests can also use headers with basic auth token if desired.
                import base64
                token = base64.b64encode(auth_value.encode("utf-8")).decode("utf-8")
                headers["Authorization"] = f"Basic {token}"

            # optional custom headers from extras
            extra_headers = extra.get("headers")
            if isinstance(extra_headers, dict):
                headers.update(extra_headers)
            elif isinstance(extra_headers, str) and extra_headers.strip():
                try:
                    parsed_headers = json.loads(extra_headers)
                    if isinstance(parsed_headers, dict):
                        headers.update(parsed_headers)
                except Exception:
                    pass

            extra_params = extra.get("query_params") or extra.get("queryParams")
            if isinstance(extra_params, dict):
                params.update(extra_params)
            elif isinstance(extra_params, str) and extra_params.strip():
                try:
                    parsed_params = json.loads(extra_params)
                    if isinstance(parsed_params, dict):
                        params.update(parsed_params)
                except Exception:
                    pass

            return base_url, headers, params

        except MorphusAirflowException:
            raise
        except Exception as e:
            raise MorphusAirflowException(
                message="Failed to resolve REST API connection details",
                task_id=self.task_id,
                error_source="LoadRestAPIOperator._get_connection_details",
                original_exception=e,
            ) from e

    def _extract_records(self, payload):
        if isinstance(payload, list):
            return payload

        if isinstance(payload, dict):
            if self.response_path:
                current = payload
                for part in self.response_path.split("."):
                    part = part.strip()
                    if not part:
                        continue
                    if isinstance(current, dict) and part in current:
                        current = current[part]
                    else:
                        raise MorphusAirflowException(
                            message=f"response_path '{self.response_path}' not found in API response",
                            task_id=self.task_id,
                            error_source="LoadRestAPIOperator._extract_records",
                        )
                if isinstance(current, list):
                    return current
                raise MorphusAirflowException(
                    message=f"response_path '{self.response_path}' did not resolve to a list",
                    task_id=self.task_id,
                    error_source="LoadRestAPIOperator._extract_records",
                )

            # automatic common key support
            for key in ("data", "results", "items", "records"):
                if key in payload and isinstance(payload[key], list):
                    return payload[key]

        raise MorphusAirflowException(
            message="REST API response must be a JSON list or dict containing a list",
            task_id=self.task_id,
            error_source="LoadRestAPIOperator._extract_records",
        )

    def _normalize_records(self, records: list) -> tuple[list[str], list[list]]:
        if not records:
            return [], []

        if not all(isinstance(r, dict) for r in records):
            raise MorphusAirflowException(
                message="REST API records must be JSON objects",
                task_id=self.task_id,
                error_source="LoadRestAPIOperator._normalize_records",
            )

        # stable header order: first record keys, then any new keys encountered later
        headers = list(records[0].keys())
        seen = set(headers)

        for rec in records[1:]:
            for k in rec.keys():
                if k not in seen:
                    headers.append(k)
                    seen.add(k)

        rows = []
        for rec in records:
            row = []
            for h in headers:
                val = rec.get(h, "")
                if val is None:
                    row.append("")
                elif isinstance(val, (dict, list)):
                    row.append(json.dumps(val, ensure_ascii=False))
                else:
                    row.append(str(val))
            rows.append(row)

        return headers, rows

    def _write_temp_csv(self, headers: list[str], rows: list[list]) -> str:
        try:
            fd, local_path = tempfile.mkstemp(prefix="rest_api_src_", suffix=".csv")
            os.close(fd)

            with open(local_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter=self.delimiter)
                writer.writerow(headers)
                writer.writerows(rows)

            return local_path

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed to write REST API payload to temp CSV",
                task_id=self.task_id,
                error_source="LoadRestAPIOperator._write_temp_csv",
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
            if self.method != "GET":
                raise MorphusAirflowException(
                    message=f"REST method '{self.method}' not supported in version 1. Use GET.",
                    task_id=self.task_id,
                    error_source="LoadRestAPIOperator.execute",
                )

            base_url, headers, params = self._get_connection_details()
            final_url = urljoin(base_url.rstrip("/") + "/", self.endpoint.lstrip("/"))

            self.log.info(
                "[REST_API] Requesting URL=%s | conn_id=%s",
                final_url,
                self.rest_conn_id,
            )

            response = requests.get(
                final_url,
                headers=headers,
                params=params,
                timeout=self.timeout,
            )
            response.raise_for_status()

            payload = response.json()
            records = self._extract_records(payload)
            csv_headers, csv_rows = self._normalize_records(records)

            if not csv_headers:
                raise MorphusAirflowException(
                    message="REST API returned zero usable columns",
                    task_id=self.task_id,
                    error_source="LoadRestAPIOperator.execute",
                )

            local_file = self._write_temp_csv(csv_headers, csv_rows)

            rows, cols = csv_profile(local_file, delimiter=self.delimiter)
            self._metrics.rows_written = rows
            self._metrics.cols_after_transform = cols
            self._metrics.cols_read = cols
            self._metrics.rows_read = max(rows - 1, 0) if rows else 0
            self._metrics.file_path = local_file

            self.log.info(
                "[REST_API] CSV created. local_file=%s | rows=%s | cols=%s",
                local_file,
                rows,
                cols,
            )

            ti = context["ti"]
            xcom_key = f"{self.task_id}_data"
            self.log.info(
                "[REST_API] Pushing XCom key=%s value=%s",
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
                error_source="LoadRestAPIOperator.execute",
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
                error_source="LoadRestAPIOperator.execute",
                error_message=str(e),
            )
            raise