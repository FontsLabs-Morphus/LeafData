import csv
from typing import Iterable, List

import oracledb
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.utils.context import Context

from custom_operators.constants import (
    NT_TRANSFORMATIONS,
    PROCESS,
    DEFAULT_STOREDPROCEDURE_QUERY_TEMPLATE,
    DEFAULT_SQL_QUERY_TEMPLATE,
)
from custom_operators.metaclass import Source, Destination

from exceptions.custom_exception import MorphusAirflowException
from custom_logger.metrics_logger import (
    MetricsContext,
    log_source_metrics,
    csv_profile,
    log_destination_metrics,
)


class LoadOracleOperator(BaseOperator, Source):
    """
    Oracle SOURCE operator
    - SELECT jobs stream rows using fetchmany()
    - PROCESS jobs execute stored procedure / custom SQL
    """

    def __init__(
        self,
        oracle_conn_id: str,
        schema_name: str,
        table_name: str,
        header_less_file: str,
        headers: List[str],
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
        self.oracle_conn_id = oracle_conn_id
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
        self._db_headers: list[str] = []

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
                message="Oracle SQL query cannot be empty.",
                task_id=self.task_id,
                error_source="LoadOracleOperator.__init__",
            )

    def _source_descriptor(self) -> dict:
        return {
            "source_type": "ORACLE",
            "source_location": self.schema_name,
            "source_object": self.table_name,
            "header_less_file": str(self.header_less_file).lower() == "yes"
            if self.header_less_file is not None
            else True,
            "delimiter": self.delimiter,
        }

    def _build_conn(self):
        af_conn = BaseHook.get_connection(self.oracle_conn_id)

        host = af_conn.host
        port = af_conn.port or 1521
        user = af_conn.login
        password = af_conn.password
        service_name = af_conn.schema or af_conn.extra_dejson.get("service_name") or "XE"

        dsn = f"{host}:{port}/{service_name}"
        self.log.info(f"Using Oracle DSN: {dsn}")

        return oracledb.connect(
            user=user,
            password=password,
            dsn=dsn,
        )

    def _setup_connection(self):
        self.log.info("Connecting to Oracle (streaming cursor)...")
        try:
            conn = self._build_conn()
            cursor = conn.cursor()
            cursor.execute(self.sql)

            if cursor.description:
                self._db_headers = [desc[0].lower() for desc in cursor.description]
            else:
                self._db_headers = []

            self.log.info(f"Oracle headers detected: {self._db_headers}")
            return conn, cursor

        except MorphusAirflowException:
            raise
        except Exception as e:
            raise MorphusAirflowException(
                message="Failed to connect to Oracle database",
                task_id=self.task_id,
                error_source="LoadOracleOperator._setup_connection",
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
                message="Error while fetching data stream from Oracle cursor",
                task_id=self.task_id,
                error_source="LoadOracleOperator._fetch_data_stream",
                original_exception=e,
            ) from e

    def execute(self, context: Context) -> None:
        dag_id = context["dag"].dag_id
        run_id = context["run_id"]

        self._metrics = MetricsContext(dag_id=dag_id, run_id=run_id, task_id=self.task_id)

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
            headers = self.headers or self._db_headers

            transformed_iter = self._transform_data_stream(
                raw_iter=raw_iter,
                header_less_file=self.header_less_file,
                headers=headers,
            )

            self._save_data_stream(
                context=context,
                row_iter=transformed_iter,
                ext=self.ext,
                delimiter=self.delimiter,
            )

            #ifself._metrics:
              # log_source_metrics(self._metrics, self._source_descriptor())

        except MorphusAirflowException:
            raise
        except Exception as e:
            raise MorphusAirflowException(
                message="Unexpected error while executing Oracle source task",
                task_id=self.task_id,
                error_source="LoadOracleOperator.execute",
                original_exception=e,
            ) from e
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass


class PushOracleOperator(BaseOperator, Destination):
    """
    Oracle DESTINATION operator
    Supports overwrite/append.
    Creates table with VARCHAR2(4000) columns inferred from CSV header if missing.
    """

    def __init__(
        self,
        oracle_conn_id: str,
        schema_name: str,
        data_write_mode: str,
        table_name: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.task_id = kwargs.get("task_id")
        self.oracle_conn_id = oracle_conn_id
        self.table_name = table_name
        self.schema_name = schema_name
        self.data_write_mode = (data_write_mode or "append").lower().strip()
        self._db_headers: list[str] = []

    def _destination_descriptor(self) -> dict:
        return {
            "destination_type": "ORACLE",
            "destination_location": self.schema_name,
            "destination_object": self.table_name,
            "write_mode": self.data_write_mode,
        }

    def _build_conn(self):
        af_conn = BaseHook.get_connection(self.oracle_conn_id)

        host = af_conn.host
        port = af_conn.port or 1521
        user = af_conn.login
        password = af_conn.password
        service_name = af_conn.schema or af_conn.extra_dejson.get("service_name") or "XE"

        dsn = f"{host}:{port}/{service_name}"
        self.log.info(f"Using Oracle DSN: {dsn}")

        return oracledb.connect(
            user=user,
            password=password,
            dsn=dsn,
        )

    def _setup_connection(self):
        self.log.info("Connecting to Oracle Database")
        try:
            conn = self._build_conn()
            self.log.info("Connection Successful")
            return conn
        except MorphusAirflowException:
            raise
        except Exception as e:
            raise MorphusAirflowException(
                message="Failed to connect to Oracle database",
                task_id=self.task_id,
                error_source="PushOracleOperator._setup_connection",
                original_exception=e,
            ) from e

    def _table_exists(self, conn) -> bool:
        sql = """
        SELECT COUNT(*)
        FROM ALL_TABLES
        WHERE OWNER = :schema
          AND TABLE_NAME = :table
        """
        try:
            cursor = conn.cursor()
            cursor.execute(
                sql,
                {
                    "schema": (self.schema_name or "").upper(),
                    "table": (self.table_name or "").upper(),
                },
            )
            row = cursor.fetchone()
            return bool(row and row[0] > 0)
        except Exception as e:
            raise MorphusAirflowException(
                message="Failed while checking if Oracle table exists",
                task_id=self.task_id,
                error_source="PushOracleOperator._table_exists",
                original_exception=e,
            ) from e
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    def _create_table_if_missing(self, conn, headers: list[str]) -> None:
        if not headers:
            raise MorphusAirflowException(
                message="Cannot create Oracle table: CSV headers are empty",
                task_id=self.task_id,
                error_source="PushOracleOperator._create_table_if_missing",
            )

        cols = ", ".join([f"\"{h}\" VARCHAR2(4000)" for h in headers])
        ddl = f'CREATE TABLE "{self.schema_name}"."{self.table_name}" ({cols})'
        self.log.info(f"Creating Oracle table if missing: {ddl}")

        cursor = conn.cursor()
        try:
            cursor.execute(ddl)
            conn.commit()
        except Exception as e:
            msg = str(e).lower()
            if "ora-00955" in msg or "already used" in msg:
                return
            raise
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    def _truncate_table(self, conn) -> None:
        stmt = f'TRUNCATE TABLE "{self.schema_name}"."{self.table_name}"'
        self.log.info(f"Truncating Oracle table: {stmt}")
        cursor = conn.cursor()
        try:
            cursor.execute(stmt)
            conn.commit()
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    def _insert_rows(self, conn, headers: list[str], rows: Iterable[list]) -> None:
        placeholders = ", ".join([f":{i+1}" for i in range(len(headers))])
        cols = ", ".join([f"\"{h}\"" for h in headers])
        stmt = f'INSERT INTO "{self.schema_name}"."{self.table_name}" ({cols}) VALUES ({placeholders})'

        cursor = conn.cursor()
        batch = []
        batch_size = 1000

        try:
            for row in rows:
                batch.append(row)
                if len(batch) >= batch_size:
                    cursor.executemany(stmt, batch)
                    conn.commit()
                    batch = []
            if batch:
                cursor.executemany(stmt, batch)
                conn.commit()
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    def execute(self, context: Context) -> None:
        dag_id = context["dag"].dag_id
        run_id = context["run_id"]

        self._metrics = MetricsContext(dag_id=dag_id, run_id=run_id, task_id=self.task_id)

        try:
            conn = self._setup_connection()

            file_path = self._fetch_data(context)
            #headers, data = self._read_file(file_path)
            result = self._read_file(file_path)
            
            if isinstance(result, tuple) and len(result) >= 2:
                headers, data = result[0], result[1]
            else:
                headers, data = None, result

            self._db_headers = [h.strip() for h in headers] if headers else []

            if not self._table_exists(conn):
                self._create_table_if_missing(conn, self._db_headers)

            if self.data_write_mode == "overwrite":
                self._truncate_table(conn)

            self._insert_rows(conn, self._db_headers, data)

            if self._metrics:
                self._metrics = csv_profile(self._metrics, file_path)
                #log_destination_metrics(self._metrics, self._destination_descriptor())

        except MorphusAirflowException:
            raise
        except Exception as e:
            raise MorphusAirflowException(
                message="Unexpected error while executing Oracle destination task",
                task_id=self.task_id,
                error_source="PushOracleOperator.execute",
                original_exception=e,
            ) from e
        finally:
            try:
                conn.close()
            except Exception:
                pass