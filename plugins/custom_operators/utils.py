# NOTE: This file is long in your project. Below is a FULL updated version
# of the existing utils.py with only v3 Oracle+normalization additions
# and minimal edits (search for "# v3:" comments).

import json
import os
from dataclasses import dataclass
from datetime import datetime

from airflow import DAG
try:
    from custom_operators.restapioperator import LoadRestAPIOperator
except Exception:
    LoadRestAPIOperator = None
from custom_logger.execution_logger import log_execution
from exceptions.custom_exception import MorphusAirflowException
from custom_operators.sftpoperator import LoadSFTPOperator
from custom_operators.mysqloperator import LoadMySqlOperator, PushMySqlOperator
from custom_operators.postgresoperator import LoadPostgresOperator, PushPostgresOperator
try:
    from custom_operators.oracleoperator import LoadOracleOperator, PushOracleOperator
except Exception:
    LoadOracleOperator = PushOracleOperator = None
try:
    from custom_operators.exceloperator import LoadExcelOperator, PushExcelOperator
except Exception:
    LoadExcelOperator = PushExcelOperator = None

try:
    from custom_operators.mssqloperator import LoadMsSqlOperator, PushMsSqlOperator
except Exception:
    LoadMsSqlOperator = PushMsSqlOperator = None
try:
    from custom_operators.mariadboperator import LoadMariaDbOperator, PushMariaDbOperator
except Exception:
    LoadMariaDbOperator = PushMariaDbOperator = None
try:
    from custom_operators.mongodboperator import LoadMongoDbOperator, PushMongoDbOperator
except Exception:
    LoadMongoDbOperator = PushMongoDbOperator = None

#from custom_operators.s3operator import PullS3Operator, PushS3Operator
#from custom_operators.azurebloboperator import PullAzureBlobOperator, PushAzureBlobOperator
try:
    from custom_operators.azureoperator import LoadAzureOperator, PushAzureOperator
except Exception:
    LoadAzureOperator = PushAzureOperator = None
#from custom_operators.gcsoperator import PullGcsOperator, PushGcsOperator
#from custom_operators.bigqueryoperator import PullBigQueryOperator, PushBigQueryOperator
try:
    from custom_operators.salesforceoperator import LoadSalesforceOperator, PushSalesforceOperator
except Exception:
    LoadSalesforceOperator = PushSalesforceOperator = None
from custom_operators.fileoperator import PullFileOperator, PushFileOperator

from custom_operators.constants import (
    PipelineConfig,
    Job,
    Source,
    SourceProperties,
    Destination,
    DestinationProperties,
    JoinMetadata,
    Transformations,
    NT_TRANSFORMATIONS,
)


from dataclasses import dataclass
from typing import Type

@dataclass(frozen=True)
class NT_TASKMAP:
    source: Type
    destination: Type

# v3: Normalize user-facing types to Airflow internal connector types
def normalize_conn_type(conn_type: str) -> str:
    if not conn_type:
        return conn_type
    t = str(conn_type).strip().upper()
    aliases = {
        "POSTGRES": "PSQL",
        "POSTGRESQL": "PSQL",
        "PG": "PSQL",
        "SQL_SERVER": "MSSQL",
        "SQLSERVER": "MSSQL",
        "AZURE_BLOB": "AZUREBLOB",
        "AZUREBLOB": "AZUREBLOB",
        "GCS": "CLOUDSTORAGE",
        "CLOUD_STORAGE": "CLOUDSTORAGE",
        "GOOGLE_CLOUD_STORAGE": "CLOUDSTORAGE",
        "MARIADB": "MARIADB"
    }
    return aliases.get(t, t)


class OperatorMapper:
    def __init__(self):
        try:
            self.operator_mapping = {
                 "MYSQL": NT_TASKMAP(LoadMySqlOperator, PushMySqlOperator),
                 "PSQL": NT_TASKMAP(LoadPostgresOperator, PushPostgresOperator),
                 #"S3": NT_TASKMAP(PullS3Operator, PushS3Operator),
                 #"AZUREBLOB": NT_TASKMAP(PullAzureBlobOperator, PushAzureBlobOperator),
                 #"CLOUDSTORAGE": NT_TASKMAP(PullGcsOperator, PushGcsOperator),
                 "FILE": NT_TASKMAP(PullFileOperator, PushFileOperator),
                 #"BIGQUERY": NT_TASKMAP(PullBigQueryOperator, PushBigQueryOperator),
                 "SFTP": NT_TASKMAP(LoadSFTPOperator, PushFileOperator),
                 "MARIADB": NT_TASKMAP(LoadMariaDbOperator, PushMariaDbOperator),
                 
            }
            if LoadRestAPIOperator:
                self.operator_mapping["REST_API"] = NT_TASKMAP(LoadRestAPIOperator, PushFileOperator)
            if LoadMongoDbOperator and PushMongoDbOperator:
                self.operator_mapping["MONGODB"] = NT_TASKMAP(LoadMongoDbOperator, PushMongoDbOperator)

# add optional ones only if import succeeded
            if LoadMsSqlOperator and PushMsSqlOperator:
                self.operator_mapping["MSSQL"] = NT_TASKMAP(LoadMsSqlOperator, PushMsSqlOperator)
            if LoadExcelOperator and PushExcelOperator:
                self.operator_mapping["EXCEL"] = NT_TASKMAP(LoadExcelOperator, PushFileOperator)

            if LoadOracleOperator and PushOracleOperator:
                self.operator_mapping["ORACLE"] = NT_TASKMAP(LoadOracleOperator, PushOracleOperator)

            if LoadSalesforceOperator and PushSalesforceOperator:
                self.operator_mapping["SALESFORCE"] = NT_TASKMAP(LoadSalesforceOperator, PushSalesforceOperator)

            if LoadAzureOperator and PushAzureOperator:
                self.operator_mapping["AZUREBLOB"] = NT_TASKMAP(LoadAzureOperator, PushAzureOperator)

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed while initializing OperatorMapper",
                error_source="OperatorMapper.__init__",
                original_exception=e
            ) from e

    def get_source_operator(self, source_type: str):
        try:
            source_type = normalize_conn_type(source_type)  # v3: normalize synonyms
            return self.operator_mapping[source_type].source

        except KeyError as e:
            raise MorphusAirflowException(
                message=f"Unsupported source type '{source_type}'",
                error_source="OperatorMapper.get_source_operator",
                original_exception=e
            ) from e

    def get_destination_operator(self, destination_type: str):
        try:
            destination_type = normalize_conn_type(destination_type)  # v3: normalize synonyms
            return self.operator_mapping[destination_type].destination

        except KeyError as e:
            raise MorphusAirflowException(
                message=f"Unsupported destination type '{destination_type}'",
                error_source="OperatorMapper.get_destination_operator",
                original_exception=e
            ) from e


class Processor:
    GLOBAL_ALLOWED = {"task_id", "tx_list"}

    # Allowed kwarg mapping for each connector type (SOURCE)
    ALLOWED_SOURCE_KWARGS = {
        "MYSQL": {
            "mysql_conn_id",
            "schema_name",
            "table_name",
            "custom_query",
            "source_job_type",
            "header_less_file",
            "headers",
        },
        "EXCEL": {
            "excel_conn_id",
            "sheet_name",
            "header_less_file",
            "headers",
        },
        "MARIADB": {
            "mariadb_conn_id",
            "schema_name",
            "table_name",
            "custom_query",
            "source_job_type",
            "header_less_file",
            "headers",
        },
        "MONGODB": {
           "mongodb_conn_id",
           "database_name",
           "collection_name",
           "custom_query",
           "header_less_file",
           "headers",
        },
        "SFTP": {
            "sftp_conn_id",
            "remote_path",
            "delimiter",
            "header_less_file",
            "headers",
        },
        "REST_API": {
            "rest_conn_id",
            "endpoint",
            "header_less_file",
           "headers",
           "delimiter",
           "response_path",
           "method",
           "timeout",
        },
        "PSQL": {
            "postgres_conn_id",
            "schema_name",
            "table_name",
            "custom_query",
            "source_job_type",
            "header_less_file",
            "headers",
        },
        "MSSQL": {
            "mssql_conn_id",
            "schema_name",
            "table_name",
            "custom_query",
            "source_job_type",
            "header_less_file",
            "headers",
        },
        "ORACLE": {  # v3: Oracle support
            "oracle_conn_id",
            "schema_name",
            "table_name",
            "custom_query",
            "source_job_type",
            "header_less_file",
            "headers",
        },
        "S3": {"aws_conn_id", "bucket_name", "object_name", "header_less_file", "headers"},
        "AZUREBLOB": {
            "azure_conn_id",
            "container_name",
            "blob_name",
            "delimiter",
            "header_less_file",
            "headers",
        },
        "CLOUDSTORAGE": {
            "gcs_conn_id",
            "bucket_name",
            "object_name",
            "header_less_file",
            "headers",
        },
        "BIGQUERY": {
            "bigquery_conn_id",
            "dataset",
            "table",
            "header_less_file",
            "headers",
        },
        "SALESFORCE": {"salesforce_conn_id", "object_name", "query", "header_less_file", "headers"},
        "FILE": {"file_path", "delimiter", "header_less_file", "headers"},
    }

    # Allowed kwarg mapping (DESTINATION)
    ALLOWED_DESTINATION_KWARGS = {
        "MYSQL": {"mysql_conn_id", "schema_name", "table_name", "data_write_mode"},
        "PSQL": {"postgres_conn_id", "schema_name", "table_name", "data_write_mode"},
        "MSSQL": {"mssql_conn_id", "schema_name", "table_name", "data_write_mode"},
        "ORACLE": {"oracle_conn_id", "schema_name", "table_name", "data_write_mode"},  # v3: Oracle support
        "S3": {"aws_conn_id", "bucket_name", "object_name", "data_write_mode"},
        "AZUREBLOB": {"azure_conn_id", "container_name", "blob_name", "data_write_mode"},
        "CLOUDSTORAGE": {"gcs_conn_id", "bucket_name", "object_name", "data_write_mode"},
        "BIGQUERY": {"bigquery_conn_id", "dataset", "table", "data_write_mode"},
        "SALESFORCE": {"salesforce_conn_id", "object_name", "data_write_mode"},
        "FILE": {"file_path", "delimiter", "data_write_mode"},
        "MARIADB": {"mariadb_conn_id", "schema_name", "table_name", "data_write_mode"},
        "MONGODB": {"mongodb_conn_id", "database_name", "collection_name","data_write_mode",},
    }

    def __init__(self):
        try:
            self.source_conn_type_mappings = {
                "MYSQL": {
                    "mysql_conn_id": "connectionId",
                    "table_name": lambda p: p.get("tableName"),
                    "schema_name": lambda p: p.get("schemaName"),
                    "custom_query": lambda p: p.get("customQuery"),
                    "source_job_type": lambda p: p.get("sourceJobType"),
                    "header_less_file": lambda p: p.get("headerlessFile"),
                    "data_write_mode": lambda p: p.get("dataWriteMode"),
                    "headers": lambda p: p.get("headers"),
                },
                "EXCEL": {
                    "excel_conn_id": "connectionId",
                    "sheet_name": lambda p: p.get("tableName"),
                    "header_less_file": lambda p: p.get("headerlessFile"),
                    "headers": lambda p: p.get("headers"),
                },
                "MONGODB": {
                    "mongodb_conn_id": "connectionId",
                    "collection_name": lambda p: p.get("tableName"),
                    "database_name": lambda p: p.get("schemaName"),
                    "custom_query": lambda p: p.get("customQuery"),
                    "header_less_file": lambda p: p.get("headerlessFile"),
                    "headers": lambda p: p.get("headers"),
                    "data_write_mode": lambda p: p.get("dataWriteMode"),
                },
                "MARIADB": {
                      "mariadb_conn_id": "connectionId",
                      "table_name": lambda p: p.get("tableName"),
                      "schema_name": lambda p: p.get("schemaName"),
                      "custom_query": lambda p: p.get("customQuery"),
                      "source_job_type": lambda p: p.get("sourceJobType"),
                      "header_less_file": lambda p: p.get("headerlessFile"),
                     "data_write_mode": lambda p: p.get("dataWriteMode"),
                      "headers": lambda p: p.get("headers"),
                },
                "SFTP": {
                    "sftp_conn_id": "connectionId",
                    "remote_path": lambda p: p.get("path"),
                    "delimiter": lambda p: p.get("delimiter"),
                    "header_less_file": lambda p: p.get("headerlessFile"),
                    "headers": lambda p: p.get("headers"),
                },
                "REST_API": {
                    "rest_conn_id": "connectionId",
                    "endpoint": lambda p: p.get("path"),
                    "header_less_file": lambda p: p.get("headerlessFile"),
                    "headers": lambda p: p.get("headers"),
                    "delimiter": lambda p: p.get("delimiter"),
                    "response_path": lambda p: p.get("customQuery"),
                    "method": lambda p: "GET",
                    "timeout": lambda p: 30,
                },
                "PSQL": {
                    "postgres_conn_id": "connectionId",
                    "table_name": lambda p: p.get("tableName"),
                    "schema_name": lambda p: p.get("schemaName"),
                    "custom_query": lambda p: p.get("customQuery"),
                    "source_job_type": lambda p: p.get("sourceJobType"),
                    "header_less_file": lambda p: p.get("headerlessFile"),
                    "data_write_mode": lambda p: p.get("dataWriteMode"),
                    "headers": lambda p: p.get("headers"),
                },
                "MSSQL": {
                    "mssql_conn_id": "connectionId",
                    "table_name": lambda p: p.get("tableName"),
                    "schema_name": lambda p: p.get("schemaName"),
                    "custom_query": lambda p: p.get("customQuery"),
                    "source_job_type": lambda p: p.get("sourceJobType"),
                    "header_less_file": lambda p: p.get("headerlessFile"),
                    "data_write_mode": lambda p: p.get("dataWriteMode"),
                    "headers": lambda p: p.get("headers"),
                },
                "ORACLE": {  # v3: Oracle support
                    "oracle_conn_id": "connectionId",
                    "table_name": lambda p: p.get("tableName"),
                    "schema_name": lambda p: p.get("schemaName"),
                    "custom_query": lambda p: p.get("customQuery"),
                    "source_job_type": lambda p: p.get("sourceJobType"),
                    "data_write_mode": lambda p: p.get("dataWriteMode"),
                    "header_less_file": lambda p: p.get("headerlessFile"),
                    "headers": lambda p: p.get("headers"),
                },
                "S3": {
                    "aws_conn_id": "connectionId",
                    "bucket_name": lambda p: p.get("bucketName"),
                    "object_name": lambda p: p.get("objectName"),
                    "header_less_file": lambda p: p.get("headerlessFile"),
                    "headers": lambda p: p.get("headers"),
                },
                "AZUREBLOB": {
                    "azure_conn_id": "connectionId",
                    "container_name": lambda p: p.get("containerName"),
                    "blob_name": lambda p: p.get("objectName"),
                    "delimiter": lambda p: p.get("delimiter"),
                    "header_less_file": lambda p: p.get("headerlessFile"),
                    "headers": lambda p: p.get("headers"),
                    "data_write_mode": lambda p: p.get("dataWriteMode"),
                },
                "CLOUDSTORAGE": {
                    "gcs_conn_id": "connectionId",
                    "bucket_name": lambda p: p.get("bucketName"),
                    "object_name": lambda p: p.get("objectName"),
                    "header_less_file": lambda p: p.get("headerlessFile"),
                    "headers": lambda p: p.get("headers"),
                    "data_write_mode": lambda p: p.get("dataWriteMode"),
                },
                "BIGQUERY": {
                    "bigquery_conn_id": "connectionId",
                    "dataset": lambda p: p.get("schemaName"),
                    "table": lambda p: p.get("tableName"),
                    "header_less_file": lambda p: p.get("headerlessFile"),
                    "headers": lambda p: p.get("headers"),
                    "data_write_mode": lambda p: p.get("dataWriteMode"),
                },
                "SALESFORCE": {
                    "salesforce_conn_id": "connectionId",
                    "object_name": lambda p: p.get("objectName"),
                    "query": lambda p: p.get("customQuery"),
                    "header_less_file": lambda p: p.get("headerlessFile"),
                    "headers": lambda p: p.get("headers"),
                    "data_write_mode": lambda p: p.get("dataWriteMode"),
                },
                "FILE": {
                    "file_path": lambda p: p.get("objectName"),
                    "delimiter": lambda p: p.get("delimiter"),
                    "header_less_file": lambda p: p.get("headerlessFile"),
                    "headers": lambda p: p.get("headers"),
                    "data_write_mode": lambda p: p.get("dataWriteMode"),
                },
            }

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed while initializing Processor",
                error_source="Processor.__init__",
                original_exception=e
            ) from e

    def get_processors(self, conn_type, source_properties):
        try:
            conn_type = normalize_conn_type(conn_type)  # v3: normalize synonyms
            mapping = self.source_conn_type_mappings.get(conn_type)
            if not mapping:
                raise MorphusAirflowException(
                    message=f"No processor mapping defined for connector type '{conn_type}'",
                    error_source="Processor.get_processors"
                )

            result = {}

            for key, rule in mapping.items():
                try:
                    if callable(rule):
                        result[key] = rule(source_properties)
                    else:
                        result[key] = source_properties.get(rule)

                except MorphusAirflowException:
                    raise

                except Exception as e:
                    raise MorphusAirflowException(
                        message="Failed while building processor kwargs",
                        error_source="Processor.get_processors:rule_loop",
                        original_exception=e
                    ) from e

            return result

        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message="Unexpected error in Processor.get_processors",
                error_source="Processor.get_processors",
                original_exception=e
            ) from e

    def filter_kwargs(self, kwargs: dict, conn_type: str, is_source: bool) -> dict:
        try:
            conn_type = normalize_conn_type(conn_type)  # v3: normalize synonyms

            allowed = set(self.GLOBAL_ALLOWED)
            if is_source:
                allowed |= set(self.ALLOWED_SOURCE_KWARGS.get(conn_type, set()))
            else:
                allowed |= set(self.ALLOWED_DESTINATION_KWARGS.get(conn_type, set()))

            return {k: v for k, v in kwargs.items() if k in allowed}

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed while filtering kwargs",
                error_source="Processor.filter_kwargs",
                original_exception=e
            ) from e


# =====================================================================
# 3. JSON Loading Utility
# =====================================================================
def get_json(file_path: str) -> dict:
    try:
        if not file_path:
            raise MorphusAirflowException(
                message="JSON file path is empty",
                error_source="get_json"
            )

        with open(file_path, "r") as f:
            content = f.read().strip()

        if not content:
            raise MorphusAirflowException(
                message=f"Pipeline JSON file is empty: {file_path}",
                error_source="get_json",
                original_exception=f"Pipeline JSON file is empty: {file_path}"
            )

        return json.loads(content)

    except json.JSONDecodeError as e:
        raise MorphusAirflowException(
            message=f"Invalid JSON format in file: {file_path}",
            error_source="get_json",
            original_exception=e
        ) from e

    except MorphusAirflowException:
        raise

    except Exception as e:
        raise MorphusAirflowException(
            message="Unexpected error while reading pipeline JSON",
            error_source="get_json",
            original_exception=e
        ) from e


def get_dag(dag_id, schedule_interval, start_date, catchup):
    return DAG(
        dag_id=dag_id,
        schedule=schedule_interval,
        start_date=start_date,
        catchup=catchup,
    )


# =====================================================================
# 4. Build Execution Sequence
# =====================================================================
def build_execution_sequence(task_sequence: dict, task_id_to_operators: dict):
    execution_list = []

    try:
        if not isinstance(task_sequence, dict):
            raise MorphusAirflowException(
                message="taskSequence must be a dictionary",
                error_source="build_execution_sequence"
            )

        for task_id, deps in task_sequence.items():
            try:
                op = task_id_to_operators.get(task_id)
                if not op:
                    continue

                for dep in deps:
                    dep_op = task_id_to_operators.get(dep)
                    if dep_op:
                        dep_op >> op

                execution_list.append(op)

            except Exception as e:
                raise MorphusAirflowException(
                    message=f"Failed while building execution dependency for task '{task_id}'",
                    error_source="build_execution_sequence:task_loop",
                    original_exception=e
                ) from e

        return execution_list

    except MorphusAirflowException:
        raise

    except Exception as e:
        raise MorphusAirflowException(
            message="Unexpected error while building execution sequence",
            error_source="build_execution_sequence",
            original_exception=e
        ) from e


# =====================================================================
# 5. PROCESS SOURCES
# =====================================================================
def process_sources(dag: DAG, sources: dict, metadata: list[dict], job: dict = None):
    processor = Processor()
    mapper = OperatorMapper()

    source_type = sources["sourceType"]
    props = sources["sourceProperties"]
    source_task_id = props["taskId"]

    destination_task_ids = {
        m["destinationTaskId"]
        for m in metadata
        if m["sourceTaskId"] == source_task_id
    }

    if not destination_task_ids and job:
        destination_task_ids = {
            d["destinationProperties"]["taskId"]
            for d in job["destinations"]
        }

    source_ops = []

    for dest_id in destination_task_ids:
        current_task_id = f"{source_task_id}_{dest_id}"

        filtered_metadata = [
            m for m in metadata
            if m["sourceTaskId"] == source_task_id
               and m["destinationTaskId"] == dest_id
        ]

        tx_list = get_transformations_for_source(filtered_metadata, source_task_id)

        operator_cls = mapper.get_source_operator(source_type)

        raw_kwargs = processor.get_processors(source_type, props)
        raw_kwargs.pop("data_write_mode", None)

        raw_kwargs["task_id"] = current_task_id
        raw_kwargs["tx_list"] = tx_list

        filtered = processor.filter_kwargs(raw_kwargs, source_type, is_source=True)
        source_ops.append(operator_cls(dag=dag, **filtered))

    return source_ops


# =====================================================================
# 6. PROCESS DESTINATIONS
# =====================================================================
def process_destinations(dag: DAG, destination: dict):
    processor = Processor()
    mapper = OperatorMapper()

    dest_task_id = None

    try:
        dest_type = destination.get("destinationType")
        props = destination.get("destinationProperties") or {}

        dest_task_id = props.get("taskId")

        operator_cls = mapper.get_destination_operator(dest_type)

        raw_kwargs = processor.get_processors(dest_type, props)

        if dest_task_id:
            raw_kwargs["task_id"] = dest_task_id

        filtered = processor.filter_kwargs(raw_kwargs, dest_type, is_source=False)

        return operator_cls(dag=dag, **filtered)

    except MorphusAirflowException as e:
        if not e.task_id and dest_task_id:
            e.task_id = dest_task_id
        raise

    except Exception as e:
        raise MorphusAirflowException(
            message="Error occurred while processing destination",
            task_id=dest_task_id,
            error_source="process_destinations",
            original_exception=e
        ) from e


# =====================================================================
# 7. TRANSFORMATION HELPERS (existing logic)
# =====================================================================
def get_transformations_for_source(metadata: list[dict], source_task_id: str) -> list[list]:
    tx_list = []
    for md in metadata:
        if md.get("sourceTaskId") == source_task_id:
            tx = md.get("transformations") or {}
            ids = tx.get("ids") or []
            for t in ids:
                tx_list.append(t)
    return tx_list