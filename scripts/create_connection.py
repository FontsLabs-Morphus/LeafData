import json
import logging
import os
from typing import Dict, Any

from airflow.models import Connection
from airflow.settings import Session
from requests import RequestException

from airflow_logging import configure_airflow_logging
from helper import connection_exists

# Configure logging
configure_airflow_logging()
logger = logging.getLogger(__name__)

# v3: accept synonyms from chatbot / payload
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
        "GCS": "CLOUDSTORAGE",
    }
    return aliases.get(t, t)

conn_type_mapper: Dict[str, str] = {
    "AZUREBLOB": "wasb",
    "S3": "s3",
    "CLOUDSTORAGE": "google_cloud_platform",
    "MYSQL": "mysql",
    "PSQL": "postgres",
    "MSSQL": "mssql",
    "ORACLE": "oracle",  # v3: Oracle support
    "SALESFORCE": "salesforce",
    "BIGQUERY": "google_cloud_platform",
    "SFTP": "sftp",
    }

connection_requirements: Dict[str, list[str]] = {
    "mysql": ["host", "port", "login", "password", "schema"],
    "postgres": ["host", "port", "login", "password", "schema"],
    "mssql": ["host", "port", "login", "password", "schema"],
    "oracle": ["host", "port", "login", "password", "schema"],  # v3: Oracle support (schema = service name)
    "sftp": ["host", "port", "login", "password"],
    "s3": ["aws_access_key_id", "aws_secret_access_key", "region_name", "bucket_name"],
    "wasb": ["connection_string"],
    "google_cloud_platform": ["key_json"],
    "salesforce": ["login", "password", "securityToken"],

}

def build_extra_field(mapped_type: str, fields: Dict[str, Any]) -> str:
    if mapped_type == "salesforce":
        token = fields.pop("securityToken", None)
        if not token:
            raise RequestException("Missing required field: securityToken")
        return json.dumps({"security_token": token})

    if mapped_type == "google_cloud_platform":
        keyfile_json = fields.get("key_json")
        if not keyfile_json:
            raise RequestException("Missing required field: key_json")
        try:
            keyfile_dict = json.loads(keyfile_json)
        except json.JSONDecodeError as e:
            raise RequestException(f"Invalid JSON in key_json: {e}")
        return json.dumps({
            "extra__google_cloud_platform__keyfile_dict": json.dumps(keyfile_dict),
            "extra__google_cloud_platform__project": keyfile_dict.get("project_id"),
            "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/cloud-platform",
        })

    return json.dumps(fields)

def validate_and_filter_fields(connection: Dict[str, Any], conn_type: str) -> Dict[str, Any]:
    """
    Validates and filters fields in the connection based on conn_type.
    Maps conn_type using conn_type_mapper and ensures only the required fields are included.
    """
    conn_type = normalize_conn_type(conn_type)  # v3
    mapped_type: str = conn_type_mapper.get(conn_type, conn_type)

    required: tuple[str, ...] = tuple(connection_requirements.get(mapped_type, ()))
    filtered: Dict[str, Any] = {k: v for k, v in connection.items() if k in required}

    database_name = connection.get("database")

    # v3: require schema/database for Postgres + Oracle
    if mapped_type in {"postgres", "oracle"} and not database_name:
        raise RequestException(f"Missing required field: 'database' for {mapped_type} connection.")

    # In this project, "database" is used as Connection.schema
    # For Oracle, schema == service_name in Airflow Connection UI.
    if database_name:
        filtered["schema"] = database_name

    if mapped_type in {"s3", "wasb", "google_cloud_platform"}:
        filtered = {"extra": build_extra_field(mapped_type, filtered)}
    elif mapped_type == "salesforce":
        filtered["extra"] = build_extra_field(mapped_type, filtered)
    if "port" in filtered and filtered["port"] not in (None, ""):
        filtered["port"] = int(filtered["port"])
    filtered["conn_type"] = mapped_type
    return filtered

def create_airflow_connection() -> str:
    json_data: str | None = os.environ.get("JSON_DATA")
    if not json_data:
        raise RequestException("Failed: JSON_DATA environment variable not set.")

    connection: Dict[str, Any] = json.loads(json_data)
    conn_type: str | None = connection.get("conn_type")
    conn_id: str | None = connection.get("conn_id")

    if not conn_type or not conn_id:
        raise RequestException("Failed: Missing conn_id or conn_type.")

    connection = validate_and_filter_fields(connection, conn_type)
    connection["conn_id"] = conn_id

    if connection_exists(conn_id):
        raise RequestException(f"Failed: Connection with ID {conn_id} already exists.")

    try:
        new_conn: Connection = Connection(**connection)
        session: Session = Session()
        session.add(new_conn)
        session.commit()
        session.close()
        logger.info(f"Connection {conn_id} created successfully.")
        return "Connection created successfully"
    except Exception as e:
        logger.error(f"Error creating connection: {e}")
        raise RequestException(f"Failed to create connection: {e}")

if __name__ == "__main__":
    create_airflow_connection()