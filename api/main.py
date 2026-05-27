import json
import logging
import os
import re
from pathlib import Path
from typing import Tuple

import docker
from flask import Flask, request, jsonify, Response

from avro_schema.validator import validate_json_against_avro
from call_functions import conn_exec
from helpper_functions import is_join_metadata_valid
from custom_exception import MorphusAPIException

# Configure logging
logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
app = Flask(__name__)
name_container = 'airflow-webserver'

conn_mapping = {
    'POST': 'create_connection',
    'PUT': 'update_connection',
    'DELETE': 'delete_connection'
}

dag_mapping = {
    'POST': 'create_dag',
    'PUT': 'update_dag',
    'DELETE': 'delete_dag'
}

message_mapping = {
    'POST': 'created',
    'PUT': 'updated',
    'DELETE': 'deleted'
}

response_dict = {
    "Message": "",
    "StatusFlag": False,
    "Status": "",
    "CompleteMessage": ""
}

DAG_DIR = Path("/opt/airflow/dag_json_data")

# List of supported database types and cloud types
database_types = ["MYSQL", "PSQL", "MSSQL", "ORACLE","MARIADB", "MONGODB", "EXCEL"]
cloud_types = ["S3", "AZUREBLOB", "CLOUDSTORAGE", "SALESFORCE", "BIGQUERY", "SFTP"]

# Dictionary to map conn_type to file paths (simplified for database and cloud)
schema_mapping = {
    "database_file_path": 'avro_schema/database_connection_schema.avsc',
    "cloud_file_path": 'avro_schema/cloud_connection_schema.avsc',
    "create_dag": 'avro_schema/create_dag_schema.avsc',
    "update_dag": 'avro_schema/create_dag_schema.avsc',
    "delete_dag": 'avro_schema/delete_dag_schema.avsc',
    "trigger_dag_file_path": 'avro_schema/trigger_dag_schema.avsc',
    "delete_connection": 'avro_schema/delete_connection_schema.avsc'
}


def get_module_name(request_type: str, mapping: dict) -> str:
    """Return the script name based on request type and mapping."""
    try:
        module = mapping.get(request_type)

        if not module:
            raise KeyError(f"No module mapping found for request_type='{request_type}'")

        return f"{module}.py"

    except MorphusAPIException as e:
        raise
    except KeyError as e:
        raise MorphusAPIException(
            message="Invalid request type provided",
            source_function="get_module_name",
            original_exception=e,
        ) from e
    except Exception as e:
        raise MorphusAPIException(
            message="Unexpected error while resolving module name",
            source_function="get_module_name",
            original_exception=e,
        ) from e

@app.route("/connection", methods=['POST', 'PUT', 'DELETE'])
def connection():
    try:
        my_dict = request.get_json()
        request_type = request.method
        logger.info(f"Connection request type: {request_type}, data: {my_dict}")
        data = json.dumps(my_dict)
        client = docker.from_env()

        conn_type = my_dict.get("conn_type")
        # Early mapping for file_path based on request_type or conn_type
        file_path = (
            schema_mapping.get("delete_connection") if request_type == 'DELETE'
            else schema_mapping.get("database_file_path") if conn_type in database_types
            else schema_mapping.get("cloud_file_path") if conn_type in cloud_types
            else None
        )

        # Handle invalid conn_type case
        if not file_path:
            response_dict.update({
                "Message": "Invalid conn_type provided.",
                "StatusFlag": False,
                "Status": "failed",
                "CompleteMessage": f"Invalid conn_type: {conn_type}"
            })
            return jsonify(response_dict), 400

        # Validate request data with the appropriate schema
        is_valid_value, error_message_value = validate_json_against_avro(my_dict, file_path)

        if not is_valid_value:
            logger.error(f"Avro schema validation failed: {error_message_value}")
            response_dict["Message"] = "Connection failed."
            response_dict["StatusFlag"] = False
            response_dict["Status"] = f"failed"
            response_dict["CompleteMessage"] = f"Avro schema validation failed: {error_message_value}"
            return jsonify(response_dict), 400

        container = client.containers.get(name_container)
        module_name = get_module_name(request_type, conn_mapping)
        func_response = conn_exec(data, container, module_name)

        response_dict["Message"] = f"Connection {message_mapping.get(request_type)} successfully."
        response_dict["StatusFlag"] = True
        response_dict["Status"] = f"connected"
        response_dict["CompleteMessage"] = func_response

        return jsonify(response_dict), 201

    except Exception as e:

        if not isinstance(e, MorphusAPIException):
            e = MorphusAPIException(
                message= "Connection failed",
                error_source= "connection",
                original_exception=e
            )
        func_response = str(e)
        logger.error(f"Error occurred: {func_response}")
        response_dict["Message"] = "Connection failed."
        response_dict["StatusFlag"] = False
        response_dict["Status"] = f"failed"
        response_dict["CompleteMessage"] = func_response
        return jsonify(response_dict), 500


@app.route("/dag", methods=["POST", "PUT", "DELETE"])
def dag():
    try:
        my_dict = request.get_json()
        request_type = request.method

        logger.info(f"DAG request type: {request_type}")

        # --------------------------------------------------
        # 1. Resolve schema
        # --------------------------------------------------
        schema_key = dag_mapping.get(request_type)
        if not schema_key:
            raise ValueError(f"Unsupported request type: {request_type}")

        avro_schema = schema_mapping.get(schema_key)
        if not avro_schema:
            raise ValueError(f"No Avro schema found for {schema_key}")

        # --------------------------------------------------
        # 2. Avro validation
        # --------------------------------------------------
        is_valid, error = validate_json_against_avro(my_dict, avro_schema)
        if not is_valid:
            return jsonify({
                "Message": "DAG validation failed",
                "StatusFlag": False,
                "Status": "failed",
                "CompleteMessage": error
            }), 400

        # --------------------------------------------------
        # 3. joinMetadata validation (POST / PUT only)
        # --------------------------------------------------
        if request_type != "DELETE":
            is_valid_join, join_error = is_join_metadata_valid(my_dict)
            if not is_valid_join:
                return jsonify({
                    "Message": "Invalid joinMetadata",
                    "StatusFlag": False,
                    "Status": "failed",
                    "CompleteMessage": join_error
                }), 400

        # --------------------------------------------------
        # 4. pipelineId validation
        # --------------------------------------------------
        pipeline_id = my_dict.get("pipelineId")
        if not pipeline_id or not pipeline_id.strip():
            raise ValueError("pipelineId is required")

        pipeline_id = pipeline_id.strip()
        dag_file = DAG_DIR / f"{pipeline_id}.json"

        # --------------------------------------------------
        # 5. CREATE / UPDATE / DELETE
        # --------------------------------------------------
        DAG_DIR.mkdir(parents=True, exist_ok=True)

        if request_type == "DELETE":
            if dag_file.exists():
                dag_file.unlink()
                message = "deleted"
            else:
                message = "does not exist (noop)"

        else:  # POST / PUT
            with open(dag_file, "w", encoding="utf-8") as f:
                json.dump(my_dict, f, indent=2)
            message = "saved"

        # --------------------------------------------------
        # 6. Success response
        # --------------------------------------------------
        return jsonify({
            "Message": f"DAG {message} successfully",
            "StatusFlag": True,
            "Status": "success",
            "CompleteMessage": str(dag_file)
        }), 201

    except Exception as e:
        logger.error("DAG API failed", exc_info=True)

        if not isinstance(e, MorphusAPIException):
            e = MorphusAPIException(
                message= "Dag failed",
                error_source= "dag",
                original_exception=e
            )

        return jsonify({
            "Message": "DAG operation failed",
            "StatusFlag": False,
            "Status": "failed",
            "CompleteMessage": str(e)
        }), 500




@app.route("/triggerdag", methods=['POST'])
def trigger():
    try:
        my_dict = request.get_json()
        logger.info(f"Triggering DAG with config: {my_dict}")
        data = json.dumps(my_dict)

        # Define the schema file for trigger DAG
        schema_file = schema_mapping.get("trigger_dag_file_path")

        # Validate the request data against the Avro schema
        is_valid_value, error_message_value = validate_json_against_avro(my_dict, schema_file)

        if not is_valid_value:
            logger.error(f"Avro schema validation failed: {error_message_value}")
            response_dict["Message"] = "DAG triggering failed."
            response_dict["StatusFlag"] = False
            response_dict["Status"] = "failed"
            response_dict["CompleteMessage"] = f"Avro schema validation failed: {error_message_value}"
            return jsonify(response_dict), 400

        # Connect to Docker client and get the container
        client = docker.from_env()
        container = client.containers.get(name_container)

        # Execute the function to trigger the DAG
        func_response = conn_exec(data, container, 'trigger_dag.py')

        # Handle the response
        response_dict["CompleteMessage"] = func_response

        if "running" in func_response.lower():
            response_dict["Message"] = "DAG is already running. The trigger request has been rejected."
            response_dict["StatusFlag"] = False
            response_dict["Status"] = f"failed"

        else:
            response_dict["Message"] = "DAG triggered successfully."
            response_dict["StatusFlag"] = True
            response_dict["Status"] = f"Triggered"

        return jsonify(response_dict), 201

    except Exception as e:

        if not isinstance(e, MorphusAPIException):
            e = MorphusAPIException(
                message= "Trigger failed",
                error_source= "tigger",
                original_exception=e
            )

        func_response = str(e)
        logger.error(f"Error occurred: {func_response}")
        response_dict["Message"] = "DAG triggering failed."
        response_dict["StatusFlag"] = False
        response_dict["Status"] = f"failed"
        response_dict["CompleteMessage"] = func_response
        return jsonify(response_dict), 500


@app.route("/statusdag", methods=['GET'])
def dagstatus() -> Tuple[Response, int]:
    try:
        dag_id: str | None = request.args.get("id")

        if not dag_id:
            response_dict["Message"] = "Missing 'id' parameter."
            response_dict["StatusFlag"] = False
            response_dict["Status"] = "failed"
            response_dict["CompleteMessage"] = f"Missing required query parameter 'id'."
            return jsonify(response_dict), 400

        logger.info(f"Status check for DAG with ID: {dag_id}")
        client: docker.DockerClient = docker.from_env()
        container: docker.models.containers.Container = client.containers.get(name_container)

        data: str = json.dumps({"pipelineId": dag_id})
        func_response: str = conn_exec(data, container, 'status_dag.py')

        response_message: str = f"{(m := re.search(r'DAG (.+) Last Run State: (.+)', func_response)).group(1)} Last Run State: {m.group(2)}" if (
            m := re.search(r'DAG (.+) Last Run State: (.+)',
                           re.sub(r'\x1b\[[0-9;]*[a-zA-Z]', '', func_response))) else "Dag Status Check Failed."
        status_response_msg: str = (
            m := re.search(r'Last Run State: (\w+)', re.sub(r'\x1b\[[0-9;]*[a-zA-Z]', '', response_message))).group(1)

        response_dict["CompleteMessage"] = func_response
        response_dict["Message"] = response_message
        response_dict["StatusFlag"] = True
        response_dict["Status"] = status_response_msg

        return jsonify(response_dict), 201

    except Exception as e:

        if not isinstance(e, MorphusAPIException):
            e = MorphusAPIException(
                message= "status check failed",
                error_source= "dagstatus",
                original_exception=e
            )
        func_response: str = str(e)
        logger.error(f"Error occurred: {func_response}")
        response_dict["Message"] = "DAG Status Check failed."
        response_dict["StatusFlag"] = False
        response_dict["Status"] = f"failed"
        response_dict["CompleteMessage"] = func_response
        return jsonify(response_dict), 500


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001)
