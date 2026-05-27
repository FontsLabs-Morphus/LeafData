import logging
import os
from datetime import datetime
from typing import Dict

from airflow import DAG
from custom_operators.utils import get_json, process_sources, process_destinations
from exceptions.custom_exception import MorphusAirflowException

from custom_logger.execution_logger import log_execution

# from custom_logger.logger_utils import log_dag_state
# from cleanup_temp_datafiles_dag import cleanup_temp_files

DAG_JSON_DATA_DIR = os.getenv('DAG_JSON_DATA_DIR', '/opt/airflow/dag_json_data')


def get_dag_properties(config: dict) -> dict:
    """
    Extracts DAG properties from the config dictionary.

    Parameters:
        config (Dict): The configuration dictionary containing DAG variables.

    Returns:
        Dict: A dictionary containing DAG properties.
    """

    dag_id = config.get("pipelineId")
    try:
        dag_vars = config.get("dag_variables", {})
        return {
            "dag_id": dag_id,
            "schedule_interval": config.get("scheduleInterval", "@daily"),
            "start_date": datetime.strptime(config.get("startDate", "2024/06/13"), "%m/%d/%Y"),
            "catchup": config.get("catchup", False),
            "max_active_runs": 1,
            "concurrency": 1,
            "is_paused_upon_creation": False
        }
    except MorphusAirflowException as e:
        e.dag_id = dag_id
        raise
    except ValueError as e:
        raise MorphusAirflowException(
            message="Invalid startDate format, expected YYYY/MM/DD",
            dag_id=dag_id,
            error_source="get_dag_properties",
            original_exception=e
        )
    except KeyError as e:
        logging.error(f"Error extracting DAG properties: {e}")
        raise MorphusAirflowException(
            "Error extracting DAG properties Dag Properties isn't set Properly",
            dag_id=dag_id,
            error_source="get_dag_properties",
            original_exception=e
        )
    except Exception as e:
        raise MorphusAirflowException(
            "Error extracting DAG properties Dag Properties isn't set Properly",
            dag_id=dag_id,
            error_source="get_dag_properties",
            original_exception=e
        )


def create_dag(config: Dict) -> DAG:
    """
    Creates a DAG object based on the provided configuration.

    Parameters:
        config (Dict): The configuration dictionary containing DAG and task information.

    Returns:
        DAG: The created DAG object.
    """
    dag_id = config.get("pipelineId")

    try:
        dag_properties = get_dag_properties(config)
        dag = DAG(
            **dag_properties
            # Demo-safe: temporarily disable DAG-level callbacks
            # on_success_callback=lambda ctx: log_dag_state(ctx, "SUCCESS"),
            # on_failure_callback=lambda ctx: log_dag_state(ctx, "FAILED"),
        )

        jobs = config["jobs"]
        task_id_to_operators = {}

        # Create operators for sources and destinations
        for job in jobs:
            source = job.get("source")
            source_task_id = source.get("sourceProperties").get("taskId")

            # Process source operators (may return multiple for each destination)
            source_operators = process_sources(dag, source, job.get("joinMetadata", []), job)
            for src_op in source_operators:
                task_id_to_operators[src_op.task_id] = src_op

            # Handle multiple destinations
            for destination in job.get("destinations"):
                destination_task_id = destination.get("destinationProperties").get("taskId")
                destination_operator = process_destinations(dag, destination)
                task_id_to_operators[destination_task_id] = destination_operator

        # Expand task sequence for per-destination source operators
        task_sequence = config.get("taskSequence", {})
        expanded_task_sequence = {}

        for src_id, dest_list in task_sequence.items():
            for dest_id in dest_list:
                expanded_source_id = f"{src_id}_{dest_id}"
                expanded_task_sequence[expanded_source_id] = [dest_id]

        # Direct source -> destination dependencies
        for src_task_id, dest_task_ids in expanded_task_sequence.items():
            src_op = task_id_to_operators[src_task_id]

            for dest_task_id in dest_task_ids:
                dest_op = task_id_to_operators[dest_task_id]
                src_op >> dest_op

        return dag

    except MorphusAirflowException as e:
        log_execution(
            dag_id=dag_id,
            task_id=e.task_id,
            status="FAILED",
            error_source=e.error_source,
            error_message=str(e),
            root_cause=repr(e.original_exception)
        )
        raise
    except Exception as e:
        logging.error(f"Error creating DAG: {e}")

        log_execution(
            dag_id=dag_id,
            status="FAILED",
            error_source="create_dag",
            error_message=str(f"Error creating DAG: {e}"),
            root_cause=repr(e)
        )
        raise


for filename in os.listdir(DAG_JSON_DATA_DIR):
    if not filename.endswith(".json"):
        continue

    file_path = os.path.join(DAG_JSON_DATA_DIR, filename)

    try:
        config = get_json(file_path)

        dag_id = config.get("pipelineId")
        if not dag_id:
            raise MorphusAirflowException(
                message="pipelineId missing in JSON",
                error_source="DAG_BOOTSTRAP",
            )

        globals()[dag_id] = create_dag(config)

    except MorphusAirflowException as e:
        logging.error(f"Skipping DAG file {filename}: {e}")
        log_execution(
            dag_id=e.dag_id or filename,
            status="FAILED",
            error_source=e.error_source,
            error_message=str(e),
            root_cause=repr(e.original_exception),
        )

    except Exception as e:
        logging.error(f"Unexpected error loading {filename}: {e}")
        log_execution(
            dag_id=filename,
            status="FAILED",
            error_source="DAG_BOOTSTRAP",
            error_message=str(e),
            root_cause=repr(e),
        )