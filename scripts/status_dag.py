import json
import os

from airflow.models import DagRun
from sqlalchemy.exc import SQLAlchemyError

from helper import logger


def check_dag_status():
    try:
        json_data = os.environ.get('JSON_DATA')

        if not json_data:
            logger.error('JSON_DATA environment variable not set.')
            return 'Failed: JSON_DATA environment variable not set.'

        dag_json_data = json.loads(json_data)
        dag_id = dag_json_data.get('pipelineId')

        if not dag_id or not dag_id.strip():
            logger.error('pipelineId is None or an empty string.')
            return 'Failed: pipelineId is None or an empty string.'

        # Query the status of the DAG runs
        dag_runs = DagRun.find(dag_id=dag_id)

        if not dag_runs:
            logger.info(f"No runs found for DAG {dag_id}.")
            return f"No runs found for DAG {dag_id}."

        # Get the last run status
        last_run = dag_runs[-1]
        logger.info(f"DAG {dag_id} Last Run State: {last_run.state}")

        return f"Last Run State: {last_run.state}"

    except ValueError as e:
        logger.error(f"Failed to check DAG status: {e}")
        return f"Failed to check DAG status: {e}"
    except SQLAlchemyError as e:
        logger.error(f"An error occurred while interacting with the database: {e}")
        return f"An error occurred while interacting with the database: {e}"
    except Exception as e:
        logger.error(f"An unexpected error occurred while checking the DAG status: {e}")
        return f"An unexpected error occurred while checking the DAG status."


if __name__ == "__main__":
    check_dag_status()
