import json
import os
from datetime import datetime

from airflow.api.client.local_client import Client
from airflow.models import DagRun
from requests import RequestException
from sqlalchemy.exc import SQLAlchemyError

from helper import logger


def is_dag_running(dag_id):
    try:
        dag_runs = DagRun.find(dag_id=dag_id)

        for run in dag_runs:
            if run.state == 'running':
                return 'running'

        return None

    except SQLAlchemyError as e:
        logger.error(f"An error occurred while checking DAG status: {e}")
        raise SQLAlchemyError(f"An error occurred while checking DAG status: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while checking DAG status: {e}")
        raise Exception(f"An unexpected error occurred while checking DAG status.")


def trigger_airflow_dag():
    try:
        json_data = os.environ.get('JSON_DATA')

        if not json_data:
            raise RequestException('Failed: JSON_DATA environment variable not set.')

        dag_json_data = json.loads(json_data)
        dag_id = dag_json_data.get('pipelineId')
        base_run_id = dag_json_data.get('runId')

        if not dag_id.strip():
            raise ValueError('Failed: pipelineId is None or an empty string.')

        if not base_run_id or not base_run_id.strip():
            raise ValueError('Failed: runId is None or an empty string.')

        if is_dag_running(dag_id) == 'running':
            logger.info(f"DAG {dag_id} is already running. No new trigger.")
            return f"DAG {dag_id} is already running."

        run_id = f"{base_run_id}_{datetime.now():%Y%m%d%H%M%S}"

        client = Client(None, None)
        client.trigger_dag(dag_id=dag_id, run_id=run_id, conf={})

        logger.info(f"DAG {dag_id} triggered successfully with run_id {run_id}.")
        return f"DAG {dag_id} triggered successfully with run_id {run_id}"

    except RequestException as e:
        logger.error(f"Failed to trigger DAG: {e}")
        raise RequestException(f"Failed to trigger DAG: {e}")
    except ValueError as e:
        logger.error(f"Failed to trigger DAG: {e}")
        raise ValueError(f"Failed to trigger DAG: {e}")
    except SQLAlchemyError as e:
        logger.error(f"An error occurred while interacting with the database: {e}")
        raise SQLAlchemyError(f"An error occurred while interacting with the database: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while triggering the DAG: {e}")
        raise Exception(f"An unexpected error occurred while triggering the DAG.")


if __name__ == "__main__":
    trigger_airflow_dag()
