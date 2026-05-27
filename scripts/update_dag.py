import json
import os
from pathlib import Path

from requests import RequestException

from helper import logger, save_dag_json


def update_airflow_dag():
    try:
        json_data = os.environ.get('JSON_DATA')
        if not json_data:
            raise RequestException('Failed: JSON_DATA environment variable not set.')

        dag_json_data = json.loads(json_data)
        pipeline_id = dag_json_data.get('pipelineId')

        file_path = Path("/opt/airflow/dag_json_data") / f"{pipeline_id}.json"
        if not file_path.exists():
            raise FileExistsError(f"File with pipeline ID {pipeline_id} does not exist.")

        save_dag_json(dag_json_data, pipeline_id)

        logger.info(f'Dag {pipeline_id} updated successfully.')
        return 'Dag updated successfully'

    except FileNotFoundError as e:
        logger.error(e)
        raise
    except RequestException as e:
        logger.error(f"Failed to update DAG: {e}")
        raise
    except ValueError as e:
        logger.error(f"Failed to update DAG: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to update DAG: {e}")
        raise Exception(f"An unexpected error occurred while creating/updating DAG JSON data.")


if __name__ == "__main__":
    update_airflow_dag()
