import json
import logging
import os
from pathlib import Path

from airflow.models import Connection
from airflow.settings import Session

from airflow_logging import configure_airflow_logging


# Configure logging for Airflow
def configure_logging():
    configure_airflow_logging()
    logger = logging.getLogger(__name__)
    return logger


logger = configure_logging()


def connection_exists(conn_id):
    session = Session()
    connection = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    session.close()
    return connection is not None


def save_dag_json(data: dict, file_name: str, directory: str = "/opt/airflow/dag_json_data"):
    if not data:
        raise TypeError(f"The 'data' argument must be a dictionary but got data = {data}.")

    try:
        os.makedirs(directory, exist_ok=True)
        file_path = Path(directory) / f"{file_name}.json"

        with file_path.open('w') as json_file:
            json.dump(data, json_file, indent=4)

        logger.info(f"Saved DAG JSON data to {file_path}")

    except OSError as e:
        logger.error(f"Failed to create directory or write file {str(file_path.absolute())}: {e}")
        raise OSError(f"Failed to create directory or write file {file_path}")

    except Exception as e:
        logger.error(f"An unexpected error occurred while saving DAG JSON data: {e}")
        raise Exception(f"An unexpected error occurred while saving DAG JSON data.")
