import json
import logging
import os

from airflow.models import Connection
from airflow.settings import Session
from requests import RequestException

from airflow_logging import configure_airflow_logging
from helper import connection_exists

# Configure logging
configure_airflow_logging()

# Set up logger
logger = logging.getLogger(__name__)


def update_airflow_connection():
    # Read JSON data from environment variable
    json_data = os.environ.get('JSON_DATA')
    if not json_data:
        raise RequestException('Failed: JSON_DATA environment variable not set.')

    # Read connection details from JSON data
    connection = json.loads(json_data)
    conn_id = connection.get('conn_id')

    if not conn_id:
        raise RequestException('Failed: conn_id not found in JSON data.')

    # Check if Airflow Connection Exists
    if not connection_exists(conn_id):
        raise RequestException(f'Failed: Connection with ID {conn_id} does not exist.')

    # If the connection exists, update the existing connection
    session = Session()
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()

    if not existing_conn:
        session.close()
        raise RequestException(f'Failed: No existing connection found with ID {conn_id}')

    # Update the connection details
    for key, value in connection.items():
        setattr(existing_conn, key, value)
    session.commit()
    session.close()

    logger.info(f"Connection {conn_id} updated successfully.")
    return 'Connection updated successfully'


if __name__ == "__main__":
    update_airflow_connection()
