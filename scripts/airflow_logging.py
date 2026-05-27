import logging


# TODO: re-visit this code to understand what is going on, and refactor it.
# Adjust Airflow logging configuration
def configure_airflow_logging():
    # Suppress logs from specific Airflow modules
    logging.getLogger('airflow').setLevel(logging.ERROR)
    logging.getLogger('airflow.utils').setLevel(logging.ERROR)
    logging.getLogger('airflow.settings').setLevel(logging.ERROR)
    logging.getLogger('airflow.models').setLevel(logging.ERROR)

    # Set global logging level to ERROR
    logging.basicConfig(level=logging.ERROR)


# Call the logging configuration function
configure_airflow_logging()
