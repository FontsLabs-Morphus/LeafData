import json
import sys
from pathlib import Path

from helper import logger

DAG_DIR = Path("/opt/airflow/dag_json_data")


def delete_airflow_dag():
    try:
        payload = json.load(sys.stdin)
        pipeline_id = payload.get("pipelineId")

        if not pipeline_id:
            raise ValueError("pipelineId is required")

        dag_file = DAG_DIR / f"{pipeline_id}.json"

        if dag_file.exists():
            dag_file.unlink()
            print(f"DAG {pipeline_id} deleted")
        else:
            print(f"DAG {pipeline_id} does not exist (noop)")

    except Exception as e:
        logger.error(f"Failed to delete DAG: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    delete_airflow_dag()
