import json
import sys
from pathlib import Path

from helper import save_dag_json, logger

DAG_DIR = Path("/opt/airflow/dag_json_data")


def create_airflow_dag():
    json_path = None

    try:
        # --------------------------------------------------
        # 1. READ JSON FROM STDIN
        # --------------------------------------------------
        dag_json_data = json.load(sys.stdin)

        # --------------------------------------------------
        # 2. VALIDATE pipelineId
        # --------------------------------------------------
        pipeline_id = dag_json_data.get("pipelineId")
        if not pipeline_id or not pipeline_id.strip():
            raise ValueError("pipelineId is required")

        pipeline_id = pipeline_id.strip()
        dag_file = DAG_DIR / f"{pipeline_id}.json"

        # --------------------------------------------------
        # 3. WRITE FINAL DAG JSON (DIRECT WRITE)
        # --------------------------------------------------
        DAG_DIR.mkdir(parents=True, exist_ok=True)
        save_dag_json(dag_json_data, pipeline_id)

        logger.info(f"DAG JSON created at {dag_file}")
        print("DAG created successfully")

    except Exception as e:
        logger.error(f"Failed to create DAG: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    create_airflow_dag()
