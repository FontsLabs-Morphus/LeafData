from custom_logger.execution_logger import log_pipeline_run_status


def log_dag_state(context, status: str):
    dag_run = context["dag_run"]

    log_pipeline_run_status(
        dag_id=dag_run.dag_id,
        run_id=dag_run.run_id,
        status=status,
        execution_date=dag_run.execution_date,
        start_time=dag_run.start_date,
        end_time=dag_run.end_date if status in ("SUCCESS", "FAILED") else None,
        trigger_type="event" if dag_run.external_trigger else "scheduled",
    )
