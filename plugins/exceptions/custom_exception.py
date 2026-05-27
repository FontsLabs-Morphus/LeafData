class MorphusAirflowException(Exception):
    def __init__(self, message, *, dag_id=None, task_id=None, error_source=None, original_exception=None):
        super().__init__(message)
        self.dag_id = dag_id
        self.task_id = task_id
        self.error_source = error_source
        self.original_exception = original_exception
