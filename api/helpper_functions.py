from typing import Tuple
import logging


logger = logging.getLogger(__name__)

def is_join_metadata_valid(payload: dict) -> Tuple[bool, str]:
    """
    Returns:
        (True, "") if valid
        (False, error_message) if invalid
    """
    jobs = payload.get("jobs", [])

    for job in jobs:
        logger.info(f" I am in the Json job: {job}")
        job_id = job.get("id", "UNKNOWN_JOB")
        join_metadata = job.get("joinMetadata")

        if join_metadata is None:
            return False, (
                f"Job {job_id}: 'joinMetadata' is missing. It is mandatory."
            )

        if not isinstance(join_metadata, list):
            return False, (
                f"Job {job_id}: 'joinMetadata' must be a list."
            )

        if len(join_metadata) == 0:
            return False, (
                f"Job {job_id}: 'joinMetadata' cannot be empty. "
                f"At least one source → destination mapping is required."
            )

    return True, ""


def is_process_job(my_dict: dict) -> bool:
    """
    Returns True if ANY job has sourceJobType == 'process'
    """
    jobs = my_dict.get("jobs", [])
    for job in jobs:
        source_props = (
            job.get("source", {})
               .get("sourceProperties", {})
        )
        job_type = source_props.get("sourceJobType")
        if isinstance(job_type, str) and job_type.lower() == "process":
            return True
    return False

