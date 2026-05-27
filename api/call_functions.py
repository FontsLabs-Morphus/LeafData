import os
from docker.models.containers import Container
from requests import RequestException


def create_exec_command(path_script: str) -> str:
    return f"python {path_script}"


def conn_exec(data: str, container: Container, path_script: str) -> str:
    """
    TRANSITION MODE (ZERO BREAKAGE)

    Supports BOTH:
    1. OLD MODE  → JSON string via ENV (JSON_DATA)
    2. NEW MODE  → JSON file path via CLI argument

    - If `data` is an existing file path → NEW MODE
    - Else → OLD MODE
    """

    # -----------------------------
    # NEW MODE: File-based (SAFE)
    # -----------------------------
    if isinstance(data, str) and os.path.exists(data):
        command = f"python {path_script} {data}"
        exec_result = container.exec_run(command)

    # -----------------------------
    # OLD MODE: ENV-based (LEGACY)
    # -----------------------------
    else:
        exec_result = container.exec_run(
            create_exec_command(path_script),
            environment={"JSON_DATA": data}
        )

    if exec_result.exit_code != 0:
        raise RequestException(exec_result.output.decode("utf-8"))

    return exec_result.output.decode("utf-8")
