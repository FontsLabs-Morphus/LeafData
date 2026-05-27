# Chatbot_New_v3_updated.py
# AI-style discussion + secure collection + @ask + @change + Avro validation
# IMPORTANT:
# - LLM is used only in DISCUSSION / AWAIT_START and one-shot @ask
# - LLM is NOT used during secure field collection
# - Review masks secrets on screen only
# - JSON files are saved with RAW values (passwords are NOT masked on disk)

import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from openai import OpenAI

try:
    from fastavro.schema import load_schema as avro_load_schema  # type: ignore
    from fastavro.validation import validate as avro_validate  # type: ignore
except Exception:
    avro_load_schema = None
    avro_validate = None


# ----------------------------
# Helpers
# ----------------------------

SENSITIVE_KEYS = {
    "password",
    "secret",
    "secret_access_key",
    "aws_secret_access_key",
    "connection_string",
    "api_key",
    "token",
    "securitytoken",
    "sas_token",
    "key_json",
    "auth_value",
    "auth_header_value",
}


def now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def new_id() -> str:
    return str(uuid.uuid4())


def mask_value(value: Any) -> Any:
    if value is None:
        return None
    s = str(value)
    if not s:
        return ""
    if len(s) <= 4:
        return "*" * len(s)
    return "*" * (len(s) - 2) + s[-2:]


def mask_secrets(obj: Any) -> Any:
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k.lower() in SENSITIVE_KEYS or any(tok in k.lower() for tok in SENSITIVE_KEYS):
                out[k] = mask_value(v)
            else:
                out[k] = mask_secrets(v)
        return out
    if isinstance(obj, list):
        return [mask_secrets(x) for x in obj]
    return obj


def is_valid_host(host: str) -> bool:
    h = (host or "").strip().lower()
    if not h:
        return False

    if h in {"ok", "okay", "yes", "y", "done", "na", "n/a", "none"}:
        return False

    if h == "localhost":
        return True

    # IPv4
    if re.match(r"^(?:\d{1,3}\.){3}\d{1,3}$", h):
        parts = h.split(".")
        return all(p.isdigit() and 0 <= int(p) <= 255 for p in parts)

    # URL host-ish / docker hostnames / normal hostnames
    if re.match(r"^(?=.{1,253}$)(?!-)[a-z0-9-]{1,63}(?<!-)(\.(?!-)[a-z0-9-]{1,63}(?<!-))*$", h):
        return True

    return False


@dataclass
class ConnectorDef:
    category: str
    collect_fields: List[str]
    prompts: Dict[str, str]
    backend_conn_schema: str
    backend_conn_type: Optional[str]


class FTChatbotAI:
    """
    PHASES:
    1) DISCUSSION: natural chat with LLM, detect source/destination
    2) AWAIT_START: human explanation, user types START
    3) COLLECTION: strict local validation only (NO LLM)
       - @ask <question> allowed
       - @change allowed
    4) REVIEW: masked summary, submit/back/@change
    """

    def __init__(self) -> None:
        OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

        if not OPENAI_API_KEY:
            raise RuntimeError("OPENAI_API_KEY environment variable is missing.")

        self.client = OpenAI(api_key=OPENAI_API_KEY)
        self.model = "gpt-4o-mini"

        self.root = Path(__file__).resolve().parent
        self.schemas_dir = self.root / "schemas"
        self.output_dir = self.root / "output"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.supported_connectors = [
            "MYSQL",
            "POSTGRES",
            "SQL_SERVER",
            "ORACLE",
            "S3",
            "AZURE_BLOB",
            "GCS",
            "SFTP",
            "REST_API",
            "EXCEL",
            "MONGODB",
            "MARIADB",
        ]

        self.aliases = {
            "POSTGRESQL": "POSTGRES",
            "PG": "POSTGRES",
            "PSQL": "POSTGRES",
            "SQLSERVER": "SQL_SERVER",
            "SQL SERVER": "SQL_SERVER",
            "MSSQL": "SQL_SERVER",
            "AZURE SQL": "SQL_SERVER",
            "AZURE BLOB": "AZURE_BLOB",
            "AZURE BLOB STORAGE": "AZURE_BLOB",
            "GCS": "GCS",
            "GOOGLE CLOUD STORAGE": "GCS",
            "MONGO": "MONGODB",
            "MARIA DB": "MARIADB",
            "REST API": "REST_API",
            "AWS S3": "S3",
            "AMAZON S3": "S3",
            "ORCL": "ORACLE",
        }

        self.connectors = self._build_connectors()
        self.schemas = self._load_schemas()

        # conversation state
        self.phase = "DISCUSSION"  # DISCUSSION | AWAIT_START | COLLECTION | REVIEW
        self.plan = {"src": None, "dst": None}
        self.state = {
            "source": {},
            "dest": {},
            "mapping": {},
            "current_hint": None,
        }

        # @change state
        self.change_menu: List[Tuple[str, str]] = []
        self.change_waiting_choice = False
        self.change_waiting_value_for_hint: Optional[str] = None

    # ----------------------------
    # Connectors
    # ----------------------------
    def _build_connectors(self) -> Dict[str, ConnectorDef]:
        return {
            "MYSQL": ConnectorDef(
                "database",
                ["host", "port", "login", "password", "database"],
                {
                    "host": "MySQL host",
                    "port": "MySQL port (example: 3306)",
                    "login": "MySQL username",
                    "password": "MySQL password",
                    "database": "MySQL database name",
                },
                "database_connection_schema",
                "MYSQL",
            ),
            "POSTGRES": ConnectorDef(
                "database",
                ["host", "port", "login", "password", "database"],
                {
                    "host": "Postgres host",
                    "port": "Postgres port (example: 5432)",
                    "login": "Postgres username",
                    "password": "Postgres password",
                    "database": "Postgres database name",
                },
                "database_connection_schema",
                "PSQL",
            ),
            "SQL_SERVER": ConnectorDef(
                "database",
                ["host", "port", "login", "password", "database"],
                {
                    "host": "SQL Server host",
                    "port": "SQL Server port (example: 1433)",
                    "login": "SQL Server username",
                    "password": "SQL Server password",
                    "database": "SQL Server database name",
                },
                "database_connection_schema",
                "MSSQL",
            ),
            "ORACLE": ConnectorDef(
                "database",
                ["host", "port", "login", "password", "database"],
                {
                    "host": "Oracle host",
                    "port": "Oracle port (example: 1521)",
                    "login": "Oracle username",
                    "password": "Oracle password",
                    "database": "Oracle service name / backend database value (example: XE)",
                },
                "database_connection_schema",
                "ORACLE",
            ),
            "MARIADB": ConnectorDef(
                "database",
                ["host", "port", "login", "password", "database"],
                {
                    "host": "MariaDB host",
                    "port": "MariaDB port (example: 3306)",
                    "login": "MariaDB username",
                    "password": "MariaDB password",
                    "database": "MariaDB database name",
                },
                "database_connection_schema",
                "MARIADB",
            ),
            "MONGODB": ConnectorDef(
                "database",
                ["host", "port", "login", "password", "database", "collection"],
                {
                    "host": "MongoDB host",
                    "port": "MongoDB port (example: 27017)",
                    "login": "MongoDB username",
                    "password": "MongoDB password",
                    "database": "MongoDB database name",
                    "collection": "MongoDB collection",
                },
                "database_connection_schema",
                "MONGODB",
            ),
            "EXCEL": ConnectorDef(
                "database",
                ["path", "sheet_name"],
                {
                    "path": "Excel file path",
                    "sheet_name": "Excel sheet name",
                },
                "database_connection_schema",
                "EXCEL",
            ),
            "S3": ConnectorDef(
                "cloud",
                ["bucket_name", "aws_access_key_id", "aws_secret_access_key", "region_name"],
                {
                    "bucket_name": "S3 bucket name",
                    "aws_access_key_id": "AWS access key id",
                    "aws_secret_access_key": "AWS secret access key",
                    "region_name": "AWS region (example: us-east-1)",
                },
                "cloud_connection_schema",
                "S3",
            ),
            "AZURE_BLOB": ConnectorDef(
                "cloud",
                ["container_name", "connection_string"],
                {
                    "container_name": "Azure Blob container name",
                    "connection_string": "Azure connection string",
                },
                "cloud_connection_schema",
                "AZUREBLOB",
            ),
            "GCS": ConnectorDef(
                "cloud",
                ["bucket_name", "key_json"],
                {
                    "bucket_name": "GCS bucket name",
                    "key_json": "GCP service account JSON or JSON path",
                },
                "cloud_connection_schema",
                "CLOUDSTORAGE",
            ),
            "SFTP": ConnectorDef(
                "cloud",
                ["host", "port", "login", "password", "path"],
                {
                    "host": "SFTP host",
                    "port": "SFTP port (example: 22)",
                    "login": "SFTP username",
                    "password": "SFTP password",
                    "path": "SFTP file/folder path",
                },
                "cloud_connection_schema",
                "SFTP",
            ),
            # Keep unsupported until backend schema truly accepts it
            "REST_API": ConnectorDef(
                "unsupported",
                ["base_url", "auth_type", "auth_value", "resource_path"],
                {
                    "base_url": "REST API base URL",
                    "auth_type": "Auth type (Bearer / Basic / API_KEY / NONE)",
                    "auth_value": "Auth token/key/value",
                    "resource_path": "Resource path or endpoint",
                },
                "unsupported",
                None,
            ),
        }

    # ----------------------------
    # Schema loading
    # ----------------------------
    def _load_schemas(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        if avro_load_schema is None:
            return out

        for name in [
            "database_connection_schema.avsc",
            "cloud_connection_schema.avsc",
            "create_dag_schema.avsc",
            "trigger_dag_schema.avsc",
            "delete_connection_schema.avsc",
            "delete_dag_schema.avsc",
        ]:
            p = self.schemas_dir / name
            if p.exists():
                out[p.stem] = avro_load_schema(str(p))
        return out

    def _validate_avro(self, schema_name: str, payload: Dict[str, Any]) -> Tuple[bool, str]:
        if avro_validate is None:
            return True, "fastavro not installed; skipped validation"

        schema = self.schemas.get(schema_name)
        if not schema:
            return False, f"Schema file not found: {schema_name}"

        try:
            ok = avro_validate(payload, schema)
            return bool(ok), "OK" if ok else "Validation failed"
        except Exception as exc:
            return False, str(exc)

    # ----------------------------
    # LLM helpers
    # ----------------------------
    def _llm_reply(self, user_text: str) -> str:
        system_prompt = (
            "You are Infodat's advanced data processing assistant.\n"
            "Be warm, helpful, and concise.\n"
            "You help users describe data transfer plans like 'Oracle to MySQL' or 'MongoDB to MySQL'.\n"
            "If the user is not yet giving a transfer pair, guide them naturally.\n"
            "If they mention a likely source and destination, confirm the transfer briefly.\n"
            "Do not ask for passwords or secret values in free chat.\n"
            "Do not invent unsupported backend guarantees.\n"
        )

        resp = self.client.chat.completions.create(
            model=self.model,
            temperature=0.5,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_text},
            ],
        )
        return resp.choices[0].message.content.strip()

    def _llm_help_for_current_step(self, question: str, hint: Optional[str]) -> str:
        step_context = f"Current collection step: {hint}." if hint else "Collection step unknown."
        system_prompt = (
            "You are helping a user complete one field in a secure ETL configuration chatbot.\n"
            "Answer only the user's clarification question.\n"
            "Do not ask for passwords or secrets.\n"
            "Do not request extra fields beyond the current step.\n"
            "Keep the answer short, practical, and beginner-friendly.\n"
        )
        user_prompt = f"{step_context}\nUser question: {question}"

        resp = self.client.chat.completions.create(
            model=self.model,
            temperature=0.3,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        return resp.choices[0].message.content.strip()

    # ----------------------------
    # Connector parsing
    # ----------------------------
    def _normalize_connector(self, text: str) -> Optional[str]:
        t = normalize_spaces(text).upper()
        t = re.sub(r"[^A-Z0-9_ ]+", " ", t)
        t = normalize_spaces(t)

        if t in self.aliases:
            t = self.aliases[t]
        if t in self.supported_connectors:
            return t

        for alias, canonical in self.aliases.items():
            if re.search(rf"\b{re.escape(alias)}\b", t):
                return canonical

        for c in self.supported_connectors:
            if re.search(rf"\b{re.escape(c)}\b", t):
                return c

        return None

    def _extract_connectors(self, text: str) -> Tuple[Optional[str], Optional[str]]:
        t = normalize_spaces(text)

        m = re.search(r"(.+?)\s*->\s*(.+)", t, flags=re.I)
        if m:
            return self._normalize_connector(m.group(1)), self._normalize_connector(m.group(2))

        m = re.search(r"\bfrom\b\s+(.+?)\s+\bto\b\s+(.+)", t, flags=re.I)
        if m:
            return self._normalize_connector(m.group(1)), self._normalize_connector(m.group(2))

        idx = t.lower().rfind(" to ")
        if idx != -1:
            left = t[:idx]
            right = t[idx + 4:]
            src = self._normalize_connector(left)
            dst = self._normalize_connector(right)
            if src and dst:
                return src, dst

        hits: List[Tuple[int, str]] = []
        up = t.upper()
        for c in self.supported_connectors:
            for mm in re.finditer(rf"\b{re.escape(c)}\b", up):
                hits.append((mm.start(), c))
        for a, c in self.aliases.items():
            for mm in re.finditer(rf"\b{re.escape(a)}\b", up):
                hits.append((mm.start(), c))
        hits.sort(key=lambda x: x[0])

        if len(hits) >= 2:
            return hits[0][1], hits[-1][1]

        return None, None

    # ----------------------------
    # Collection flow
    # ----------------------------
    def _next_hint(self) -> Optional[str]:
        src = self.plan["src"]
        dst = self.plan["dst"]
        if not src or not dst:
            return None

        for field in self.connectors[src].collect_fields:
            if field not in self.state["source"]:
                return f"source.{field}"

        for field in self.connectors[dst].collect_fields:
            if field not in self.state["dest"]:
                return f"dest.{field}"

        for field in ["source_table", "dest_table", "columns", "scheduleInterval"]:
            if field not in self.state["mapping"]:
                return f"mapping.{field}"

        return None

    def _prompt_for_hint(self, hint: str) -> str:
        src = self.plan["src"]
        dst = self.plan["dst"]

        if hint.startswith("source."):
            field = hint.split(".", 1)[1]
            return f"Please enter Source ({src}) {self.connectors[src].prompts[field]}:"
        if hint.startswith("dest."):
            field = hint.split(".", 1)[1]
            return f"Please enter Destination ({dst}) {self.connectors[dst].prompts[field]}:"
        if hint == "mapping.source_table":
            return "Please enter the source table/object/path:"
        if hint == "mapping.dest_table":
            return "Please enter the destination table/object/path:"
        if hint == "mapping.columns":
            return "Type ALL or mapping like: col1,col2 -> colA,colB"
        if hint == "mapping.scheduleInterval":
            return "Enter schedule: @once / Daily / Weekly / Monthly"
        return "Enter value:"

    def _parse_columns(self, text: str) -> Optional[Dict[str, Any]]:
        a = (text or "").strip()
        if a.upper() == "ALL":
            return {"all": True, "source": [], "dest": []}

        m = re.search(r"(.+?)\s*->\s*(.+)", a)
        if not m:
            return None

        src_cols = [x.strip() for x in m.group(1).split(",") if x.strip()]
        dst_cols = [x.strip() for x in m.group(2).split(",") if x.strip()]

        if not src_cols or not dst_cols or len(src_cols) != len(dst_cols):
            return None

        return {"all": False, "source": src_cols, "dest": dst_cols}

    def _validate_and_store(self, hint: str, value: str) -> Tuple[bool, str]:
        v = (value or "").strip()

        if hint.startswith("source.") or hint.startswith("dest."):
            side = "source" if hint.startswith("source.") else "dest"
            field = hint.split(".", 1)[1]

            if not v:
                return False, "This value cannot be empty."

            if field == "host":
                if not is_valid_host(v) and not v.startswith("http"):
                    return False, "Invalid host."

            if field == "port":
                if not v.isdigit():
                    return False, "Port must be numeric."
                self.state[side][field] = int(v)
                return True, "OK"

            self.state[side][field] = v
            return True, "OK"

        if hint == "mapping.source_table":
            if not v:
                return False, "Source table/object/path cannot be empty."
            self.state["mapping"]["source_table"] = v
            return True, "OK"

        if hint == "mapping.dest_table":
            if not v:
                return False, "Destination table/object/path cannot be empty."
            self.state["mapping"]["dest_table"] = v
            return True, "OK"

        if hint == "mapping.columns":
            parsed = self._parse_columns(v)
            if not parsed:
                return False, "Use ALL or col1,col2 -> colA,colB"
            self.state["mapping"]["columns"] = parsed
            return True, "OK"

        if hint == "mapping.scheduleInterval":
            low = v.lower()
            if low in {"once", "@once"}:
                self.state["mapping"]["scheduleInterval"] = "@once"
                return True, "OK"
            if low in {"daily", "weekly", "monthly"}:
                self.state["mapping"]["scheduleInterval"] = low.capitalize()
                return True, "OK"
            return False, "Use @once, Daily, Weekly, or Monthly."

        return False, "Unknown step."

    # ----------------------------
    # @change support
    # ----------------------------
    def _build_change_menu(self) -> List[Tuple[str, str]]:
        src = self.plan["src"]
        dst = self.plan["dst"]
        out: List[Tuple[str, str]] = []

        for k in self.state["source"].keys():
            out.append((f"source.{k}", f"Source ({src}) {k}"))

        for k in self.state["dest"].keys():
            out.append((f"dest.{k}", f"Destination ({dst}) {k}"))

        for k in self.state["mapping"].keys():
            out.append((f"mapping.{k}", f"Mapping {k}"))

        return out

    # ----------------------------
    # Payload builders
    # ----------------------------
    def _build_connection_payload(self, side: str, conn_id: str, connector_name: str) -> Dict[str, Any]:
        cfg = self.connectors[connector_name]
        data = self.state[side]

        # EXCEL special case first
        if connector_name == "EXCEL":
            return {
                "conn_id": conn_id,
                "conn_type": "EXCEL",
                "host": "",
                "port": "",
                "login": "",
                "password": "",
                "database": data.get("path", ""),
            }

        if cfg.backend_conn_schema == "database_connection_schema":
            return {
                "conn_id": conn_id,
                "conn_type": cfg.backend_conn_type,
                "host": data.get("host", ""),
                "port": str(data.get("port", "")),
                "login": data.get("login", ""),
                "password": data.get("password", ""),
                "database": data.get("database", None),
            }

        if cfg.backend_conn_schema == "cloud_connection_schema":
            return {
                "conn_id": conn_id,
                "conn_type": cfg.backend_conn_type,
                "connection_string": data.get("connection_string", ""),
                "aws_access_key_id": data.get("aws_access_key_id", ""),
                "aws_secret_access_key": data.get("aws_secret_access_key", ""),
                "region_name": data.get("region_name", ""),
                "container_name": data.get("container_name", ""),
                "bucket_name": data.get("bucket_name", ""),
                "key_json": data.get("key_json", ""),
                "host": data.get("host", ""),
                "port": str(data.get("port", "")),
                "path": data.get("path", ""),
                "login": data.get("login", ""),
                "password": data.get("password", ""),
                "securityToken": data.get("securityToken", ""),
            }

        # unsupported backend schema: draft only
        return {
            "conn_id": conn_id,
            "conn_type": connector_name,
            "details": data,
            "note": f"{connector_name} is collected by chatbot, but current backend Avro schemas do not yet support it.",
        }

    def _build_dag_payload(self, pipeline_id: str, src_conn_id: str, dst_conn_id: str) -> Dict[str, Any]:
        src = self.plan["src"]
        dst = self.plan["dst"]
        cols = self.state["mapping"]["columns"]

        job_id = new_id()
        src_task_id = new_id()
        dst_task_id = new_id()

        if cols["all"]:
            source_col = "ALL"
            dest_col = "ALL"
        else:
            source_col = ",".join(cols["source"])
            dest_col = ",".join(cols["dest"])

        def source_props_for(connector: str, task_id: str, conn_id: str, object_name: str) -> Dict[str, Any]:
            if connector in {"MYSQL", "POSTGRES", "SQL_SERVER", "ORACLE", "MARIADB"}:
                return {
                    "taskId": task_id,
                    "connectionId": conn_id,
                    "tableName": object_name,
                    "bucketName": None,
                    "containerName": None,
                    "schemaName": None,
                    "objectName": None,
                    "sourceJobType": None,
                    "customQuery": None,
                    "path": None,
                    "delimiter": None,
                    "headerlessFile": "N",
                    "headers": [],
                    "dataWriteMode": "append",
                }

            if connector in {"S3", "GCS"}:
                return {
                    "taskId": task_id,
                    "connectionId": conn_id,
                    "tableName": None,
                    "bucketName": object_name,
                    "containerName": None,
                    "schemaName": None,
                    "objectName": None,
                    "sourceJobType": None,
                    "customQuery": None,
                    "path": None,
                    "delimiter": None,
                    "headerlessFile": "N",
                    "headers": [],
                    "dataWriteMode": "append",
                }

            if connector == "AZURE_BLOB":
                return {
                    "taskId": task_id,
                    "connectionId": conn_id,
                    "tableName": None,
                    "bucketName": None,
                    "containerName": object_name,
                    "schemaName": None,
                    "objectName": None,
                    "sourceJobType": None,
                    "customQuery": None,
                    "path": None,
                    "delimiter": None,
                    "headerlessFile": "N",
                    "headers": [],
                    "dataWriteMode": "append",
                }

            if connector == "SFTP":
                return {
                    "taskId": task_id,
                    "connectionId": conn_id,
                    "tableName": None,
                    "bucketName": None,
                    "containerName": None,
                    "schemaName": None,
                    "objectName": None,
                    "sourceJobType": None,
                    "customQuery": None,
                    "path": object_name,
                    "delimiter": ",",
                    "headerlessFile": "N",
                    "headers": [],
                    "dataWriteMode": "append",
                }

            if connector == "MONGODB":
                return {
                    "taskId": task_id,
                    "connectionId": conn_id,
                    "tableName": object_name,  # collection name
                    "bucketName": None,
                    "containerName": None,
                    "schemaName": None,
                    "objectName": None,
                    "sourceJobType": None,
                    "customQuery": None,
                    "path": None,
                    "delimiter": None,
                    "headerlessFile": "N",
                    "headers": [],
                    "dataWriteMode": "append",
                }

            if connector == "EXCEL":
                return {
                    "taskId": task_id,
                    "connectionId": conn_id,
                    "tableName": object_name,  # sheet name
                    "bucketName": None,
                    "containerName": None,
                    "schemaName": None,
                    "objectName": None,
                    "sourceJobType": None,
                    "customQuery": None,
                    "path": None,
                    "delimiter": None,
                    "headerlessFile": "N",
                    "headers": [],
                    "dataWriteMode": "append",
                }

            return {
                "taskId": task_id,
                "connectionId": conn_id,
                "tableName": object_name,
                "bucketName": None,
                "containerName": None,
                "schemaName": None,
                "objectName": None,
                "sourceJobType": None,
                "customQuery": None,
                "path": None,
                "delimiter": None,
                "headerlessFile": "N",
                "headers": [],
                "dataWriteMode": "append",
            }

        payload = {
            "pipelineId": pipeline_id,
            "scheduleInterval": self.state["mapping"]["scheduleInterval"],
            "startDate": datetime.now().strftime("%m/%d/%Y"),
            "catchup": False,
            "jobs": [
                {
                    "id": job_id,
                    "source": {
                        "sourceName": f"{src.lower()}_src",
                        "sourceType": src,
                        "sourceProperties": source_props_for(
                            src, src_task_id, src_conn_id, self.state["mapping"]["source_table"]
                        ),
                    },
                    "destinations": [
                        {
                            "destinationName": f"{dst.lower()}_dst",
                            "destinationType": dst,
                            "destinationProperties": source_props_for(
                                dst, dst_task_id, dst_conn_id, self.state["mapping"]["dest_table"]
                            ),
                        }
                    ],
                    "joinMetadata": [
                        {
                            "joinMetadataTaskId": new_id(),
                            "sourceMetadataId": f"srcmeta-{job_id}",
                            "destinationMetadataId": f"dstmeta-{dst_task_id}",
                            "sourceColumnName": source_col,
                            "destinationColumnName": dest_col,
                            "teamID": None,
                            "tagID": None,
                            "sourceGroupId": new_id(),
                            "destinationTaskId": dst_task_id,
                            "sourceTaskId": src_task_id,
                            "transformations": {
                                "ids": [],
                                "parent": None,
                            },
                        }
                    ],
                }
            ],
            "sequence": {job_id: []},
            "taskSequence": {src_task_id: [dst_task_id]},
        }
        return payload

    def _build_trigger_payload(self, pipeline_id: str, schedule_interval: str) -> Dict[str, Any]:
        return {
            "pipelineId": pipeline_id,
            "runId": new_id(),
            "scheduleInterval": schedule_interval,
            "startDate": datetime.now().strftime("%m/%d/%Y"),
            "catchup": False,
        }

    def _build_delete_payloads(
        self, src_conn_id: str, dst_conn_id: str, pipeline_id: str
    ) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        return (
            {"conn_id": src_conn_id},
            {"conn_id": dst_conn_id},
            {"pipelineId": pipeline_id},
        )

    # ----------------------------
    # Final generation
    # ----------------------------
    def _generate_all_payloads(self) -> Dict[str, Any]:
        src = self.plan["src"]
        dst = self.plan["dst"]
        pipeline_id = new_id()
        src_conn_id = f"{src.lower()}-{pipeline_id[:8]}"
        dst_conn_id = f"{dst.lower()}-{pipeline_id[:8]}-dst"

        src_conn = self._build_connection_payload("source", src_conn_id, src)
        dst_conn = self._build_connection_payload("dest", dst_conn_id, dst)
        dag_payload = self._build_dag_payload(pipeline_id, src_conn_id, dst_conn_id)
        trigger_payload = self._build_trigger_payload(pipeline_id, dag_payload["scheduleInterval"])
        delete_src_payload, delete_dst_payload, delete_dag_payload = self._build_delete_payloads(
            src_conn_id, dst_conn_id, pipeline_id
        )

        results: Dict[str, Any] = {
            "pipeline_id": pipeline_id,
            "source_connection": src_conn,
            "destination_connection": dst_conn,
            "dag": dag_payload,
            "trigger": trigger_payload,
            "delete_source_connection": delete_src_payload,
            "delete_destination_connection": delete_dst_payload,
            "delete_dag": delete_dag_payload,
            "validation": {},
        }

        src_schema_name = self.connectors[src].backend_conn_schema
        dst_schema_name = self.connectors[dst].backend_conn_schema

        if src_schema_name in {"database_connection_schema", "cloud_connection_schema"}:
            ok, msg = self._validate_avro(src_schema_name, src_conn)
            results["validation"]["source_connection"] = {
                "ok": ok,
                "message": msg,
                "schema": src_schema_name,
            }
        else:
            results["validation"]["source_connection"] = {
                "ok": False,
                "message": f"{src} is not present in current backend Avro connection schemas",
                "schema": None,
            }

        if dst_schema_name in {"database_connection_schema", "cloud_connection_schema"}:
            ok, msg = self._validate_avro(dst_schema_name, dst_conn)
            results["validation"]["destination_connection"] = {
                "ok": ok,
                "message": msg,
                "schema": dst_schema_name,
            }
        else:
            results["validation"]["destination_connection"] = {
                "ok": False,
                "message": f"{dst} is not present in current backend Avro connection schemas",
                "schema": None,
            }

        ok, msg = self._validate_avro("create_dag_schema", dag_payload)
        results["validation"]["dag"] = {"ok": ok, "message": msg, "schema": "create_dag_schema"}

        ok, msg = self._validate_avro("trigger_dag_schema", trigger_payload)
        results["validation"]["trigger"] = {"ok": ok, "message": msg, "schema": "trigger_dag_schema"}

        ok, msg = self._validate_avro("delete_connection_schema", delete_src_payload)
        results["validation"]["delete_source_connection"] = {
            "ok": ok,
            "message": msg,
            "schema": "delete_connection_schema",
        }

        ok, msg = self._validate_avro("delete_connection_schema", delete_dst_payload)
        results["validation"]["delete_destination_connection"] = {
            "ok": ok,
            "message": msg,
            "schema": "delete_connection_schema",
        }

        ok, msg = self._validate_avro("delete_dag_schema", delete_dag_payload)
        results["validation"]["delete_dag"] = {"ok": ok, "message": msg, "schema": "delete_dag_schema"}

        return results

    def _save_payloads(self, all_payloads: Dict[str, Any]) -> List[str]:
        """
        IMPORTANT:
        Saves RAW payloads.
        Passwords are NOT masked in saved JSON files.
        """
        ts = now_ts()
        src = self.plan["src"].lower()
        dst = self.plan["dst"].lower()
        pipeline_id = all_payloads["pipeline_id"]

        files = [
            ("source_connection.json", all_payloads["source_connection"]),
            ("destination_connection.json", all_payloads["destination_connection"]),
            ("create_dag.json", all_payloads["dag"]),
            ("trigger_dag.json", all_payloads["trigger"]),
            ("delete_source_connection.json", all_payloads["delete_source_connection"]),
            ("delete_destination_connection.json", all_payloads["delete_destination_connection"]),
            ("delete_dag.json", all_payloads["delete_dag"]),
            ("validation_report.json", all_payloads["validation"]),
        ]

        saved_paths = []
        run_dir = self.output_dir / f"{src}_to_{dst}_{pipeline_id}_{ts}"
        run_dir.mkdir(parents=True, exist_ok=True)

        for name, payload in files:
            p = run_dir / name
            with open(p, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
            saved_paths.append(str(p))

        return saved_paths

    # ----------------------------
    # Human discussion messages
    # ----------------------------
    def _greeting(self) -> str:
        return (
            "Hey! Tell me what you want to transfer today.\n"
            "For example:\n"
            "- Oracle -> Oracle\n"
            "- MongoDB -> MySQL\n"
            "- MariaDB -> MySQL"
        )

    def _plan_ready_message(self, src: str, dst: str) -> str:
        src_fields = ", ".join(self.connectors[src].prompts[f] for f in self.connectors[src].collect_fields)
        dst_fields = ", ".join(self.connectors[dst].prompts[f] for f in self.connectors[dst].collect_fields)

        warning_lines = []
        if self.connectors[src].backend_conn_schema == "unsupported":
            warning_lines.append(f"- Source {src}: backend schema support is still marked unsupported")
        if self.connectors[dst].backend_conn_schema == "unsupported":
            warning_lines.append(f"- Destination {dst}: backend schema support is still marked unsupported")

        msg = [
            f"Got it — **{src} -> {dst}**.",
            "",
            "Before we start, please keep these details ready:",
            f"- Source ({src}): {src_fields}",
            f"- Destination ({dst}): {dst_fields}",
            "- Also: source table/object/path, destination table/object/path",
            "- Columns: ALL or mapping like col1,col2 -> colA,colB",
            "- Schedule: Daily / Weekly / Monthly / Once",
            "",
            "When you're ready, type **START**.",
            "If you have any doubt before starting, ask me naturally.",
        ]
        if warning_lines:
            msg.extend(["", "Schema warnings:"] + warning_lines)

        return "\n".join(msg)

    # ----------------------------
    # Main state machine
    # ----------------------------
    def respond(self, user_text: str) -> str:
        text = normalize_spaces(user_text)

        if text.lower() in {"exit", "quit"}:
            return "Goodbye!"

        # ---------------- REVIEW ----------------
        if self.phase == "REVIEW":
            low = text.lower()

            if low in {"@change", "change"}:
                self.change_menu = self._build_change_menu()
                if not self.change_menu:
                    return "Nothing has been entered yet to change."
                self.change_waiting_choice = True
                lines = ["What would you like to change? Reply with the number:\n"]
                for i, (_, label) in enumerate(self.change_menu, start=1):
                    lines.append(f"{i}. {label}")
                return "\n".join(lines)

            if self.change_waiting_choice:
                if not text.isdigit():
                    return "Please reply with the number from the list."
                idx = int(text)
                if idx < 1 or idx > len(self.change_menu):
                    return "That number is out of range."
                hint, label = self.change_menu[idx - 1]
                self.change_waiting_choice = False
                self.change_waiting_value_for_hint = hint
                return f"Enter the new value for **{label}**:"

            if self.change_waiting_value_for_hint:
                hint = self.change_waiting_value_for_hint
                ok, msg = self._validate_and_store(hint, text)
                if not ok:
                    return msg + "\nPlease try again:"
                self.change_waiting_value_for_hint = None
                summary = {
                    "plan": self.plan,
                    "source": mask_secrets(self.state["source"]),
                    "dest": mask_secrets(self.state["dest"]),
                    "mapping": self.state["mapping"],
                }
                return (
                    "Updated.\n\nHere is the refreshed review:\n"
                    + json.dumps(summary, indent=2)
                    + "\n\nType **submit** to save payloads, **@change** to edit more, or **back** to continue."
                )

            if low == "back":
                self.phase = "COLLECTION"
                hint = self._next_hint()
                self.state["current_hint"] = hint
                return self._prompt_for_hint(hint) if hint else "No pending fields."

            if low == "submit":
                payloads = self._generate_all_payloads()
                paths = self._save_payloads(payloads)

                summary = {
                    "saved_files": paths,
                    "validation": payloads["validation"],
                    "note": "JSON payloads were saved with raw values. Review validation before backend submission.",
                }

                # reset to discussion
                self.phase = "DISCUSSION"
                self.plan = {"src": None, "dst": None}
                self.state = {"source": {}, "dest": {}, "mapping": {}, "current_hint": None}
                self.change_menu = []
                self.change_waiting_choice = False
                self.change_waiting_value_for_hint = None

                return "Saved payloads.\n\n" + json.dumps(summary, indent=2)

            return "Type **submit**, **@change**, or **back**."

        # ---------------- COLLECTION ----------------
        if self.phase == "COLLECTION":
            low = text.lower()

            if low.startswith("@ask"):
                q = text[4:].strip()
                if not q:
                    return "Use: @ask <your question>"
                hint = self.state.get("current_hint")
                answer = self._llm_help_for_current_step(q, hint)
                prompt = self._prompt_for_hint(hint) if hint else ""
                return answer + ("\n\nNow back to the setup:\n" + prompt if prompt else "")

            if low in {"@change", "change"}:
                self.change_menu = self._build_change_menu()
                if not self.change_menu:
                    hint = self.state.get("current_hint")
                    return "Nothing entered yet to change.\n" + (self._prompt_for_hint(hint) if hint else "")
                self.change_waiting_choice = True
                lines = ["What would you like to change? Reply with the number:\n"]
                for i, (_, label) in enumerate(self.change_menu, start=1):
                    lines.append(f"{i}. {label}")
                return "\n".join(lines)

            if self.change_waiting_choice:
                if not text.isdigit():
                    return "Please reply with the number from the list."
                idx = int(text)
                if idx < 1 or idx > len(self.change_menu):
                    return "That number is out of range."
                hint, label = self.change_menu[idx - 1]
                self.change_waiting_choice = False
                self.change_waiting_value_for_hint = hint
                return f"Enter the new value for **{label}**:"

            if self.change_waiting_value_for_hint:
                hint = self.change_waiting_value_for_hint
                ok, msg = self._validate_and_store(hint, text)
                if not ok:
                    return msg + "\nPlease try again:"
                self.change_waiting_value_for_hint = None
                hint2 = self.state.get("current_hint") or self._next_hint()
                self.state["current_hint"] = hint2
                return "Updated.\n\n" + (self._prompt_for_hint(hint2) if hint2 else "")

            hint = self.state.get("current_hint") or self._next_hint()
            self.state["current_hint"] = hint

            if not hint:
                self.phase = "REVIEW"

            ok, msg = self._validate_and_store(hint, text)
            if not ok:
                return msg + "\n" + self._prompt_for_hint(hint)

            next_hint = self._next_hint()
            self.state["current_hint"] = next_hint
            if next_hint:
                return self._prompt_for_hint(next_hint)

            self.phase = "REVIEW"
            summary = {
                "plan": self.plan,
                "source": mask_secrets(self.state["source"]),
                "dest": mask_secrets(self.state["dest"]),
                "mapping": self.state["mapping"],
            }
            return (
                "All fields collected.\n\n"
                + json.dumps(summary, indent=2)
                + "\n\nType **submit** to save JSON payloads.\n"
                "Type **@change** if you want to edit anything.\n"
                "Type **back** to continue."
            )

        # ---------------- AWAIT_START ----------------
        if self.phase == "AWAIT_START":
            if text.lower() == "start":
                self.phase = "COLLECTION"
                hint = self._next_hint()
                self.state["current_hint"] = hint
                return (
                    "Perfect. Starting secure collection now.\n"
                    "You can use **@ask** if you have a doubt.\n"
                    "You can use **@change** later if you want to edit a field.\n\n"
                    + (self._prompt_for_hint(hint) if hint else "")
                )

            # allow normal human questions before start
            src, dst = self.plan["src"], self.plan["dst"]
            safe_context = f"The current intended transfer is {src} -> {dst}. The user has not started secure collection yet."
            llm_text = self._llm_reply(safe_context + "\nUser says: " + text)
            return llm_text + "\n\nWhen you're ready, type **START**."

        # ---------------- DISCUSSION ----------------
        if not text:
            return self._greeting()

        # greetings
        if re.match(r"^(hi|hello|hey)\b", text.lower()):
            return self._greeting()

        src, dst = self._extract_connectors(text)
        if src and dst:
            self.plan["src"] = src
            self.plan["dst"] = dst
            self.phase = "AWAIT_START"
            return self._plan_ready_message(src, dst)

        # no clear pair yet -> LLM discussion
        return self._llm_reply(text)

    def chat(self) -> None:
        print("Assistant:", self._greeting())
        while True:
            user_text = input("User: ")
            reply = self.respond(user_text)
            print("Assistant:", reply)
            if reply == "Goodbye!":
                break


if __name__ == "__main__":
    FTChatbotAI().chat()
