import csv
from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.hooks.base import BaseHook

from pymongo import MongoClient

from custom_operators.metaclass import Source, Destination
from custom_logger.execution_logger import log_execution
from exceptions.custom_exception import MorphusAirflowException


# ==========================================================
# SOURCE: MongoDB → CSV → XCom
# ==========================================================
class LoadMongoDbOperator(BaseOperator, Source):

    def __init__(
        self,
        mongodb_conn_id: str,
        database_name: str,
        collection_name: str,
        tx_list: list = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.mongodb_conn_id = mongodb_conn_id
        self.database_name = database_name
        self.collection_name = collection_name
        self.tx_list = tx_list or []

    def _get_connection(self):
        try:
            conn = BaseHook.get_connection(self.mongodb_conn_id)

            uri = f"mongodb://{conn.login}:{conn.password}@{conn.host}:{conn.port}"

            client = MongoClient(uri)
            return client

        except Exception as e:
            raise MorphusAirflowException(
                message="MongoDB connection failed",
                task_id=self.task_id,
                error_source="LoadMongoDbOperator._get_connection",
                original_exception=e,
            )

    def execute(self, context: Context):

        try:
            client = self._get_connection()
            db = client[self.database_name]
            collection = db[self.collection_name]

            docs = list(collection.find({}))

            if not docs:
                raise MorphusAirflowException(
                    message="No data found in MongoDB collection",
                    task_id=self.task_id,
                    error_source="LoadMongoDbOperator.execute",
                )

            # remove _id
            for d in docs:
                d.pop("_id", None)

            headers = list(docs[0].keys())

            file_path = f"/tmp/{self.task_id}_mongo.csv"

            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(docs)

            self.log.info(f"MongoDB data written to {file_path}")

            # ✅ XCom push
            context["ti"].xcom_push(
                key=f"{self.task_id}_data",
                value=file_path
            )

        except Exception as e:
            raise MorphusAirflowException(
                message="MongoDB source execution failed",
                task_id=self.task_id,
                error_source="LoadMongoDbOperator.execute",
                original_exception=e,
            )


# ==========================================================
# DESTINATION: CSV → MongoDB
# ==========================================================
class PushMongoDbOperator(BaseOperator, Destination):

    def __init__(
        self,
        mongodb_conn_id: str,
        database_name: str,
        collection_name: str,
        data_write_mode: str = "append",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.mongodb_conn_id = mongodb_conn_id
        self.database_name = database_name
        self.collection_name = collection_name
        self.data_write_mode = data_write_mode

    def _get_connection(self):
        try:
            conn = BaseHook.get_connection(self.mongodb_conn_id)

            uri = f"mongodb://{conn.login}:{conn.password}@{conn.host}:{conn.port}"

            client = MongoClient(uri)
            return client

        except Exception as e:
            raise MorphusAirflowException(
                message="MongoDB connection failed",
                task_id=self.task_id,
                error_source="PushMongoDbOperator._get_connection",
                original_exception=e,
            )

    def execute(self, context: Context):

        try:
            #  Pull from XCom
            source_task_id = self.task_id.split("_")[0]

            file_path = context["ti"].xcom_pull(
                key=f"{source_task_id}_data"
            )

            client = self._get_connection()
            db = client[self.database_name]
            collection = db[self.collection_name]

            if self.data_write_mode == "overwrite":
                collection.delete_many({})

            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                docs = list(reader)

            if docs:
                collection.insert_many(docs)

            self.log.info("Data inserted into MongoDB successfully")

        except Exception as e:
            raise MorphusAirflowException(
                message="MongoDB destination execution failed",
                task_id=self.task_id,
                error_source="PushMongoDbOperator.execute",
                original_exception=e,
            )