from collections import namedtuple

# File paths
TEMP_FILE_PATH = '/opt/airflow/test/data/result'
INPUT_FILES_PATH = "/opt/airflow/files/input_files"
TRANSFORMED_FILES_PATH = "/opt/airflow/files/transformed_files"

PipelineConfig = namedtuple('PipelineConfig',
                            ['pipelineId', 'scheduleInterval', 'startDate', 'catchup', 'jobs', 'sequence'])
Job = namedtuple('Job', ['jobId', 'source', 'destination', 'joinMetadata'])
Source = namedtuple('Source', ['sourceName', 'sourceType', 'sourceProperties'])
SourceProperties = namedtuple('SourceProperties', ['taskId', 'connId', 'tableName',
                                                   'bucketName', 'containerName', 'schemaName', 'objectName',
                                                   'customQuery'])
Destination = namedtuple('Destination', ['destinationName', 'destinationType', 'destinationProperties'])
DestinationProperties = namedtuple('DestinationProperties', ['taskId', 'connId', 'tableName',
                                                             'bucketName', 'containerName', 'schemaName', 'objectName',
                                                             'customQuery'])
JoinMetadata = namedtuple('JoinMetadata',
                          ['joinMetadataTaskId', 'sourceMetadataId', 'destinationMetadataId', 'sourceColumnName',
                           'destinationColumnName', 'teamID', 'tagID', 'sourceGroupId', 'destinationTaskId', 'sourceTaskId',
                           'transformations'])
Transformations = namedtuple('Transformations', ['ids'])

NT_TRANSFORMATIONS = namedtuple(
    'TransformationList',
    ['source_name', 'destination_name', 'tx', 'teamID', 'tagID', 'is_expr', 'expression'],
    defaults=[False, None]
)

SQL_SELECT_COLUMNS_QUERY = """
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{table_name}'
"""

MSSQL_DEFAULT_SQL_QUERY_TEMPLATE = "SELECT * FROM [{schema_name}].[{table_name}]"
MSSQL_DEFAULT_STOREDPROCEDURE_QUERY_TEMPLATE = "CALL {schema_name}].[{table_name}]()"

DEFAULT_SQL_QUERY_TEMPLATE = "SELECT * FROM {schema_name}.{table_name}"
DEFAULT_STOREDPROCEDURE_QUERY_TEMPLATE = "CALL {schema_name}.{table_name}()"

# BigQuery
BIGQUERY_DEFAULT_SQL_QUERY_TEMPLATE = (
    "SELECT * FROM `{schema_name}.{table_name}`"
)

BIGQUERY_DEFAULT_STOREDPROCEDURE_QUERY_TEMPLATE = (
    "CALL `{schema_name}.{table_name}`()"
)

# Salesforce Constants
SALESFORCE_RECORDS_LIMIT = 200
SALESFORCE_BULK_BATCH_SIZE = 10000

PROCESS = "process"

SQL_TABLE_EXISTS = """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        LIMIT 1
        """

# v3: Oracle table existence check (OracleHook supports named parameters)
ORACLE_TABLE_EXISTS = """
SELECT 1
FROM ALL_TABLES
WHERE OWNER = :schema
  AND TABLE_NAME = :table
"""