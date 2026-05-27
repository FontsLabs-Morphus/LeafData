class SalesforceNoRecordsFound(Exception):
    """Exception raised when no records are retrieved from Salesforce."""

    def __init__(self, object_name: str, sql_query: str):
        self.object_name = object_name
        self.sql_query = sql_query
        self.message = f"No records found for object: {self.object_name}. Executed SQL: {self.sql_query}."
        super().__init__(self.message)

    def __str__(self):
        return self.message
