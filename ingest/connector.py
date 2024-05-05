# from .adf import ADFManager, Pipeline
from .task import DataIngestionTask

class OnPremSQLServerConnector:
    def __init__(self, linked_service_name):
        self.linked_service_name = linked_service_name

    def ingest(self, schema, table, sql_query):
        ingestion_task = DataIngestionTask(self.linked_service_name, schema, table, sql_query)
        ingestion_task.setup()
        return ingestion_task.run()
