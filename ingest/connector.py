from .task import DataIngestionTask, MetadataLookupTask


class OnPremSQLServerConnector:
    def __init__(self, linked_service_name):
        self.linked_service_name = linked_service_name

    def ingest(self, schema, table, sql_query):
        ingestion_task = DataIngestionTask(
            self.linked_service_name, schema, table, sql_query
        )
        ingestion_task.setup()
        return ingestion_task.run()

    def prepare_ingestion_metadata(self, output_path):
        metadata_task = MetadataLookupTask(self.linked_service_name, output_path)
        metadata_task.setup()
        result_status = metadata_task.run()
        return result_status
