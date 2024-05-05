from ingest.connector import OnPremSQLServerConnector

linked_service = "FHC_Booking"
connector = OnPremSQLServerConnector(linked_service)

schema_name = 'dbo'
table_name = 'ClientGroupPurchasedTreatment'
status = connector.ingest(schema_name, table_name, f"SELECT TOP 10 * FROM {schema_name}.{table_name}")
print(f"Pipeline completed with status: {status}")