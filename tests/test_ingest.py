from ingest.connector import OnPremSQLServerConnector

def test_data_ingestion():
    linked_service = "Integration_Test"  # Ensure this is a test-specific linked service
    connector = OnPremSQLServerConnector(linked_service)

    schema_name = 'dbo'
    table_name = 'ClientGroupPurchasedTreatment'
    query = f"SELECT TOP 10 * FROM {schema_name}.{table_name}"  # Limit the rows for the test

    # Run the ingestion process
    status = connector.ingest(schema_name, table_name, query)
    assert status == "Succeeded", f"Pipeline should complete successfully, got {status}"