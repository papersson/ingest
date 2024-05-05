import pandas as pd
from ingest.connector import OnPremSQLServerConnector


def run_bulk_ingestion(metadata_csv, linked_service_name):
    df = pd.read_csv(metadata_csv)
    connector = OnPremSQLServerConnector(linked_service_name)

    for index, row in df.iterrows():
        schema = row["source_schema"]
        table = row["table_name"]
        sql_query = f"SELECT TOP 10 * FROM {schema}.{table};"

        print(f"Ingesting data from {schema}.{table}...")
        status = connector.ingest(schema, table, sql_query)


if __name__ == "__main__":
    metadata_csv = "metadata.csv"
    linked_service_name = "FHC_Booking"
    run_bulk_ingestion(metadata_csv, linked_service_name)
