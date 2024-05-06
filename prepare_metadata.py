import pandas as pd
from ingest.connector import OnPremSQLServerConnector

if __name__ == "__main__":
    connector = OnPremSQLServerConnector("Sophus_DK")
    connector.prepare_ingestion_metadata('sophus_ingestion.csv')
