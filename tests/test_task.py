import pytest
from ingest.task import DataIngestionTask
import pandas as pd
from pathlib import Path
from ingest.task import MetadataLookupTask


def test_prepare_ingestion_metadata():
    # Sample data mimicking the expected structure
    data = {
        "TABLE_SCHEMA": ["dbo", "dbo", "dbo"],
        "TABLE_NAME": ["Table1", "Table1", "Table2"],
        "PRIMARY_KEY": ["id", "name", "id"],
    }
    df = pd.DataFrame(data)

    # Setup for the task
    task = MetadataLookupTask(
        linked_service_name="ADLS2_LinkedService", output_file="test_metadata.csv"
    )
    task.prepare_ingestion_metadata(df)

    # Check if the file exists and contains correct data
    output_path = Path("test_metadata.csv")
    assert output_path.exists(), "Output file was not created."

    # Read the output file and verify contents
    result_df = pd.read_csv(output_path)
    expected_columns = [
        "source_schema",
        "table_name",
        "primary_key",
        "watermark_column",
        "enabled",
    ]
    assert (
        list(result_df.columns) == expected_columns
    ), "Output columns do not match expected columns."
    assert len(result_df) == 2, "Unexpected number of rows in output."

    # Clean up the file after test
    output_path.unlink()


def test_existing_linked_service():
    # Create an instance of DataIngestionTask with a known existing linked service name
    DataIngestionTask(
        linked_service_name="ADLS2_LinkedService",
        schema="dbo",
        table="testTable",
        sql_query="SELECT * FROM dbo.testTable",
    )
    # The linked service check occurs in the constructor, no need to assert here if you're raising exceptions on failure.


def test_non_existing_linked_service():
    # Test initialization of DataIngestionTask with a non-existing linked service name
    non_existing_service_name = "non-existing-linked-service"
    with pytest.raises(RuntimeError) as excinfo:
        # Instantiation should fail and raise RuntimeError if the linked service does not exist
        DataIngestionTask(
            linked_service_name=non_existing_service_name,
            schema="dbo",
            table="testTable",
            sql_query="SELECT * FROM dbo.testTable",
        )
    assert "does not exist" in str(
        excinfo.value
    ), "Expected exception for non-existing linked service was not raised"
