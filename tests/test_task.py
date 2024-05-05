import pytest
from ingest.task import DataIngestionTask

def test_existing_linked_service():
    # Create an instance of DataIngestionTask with a known existing linked service name
    task = DataIngestionTask(linked_service_name="ADLS2_LinkedService", schema="dbo", table="testTable", sql_query="SELECT * FROM dbo.testTable")
    # The linked service check occurs in the constructor, no need to assert here if you're raising exceptions on failure.

def test_non_existing_linked_service():
    # Test initialization of DataIngestionTask with a non-existing linked service name
    non_existing_service_name = "non-existing-linked-service"
    with pytest.raises(RuntimeError) as excinfo:
        # Instantiation should fail and raise RuntimeError if the linked service does not exist
        DataIngestionTask(linked_service_name=non_existing_service_name, schema="dbo", table="testTable", sql_query="SELECT * FROM dbo.testTable")
    assert "does not exist" in str(excinfo.value), "Expected exception for non-existing linked service was not raised"
