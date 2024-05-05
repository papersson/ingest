from abc import ABC, abstractmethod
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import DatasetResource, SqlServerTableDataset, PipelineResource, ActivityRun, CopyActivity, DatasetReference, SqlSource, ParquetSink
import time
from datetime import datetime
import sys

class TaskBase(ABC):
    def __init__(self, linked_service_name):
        self.linked_service_name = linked_service_name
        self.adf_client = self._create_adf_client()
        self.resource_group = "datafactory-rg685"
        self.data_factory_name = "sql-server-ingestion"
        self._check_linked_service_exists()

    def _create_adf_client(self):
        tenant_id = "b131966a-2068-4b91-aeab-577ed32cecd1"
        client_id = "052c76eb-cdb0-4816-b25c-b90281b40a91"
        client_secret = "DzY8Q~AwHQM8O6bHRTf1YuWkmAU~c3P-XaxwpaE8"
        subscription_id = "5c8811c7-d0f8-4878-9960-c5e01b6bf0b1"
        credential = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
        return DataFactoryManagementClient(credential, subscription_id)

    def _check_linked_service_exists(self):
        try:
            self.adf_client.linked_services.get(self.resource_group, self.data_factory_name, self.linked_service_name)
            return True
        except Exception:
            raise RuntimeError(f"Linked service '{self.linked_service_name}' does not exist.")

    @abstractmethod
    def setup(self):
        pass

    def run(self):
        run_response = self.adf_client.pipelines.create_run(self.resource_group, self.data_factory_name, self.pipeline_name)
        return self.monitor(run_response.run_id)

    def monitor(self, run_id):
        start_time = datetime.now()
        status = None
        while True:
            run = self.adf_client.pipeline_runs.get(self.resource_group, self.data_factory_name, run_id)
            status = run.status
            elapsed = datetime.now() - start_time
            # Format elapsed time as HH:MM:SS
            elapsed_time_str = str(elapsed).split('.')[0]  # Remove microseconds
            sys.stdout.write(f"\r{elapsed_time_str}. Pipeline run status: {status}      ")  # Use spaces to clear longer previous messages
            sys.stdout.flush()

            if run.status in ["Succeeded", "Failed", "Cancelled"]:
                print("\n")  # Move to a new line after completion
                return run.status
            time.sleep(1)  # Check every 5 seconds

class DataIngestionTask(TaskBase):
    def __init__(self, linked_service_name, schema, table, sql_query):
        super().__init__(linked_service_name)
        self.schema = schema
        self.table = table
        self.sql_query = sql_query

    def setup(self):
        dataset_name = f"{self.linked_service_name}_Dataset"
        self.setup_dataset(dataset_name, self.schema, self.table)
        self.setup_pipeline(dataset_name)

    def setup_dataset(self, dataset_name, schema, table):
        dataset_properties = SqlServerTableDataset(
            linked_service_name={"reference_name": self.linked_service_name, "type": "LinkedServiceReference"},
            schema=schema,
            table_name=table
        )
        dataset_resource = DatasetResource(properties=dataset_properties)
        self.adf_client.datasets.create_or_update(self.resource_group, self.data_factory_name, dataset_name, dataset_resource)

    def setup_pipeline(self, dataset_name):
        self.pipeline_name = f"{self.linked_service_name}_to_ADLS2"
        source_dataset_ref = DatasetReference(reference_name=dataset_name, type="DatasetReference")
        source = SqlSource(sql_reader_query=self.sql_query)
        target_dataset_ref = DatasetReference(reference_name="ADLS_Parquet_Sink_Template", type="DatasetReference", parameters={
            "dataLakeDirectory": f"ADF_Test/{self.linked_service_name}/{self.table}/",
            "dataLakeFileName": "increment.parquet"
        })
        copy_activity = CopyActivity(
            name="CopyDataFromSqlToAdls",
            inputs=[source_dataset_ref],
            outputs=[target_dataset_ref],
            source=source,
            sink=ParquetSink()
        )
        pipeline_resource = PipelineResource(activities=[copy_activity])
        self.adf_client.pipelines.create_or_update(self.resource_group, self.data_factory_name, self.pipeline_name, pipeline_resource)