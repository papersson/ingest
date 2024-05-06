import sys
import time
from abc import ABC, abstractmethod
from datetime import datetime
from datetime import timedelta, timezone
import csv
import pandas as pd

from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    CopyActivity,
    DatasetReference,
    DatasetResource,
    ParquetSink,
    PipelineResource,
    SqlServerTableDataset,
    SqlSource,
    ActivityPolicy,
    LookupActivity,
    RunFilterParameters,
)


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
        credential = ClientSecretCredential(
            tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
        )
        return DataFactoryManagementClient(credential, subscription_id)

    def _check_linked_service_exists(self):
        try:
            self.adf_client.linked_services.get(
                self.resource_group, self.data_factory_name, self.linked_service_name
            )
            return True
        except Exception:
            raise RuntimeError(
                f"Linked service '{self.linked_service_name}' does not exist."
            )

    @abstractmethod
    def setup(self):
        pass

    def run(self):
        run_response = self.adf_client.pipelines.create_run(
            self.resource_group, self.data_factory_name, self.pipeline_name
        )
        return self.monitor(run_response.run_id)

    def monitor(self, run_id):
        start_time = datetime.now()
        status = None
        while True:
            run = self.adf_client.pipeline_runs.get(
                self.resource_group, self.data_factory_name, run_id
            )
            status = run.status
            elapsed = datetime.now() - start_time
            elapsed_time_str = str(elapsed).split(".")[0]  # Remove microseconds
            sys.stdout.write(
                f"\r{elapsed_time_str}. Pipeline run status: {status}      "
            )
            sys.stdout.flush()

            if run.status in ["Succeeded", "Failed", "Cancelled"]:
                print("\n")
                return run.status
            time.sleep(1)


class DataIngestionTask(TaskBase):
    def __init__(self, linked_service_name, schema, table, sql_query):
        super().__init__(linked_service_name)
        self.schema = schema
        self.table = table
        self.sql_query = sql_query

    def setup(self):
        dataset_name = f"{self.linked_service_name}_Dataset"
        self._setup_dataset(dataset_name, self.schema, self.table)
        self._setup_pipeline(dataset_name)

    def _setup_dataset(self, dataset_name, schema, table):
        dataset_properties = SqlServerTableDataset(
            linked_service_name={
                "reference_name": self.linked_service_name,
                "type": "LinkedServiceReference",
            },
            schema=schema,
            table_name=table,
        )
        dataset_resource = DatasetResource(properties=dataset_properties)
        self.adf_client.datasets.create_or_update(
            self.resource_group, self.data_factory_name, dataset_name, dataset_resource
        )

    def _setup_pipeline(self, dataset_name):
        self.pipeline_name = f"{self.linked_service_name}_to_ADLS2"
        source_dataset_ref = DatasetReference(
            reference_name=dataset_name, type="DatasetReference"
        )
        source = SqlSource(sql_reader_query=self.sql_query)
        target_dataset_ref = DatasetReference(
            reference_name="ADLS_Parquet_Sink_Template",
            type="DatasetReference",
            parameters={
                "dataLakeDirectory": f"ADF_Test/{self.linked_service_name}/{self.table}/",
                "dataLakeFileName": "increment.parquet",
            },
        )
        copy_activity = CopyActivity(
            name="CopyDataFromSqlToAdls",
            inputs=[source_dataset_ref],
            outputs=[target_dataset_ref],
            source=source,
            sink=ParquetSink(),
        )
        pipeline_resource = PipelineResource(activities=[copy_activity])
        self.adf_client.pipelines.create_or_update(
            self.resource_group,
            self.data_factory_name,
            self.pipeline_name,
            pipeline_resource,
        )


class MetadataLookupTask(TaskBase):
    def __init__(self, linked_service_name, output_file):
        super().__init__(linked_service_name)
        self.output_file = output_file

    def setup(self):
        self.pipeline_name = f"{self.linked_service_name}_MetadataLookup"
        dataset_name = f"{self.linked_service_name}_Dataset"
        self._setup_lookup_pipeline(dataset_name)

    def _setup_lookup_pipeline(self, dataset_name):
        self.pipeline_name = f"{self.linked_service_name}_MetadataLookup"

        # Define the dataset for the Lookup activity
        dataset_properties = SqlServerTableDataset(
            linked_service_name={
                "reference_name": self.linked_service_name,
                "type": "LinkedServiceReference",
            },
            schema="INFORMATION_SCHEMA",
        )
        dataset_resource = DatasetResource(properties=dataset_properties)
        self.adf_client.datasets.create_or_update(
            self.resource_group, self.data_factory_name, dataset_name, dataset_resource
        )

        # Setup the pipeline with a Lookup activity
        sql_query = """
        SELECT 
            T.TABLE_SCHEMA, 
            T.TABLE_NAME, 
            K.COLUMN_NAME AS PRIMARY_KEY
        FROM 
            INFORMATION_SCHEMA.TABLES T
            INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS C ON 
                T.TABLE_SCHEMA = C.TABLE_SCHEMA AND 
                T.TABLE_NAME = C.TABLE_NAME AND 
                C.CONSTRAINT_TYPE = 'PRIMARY KEY'
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE K ON 
                C.CONSTRAINT_NAME = K.CONSTRAINT_NAME AND 
                C.TABLE_SCHEMA = K.TABLE_SCHEMA AND 
                C.TABLE_NAME = K.TABLE_NAME;
        """
        source_dataset_ref = DatasetReference(
            reference_name=dataset_name, type="DatasetReference"
        )
        lookup_activity = LookupActivity(
            name="FetchMetadata",
            source=SqlSource(sql_reader_query=sql_query),
            dataset=source_dataset_ref,
            first_row_only=False,
        )
        pipeline_resource = PipelineResource(activities=[lookup_activity])
        self.adf_client.pipelines.create_or_update(
            self.resource_group,
            self.data_factory_name,
            self.pipeline_name,
            pipeline_resource,
        )

    def monitor(self, run_id):
        output = super().monitor(run_id)
        if output == "Succeeded":
            self.fetch_and_save_results(run_id)
        return output

    def fetch_and_save_results(self, run_id):
        # Fetching results as before
        activity_runs = self.adf_client.activity_runs.query_by_pipeline_run(
            self.resource_group,
            self.data_factory_name,
            run_id,
            RunFilterParameters(
                last_updated_after=datetime.now(timezone.utc) - timedelta(days=1),
                last_updated_before=datetime.now(timezone.utc),
            ),
        )
        results = (
            activity_runs.value[0].output["value"] if activity_runs.value else None
        )

        if results:
            df = pd.DataFrame(results)
            self.prepare_ingestion_metadata(df)
        else:
            print("No results found.")

    def prepare_ingestion_metadata(self, df):
        # Concatenate composite keys
        df = df.copy()
        df["PRIMARY_KEY"] = df.groupby(["TABLE_SCHEMA", "TABLE_NAME"])[
            "PRIMARY_KEY"
        ].transform(lambda x: "|".join(x))
        df = df.drop_duplicates(subset=["TABLE_SCHEMA", "TABLE_NAME"])

        # Adding manual 'watermark_column' and 'enabled' columns
        df.loc[:, "source_schema"] = df["TABLE_SCHEMA"]
        df.loc[:, "table_name"] = df["TABLE_NAME"]
        df.loc[:, "primary_key"] = df["PRIMARY_KEY"]
        df.loc[:, "watermark_column"] = ""
        df.loc[:, "enabled"] = 0

        df.to_csv(
            self.output_file,
            index=False,
            columns=[
                "source_schema",
                "table_name",
                "primary_key",
                "watermark_column",
                "enabled",
            ],
        )
        print(f"Metadata saved to '{self.output_file}'.")


# Usage example
# lookup_task = MetadataLookupTask("FHC_Booking", "out.csv")
# lookup_task.setup()
# result_status = lookup_task.run()
# print(f"Lookup task completed with status: {result_status}")
