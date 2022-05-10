from uuid import uuid4
import agate
import re
import boto3
from botocore.exceptions import ClientError
from typing import Optional
from threading import Lock

from dbt.adapters.base import available
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.athena import AthenaConnectionManager
from dbt.adapters.athena.relation import AthenaRelation
from dbt.events import AdapterLogger
logger = AdapterLogger("Athena")

boto3_client_lock = Lock()

class AthenaAdapter(SQLAdapter):
    ConnectionManager = AthenaConnectionManager
    Relation = AthenaRelation

    @classmethod
    def date_function(cls) -> str:
        return "now()"

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "string"

    @classmethod
    def convert_number_type(
        cls, agate_table: agate.Table, col_idx: int
    ) -> str:
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "double" if decimals else "integer"

    @classmethod
    def convert_datetime_type(
            cls, agate_table: agate.Table, col_idx: int
    ) -> str:
        return "timestamp"

    @available
    def s3_uuid_table_location(self):
        conn = self.connections.get_thread_connection()
        client = conn.handle

        return f"{client.s3_staging_dir}tables/{str(uuid4())}/"

    @available
    def clean_up_partitions(
        self, database_name: str, table_name: str, where_condition: str
    ):
        # Look up Glue partitions & clean up
        conn = self.connections.get_thread_connection()
        client = conn.handle

        with boto3_client_lock:
            glue_client = boto3.client('glue', region_name=client.region_name)
        s3_resource = boto3.resource('s3', region_name=client.region_name)
        partitions = glue_client.get_partitions(
            # CatalogId='123456789012', # Need to make this configurable if it is different from default AWS Account ID
            DatabaseName=database_name,
            TableName=table_name,
            Expression=where_condition
        )
        p = re.compile('s3://([^/]*)/(.*)')
        for partition in partitions["Partitions"]:
            logger.debug("Deleting objects for partition '{}' at '{}'", partition["Values"], partition["StorageDescriptor"]["Location"])
            m = p.match(partition["StorageDescriptor"]["Location"])
            if m is not None:
                bucket_name = m.group(1)
                prefix = m.group(2)
                s3_bucket = s3_resource.Bucket(bucket_name)
                s3_bucket.objects.filter(Prefix=prefix).delete()

    @available
    def clean_up_table(
        self, database_name: str, table_name: str
    ):
        # Look up Glue partitions & clean up
        conn = self.connections.get_thread_connection()
        client = conn.handle
        with boto3_client_lock:
            glue_client = boto3.client('glue', region_name=client.region_name)
        try:
            table = glue_client.get_table(
                DatabaseName=database_name,
                Name=table_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                logger.debug("Table '{}' does not exists - Ignoring", table_name)
                return

        if table is not None:
            logger.debug("Deleting table data from'{}'", table["Table"]["StorageDescriptor"]["Location"])
            p = re.compile('s3://([^/]*)/(.*)')
            m = p.match(table["Table"]["StorageDescriptor"]["Location"])
            if m is not None:
                bucket_name = m.group(1)
                prefix = m.group(2)
                s3_resource = boto3.resource('s3', region_name=client.region_name)
                s3_bucket = s3_resource.Bucket(bucket_name)
                s3_bucket.objects.filter(Prefix=prefix).delete()

    @available
    def quote_seed_column(
        self, column: str, quote_config: Optional[bool]
    ) -> str:
        return super().quote_seed_column(column, False)
    
    @available
    def create_temp_table(self, database: Optional[str] = None, schema: Optional[str] = None) -> str:
        # todo: defaults
        identifier = str(uuid4()).replace("-", "_") # athena table names cannot contain hyphens
        print(identifier)
        return AthenaRelation.create(database=database, schema=schema, identifier=identifier, type="table")


    @available
    def replace_table(self, target_relation, source_relation):
        conn = self.connections.get_thread_connection()
        client = conn.handle

        with boto3_client_lock:
            glue_client = boto3.client('glue', region_name=client.region_name)

            # fetch current state of target and soruce relation
            source_glue_table = glue_client.get_table(
                DatabaseName=source_relation.schema,
                Name=source_relation.identifier,
            )["Table"]

            original_target_table = glue_client.get_table(
                DatabaseName=target_relation.schema,
                Name=target_relation.identifier,
            )["Table"]


            # overwrite the target glue table with the target table metadata
            attributes_to_copy = {
                "Description",
                "Owner",
                "Retention",
                "StorageDescriptor",
                "PartitionKeys",
                "TableType",
                "Parameters",
            }

            table_input = {k: v for k, v in source_glue_table.items() if k in attributes_to_copy}
            table_input["Name"] = target_relation.identifier

            glue_client.update_table(
                DatabaseName=target_relation.schema,
                TableInput=table_input,
            )

            # delete the target table from the glue catalog
            glue_client.delete_table(
                DatabaseName=source_relation.schema,
                Name=source_relation.table,
            )

            # delete the data files that the original target table metadata pointed to
            # TODO: what happens when we delete the data while a query is running?
            # maybe it would be better to keep it here, and have a "garbage collector" delete
            # unused data folders after a while
            p = re.compile('s3://([^/]*)/(.*)')
            m = p.match(original_target_table["StorageDescriptor"]["Location"])
            if m is not None:
                bucket_name = m.group(1)
                prefix = m.group(2)
                s3_resource = boto3.resource('s3', region_name=client.region_name)
                s3_bucket = s3_resource.Bucket(bucket_name)
                s3_bucket.objects.filter(Prefix=prefix).delete()

            print(f"deleted {source_relation.schema}.{source_relation.table}")
