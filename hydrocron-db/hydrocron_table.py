import logging
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

logger = logging.getLogger(__name__)

class Hydrocron_Table:
    def __init__(self, dyn_resource):
        """
        Parameters
        -----------
        dyn_resource : boto3.session.resource('dynamodb')
            A Boto3 DynamoDB resource.

        """
        self.dyn_resource = dyn_resource
        self.table = None

    def exists(self, table_name):
        """
        Determines whether a table exists. As a side effect, stores the table in
        a member variable.

        Parameters
        ----------
        table_name : string
            The name of the table to check.
        
        Returns
        -------
        boolean
            True when the table exists; otherwise, False.
        """
        try:
            table = self.dyn_resource.Table(table_name)
            table.load()
            exists = True
        except ClientError as err:
            if err.response['Error']['Code'] == 'ResourceNotFoundException':
                exists = False
            else:
                logger.error(
                    "Couldn't check for existence of %s. Here's why: %s: %s",
                    table_name,
                    err.response['Error']['Code'], err.response['Error']['Message'])
                raise
        else:
            self.table = table
        return exists
    
    def create_table(self, table_name, partition_key, partition_key_type, sort_key, sort_key_type):
        """
        Creates an Amazon DynamoDB table to store SWOT River Reach, Node, Lake data for the Hydrocron API.
        The table uses the item id (reach_id, node_id, lake_id) as the partition key and
        time as the sort key.

        Parameters
        ---------
        table_name : string
            The name of the table to create.

        Returns
        -------
        dict 
            The newly created table.
        """
        try:
            self.table = self.dyn_resource.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': partition_key, 'KeyType': 'HASH'},  # Partition key
                    {'AttributeName': sort_key, 'KeyType': 'RANGE'}  # Sort key
                ],
                AttributeDefinitions=[
                    {'AttributeName': partition_key, 'AttributeType': partition_key_type},
                    {'AttributeName': sort_key, 'AttributeType': sort_key_type}
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10})
            self.table.wait_until_exists()
        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", table_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return self.table
        
    def list_tables(self):
        """
        Lists the Amazon DynamoDB tables for the current account.

        Returns
        -------
        list
            The list of tables.
        """
        try:
            tables = []
            for table in self.dyn_resource.tables.all():
                print(table.name)
                tables.append(table)
        except ClientError as err:
            logger.error(
                "Couldn't list tables. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return tables