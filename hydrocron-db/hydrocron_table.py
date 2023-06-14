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
        Determines whether a table exists. If table exists, load it.

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
                    "Couldn't check for existence of %s. %s: %s",
                    table_name,
                    err.response['Error']['Code'], err.response['Error']['Message'])
                raise
        else:
            self.table = table
        return exists
    
    def create_table(self, table_name, partition_key, partition_key_type, sort_key, sort_key_type):
        """
        Creates an Amazon DynamoDB table to store SWOT River Reach, Node, or Lake data for the Hydrocron API.

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
                "Couldn't create table %s. %s: %s", table_name,
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
                "Couldn't list tables. %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return tables
        
    def add_data(self, partition_key_name, **kwargs):
        """
        Adds a data item to the table.

        Parameters
        ---------
        partition_key_name : string
            The name of the partition key for the table
        **kwargs: All attributes to add to the item. Must include partition and sort keys
        """

        item_dict = {}

        for key, value in kwargs.items():
            item_dict[key] = value

        item_id = item_dict[partition_key_name]
        try:
            self.table.put_item(
                Item=item_dict
            )
        except ClientError as err:
            logger.error(
                "Couldn't add item %s to table %s. Here's why: %s: %s",
                partition_key_name, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def get_item(self, partition_key_name, partition_key, sort_key_name, sort_key):
        """
        Gets data from the table for a single item.

        Parameters
        ----------
        partition_key_name : string
            the name of the partition key
        partition_key : string
            the feature ID
        sort_key_name : string
            the name of the sort key, usually time
        sort_key: string
            the value of the sort key item.
        
        Returns
        -------
        The data about the requested item.

        """
        try:
            item_key = {}
            item_key[partition_key_name] = partition_key
            item_key[sort_key_name] = sort_key

            print(item_key)

            response = self.table.get_item(Key=item_key)
        except ClientError as err:
            logger.error(
                "Couldn't get item %s from table %s. %s: %s",
                partition_key, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Item']
        
    def query(self, partition_key_name, partition_key, sort_key_name, sort_key):
        """
        Perform a query for multiple items.

        Parameters
        ----------
        partition_key_name : string
            the name of the partition key
        partition_key : string
            the feature id to query
        sort_key_name : string
            the name of the sort key, usually time
        sort_key : integer
            the value of the sort keys to query


        Returns
        -------
            The item.

        """
        try:
            response = self.table.query(
                KeyConditionExpression=(Key(partition_key_name).eq(partition_key) & 
                                      Key(sort_key_name).eq(sort_key)))
        except ClientError as err:
            logger.error(
                "Couldn't query for movies released in %s. Here's why: %s: %s", year,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Items']
        
    def update_item(self, partition_key_name, partition_key, sort_key_name, sort_key, **kwargs):
        """
        Updates attribute data for a reach, node, or lake in the table.

        Parameters
        ----------
        partition_key_name: string
            The name of the partition key.
        partition_key: string
            The ID of the feature to update.
        sort_key_name: string
            The name of the sort key.
        sort_key: string
            The timestamp of the item to update.
        **kwargs: The fields to update with their new values
        
        Returns
        -------
        The fields that were updated, with their new values.

        """

        update_expr = "set "
        expr_att_values = {}

        for key, value in kwargs.items():
            update_expr = update_expr + "{}=:{}, ".format(key, key)
            expr_att_values[":{}".format(key)] = value


        try:
            response = self.table.update_item(
                Key={partition_key_name: partition_key, sort_key_name: sort_key},
                UpdateExpression=update_expr,
                ExpressionAttributeValues=expr_att_values,
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.error(
                "Couldn't update item %s in table %s. %s: %s",
                partition_key, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Attributes']
        
    def delete_item(self, partition_key_name, partition_key, sort_key_name, sort_key):
        """
        Deletes an item from the table.

        Parameters
        ----------
        partition_key_name: string
            The name of the partition key.
        partition_key: string
            The ID of the item to delete.
        sort_key_name: string
            The name of the sort key.
        sort_key: string
            The timestamp of the item to delete.
        """
        try:
            self.table.delete_item(Key={partition_key_name: partition_key, sort_key_name: sort_key})
        except ClientError as err:
            logger.error(
                "Couldn't delete item %s. %s: %s", partition_key,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise


    def delete_table(self):
        """
        Deletes the table.
        """
        try:
            self.table.delete()
            self.table = None
        except ClientError as err:
            logger.error(
                "Couldn't delete table. %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise