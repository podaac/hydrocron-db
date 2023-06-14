"""
==============
test_create_table.py
==============
Test creating a Hydrocron dynamodb table.

Unit tests for creating tables and adding items to the Hydrocron Database. 
Requires a local install of DynamoDB to be running. See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html

"""
import pytest
import boto3
import json
from decimal import Decimal
import geopandas as gpd
from botocore.exceptions import ClientError

from hydrocron_table import Hydrocron_Table


test_shapefile_path = '/tests/data/SWOT_L2_HR_RiverSP_Node_540_010_AS_20230602T193520_20230602T193521_PIA1_01/SWOT_L2_HR_RiverSP_Node_540_010_AS_20230602T193520_20230602T193521_PIA1_01.shp'
test_table_name = 'hydrocron_test_table'
test_partition_key_name = 'reach_id'
test_sort_key_name = 'time'

def test_create_table(dyndb_resource_connection, table_name=test_table_name):
    '''
    Tests table creation function
    '''
    hydrocron_test_table = Hydrocron_Table(dyn_resource=dyndb_resource_connection)

    if hydrocron_test_table.exists(test_table_name):
        hydrocron_test_table.delete_table()

        hydrocron_test_table.create_table(table_name, 
                    partition_key='reach_id', partition_key_type='S', 
                    sort_key='time', sort_key_type='N')
    else:
        hydrocron_test_table.create_table(table_name, 
                    partition_key='reach_id', partition_key_type='S', 
                    sort_key='time', sort_key_type='N')

    assert hydrocron_test_table.table.name == table_name


def test_table_exists(dyndb_resource_connection, table_name=test_table_name):
    '''
    Test that a table exists in the database
    '''
    hydrocron_test_table = Hydrocron_Table(dyn_resource=dyndb_resource_connection)

    assert hydrocron_test_table.exists(table_name)


def test_list_tables(dyndb_resource_connection):
    '''
    Test listing tables that exist in database
    '''

    hydrocron_test_table = Hydrocron_Table(dyn_resource=dyndb_resource_connection)

    list_of_tables = hydrocron_test_table.list_tables()

    assert len(list_of_tables) > 0
    assert list_of_tables[0].table_name == test_table_name

def test_add_data(dyndb_resource_connection):
    hydrocron_test_table = Hydrocron_Table(dyn_resource=dyndb_resource_connection)
            
    # read shapefile into geopandas dataframe
    shp_file = gpd.read_file(test_shapefile_path)

    item_attrs = {}
    for index, row in shp_file.iterrows():
        # convert each reach into a dictionary of attributes that dynamo can read
        item_atts = json.loads(row.to_json(default_handler=str), parse_float=Decimal)

        # write to the table
        hydrocron_test_table.add_data(partition_key_name=test_partition_key_name, **item_attrs)
        break # just test one item

    item = hydrocron_test_table.get_item(partition_key_name=test_partition_key_name,
                                         partition_key=item_attrs[test_partition_key_name],
                                         sort_key_name = test_sort_key_name,
                                         sort_key = item_attrs[test_sort_key_name])
    
    assert item[test_partition_key_name] == test_partition_key_name

def test_get_item(dyndb_resource_connection):
    pass;

def test_query(dyndb_resource_connection, partition_key_name):
    pass;

def test_delete_item(dyndb_resource_connection):
    pass;

def test_delete_table(dyndb_resource_connection):
    '''
    Test delete table
    '''

    hydrocron_test_table = Hydrocron_Table(dyn_resource=dyndb_resource_connection)
    
    if hydrocron_test_table.exists(test_table_name):
        hydrocron_test_table.delete_table()
    else:
        hydrocron_test_table.create_table(table_name=test_table_name, 
                            partition_key='reach_id', partition_key_type='S', 
                            sort_key='time', sort_key_type='N')
        
        hydrocron_test_table.delete_table()

    assert not hydrocron_test_table.exists(test_table_name)

@pytest.fixture(scope='session')
def dyndb_resource_connection():
    '''
    Set up a boto3 resource connection to a local dynamodb instance. 
    Assumes Local DynamoDB instance installed and running. See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html 

    Returns
    -------
    dyndb_resource
        A dynamodb local resource
    '''

    session = boto3.session.Session(aws_access_key_id='fake_access_key',
                                    aws_secret_access_key='fake_secret_access_key',
                                    aws_session_token='fake_session_token',
                                    region_name='us-west-2')
    
    dyndb_resource = session.resource('dynamodb', endpoint_url='http://localhost:8000')

    return dyndb_resource
