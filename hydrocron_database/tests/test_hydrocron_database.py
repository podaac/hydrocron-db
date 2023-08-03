"""
==============
test_create_table.py
==============
Test creating a Hydrocron dynamodb table.

Unit tests for creating tables and adding items to the Hydrocron Database.
Requires a local install of DynamoDB to be running.
See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html # noqa

"""
import boto3
import pytest
from hydrocron_database.hydrocron_database import HydrocronDB
from hydrocron_database.hydrocron_database import DynamoKeys
from hydrocron_database.io import swot_reach_node_shp


TEST_SHAPEFILE_PATH = (
    "hydrocron_database/tests/data/"
    "SWOT_L2_HR_RiverSP_Reach_548_011_NA_"
    "20230610T193337_20230610T193344_PIA1_01/"
    "SWOT_L2_HR_RiverSP_Reach_548_011_NA_"
    "20230610T193337_20230610T193344_PIA1_01.shp")

TEST_TABLE_NAME = 'hydrocron_test_table'
TEST_PARTITION_KEY_NAME = 'reach_id'
TEST_SORT_KEY_NAME = 'range_start_time'

DYNAMO_KEYS = DynamoKeys(
            partition_key=TEST_PARTITION_KEY_NAME,
            partition_key_type='S',
            sort_key=TEST_SORT_KEY_NAME,
            sort_key_type='S')


def test_create_table(dynamo_instance):
    '''
    Tests table creation function
    '''
    if dynamo_instance.table_exists(TEST_TABLE_NAME):
        print(dynamo_instance.tables)
        dynamo_instance.delete_table(TEST_TABLE_NAME)

        hydrocron_test_table = dynamo_instance.create_table(
            TEST_TABLE_NAME,
            DYNAMO_KEYS
            )
    else:
        hydrocron_test_table = dynamo_instance.create_table(
            TEST_TABLE_NAME,
            DYNAMO_KEYS)

    assert dynamo_instance.table_exists(TEST_TABLE_NAME)
    assert hydrocron_test_table.table_name == TEST_TABLE_NAME


def test_table_exists(dynamo_instance):
    '''
    Test that a table exists in the database
    '''

    assert dynamo_instance.table_exists(TEST_TABLE_NAME)


def test_list_tables(dynamo_instance):
    '''
    Test listing tables that exist in database
    '''

    if dynamo_instance.table_exists(TEST_TABLE_NAME):
        list_of_tables = dynamo_instance.list_tables()

        assert len(list_of_tables) > 0
        assert TEST_TABLE_NAME in list_of_tables

    else:
        assert len(list_of_tables) == 0


def test_add_data(dynamo_instance):
    '''
    Test adding data from one Reach shapefile to db
    '''

    if dynamo_instance.table_exists(TEST_TABLE_NAME):
        dynamo_instance.delete_table(TEST_TABLE_NAME)

        hydrocron_test_table = dynamo_instance.create_table(
            TEST_TABLE_NAME,
            DYNAMO_KEYS)

        items = swot_reach_node_shp.read_shapefile(TEST_SHAPEFILE_PATH)
        for item_attrs in items:
            # write to the table
            hydrocron_test_table.add_data(**item_attrs)

    assert hydrocron_test_table.table.item_count == 687


def test_query(dynamo_instance):
    '''
    Test a query for a reach id
    '''

    if dynamo_instance.table_exists(TEST_TABLE_NAME):
        dynamo_instance.delete_table(TEST_TABLE_NAME)

    hydrocron_test_table = dynamo_instance.create_table(
        TEST_TABLE_NAME,
        DYNAMO_KEYS)

    items = swot_reach_node_shp.read_shapefile(TEST_SHAPEFILE_PATH)
    for item_attrs in items:
        # write to the table
        hydrocron_test_table.add_data(**item_attrs)

    items = hydrocron_test_table.run_query(partition_key='71224100223')

    assert items[0]['wse'] == str(286.2983)


def test_delete_item(dynamo_instance):
    '''
    Test delete an item
    '''
    if dynamo_instance.table_exists(TEST_TABLE_NAME):
        dynamo_instance.delete_table(TEST_TABLE_NAME)

    hydrocron_test_table = dynamo_instance.create_table(
        TEST_TABLE_NAME,
        DYNAMO_KEYS)

    items = swot_reach_node_shp.read_shapefile(TEST_SHAPEFILE_PATH)
    for item_attrs in items:
        # write to the table
        hydrocron_test_table.add_data(**item_attrs)

    hydrocron_test_table.delete_item(
        partition_key='71224100203', sort_key='2023-06-10T19:33:37Z')
    assert hydrocron_test_table.table.item_count == 686


def test_delete_table(dynamo_instance):
    '''
    Test delete table
    '''

    if dynamo_instance.table_exists(TEST_TABLE_NAME):
        dynamo_instance.delete_table(TEST_TABLE_NAME)
    else:
        dynamo_instance.create_table(
            TEST_TABLE_NAME,
            DYNAMO_KEYS)

        dynamo_instance.delete_table()

    assert not dynamo_instance.table_exists(TEST_TABLE_NAME)


@pytest.fixture(scope='session', name="dynamo_instance")
def dynamo_instance_fixture():
    '''
    Set up a boto3 resource connection to a local dynamodb instance. 
    Assumes Local DynamoDB instance installed and running. 
    See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html # noqa

    Returns
    -------
    dyndb_resource
        A dynamodb local resource
    '''

    session = boto3.session.Session(
        aws_access_key_id='fake_access_key',
        aws_secret_access_key='fake_secret_access_key',
        aws_session_token='fake_session_token',
        region_name='us-west-2')

    dyndb_resource = session.resource(
        'dynamodb', endpoint_url='http://localhost:8000')

    dynamo_instance = HydrocronDB(dyn_resource=dyndb_resource)

    return dynamo_instance
