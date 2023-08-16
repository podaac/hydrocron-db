'''
This module searches for new granules and loads data into
the appropriate DynamoDB table
'''
import logging

import boto3
import earthaccess
from hydrocron_db.hydrocron_database import HydrocronDB
from hydrocron_db.hydrocron_database import DynamoKeys
from hydrocron_db.io import swot_reach_node_shp


def setup_connection():
    '''
    Set up DynamoDB connection

    Returns
    -------
    dynamo_instance : HydrocronDB
    '''
    session = boto3.session.Session(
        aws_access_key_id='fake_access_key',
        aws_secret_access_key='fake_secret_access_key',
        aws_session_token='fake_session_token',
        region_name='us-west-2')

    dyndb_resource = session.resource(
        'dynamodb',
        endpoint_url='http://localhost:8000')

    dynamo_instance = HydrocronDB(dyn_resource=dyndb_resource)

    return dynamo_instance


def find_new_granules(collection_shortname, start_date, end_date):
    '''
    Find granules to ingest

    Parameters
    ----------
    collection_shortname : string
        The shortname of the collection to search

    Returns
    -------
    granule_paths : list of strings
        List of S3 paths to the granules that have not yet been ingested
    '''
    auth = earthaccess.login()

    cmr_search = earthaccess.DataGranules(auth).\
        short_name(collection_shortname).temporal(start_date, end_date)

    results = cmr_search.get()

    granule_paths = [g.data_links(access='direct') for g in results]
    print(granule_paths)
    return granule_paths


def load_data(hydrocron_table, granule_path):
    '''
    Create table and load data

    hydrocron_table : HydrocronTable
        The table to load data into
    granules : list of strings
        The list of S3 paths of granules to load data from
    '''

    if hydrocron_table.table_name == "hydrocron-swot-reach-table":
        if 'Reach' in granule_path:
            items = swot_reach_node_shp.read_shapefile(granule_path)

            for item_attrs in items:
                # write to the table
                hydrocron_table.add_data(**item_attrs)

    elif hydrocron_table.table_name == "hydrocron-swot-node-table":
        if 'Node' in granule_path:
            items = swot_reach_node_shp.read_shapefile(granule_path)

            for item_attrs in items:
                # write to the table
                hydrocron_table.add_data(**item_attrs)

    else:
        print('Items cannot be parsed, file reader not implemented for table '
              + hydrocron_table.table_name)


def run(table_name, start_date, end_date):
    '''
    Main function to manage loading data into Hydrocron

    Parameters
    ----------
    dynamo_instance : HydrocronDB
        The Hydrocron database instance containing the table
    table_name : string
        The name of the table to load data to
    start_date : string
        The starting date to search new granules
    end_date : string
        The end date to search new granules
    '''

    match table_name:
        case "hydrocron-swot-reach-table":
            collection_shortname = "SWOT_L2_HR_RIVERSP_1.0"
        case "hydrocron-swot-node-table":
            collection_shortname = "SWOT_L2_HR_RIVERSP_1.0"
        case _:
            logging.warning(
                "Hydrocron table %(table_name) does not exist.", table_name)

    dynamo_instance = setup_connection()

    if dynamo_instance.table_exists(table_name):
        hydrocron_table = dynamo_instance.load_table(table_name)
    else:
        logging.info("creating new table... ")
        dynamo_keys = DynamoKeys(
            partition_key='reach_id',
            partition_key_type='S',
            sort_key='range_start_time',
            sort_key_type='S')

        hydrocron_table = dynamo_instance.create_table(table_name, dynamo_keys)

    new_granules = find_new_granules(
        collection_shortname,
        start_date,
        end_date)

    load_data(hydrocron_table, new_granules)


if __name__ == "__main__":
    try:
        run(
            table_name='hydrocron-swot-reach-table',
            start_date='2023-01-01T00:00:00',
            end_date='2023-03-01T00:00:00')

    except Exception as e:  # pylint: disable=broad-except
        logging.exception("Uncaught exception occurred during execution.")
        exit(hash(e))
