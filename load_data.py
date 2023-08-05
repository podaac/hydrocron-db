'''
This module searches for new granules and loads data into
the appropriate DynamoDB table
'''
import argparse
from datetime import date
import logging

import boto3
import earthaccess
from hydrocron_database.hydrocron_database import HydrocronDB
from hydrocron_database.hydrocron_database import DynamoKeys
from hydrocron_database.io import swot_reach_node_shp


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
    earthaccess.login()

    results = earthaccess.search_data(
        short_name=collection_shortname,
        cloud_hosted=True,
        temporal=(start_date, end_date)
    )

    granule_paths = [g.data_links(access='direct') for g in results]

    return granule_paths


def load_data(hydrocron_table, granule_paths):
    '''
    Create table and load data

    hydrocron_table : HydrocronTable
        The table to load data into
    granules : list of strings
        The list of S3 paths of granules to load data from
    '''

    for granule in granule_paths:
        if hydrocron_table == "hydrocron-swot-reach-table":
            if 'Reach' in granule:
                items = swot_reach_node_shp.read_shapefile(granule)

                for item_attrs in items:
                    # write to the table
                    hydrocron_table.add_data(**item_attrs)

        elif hydrocron_table == "hydrocron-swot-node-table":
            if 'Node' in granule:
                items = swot_reach_node_shp.read_shapefile(granule)

                for item_attrs in items:
                    # write to the table
                    hydrocron_table.add_data(**item_attrs)

        else:
            print('Items cannot be parsed, file reader not implemented')


def create_parser():
    '''
    Creates argument parser
    '''
    parser = argparse.ArgumentParser(prog='PO.DAAC Hydrocron Database')

    parser.add_argument(
        "-tn", "--table-name",
        dest="table_name",
        required=True,
        help="The name of the table to populate.")
    parser.add_argument(
        "-sd", "--start-date",
        dest="start_date",
        help="The ISO date after which data should be retrieved. For Example, --start-date 2023-06-14",  # noqa
        default="2023-03-01")
    parser.add_argument(
        "-ed", "--end-date",
        dest="end_date",
        help="The ISO date before which data should be retrieved. For Example, --end-date 2023-07-14",  # noqa
        default=date.today().strftime('%Y-%m-%d'))
    
    return parser


def run(args=None):
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
    if args is None:
        parser = create_parser()
        args = parser.parse_args()

    table_name = args.table_name
    start_date = args.start_date
    end_date = args.end_date

    match table_name:
        case "hydrocron-swot-reach-table":
            collection_shortname = "L2_HR_RiverSP"
        case "hydrocron-swot-node-table":
            collection_shortname = "L2_HR_RiverSP"
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

    table_name = 'hydrocron_swot_reaches'

    new_granules = find_new_granules(
        collection_shortname,
        start_date,
        end_date)

    load_data(hydrocron_table, new_granules)


def main():
    '''
    Main function to run database loading
    '''
    try:
        run()
    except Exception as e:  # pylint: disable=broad-except
        logging.exception("Uncaught exception occurred during execution.")
        exit(hash(e))


if __name__ == "__main__":
    main()
