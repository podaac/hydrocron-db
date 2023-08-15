'''
Database fixture to use to test with local dynamodb
'''
import pytest
from hydrocron_db.hydrocron_database import HydrocronDB


@pytest.fixture()
def hydrocron_dynamo_instance(dynamo_db_resource):
    '''
    Set up a connection to a local dynamodb instance and 
    create a table for testing
    '''
    hydrodb = HydrocronDB(dyn_resource=dynamo_db_resource)

    return hydrodb
