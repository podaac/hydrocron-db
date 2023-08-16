'''
conftest file to set up local dynamodb connection
'''
from pytest_dynamodb import factories
from tests.database_fixtures import hydrocron_dynamo_instance

__all__ = ['hydrocron_dynamo_instance']

dynamo_test_proc = factories.dynamodb_proc(
        dynamodb_dir="tests/dynamodb_local",
        port=8000)

dynamo_db_resource = factories.dynamodb("hydrocron_swot_reaches")
