"""
conftest file to set up local dynamodb connection
"""
import os.path
from pytest_dynamodb import factories
from tests.database_fixtures import hydrocron_dynamo_table

__all__ = ['hydrocron_dynamo_table']

dynamo_test_proc = factories.dynamodb_proc(
        dynamodb_dir=os.path.join(os.path.dirname(os.path.realpath(__file__)), 'dynamodb_local'),
        port=8000)

dynamo_db_resource = factories.dynamodb("dynamo_test_proc")
