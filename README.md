# hydrocron-db

Hydrocron-DB manages setting up AWS DynamoDB instance to serve as the backend to the Hydrocron API.

The hydrocron_database module wraps operations like creating and deleting tables, and adding, updating, and deleting items.

The io module contains dataset-specific functions to unpack the various source data. Hydrocron-db currently only supports loading SWOT river reach and node shapefiles.

## Test with local DynamoDB

This repository contains a local AWS DynamoDB instance to be used for testing. Unit tests use the pytest-dynamodb package to set up the dynamodb resource fixtures.
Unit tests can be run as follows:

    poetry run pytest
