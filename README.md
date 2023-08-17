# hydrocron-db

Hydrocron-DB manages setting up AWS DynamoDB instance to serve as the backend to the Hydrocron API.

The hydrocron_database module wraps operations like creating and deleting tables, and adding, updating, and deleting items.

The io module contains dataset-specific functions to unpack the various source data. Hydrocron-db currently only supports loading SWOT river reach and node shapefiles.

Main script to create tables and start loading data into the database is load_data.py which takes the table name, start date, and end date to load data for as arguments.  It can be run as follows:

    load_data.py -t hydrocron-swot-reach-table  -sd 2023-01-01T00:00:00 -ed 2023-03-01T00:00:00

## Test with local DynamoDB

This repository contains a local AWS DynamoDB instance to be used for testing. Unit tests use the pytest-dynamodb package to set up the dynamodb resource fixtures.
Unit tests can be run as follows:

    poetry run pytest
