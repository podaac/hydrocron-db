#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pytest_dynamodb import factories


import boto3
import json
from decimal import Decimal
import geopandas as gpd

from hydrocron_database import Hydrocron_DB


dynamo_test_proc = factories.dynamodb_proc(
        dynamodb_dir="tests/dynamodb_local",
        port=8000)

dyndb_resource = factories.dynamodb("hydrocron_swot_reaches")
dynamo_instance = Hydrocron_DB(dyn_resource=dyndb_resource)


table_name = 'hydrocron_swot_reaches'
test_shapefile_path = 'tests/data/SWOT_L2_HR_RiverSP_Reach_548_011_NA_20230610T193337_20230610T193344_PIA1_01/SWOT_L2_HR_RiverSP_Reach_548_011_NA_20230610T193337_20230610T193344_PIA1_01.shp'

if dynamo_instance.table_exists(table_name):
    dynamo_instance.delete_table(table_name)

hydrocron_reach_table = dynamo_instance.create_table(table_name,
                                                     partition_key='feature_id', partition_key_type='S',
                                                     sort_key='time', sort_key_type='N')

# read shapefile into geopandas dataframe
shp_file = gpd.read_file(test_shapefile_path)

item_attrs = {}
for index, row in shp_file.iterrows():
    # convert each reach into a dictionary of attributes that dynamo can read
    item_attrs = json.loads(row.to_json(default_handler=str), parse_float=Decimal)

    # write to the table
    hydrocron_reach_table.add_data(**item_attrs)

items = hydrocron_reach_table.run_query(partition_key='71224100223')
print(items)

