import boto3
import json
from decimal import Decimal
import geopandas as gpd

dynamodb = boto3.client('dynamodb')


def upload():
    test_shapefile_path = 'tests/data/SWOT_L2_HR_RiverSP_Reach_548_011_NA_20230610T193337_20230610T193344_PIA1_01/SWOT_L2_HR_RiverSP_Reach_548_011_NA_20230610T193337_20230610T193344_PIA1_01.shp'

    # read shapefile into geopandas dataframe
    shp_file = gpd.read_file(test_shapefile_path)

    item_attrs = {}
    for index, row in shp_file.iterrows():
        print("item")
        print(index)
        print(row)
        print(row.to_json(default_handler=str))
        # convert each reach into a dictionary of attributes that dynamo can read
        item_attrs = json.loads(row.to_json(default_handler=str), parse_float=Decimal)

        print(item_attrs)
        print("--------")
        # write to the table
        response = dynamodb.put_item(
            TableName='hydrocron_swot_reaches',
            Item=item_attrs
        )
    print(response)


upload()
