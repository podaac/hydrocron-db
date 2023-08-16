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
        print("ITEM")
        print(index)
        print("ROW")
        object = {}
        for k, v in row.items():
            print(k)
            print(v)
            object[k] = {'S' : v}
        print(object)
        #print(type(row))
        print("^^^^^^^^")
        #print(row.to_json(default_handler=str))

    test = {
        'feature_id': {'S' : '71224100223'},
        'area_total': {'S' : '-999999999999'},
        'dschg_gcsf': {'S' : '-999999999999'},
        'dschg_b': {'S' : '-999999999999'},
        'dschg_gmsf': {'S' : '-999999999999'}
    }
    # convert each reach into a dictionary of attributes that dynamo can read
    #item_attrs = json.loads(row.to_json(default_handler=str), parse_float=Decimal)

    print("--------")
    # write to the table
    response = dynamodb.put_item(
        TableName='hydrocron_swot_reaches',
        Item=test
    )
    print(response)


upload()
