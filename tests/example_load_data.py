import boto3
import json

dynamodb = boto3.client('dynamodb')


def upload():
    song = "test01"
    print(song)
    item = {
        'feature_id': '001'
    }
    print(item)
    response = dynamodb.put_item(
        TableName='hydrocron_swot_reaches',
        Item=item
    )
    print("UPLOADING ITEM")
    print(response)


upload()
