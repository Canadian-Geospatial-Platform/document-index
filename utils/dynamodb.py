"""
Filename: dynamodb.py
Description: This script includes functions to integrate DynamoDB table using Python SDK boto3, such as create/delete/update a table and or a item.  
Author: Xinli Cai
Date: October 23, 2023
Version: 1.0
"""

import boto3 
from botocore.exceptions import ClientError
import datetime
import os
import ast
import mimetypes

region = 'ca-central-1'
# Create a dynamodb table
def create_table(TableName, dynamodb=None):
    """     
    The size of the 'similarity' column contains large JSON objects, which exceeds the 1024 bytes limit of the DynamoDB key size.
    Non-key attributes in DynamoDB can be up to 400 KB in size, therefore, we will remove the 'similarity' column from the key schema.
    The primariary key will be the 'features_properties_id' column only. This might cause significant impact on the performance and cost-effectiveness of DynamoDB
    """
    
    dynamodb = boto3.resource('dynamodb', region_name=region)
    
    table = dynamodb.create_table(
        TableName=TableName,
        KeySchema=[
            {
                'AttributeName': 'features_properties_id',
                'KeyType': 'HASH'  # Partition key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'features_properties_id',
                'AttributeType': 'S'
            }
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    # Wait until the table exists.
    table.meta.client.get_waiter('table_exists').wait(TableName=TableName)
    print(f"Table {TableName} created successfully!")
    return table
    
    
# Batch writing items into a table 
def batch_write_items_into_table(df, TableName):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TableName)

    #Get current date and time
    dateTime = datetime.datetime.utcnow().now()
    dateTime = dateTime.isoformat()[:-7] + 'Z'
    
    with table.batch_writer(overwrite_by_pkeys=["features_properties_id"]) as batch:
        for i in range(len(df)):
            print("Row: ", i)
            # try: 
            # Get the application type.
            url = 'None'
            data_type = "None"
            features_options = ast.literal_eval(df.iloc[i]['features_properties_options'])
            uuid = df.iloc[i]['features_properties_id']
            name = df.iloc[i]['features_properties_title_en']
            # print(features_options)
            if len(features_options) > 0:
                options_data = []
                for item in features_options:
                    url = item['url']
                    mimetype,encoding = mimetypes.guess_type(url)
                
                    if url.endswith('pjson'):
                        data_type = 'application/json'
                        
                    elif mimetype is not None:
                        data_type = mimetype
                    
                    else:
                        data_type = 'application/html'
                    options_data.append(
                        {
                            'data_type': data_type,
                            'url': url
                        }
                    )
                    
            try:
                if uuid is not None:
                    batch.put_item(
                        Item={
                            'features_properties_id': uuid,
                            'name_en': name,
                            'options': options_data
                        }
                    )
            except:
                print("Could not store data in dynamodb table")
                
            # except ClientError as e:
            #     print(e.response['Error']['Message'])
    print("All items added to the table successfully!")


#Delete a table  
def delete_table(TableName):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TableName)
    # Check if the table exists
    try:
        response = table.delete()
        print(f"Table {TableName} deleted successfully!")
    except Exception as e:
        print("Error deleting table. It might not exist. Details:", e)