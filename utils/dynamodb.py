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
    
    
def batch_write_items_into_table(df, TableName):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TableName)
    
    with table.batch_writer(overwrite_by_pkeys=["features_properties_id"]) as batch:
        for i in range(len(df)):
            print("Row: ", i)
            features_properties_id = df.iloc[i]['features_properties_id']
            features_properties_options = ast.literal_eval(df.iloc[i]['features_properties_options'])

            documents = []
            for item in features_properties_options:
                url = item.get('url', 'None')
                title = item.get('title', 'None')
                text = item.get('text', 'None')

                mimetype, encoding = mimetypes.guess_type(url)
                doc_type = mimetype if mimetype else 'application/html'

                document = {
                    "url": url,
                    "title": title,
                    "type": doc_type,
                    "text": text
                }
                documents.append(document)
            
            item_data = {
                "features_properties_id": features_properties_id,
                "documents": documents
            }

            try:
                batch.put_item(Item=item_data)
            except Exception as e:
                print(f"Could not store data for row {i} in dynamodb table: {e}")
    
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