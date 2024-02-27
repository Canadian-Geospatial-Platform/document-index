

import boto3 
from botocore.exceptions import ClientError
import datetime
import os
import ast
import mimetypes
from process_text import process_url
from datetime import datetime
from urllib.parse import urlparse, parse_qs


region = 'ca-central-1'
EXTENSION_TO_MIME = {
    'json': 'application/json',
    'xml': 'application/xml',
    'kml': 'application/vnd.google-earth.kml+xml',
    'vrt': 'application/vnd.vrt',
    'geojson': 'application/json',
    
    # Add other extensions and MIME types as needed
}
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
    
def update_item_text(TableName):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TableName)

def get_mime_type(url):
    if url:
        # Check if the URL ends with a known file extension
        for ext, mime_type in EXTENSION_TO_MIME.items():
            if url.lower().endswith(f"{ext}"):
                return mime_type
        # Fallback to using mimetypes if no known extension is found
        mimetype, _ = mimetypes.guess_type(url)
        if mimetype:
            return mimetype 
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)

        # Check if any of the query parameters indicate a content type
        for key, value in query_params.items():
            if 'json' in value[0].lower():
                return 'application/json'
            elif 'text' in value[0].lower():
                return 'text/plain'
            elif 'xml' in value[0].lower():
                return 'application/xml'
        return 'application/html'
    
def batch_write_items_into_table(df, TableName):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TableName)
    # print(f"an items is ",table.get_item(Key={"features_properties_id": 'ffab6e8d-fa7f-41d8-bb16-7b1e70f9fd0b'}))
    # return 
    
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
                current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                
                doc_type = doc_type = get_mime_type(url)
                # text = process_url(url,doc_type)
                document = {
                    "url": url,
                    "title": title,
                    "type": doc_type,
                    "text": text
                }
                documents.append(document)
            
            item_data = {
                "features_properties_id": features_properties_id,
                "documents": documents,
                "created_at": current_timestamp,
                "updated_at": current_timestamp
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