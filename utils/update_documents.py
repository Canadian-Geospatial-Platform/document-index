import boto3 
import datetime
from process_text import process_url
from datetime import datetime
import asyncio
from dateutil.parser import parse
import json

# Importing Logger
from aws_lambda_powertools import Logger

# Initialize Logger
logger = Logger(service="doc_index_update_documents", level="INFO")
s3 = boto3.client('s3')
bucket_name = 'nlp-data-preprocessing'
def save_checkpoint(processed_items):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"doc_index_processed_items_checkpoint_{timestamp}.json"
    s3.put_object(Bucket=bucket_name, Key=filename, Body=json.dumps(processed_items))
    print(f"Checkpoint saved: {filename}")
async def update_single_document(document):
    url = document.get('url')
    doc_type = document.get('type')
    if url:
        new_text = await process_url(url, doc_type)
        document['text'] = new_text

async def update_documents(ids, TableName):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TableName)
    processed_items = []  # List to track processed items
    
    logger.info("Starting the document update process")
    count = 0
    checkpoint_size = 20
    
    for features_properties_id in ids:
        try:
            # Retrieve item from DynamoDB table
            response = table.get_item(Key={'features_properties_id': features_properties_id})
            item = response.get('Item')
            if not item:
                logger.warn(f'Item not found: {features_properties_id}')
                continue
    
            updated_at = item.get('updated_at')
            if updated_at:
                updated_at = parse(updated_at)
    
                documents = item['documents']
                await asyncio.gather(*[update_single_document(document) for document in documents])
                new_updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                item['updated_at'] = new_updated_at
                item['documents'] = documents
                table.put_item(Item=item)
                processed_items.append(features_properties_id)
                logger.info(f'Updated item: {features_properties_id}')
                count += 1
                print(count)
    
            if count % checkpoint_size == 0 and count > 0:
                save_checkpoint(processed_items)
    
        except Exception as e:
            logger.warn(f'Unsuccessful attempt for {features_properties_id} error: {e}')
    
    logger.info("Document update process completed")

    # Save processed items to a JSON file
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"processed_items_{timestamp}.json"




