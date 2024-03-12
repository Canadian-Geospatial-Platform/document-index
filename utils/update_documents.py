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

async def update_documents(TableName, last_updated_before=None):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TableName)
    processed_items = []  # List to track processed items

    if isinstance(last_updated_before, str):
        last_updated_before = datetime.fromisoformat(last_updated_before)

    logger.info("Starting the document update process")
    response = table.scan()
    items = response['Items']
    count=0
    checkpoint_size=100
    for item in items:
        updated_at = item.get('updated_at')
        if updated_at:
            updated_at = parse(updated_at)
            if last_updated_before is None or updated_at < last_updated_before:
                try:
                    documents = item['documents']
                    await asyncio.gather(*[update_single_document(document) for document in documents])
                    new_updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    # table.update_item(
                    #     Key={'features_properties_id': item['features_properties_id']},
                    #     UpdateExpression='SET documents = :val1, updated_at = :val2',
                    #     ExpressionAttributeValues={
                    #         ':val1': documents,
                    #         ':val2': new_updated_at
                    #     }
                    # )
                    item['updated_at'] = new_updated_at
                    item['documents'] = documents
                    table.put_item(Item=item)
                    processed_items.append(item["features_properties_id"])
                    logger.info(f'Updated item: {item["features_properties_id"]}')
                    count+=1
                    print(count)
                except Exception as e:
                    logger.warn(f'unsucessfull attempt for {item["features_properties_id"]} error : {e}')
        if count%checkpoint_size==0 and count>0:
            save_checkpoint(processed_items)
        
        
    logger.info("All applicable documents updated successfully")

    # Save processed items to a JSON file
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"processed_items_{timestamp}.json"
    with open(filename, 'w') as file:
        json.dump(processed_items, file)
    logger.info(f"Processed items saved to {filename}")

    # Optionally, upload the file to S3
    # s3_client = boto3.client('s3')
    # bucket_name = 'your_bucket_name'
    # s3_client.upload_file(filename, bucket_name, filename)
    # logger.info(f"File uploaded to S3: {bucket_name}/{filename}")

