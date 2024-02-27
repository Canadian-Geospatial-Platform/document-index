

import boto3 
import datetime
from process_text import process_url
from datetime import datetime
import asyncio
from dateutil.parser import parse

    
async def update_single_document(document):
    url = document.get('url')
    doc_type = document.get('type')
    if url:
        # Process the URL and update the text asynchronously
        new_text = await process_url(url, doc_type)
        document['text'] = new_text

async def update_documents(TableName, last_updated_before=None):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TableName)

    # Convert last_updated_before to datetime if it's a string
    if isinstance(last_updated_before, str):
        last_updated_before = datetime.fromisoformat(last_updated_before)
    print("fksdjlfa")
    # Scan the table
    response = table.scan()
    items = response['Items']

    # Loop through each item
    for item in items:
        updated_at = item.get('updated_at')
        if updated_at:
            updated_at = parse(updated_at)

            # Check if item was last updated before the given time
            if last_updated_before is None or updated_at < last_updated_before:
                documents = item['documents']

                # Process each document asynchronously
                await asyncio.gather(*[update_single_document(document) for document in documents])

                # Update the 'updated_at' field
                new_updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # Update the item in the DynamoDB table
                table.update_item(
                    Key={'features_properties_id': item['features_properties_id']},
                    UpdateExpression='SET documents = :val1, updated_at = :val2',
                    ExpressionAttributeValues={
                        ':val1': documents,
                        ':val2': new_updated_at
                    }
                )
                print('updated {}'.format(item["features_properties_id"]))
    
    print("All applicable documents updated successfully!")