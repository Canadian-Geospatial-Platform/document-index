import json
import awswrangler as wr
import pandas as pd
# import requests
import asyncio
import datetime


from dynamodb import update_documents

BUCKET_NAME = 'webpresence-geocore-geojson-to-parquet-dev'



def lambda_handler(event, context):
    table_name = event['tablename']
    ids = event['ids']

    

    # Call update_documents function
    asyncio.run(update_documents(ids,table_name))
  
    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Batch processed successfully"})
    }
