import json
import awswrangler as wr
import pandas as pd
# import requests
import asyncio
import datetime


from utils.update_documents import update_documents

BUCKET_NAME = 'webpresence-geocore-geojson-to-parquet-dev'



def lambda_handler(event, context):
    table_name = 'docindex_v2'

    # Calculate yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%dT%H:%M:%S')

    # Call update_documents function
    asyncio.run(update_documents(table_name))
  
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "success",
            # "location": ip.text.replace("\n", "")
        }),
    }
