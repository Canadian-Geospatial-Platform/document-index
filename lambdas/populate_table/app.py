import json
import awswrangler as wr
import pandas as pd
# import requests

from utils.dynamodb import create_table,batch_write_items_into_table

BUCKET_NAME = 'webpresence-geocore-geojson-to-parquet-dev'

def get_records():
    df = wr.s3.read_parquet(f"s3://{BUCKET_NAME}/", dataset=True)
    return df

def lambda_handler(event, context):
    df = get_records()
    table_name = 'docinx'
    # create_table('docindx_v2')
    batch_write_items_into_table(df,table_name)
    
    


    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "success",
            # "location": ip.text.replace("\n", "")
        }),
    }
