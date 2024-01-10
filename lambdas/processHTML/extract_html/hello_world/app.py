import sys
import json
import pandas as pd
import io
from botocore.vendored import requests
import boto3

import s3fs
import pyarrow.parquet as pq

# print("df len: ", df.shape)
# print(df.head())





def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """
    try:
        # if s3_client is None:
        #     s3_client = boto3.client('s3')
        # obj = s3_client.get_object(Bucket=bucket, Key=key)
        # return pd.read_parquet(io.BytesIO(obj['Body'].read()), **args)
        
        # Read the dataframe from the s3 bucket inside a lambda function.
        s3 = boto3.resource('s3')
        obj = s3.Object("webpresence-geocore-geojson-to-parquet-dev", "records.parquet")
        df = pd.read_parquet(io.BytesIO(obj.get()['Body'].read()))
        
        
    except ClientError as e:
        logging.error(e)
        return False 
        
    # return {
    #     "statusCode": 200,
    #     "body": json.dumps({
    #         "message": "hello world",
    #         # "location": ip.text.replace("\n", "")
    #     }),
    # }
