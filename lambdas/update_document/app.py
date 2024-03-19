import json
import boto3
import datetime

lambda_client = boto3.client('lambda')

def lambda_handler(event, context):
    table_name = 'docindx_v2'
    batch_size = 50  # Number of URLs per batch

    # Fetch URLs from the database (to be implemented)
    ids = fetch_primary_keys_from_database(table_name)
    print("the ids has the length of ",len(ids))
    ids = ids[:500]
    # print(ids)
    # assert False
    # Split URLs into batches
    for i in range(0, len(ids), batch_size):
        batch = ids[i:i + batch_size]

        # Invoke worker lambda function for each batch
        payload = {
            "ids": batch,
            "tablename": table_name
        }
        lambda_client.invoke(
            FunctionName='documentupdateworker',
            InvocationType='Event',
            Payload=json.dumps(payload)
        )

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Worker Lambdas invoked successfully"})
    }



import boto3

def fetch_primary_keys_from_database(table_name):
    # Initialize a boto3 DynamoDB resource
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    # List to store the primary keys
    primary_keys = []

    # Scan operation to fetch only the primary key attribute
    # Note: Consider using a Paginated scan or Query if your table is large
    response = table.scan(
        ProjectionExpression="features_properties_id"
    )

    # Extracting primary keys from the response
    items = response.get('Items', [])
    for item in items:
        primary_key = item.get('features_properties_id')
        if primary_key:
            primary_keys.append(primary_key)

    # Handling pagination if the dataset is large
    while 'LastEvaluatedKey' in response:
        response = table.scan(
            ProjectionExpression="features_properties_id",
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        for item in response.get('Items', []):
            primary_key = item.get('features_properties_id')
            if primary_key:
                primary_keys.append(primary_key)

    return primary_keys
