import json
import boto3
import ast
from extract_data import scrape_data_from_url
from tqdm import tqdm


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
    # Read in the DynamDB table Docinx 
    # pdf_url = "https://web.stanford.edu/~jurafsky/slp3/10.pdf"
    #pdf_url = event['url']
    
    # Scan the dynamodb table.
    table_name = 'docinx'
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    # First scan the table to get all the items. Make sure to have the credentials set up.
    scan_response = table.scan()
    data = scan_response['Items']
    while 'LastEvaluatedKey' in scan_response:
        print(scan_response['LastEvaluatedKey'])
        scan_response = table.scan(ExclusiveStartKey=scan_response['LastEvaluatedKey'])
        data.extend(scan_response['Items'])
        
    print(len(data))
    # data = data[4000:6000] # Doing 2000 items at a time because the short-term credentials expire after 4 hours.
    for item in tqdm(data):
        uuid = item['features_properties_id']
        all_options = item['options']
        all_text_data = []
        
        if len(all_options) > 0:
            for op_num, option in enumerate(all_options):
                print("Option number: ", op_num)
                data_type = option['data_type']
                url = option['url']
                print("URL: ", url)
                
                # Retrieve the text from the url.
                try:
                    text = scrape_data_from_url(data_type, url)  # If this fails, the 'text' variable will contain 'cannot_fetch_url'.
                    print("fetched text")
                except Exception as e:
                    print("Cannot fetch url data or taking too long.")
                    print(e)
                    text = 'cannot_fetch_url'
                
                # If the size of the 'text' variable is greater than 0.5 KB, then truncate the text to that many characters.
                # Commenting it out for now. This was the previous code snippet.
                # if len(text) > 10000:
                #     text = text[:10000]
                all_text_data.append({'data_type': data_type, 'data': text})
            print("Length of all_text_data: ", len(str(all_text_data).encode('utf-8')))
            
        try:
            response = table.update_item(
                Key={'features_properties_id': uuid},
                UpdateExpression="set text_data=:p",
                ExpressionAttributeValues={":p": str(all_text_data)},
                ReturnValues="UPDATED_NEW"
            )
            
        except:
            print("Could not update the table. Storing an empty list.")
            response = table.update_item(
                Key={'features_properties_id': uuid},
                UpdateExpression="set text_data=:p",
                ExpressionAttributeValues={":p": str([])},
                ReturnValues="UPDATED_NEW"
            )
            
    return True
    
    

# Running 'python app.py' will run the lambda_handler function.
lambda_handler(None, None)

    
