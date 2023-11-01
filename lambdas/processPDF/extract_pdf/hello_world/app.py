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
    
    scan_response = table.scan()
    data = scan_response['Items']
    while 'LastEvaluatedKey' in scan_response:
        print(scan_response['LastEvaluatedKey'])
        scan_response = table.scan(ExclusiveStartKey=scan_response['LastEvaluatedKey'])
        data.extend(scan_response['Items'])
        
    # print(data['Items'][11])
    # print(data['Items'][11].keys())
    # print(data['Items'][11]['text_data'])
    # print("text_data length", len(ast.literal_eval(data['Items'][11]['text_data'])))
    # print(len(data['Items'][11]['options']))
    # print("\n")
    # print(data['Items'][12])
    # print(len(data))
    
    for item in tqdm(data):
        uuid = item['features_properties_id']
        all_options = item['options']
        all_text_data = []
        
        if len(all_options) > 0:
            for option in all_options:
                data_type = option['data_type']
                url = option['url']
                # print("URL: ", url)
                
                # Retrieve the text from the url.
                try:
                    text = scrape_data_from_url(data_type, url)  # If this fails, the 'text' variable will contain 'cannot_fetch_url'.
                    # print("retrieved text is: ", text)
                except Exception as e:
                    print("Cannot fetch url data")
                    # print(e)
                    # text = 'cannot_fetch_url'
                all_text_data.append({'data_type': data_type, 'data': text})
            
        # print("Length of all_text_data: ", len(all_text_data))
        # Now update the record in the dynamodb table for the specifc uuid.
        # try:
        # print("All_text_data: ", all_text_data)
        # try:
        response = table.update_item(
            Key={'features_properties_id': uuid},
            UpdateExpression="set text_data=:p",
            ExpressionAttributeValues={":p": str(all_text_data)},
            ReturnValues="UPDATED_NEW"
        )
            
        # except
        #     print()
            
        # else:
        # print(response["Attributes"])
        
    return True
    
    
    
    # # Store the data into the dynamodb table.        
    # return {
    #     'success': success_variable, 
    #     'body': json.dumps(
    #             {
    #                 'text': str(text_data)
    #             }
    #         )
    #     }
   

    
