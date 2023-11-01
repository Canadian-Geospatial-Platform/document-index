import json
import PyPDF2
import requests
import boto3
from io import BytesIO

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
    
    data = table.scan()
    print(data['Items'][11])
    
    
    # Now we extract the text from the pdf at the url.
    # response = requests.get(pdf_url)
    # pdf_data = response.content
    # text_data = ""
    # success_variable = False
    
    # if response.status_code == 200: # Success
    #     pdf = PyPDF2.PdfReader(BytesIO(pdf_data))
    #     success_variable = True # Change it to True
    #     for page in range(len(pdf.pages)):
    #         text_data += pdf.pages[page].extract_text()
    
    
    # # Store the data into the dynamodb table.        
    # return {
    #     'success': success_variable, 
    #     'body': json.dumps(
    #             {
    #                 'text': str(text_data)
    #             }
    #         )
    #     }
   

    
