import ast

# Other libraries for extracting data.
import requests
from bs4 import BeautifulSoup
import json
import PyPDF2
from io import BytesIO
import mimetypes
from wrapt_timeout_decorator import *

# Define data preprocessing functions here for each data type.

def extract_text_from_pdf(pdf_data):

    pdf = PyPDF2.PdfReader(BytesIO(pdf_data))
    text_data = ""
    
    for page in range(len(pdf.pages)):
        text_data += pdf.pages[page].extract_text()

    return text_data

@timeout(5)  # Five second timeout so that the main thread does not get stuck.
def scrape_data_from_url(data_type, url):
    # Check the content type of the URL
    
    try:
        response = requests.get(url)
        mimetype,encoding = mimetypes.guess_type(url)
        if url.endswith(('.png', '.tiff', '.tif')):
            print("url inside scrape_data_from_url: ", url)
            return 'image_data'
    except Exception as e:
        print("Cannot fetch url data")
        return 'cannot_fetch_url'
    
    if response.status_code == 200:
        if url.endswith('.pdf'):
            # print("Application type: pdf")
            pdf_data = response.content
            text_data = extract_text_from_pdf(pdf_data)
    
            return str(text_data)
        
        elif data_type in ['application/html']: # , 'application/xml']  #NOTE: Need to add xml support.
            soup = BeautifulSoup(response.text, 'html.parser')
            text_data = soup.get_text()
            # print("html data: ", text_data)
            return str(text_data)
        
        elif url.endswith(('.png', '.tiff', '.tif')):
            print("url inside scrape_data_from_url: ", url)
            return 'image_data'
            # if 'image' in mimetype:
            #     return 'image_data'
        
        elif data_type == 'application/json':
            print("Application type: json")
            json_data = json.loads(response.text)
            # print("json data: ", json_data)
            if json_data['data']['error']['code'] == 500:
                return 'cannot_fetch_url'
            return str(json_data)
            
        return f'unsupported_type: {mimetype}'
    else:
        print("Cannot fetch url")
        return f'cannot_fetch_url'
        
        