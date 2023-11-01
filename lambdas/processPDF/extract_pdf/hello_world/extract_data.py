import pandas as pd
import numpy as np
import ast
from tqdm import tqdm
import sys
sys.path.append("/home/rsaha/projects/document-index/utils/")

from s3 import open_file_s3

# Other libraries for extracting data.
import requests
from bs4 import BeautifulSoup
import json
import PyPDF2
from io import BytesIO



# Define data preprocessing functions here for each data type.

def extract_text_from_pdf(pdf_data):
    pdf = PyPDF2.PdfReader(BytesIO(pdf_data))
    text_data = ""
    for page in range(len(pdf.pages)):
        text_data += pdf.pages[page].extract_text()
    # print("pdf data")
    # print(text_data)
    return text_data


def scrape_data_from_url(url):
    # Check the content type of the URL
    response = requests.get(url)
    if '.tiff' not in url:
        if response.status_code == 200:
            content_type = response.headers.get('content-type')
            if 'text/html' in content_type:
                # Scrape data from a website
                print("Application type: html")
                soup = BeautifulSoup(response.text, 'html.parser')
                text_data = soup.get_text()
                # print("html data: ", text_data)
                return "html: " + str(text_data)

            elif 'application/pdf' in content_type:
                # Scrape data from a PDF
                print("Application type: pdf")
                pdf_data = response.content
                text_data = extract_text_from_pdf(pdf_data)
                # print("Pdf data: ", text_data[:100])
                return "pdf: " + str(text_data)

            elif 'image' in content_type:
                # You can handle image scraping here
                # For example, if you want to download and save the image:
                print("Skipping image type")
                # raise ValueError("Skipping image type")
                return "failed: image_type"
                # with open('downloaded_image.jpg', 'wb') as f:
                #     f.write(response.content)
                # return "Image saved as 'downloaded_image.jpg'"

            elif 'application/json' in content_type:
                # Scrape data from JSON
                print("Application type: json")
                json_data = json.loads(response.text)
                # print("json data: ", json_data)
                return "json: " + str(json_data)

            else:
                # print("Unsupported content type: " + content_type)
                # raise ValueError("Unsupported content type: " + content_type)
                return "failed: " + str(url)
        else:
            # print("Failed to retrieve the URL: " + str(response.status_code))
            # raise ValueError("Failed to retrieve the URL: " + str(response.status_code))
            return "failed: " + str(url)
    else:
        return "failed: image_data"