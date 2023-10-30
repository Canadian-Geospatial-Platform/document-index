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



bucket_name = "webpresence-geocore-geojson-to-parquet-dev"
file_name = "records.parquet"

result = pd.read_parquet("/home/rsaha/projects/document-index/data/" + file_name)
print("Length of dataframe: ", len(result))




# Define data preprocessing functions here for each data type.


def extract_text_from_pdf(pdf_data):
    pdf = PyPDF2.PdfReader(BytesIO(pdf_data))
    text_data = ""
    for page in range(len(pdf.pages)):
        text_data += pdf.pages[page].extract_text()
    print("pdf data")
    print(text_data)
    return text_data


def scrape_data_from_url(url):
    # Check the content type of the URL
    response = requests.get(url)
    if '.tif' not in url:
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


start_row = 8000
end_row = len(result)

# NOTE: 6143 index is of interest, where the process just hangs.

# with open('/home/rsaha/projects/document-index/data/url_text.txt', 'a+') as filehandle:


# Store the files in a dataframe so that it can be stored in a dynamodb table.

all_texts = {
    'uuid':[],
    'text':[]
}

# Only select the uuid and the features_properties_options columns.
result = result[['features_properties_id', 'features_properties_options']]
result = result.iloc[start_row:end_row, :]
# print(result.head())

i = 0
for row in tqdm(result.iterrows()):
    print("Print: ", i)
    i += 1
    # print(row[1]['features_properties_id'])
    all_texts['uuid'].append(row[1]['features_properties_id'])
    try:    
        # print("hello: ", ast.literal_eval(row[1]['features_properties_options']))
        text = scrape_data_from_url(ast.literal_eval(row[1]['features_properties_options'])[0]['url'])
        # print(text)
        all_texts['text'].append(text)
    except:
        all_texts['text'].append(['failed: []'])
        
    
    # all_texts['uuid'] += [row]

df = pd.DataFrame.from_dict(all_texts)
# df.head()
        
df.to_csv(f"/home/rsaha/projects/document-index/data/data_rows_{start_row}_to_{end_row}.csv", index=False)