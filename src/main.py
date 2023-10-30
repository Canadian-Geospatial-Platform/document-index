# Script to create the dynamodb table from the dataframe.

import sys
sys.path.append("/home/ec2-user/environment/document-index/utils/")

# The dynamodb file contains the functions for creating, writing into a dynamodb table.
from dynamodb import *

import requests
import sys
import json
import pandas as pd
import io
import boto3
import ast
import mimetypes


s3 = boto3.resource('s3')
obj = s3.Object("webpresence-geocore-geojson-to-parquet-dev", "records.parquet")
df = pd.read_parquet(io.BytesIO(obj.get()['Body'].read()))
# df = df.iloc[6974:]
# print(df.columns)
# print(df.iloc[0]['features_properties_id'])
# print(type(df.iloc[6974]['features_properties_title_en']))



# print(ast.literal_eval(df.loc[4, 'features_properties_options'])[0])
# 32a2842e-491d-744a-bfbf-b33f4a4a2e1c
# print(df[df['features_properties_id']=='32a2842e-491d-744a-bfbf-b33f4a4a2e1c']['features_properties_options'])

# # url = "https://upload.wikimedia.org/wikipedia/commons/b/b6/Image_created_with_a_mobile_phone.png"
# url = "https://web.stanford.edu/~jurafsky/slp3/3.pdf"
# response = requests.get(url)
# print(response.headers.get('content-type'))


# mimetype,encoding = mimetypes.guess_type(url)
# print(mimetype)
# print(mimetype.startswith('image'))
# The following section contains code to create a dynamodb table with key, value pairs.

# Create a table.
table_name = "docinx"

# table = create_table(table_name)

# # # Populate the table with the features_properties_options json. (for each uuid).

ranges = [(7000, len(df))] # Exclusive second argument.
#, (1801, 4000), (4000, 6000), (6000, 9000), (9000, len(df))]

for start_row, end_row in ranges:
    print("start ",start_row)
    print("end ", end_row)
    df_temp = df.iloc[start_row:end_row, :]
    batch_write_items_into_table(df_temp, table_name)