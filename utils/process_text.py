import requests
from bs4 import BeautifulSoup
# import docx
# import PyPDF2
import io


"""
remove extra white spaces
headers
-> lambda function to be triggered
how big the modules are ?
"""
def fetch_data(url):
    """ Fetch data from URL """
    response = requests.get(url)
    return response.content

# def extract_text_from_docx(docx_content):
#     """ Extract text from a docx file """
#     doc = docx.Document(io.BytesIO(docx_content))
#     return "\n".join([para.text for para in doc.paragraphs])

# def extract_text_from_pdf(pdf_content):
#     """ Extract text from a pdf file """
#     with io.BytesIO(pdf_content) as open_pdf_file:
#         reader = PyPDF2.PdfReader(open_pdf_file)
#         text = []
#         for page in range(len(reader.pages)):
#             text.append(reader.pages[page].extract_text())
#         return "\n".join(text)

def extract_text_from_html(html_content):
    """ Extract text from an HTML file """
    soup = BeautifulSoup(html_content, 'html.parser')
    return soup.get_text()

def extract_text_from_txt(txt_content):
    """ Extract text from a plain text file """
    return txt_content.decode('utf-8')

def save_text_to_file(text, file_type, index):
    filename = f"extracted_text_{index}.{file_type}.txt"
    with open(filename, 'w', encoding='utf-8') as file:
        file.write(text)
    print(f"Saved extracted text to {filename}")
    
def process_url(url,file_type):
    print(f"Fetching and extracting text from: {url}")

    try:
        content = fetch_data(url)
        
        # if file_type == 'pdf':
        #     extracted_text = extract_text_from_pdf(content)
        # elif file_type == 'docx':
        #     extracted_text = extract_text_from_docx(content)
        if file_type == 'text':
            extracted_text = extract_text_from_txt(content)
        elif file_type == 'application/html':
            extracted_text = extract_text_from_html(content)
        else:
            extracted_text = "Unsupported file type"
        
        return extracted_text

    except Exception as e:
        print(f"Failed to process {url}. Error: {e}")


if __name__ == '__main__':
    # Assume the previous functions are already defined

    # List of tuples (url, type)
    urls_to_test = [
        ("https://ftp.maps.canada.ca/pub/nrcan_rncan/elevation/cdem_mnec/doc/CDEM_product_specs.pdf", "pdf"),
        # ("http://files.ontario.ca/datadictionary_go_train_stations.docx", "docx"),
        ("https://maps-cartes.services.geo.ca/server_serveur/rest/services/NRCan/900A_and_top_100_fr/MapServer/1", "html")
    ]

    for index, (url, file_type) in enumerate(urls_to_test):
        print(f"Fetching and extracting text from: {url}")

        try:
            content = fetch_data(url)
            
            if file_type == 'pdf':
                extracted_text = extract_text_from_pdf(content)
            elif file_type == 'docx':
                extracted_text = extract_text_from_docx(content)
            elif file_type == 'text':
                extracted_text = extract_text_from_txt(content)
            elif file_type == 'html':
                extracted_text = extract_text_from_html(content)
            else:
                extracted_text = "Unsupported file type"
            
            save_text_to_file(extracted_text, file_type, index)

        except Exception as e:
            print(f"Failed to process {url}. Error: {e}")

