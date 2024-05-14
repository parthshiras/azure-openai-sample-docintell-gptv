import os
import requests
import json
from fuzzywuzzy import fuzz

#"Filename": "131060100000-378eb94a-22fe-45f5-860c-c89435739685.jpeg",
#    "ArticleName": "SCHNITTLAUCH\r\nCIBOULETTE | ERBA CIPOLLINA\r\n",    20
#    "ArticleNumber": null,                                               50
#    "BarcodeNumber": null,                                               50
#    "PimArticle": {
#      "ArticleNumber": "131060100000",                                   30
#      "ArticleName": {                                                   10
#        "de": "Anna\u0027s Best Schnittlauch",
#        "fr": "Anna\u0027s Best Ciboulette coup\u00E9e",
#        "it": "Anna\u0027s Best Cipollina tagliata"
#      },
#      "Eans": [
#        "7617027632867",
#        "7617027632874"
#      ]
#    }

meta_data = {}
match_threshold = 80

def fuzzy_compare(a, b):
    if a == b or fuzz.ratio(a, b) >= match_threshold:
        return False
    return True

def verify(file_name, pic_data):
    errors = []
    entry = next((entry for entry in meta_data if entry["Filename"] == file_name), None)
    if entry is None:
        errors.append(f"Entry for file {file_name} not found")

    if fuzzy_compare(pic_data["brand"], entry["ArticleName"]):
        errors.append(f"Product Name (brand) mismatch: {pic_data['brand']} != {entry['ArticleName']} (threshold: {fuzz.ratio(pic_data["brand"], entry["ArticleName"])})")

    if fuzzy_compare(pic_data["product_name"], entry["ArticleName"]):
        errors.append(f"Product Name mismatch: {pic_data['product_name']} != {entry['ArticleName']} (threshold: {fuzz.ratio(pic_data["product_name"], entry["ArticleName"])})")

    if pic_data["article_number"] != entry["ArticleNumber"]:
        errors.append(f"Article Number mismatch: {pic_data['article_number']} != {entry['ArticleNumber']}")

    if pic_data["article_number"].replace(".", "") != entry["PimArticle"]["ArticleNumber"]:
        errors.append(f"(Pim) Article Number mismatch: {pic_data['article_number']} != {entry["PimArticle"]["ArticleNumber"]}")

    if pic_data["product_name"] not in entry["PimArticle"]["ArticleName"].values():
        errors.append(f"(Pim) Product Name mismatch: {pic_data['product_name']} not in {entry["PimArticle"]["ArticleName"].values()}")

    if pic_data["bar_code_available"] != (entry["BarcodeNumber"] is not None):  
        errors.append(f"Barcode available mismatch: {pic_data['bar_code_available']} != {entry['BarcodeNumber'] is not None}")

    if pic_data["bar_code_numbers"] != entry["BarcodeNumber"].replace(" ", ""):
        errors.append(f"Barcode Numbers mismatch: {pic_data['bar_code_numbers']} != {entry['BarcodeNumber']}")

    if len(errors) > 0: print(f"Errors for file {file_name}: {','.join(errors)}")

directory = os.path.join('.', 'data')
meta_file_path = os.path.join('.', 'data', 'meta.json')
with open(meta_file_path, 'r') as f:
    meta_data = json.load(f)

files = os.listdir(directory)

for file in files:
    file_path = os.path.join(directory, file)
    
    if os.path.isfile(file_path):
        with open(file_path, 'rb') as f:
            file_data = f.read()
            
            response = requests.post('http://localhost:5000', files={'image': file_data})
        
        if response.status_code == 200:
            response_data = response.json()

            verify(file, response_data)
            
            print(f"File: {file} processed successfully.")
        else:
            print(f"File: {file}, Response Code: {response.status_code} Error: {response.text}")