import os
import logging, sys
import argparse
import requests
import json
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

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

class ResultProcessor:
    def __init__(self, match_threshold) -> None:
        self.stats = {}
        self.match_threshold = match_threshold
        self._load_data_set()

    def _load_data_set(self):
        p = os.path.join('.', 'data', 'meta.json')
        with open(p, 'r') as f:
            self.data_set = json.load(f)

    def _fuzzy_compare(self, a, b):
        if a == b or fuzz.ratio(a, b) >= self.match_threshold:
            return True
        return False
    
    def _get_pic_article_name(self, pic_data):
        return ' '.join(filter(None, (pic_data["brand"], pic_data["product_name"])))
    
    def _process_name(self, pic_data, entry_data):
        pic_name = self._get_pic_article_name(pic_data)
        entry_name = entry_data["ArticleName"] or ""
        entry_name = "|".join(l for l in entry_name.splitlines() if l)

        if pic_name != entry_name and not self._fuzzy_compare(pic_name, entry_name):
            self.stats['product_name_mismatch'] = self.stats.get('product_name_mismatch', 0) + 1
            logging.info(f"Product Name mismatch: {pic_name} != {entry_name} (threshold: {fuzz.ratio(pic_name, entry_name)})")
        else:
            self._add_score(20)

    def _process_article_number(self, pic_data, entry_data):
        if pic_data["article_number"] != entry_data["ArticleNumber"]:
            self.stats['article_number_mismatch'] = self.stats.get('article_number_mismatch', 0) + 1
            logging.info(f"Article Number mismatch: {pic_data['article_number']} != {entry_data['ArticleNumber']}")
        else:
            self._add_score(50)

    def _process_barcode(self, pic_data, entry_data):
        if pic_data["bar_code_available"] != (entry_data["BarcodeNumber"] is not None):
            self.stats['barcode_available_mismatch'] = self.stats.get('barcode_available_mismatch', 0) + 1
            logging.info(f"Barcode available mismatch: {pic_data['bar_code_available']} != {entry_data['BarcodeNumber'] is not None}")
        else:
            self._add_score(25)

        if pic_data["bar_code_numbers"] != (entry_data["BarcodeNumber"] or "").replace(" ", ""):
            self.stats['barcode_number_mismatch'] = self.stats.get('barcode_number_mismatch', 0) + 1
            logging.info(f"Barcode Numbers mismatch: {pic_data['bar_code_numbers']} != {entry_data['BarcodeNumber']}")
        else:
            self._add_score(25)

    def _process_pim(self, pic_data, entry_data):
        pim_article = entry_data["PimArticle"]
        if (pic_data["article_number"] or "").replace(".", "") != (pim_article["ArticleNumber"] or ""):
            self.stats['pim_article_number_mismatch'] = self.stats.get('pim_article_number_mismatch', 0) + 1
            logging.info(f"Pim Article Number mismatch: {pic_data['article_number']} != {pim_article['ArticleNumber']}")
        else:
            self._add_score(30)

        pic_name = self._get_pic_article_name(pic_data)
        pim_article_names = (pim_article["ArticleName"] or {}).values()
        (best, thresh) = process.extractOne(pic_name, pim_article_names)

        if thresh < self.match_threshold:
            self.stats['pim_product_name_mismatch'] = self.stats.get('pim_product_name_mismatch', 0) + 1
            logging.info(f"Pim Product Name mismatch: {pic_name} not in {pim_article_names} (threshold: {thresh})")
        else:
            self._add_score(10)

    def process(self, file_name, pic_data):
        self.stats['processed'] = self.stats.get('processed', 0) + 1
        self._current_file_name = file_name

        entry = next((entry for entry in self.data_set if entry["Filename"] == file_name), None)

        if entry is None:
            self.failed(file_name, "No Dataset Entry found")
            logging.warning(f"Entry for file {file_name} not found")

        self._process_name(pic_data, entry)
        self._process_article_number(pic_data, entry)
        self._process_barcode(pic_data, entry)
        self._process_pim(pic_data, entry)

    def failed(self, file_name, response):
        failed = self.stats.get('failed', {})
        failed[file_name] = response
        self.stats['failed'] = failed

    def _add_score(self, value):
        score = self.stats['score'] = self.stats.get('score', {})
        score[self._current_file_name] = score.get(self._current_file_name, 0) + value
        self.stats['score'] = score

    def print_stats(self):
        print(json.dumps(self.stats, indent=4))

def main():
    parser = argparse.ArgumentParser(description='Test Image Processing')
    parser.add_argument('--directory', type=str, help='The directory containing the images to process', default='data')
    parser.add_argument('--threshold', type=int, help='The threshold for fuzzy matching', default=80)
    parser.add_argument('--max', type=int, help='The maximum number of files to process', default=1000000)
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    rp = ResultProcessor(args.threshold)
    files = os.listdir(args.directory)
    
    i = 0
    for file in files:
        i += 1
        if i > args.max:
            logging.debug(f"Reached maximum number of files to process: {args.max}")
            break

        logging.debug(f'processing file {file}')
        file_path = os.path.join(args.directory, file)
        
        if os.path.isfile(file_path):
            with open(file_path, 'rb') as f:
                file_data = f.read()
                
                response = requests.post('http://localhost:5000', files={'image': file_data})
                
                if response.status_code == 200:
                    response_data = response.json()
                    rp.process(file, response_data)
                    
                    print(f"File: {file} processed successfully.")
                else:
                    rp.failed(file, response)
                    print(f"File: {file}, Response Code: {response.status_code} Error: {response.text}")
        else:
            logging.error(f"File {file} was not a file. Skipping.")

    rp.print_stats()

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    main()



