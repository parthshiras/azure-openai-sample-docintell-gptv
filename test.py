import os
import logging, sys
import argparse
import requests
import json
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from pathlib import Path
import shutil

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
    
    def _store_failed_info(self, check_type, pic_data, entry_data):
        path = Path(f'./results/{check_type}/{self._current_file_name}')
        path.mkdir(parents=True)
        with open(path / 'pic_data.json', 'w') as f:
            json.dump(pic_data, f, indent=4)
        with open(path / 'entry_data.json', 'w') as f:
            json.dump(entry_data, f, indent=4)
        shutil.copy2(self._current_file_path, path)

    def _process_name(self, pic_data, entry_data):
        pic_name = self._get_pic_article_name(pic_data)
        entry_name = entry_data["ArticleName"] or ""
        entry_names = entry_name.splitlines()
        (_, thresh) = process.extractOne(pic_name, entry_names) or (0, 0)

        if pic_name != entry_name and thresh < self.match_threshold:
            self.stats['product_name_mismatch'] = self.stats.get('product_name_mismatch', 0) + 1
            logging.info(f"Product Name mismatch: {pic_name} not in {entry_names} (threshold: {thresh})")
            self._store_failed_info('product_name', pic_data, entry_data)
            self._current_failed = True

            if entry_data["ArticleNumber"] is None:
                self._add_score(15)
        else:
            self._add_score(20)

    def _process_article_number(self, pic_data, entry_data):
        if pic_data["article_number"] != entry_data["ArticleNumber"]:
            self.stats['article_number_mismatch'] = self.stats.get('article_number_mismatch', 0) + 1
            logging.info(f"Article Number mismatch: {pic_data['article_number']} != {entry_data['ArticleNumber']}")
            self._store_failed_info('article_number', pic_data, entry_data)
            self._current_failed = True

            if entry_data["ArticleNumber"] is None:
                self._add_score(35)
        else:
            self._add_score(50)

    def _process_barcode(self, pic_data, entry_data):
        succeeded = True
        if pic_data["bar_code_available"] != (entry_data["BarcodeNumber"] is not None):
            self.stats['barcode_available_mismatch'] = self.stats.get('barcode_available_mismatch', 0) + 1
            logging.info(f"Barcode available mismatch: {pic_data['bar_code_available']} != {entry_data['BarcodeNumber'] is not None}")
            succeeded = False
        else:
            self._add_score(25)

        if (pic_data["bar_code_numbers"] or "") != (entry_data["BarcodeNumber"] or "").replace(" ", ""):
            self.stats['barcode_number_mismatch'] = self.stats.get('barcode_number_mismatch', 0) + 1
            logging.info(f"Barcode Numbers mismatch: {pic_data['bar_code_numbers']} != {entry_data['BarcodeNumber']}")
            succeeded = False
        else:
            self._add_score(25)

        if not succeeded:
            self._store_failed_info('barcode', pic_data, entry_data)
            self._current_failed = True

            if entry_data["BarcodeNumber"] is None:
                self._add_score(25)

    def _process_pim(self, pic_data, entry_data):
        succeeded = True

        pim_article = entry_data["PimArticle"]
        if (pic_data["article_number"] or "").replace(".", "") != (pim_article["ArticleNumber"] or ""):
            self.stats['pim_article_number_mismatch'] = self.stats.get('pim_article_number_mismatch', 0) + 1
            logging.info(f"Pim Article Number mismatch: {pic_data['article_number']} != {pim_article['ArticleNumber']}")
            succeeded = False
        else:
            self._add_score(30)

        pic_name = self._get_pic_article_name(pic_data)
        pim_article_names = (pim_article["ArticleName"] or {}).values()
        (_, thresh) = process.extractOne(pic_name, pim_article_names) or (0, 0)

        if thresh < self.match_threshold:
            self.stats['pim_product_name_mismatch'] = self.stats.get('pim_product_name_mismatch', 0) + 1
            logging.info(f"Pim Product Name mismatch: {pic_name} not in {pim_article_names} (threshold: {thresh})")
            succeeded = False
        else:
            self._add_score(15)

        if not succeeded:
            self._store_failed_info('pim', pic_data, entry_data)
            self._current_failed = True

    def process(self, file_name, full_path, pic_data):
        self.stats['processed'] = self.stats.get('processed', 0) + 1
        self._current_file_name = file_name
        self._current_file_path = full_path
        self._current_failed = False

        entry = next((entry for entry in self.data_set if entry["Filename"] == file_name), None)

        if entry is None:
            self.failed(file_name, pic_data, "No Dataset Entry found")
            logging.warning(f"Entry for file {file_name} not found")
            return

        self._process_name(pic_data, entry)
        self._process_article_number(pic_data, entry)
        self._process_barcode(pic_data, entry)
        self._process_pim(pic_data, entry)

        if not self._current_failed:
            self.stats['succeeded'] = self.stats.get('succeeded', 0) + 1
            self._store_failed_info('succeeded', pic_data, entry)

    def failed(self, file_name, pic_data, response):
        self._current_file_name = file_name
        failed = self.stats.get('failed', {})
        failed[file_name] = response
        self.stats['failed'] = failed
        self._store_failed_info('failed', pic_data, { response: response })

    def _add_score(self, value):
        score = self.stats['score'] = self.stats.get('score', {})
        score[self._current_file_name] = score.get(self._current_file_name, 0) + value
        self.stats['score'] = score

    def print_stats(self):
        logging.info(json.dumps(self.stats, indent=4, default=lambda o: '<not serializable>'))

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
    
    for i, file in enumerate(files):
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
                    rp.process(file, file_path, response_data)
                    
                    logging.info(f"File: {file} ({i}) processed successfully.")
                else:
                    rp.failed(file, {}, response.text)
                    logging.info(f"File: {file}, Response Code: {response.status_code} Error: {response.text}")
        else:
            logging.error(f"File {file} was not a file. Skipping.")

    rp.print_stats()

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    main()



