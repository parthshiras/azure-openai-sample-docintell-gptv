import os
import logging
import sys
import argparse
import requests
import json
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from pathlib import Path
import shutil
from queue import Queue
import threading

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
        return a == b or fuzz.ratio(a, b) >= self.match_threshold
    
    def _get_pic_article_name(self, pic_data):
        return ' '.join(filter(None, (pic_data.get("brand"), pic_data.get("product_name"))))
    
    def _store_failed_info(self, check_type, pic_data, entry_data):
        path = Path(f'./results/{check_type}/{self._current_file_name}')
        path.mkdir(parents=True, exist_ok=True)
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
        else:
            self._add_score(20)

    def _process_article_number(self, pic_data, entry_data):
        if pic_data["article_number"] != entry_data["ArticleNumber"]:
            self.stats['article_number_mismatch'] = self.stats.get('article_number_mismatch', 0) + 1
            logging.info(f"Article Number mismatch: {pic_data['article_number']} != {entry_data['ArticleNumber']}")
            self._store_failed_info('article_number', pic_data, entry_data)
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
            self._add_score(10)

        if not succeeded:
            self._store_failed_info('pim', pic_data, entry_data)

    def process(self, file_name, full_path, pic_data):
        self.stats['processed'] = self.stats.get('processed', 0) + 1
        self._current_file_name = file_name
        self._current_file_path = full_path

        entry = next((entry for entry in self.data_set if entry["Filename"] == file_name), None)

        if entry is None:
            self.failed(file_name, "No Dataset Entry found")
            logging.warning(f"Entry for file {file_name} not found")
            return

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

    @staticmethod
    def merge_stats(stats_list):
        merged_stats = {}
        for stats in stats_list:
            for key, value in stats.items():
                if key not in merged_stats:
                    merged_stats[key] = value
                elif isinstance(value, dict):
                    if not isinstance(merged_stats[key], dict):
                        merged_stats[key] = value
                    else:
                        for sub_key, sub_value in value.items():
                            merged_stats[key][sub_key] = merged_stats[key].get(sub_key, 0) + sub_value
                else:
                    merged_stats[key] += value
        return merged_stats


def worker(input_queue, output_queue, match_threshold, api_url):
    result_processor = ResultProcessor(match_threshold)
    while True:
        file_name, file_path = input_queue.get()
        if file_name is None:
            break
        with open(file_path, 'rb') as f:
            file_data = f.read()
            response = requests.post(api_url, files={'image': file_data})
            if response.status_code == 200:
                response_data = response.json()
                result_processor.process(file_name, file_path, response_data)
                output_queue.put((file_name, 'success'))
            else:
                result_processor.failed(file_name, response.text)
                output_queue.put((file_name, f'Error: {response.status_code}'))
        input_queue.task_done()
    output_queue.put(('stats', result_processor.stats))


def main():
    parser = argparse.ArgumentParser(description='Test Image Processing')
    parser.add_argument('--directory', type=str, help='The directory containing the images to process', default='data')
    parser.add_argument('--threshold', type=int, help='The threshold for fuzzy matching', default=80)
    parser.add_argument('--max', type=int, help='The maximum number of files to process', default=1000000)
    parser.add_argument('--parallel', type=int, help='Number of parallel threads', default=1)
    parser.add_argument('--api-url', type=str, help='API URL to post the image data', default='http://localhost:5000')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    files = os.listdir(args.directory)
    
    input_queue = Queue()
    output_queue = Queue()

    for i, file in enumerate(files):
        if i >= args.max:
            logging.debug(f"Reached maximum number of files to process: {args.max}")
            break
        file_path = os.path.join(args.directory, file)
        if os.path.isfile(file_path):
            input_queue.put((file, file_path))
    
    threads = []
    for _ in range(args.parallel):
        t = threading.Thread(target=worker, args=(input_queue, output_queue, args.threshold, args.api_url))
        t.start()
        threads.append(t)

    for _ in threads:
        input_queue.put((None, None))

    for t in threads:
        t.join()

    all_stats = []
    while not output_queue.empty():
        item = output_queue.get()
        if item[0] == 'stats':
            all_stats.append(item[1])

    merged_stats = ResultProcessor.merge_stats(all_stats)
    print(json.dumps(merged_stats, indent=4))


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    main()
