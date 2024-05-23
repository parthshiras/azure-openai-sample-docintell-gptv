import asyncio
import os
import logging, sys
import argparse
import traceback
import aiohttp
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
        p = os.path.join(".", "data", "meta.json")
        with open(p, "r") as f:
            self.data_set = json.load(f)

    def _fuzzy_compare(self, a, b):
        a == b or fuzz.ratio(a, b) >= self.match_threshold

    def _get_pic_article_name(self, pic_data):
        return " ".join(filter(None, (pic_data["brand"], pic_data["product_name"])))

    def _store_failed_info(self, check_type, pic_data, entry_data):
        path = Path(f"./results/{check_type}/{self._current_file_name}")
        path.mkdir(parents=True)
        with open(path / "pic_data.json", "w") as f:
            json.dump(pic_data, f, indent=4)
        with open(path / "entry_data.json", "w") as f:
            json.dump(entry_data, f, indent=4)
        shutil.copy2(self._current_file_path, path)

    def _process_name(self, pic_data, entry_data):
        pic_name = self._get_pic_article_name(pic_data)
        entry_name = entry_data["ArticleName"] or ""
        entry_names = entry_name.splitlines()
        (_, thresh) = process.extractOne(pic_name, entry_names) or (0, 0)

        if pic_name != entry_name and thresh < self.match_threshold:
            self.stats["product_name_mismatch"] = (
                self.stats.get("product_name_mismatch", 0) + 1
            )
            logging.info(
                f"Product Name mismatch: {pic_name} not in {entry_names} (threshold: {thresh})"
            )
            self._store_failed_info("product_name", pic_data, entry_data)
            self._current_failed = True

            if entry_data["ArticleNumber"] is None:
                self._add_score(15)
        else:
            self._add_score(20)

    def _process_article_number(self, pic_data, entry_data):
        if pic_data["article_number"] != entry_data["ArticleNumber"]:
            self.stats["article_number_mismatch"] = (
                self.stats.get("article_number_mismatch", 0) + 1
            )
            logging.info(
                f"Article Number mismatch: {pic_data['article_number']} != {entry_data['ArticleNumber']}"
            )
            self._store_failed_info("article_number", pic_data, entry_data)
            self._current_failed = True

            if entry_data["ArticleNumber"] is None:
                self._add_score(35)
        else:
            self._add_score(50)

    def _process_barcode(self, pic_data, entry_data):
        succeeded = True
        if pic_data["bar_code_available"] != (entry_data["BarcodeNumber"] is not None):
            self.stats["barcode_available_mismatch"] = (
                self.stats.get("barcode_available_mismatch", 0) + 1
            )
            logging.info(
                f"Barcode available mismatch: {pic_data['bar_code_available']} != {entry_data['BarcodeNumber'] is not None}"
            )
            succeeded = False
        else:
            self._add_score(25)

        if (pic_data["bar_code_numbers"] or "") != (
            entry_data["BarcodeNumber"] or ""
        ).replace(" ", ""):
            self.stats["barcode_number_mismatch"] = (
                self.stats.get("barcode_number_mismatch", 0) + 1
            )
            logging.info(
                f"Barcode Numbers mismatch: {pic_data['bar_code_numbers']} != {entry_data['BarcodeNumber']}"
            )
            succeeded = False
        else:
            self._add_score(25)

        if not succeeded:
            self._store_failed_info("barcode", pic_data, entry_data)
            self._current_failed = True

            if entry_data["BarcodeNumber"] is None:
                self._add_score(25)

    def _process_pim(self, pic_data, entry_data):
        succeeded = True

        pim_article = entry_data["PimArticle"]
        if (pic_data["article_number"] or "").replace(".", "") != (
            pim_article["ArticleNumber"] or ""
        ):
            self.stats["pim_article_number_mismatch"] = (
                self.stats.get("pim_article_number_mismatch", 0) + 1
            )
            logging.info(
                f"Pim Article Number mismatch: {pic_data['article_number']} != {pim_article['ArticleNumber']}"
            )
            succeeded = False
        else:
            self._add_score(30)

        pic_name = self._get_pic_article_name(pic_data)
        pim_article_names = (pim_article["ArticleName"] or {}).values()
        (_, thresh) = process.extractOne(pic_name, pim_article_names) or (0, 0)

        if thresh < self.match_threshold:
            self.stats["pim_product_name_mismatch"] = (
                self.stats.get("pim_product_name_mismatch", 0) + 1
            )
            logging.info(
                f"Pim Product Name mismatch: {pic_name} not in {pim_article_names} (threshold: {thresh})"
            )
            succeeded = False
        else:
            self._add_score(15)

        if not succeeded:
            self._store_failed_info("pim", pic_data, entry_data)
            self._current_failed = True

    def process(self, file_name, full_path, pic_data):
        logging.debug(f"rp.process({file_name}, {full_path}, {pic_data})")

        self.stats["processed"] = self.stats.get("processed", 0) + 1
        self._current_file_name = file_name
        self._current_file_path = full_path
        self._current_failed = False

        entry = next(
            (entry for entry in self.data_set if entry["Filename"] == file_name), None
        )

        if entry is None:
            self.failed(file_name, pic_data, "No Dataset Entry found")
            logging.warning(f"Entry for file {file_name} not found")
            return

        self._process_name(pic_data, entry)
        self._process_article_number(pic_data, entry)
        self._process_barcode(pic_data, entry)
        self._process_pim(pic_data, entry)

        if not self._current_failed:
            self.stats["succeeded"] = self.stats.get("succeeded", 0) + 1
            self._store_failed_info("succeeded", pic_data, entry)

    def failed(self, file_name, pic_data, response):
        logging.debug(f"rp.failed({file_name}, {pic_data}, {response})")
        self._current_file_name = file_name
        failed = self.stats.get("failed", {})
        failed[file_name] = response
        self.stats["failed"] = failed
        self._store_failed_info("failed", pic_data, {response: response})

    def _add_score(self, value):
        score = self.stats["score"] = self.stats.get("score", {})
        score[self._current_file_name] = score.get(self._current_file_name, 0) + value
        self.stats["score"] = score

    def print_stats(self):
        logging.info(
            json.dumps(self.stats, indent=4, default=lambda o: "<not serializable>")
        )

async def post(url, file, file_path, session):
    logging.debug(f"processing file {file_path}")
    try:
        if os.path.isfile(file_path):
            with open(file_path, "rb") as f:
                file_data = f.read()
                data = aiohttp.FormData()
                data.add_field('image', file_data, filename='image.jpg', content_type='image/jpeg')

                async with session.post(url, data=data) as response:
                    if response.status == 200:
                        response_data = await response.json()
                        logging.info(f"File: {file_path} processed successfully.")
                        return (True, file, file_path, response_data)
                    elif response.status == 429:
                        logging.info(f"Rate limit reached. File: {file}, will retry")
                        return (False, file, file_path, "REDO")
                    else:
                        text = await response.text()
                        logging.error(f"File: {file_path}, Response Code: {response.status} Error: {text}")
                        return (False, file, file_path, text)
        else:
            logging.error(f"File {file_path} was not a file. Skipping.")
            return (False, file, file_path, "File not found.")
    except asyncio.TimeoutError as e:
        logging.error(f"Timeout error, retrying {file}")
        return (False, file, file_path, "REDO")
    except Exception as e:
        logging.error(f"Unable to get url {url} due to {e}")
        return (False, file, file_path, f'{str(e)}({type(e)})')
    
def process_results(rp, results):
    redo = []

    for (success, file, file_path, response) in results:
        if success:
            rp.process(file, file_path, response)
        elif response == "REDO":
            redo.append((file, file_path))
        else:
            rp.failed(file, {}, response)

    return redo


async def main():
    parser = argparse.ArgumentParser(description="Test Image Processing")
    parser.add_argument("--directory", type=str, help="The directory containing the images to process", default="data")
    parser.add_argument("--threshold", type=int, help="The threshold for fuzzy matching", default=80)
    parser.add_argument("--concurrent", type=int, help="The number of concurrent requests to make", default=1)
    parser.add_argument("--max", type=int, help="The maximum number of files to process", default=0)
    parser.add_argument("--retries", type=int, help="Amount of times to retry rate limited files", default=3)
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    files = os.listdir(args.directory)
    files = list(filter(lambda f: f != 'meta.json', files))
    if args.max > 0:
        files = files[:args.max]
    logging.debug(f"Processing {len(files)} files.")

    conn = aiohttp.TCPConnector(limit=args.concurrent)

    async with aiohttp.ClientSession(connector=conn) as session:
        results = await asyncio.gather(*(post("http://localhost:5000", file, os.path.join(args.directory, file), session) for file in files))
        logging.info("Finalized all. Return is a list of len {} outputs.".format(len(results)))

        rp = ResultProcessor(args.threshold)

        redo = process_results(rp, results)
        retries = args.retries

        while redo and retries > 0:
            logging.info(f"Retrying {len(redo)} files.")
            results = await asyncio.gather(*(post("http://localhost:5000", file, file_path, session) for (file, file_path) in redo))
            redo = process_results(rp, results)
            retries -= 1

        rp.print_stats()


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    asyncio.run(main())
