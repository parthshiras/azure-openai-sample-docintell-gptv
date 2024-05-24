import asyncio
import os
import logging, sys
import argparse
import aiofiles
import aiohttp
import json
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from pathlib import Path
import shutil
import yaml

class Comparitor:
    def __init__(self, config) -> None:
        self.config = config
        self.fields = config.get("fields", {})
        self.field_default = config.get("field_default")
        self.logger = logging.getLogger(__class__.__name__)

    def _join_field_with(self, config):
        return config.get("join_with", " ")
    
    def _field_default(self, config):
        return config.get("default", self.field_default)

    def _get_field_value(self, comparison, config, data):
        log = self.logger.getChild(self._get_field_value.__name__)
        path_seperator = config.get("path_seperator")
        field = config.get("field") or comparison
        value = None
        if path_seperator:
            log.debug(f"Splitting {field} with '{path_seperator}'")
            value = data
            for p in field.split(path_seperator):
                if value is None:
                    value = self._field_default(config)
                    break
                value = value.get(p)
                if value is None:
                    log.debug(f"Path '{p}' not found in {data}")
        elif type(field) is list:
            sep = self._join_field_with(config)
            log.debug(f"Joining {field} with '{sep}'")
            value = sep.join(filter(None, (data.get(f) for f in field)))
        else:
            log.debug(f"Getting '{field}'")
            value = data.get(field) or self._field_default(config)

        if "value_apply" in config:
            f_name = config["value_apply"]
            f = getattr(value, f_name)
            if f is not None:
                log.debug(f"Applying '{f_name}' to '{value}'")
                value = f()

                if "value_apply_list" in config and config.get("value_apply_list"):
                    log.debug(f"Applying list() to '{value}'")
                    value = list(value)

        if config.get("splitlines", False):
            log.debug(f"Splitting lines '{value}'")
            return value.splitlines()
        
        if "remove" in config:
            r = config.get("remove")
            log.debug(f"Removing '{r}' from '{value}'")
            return value.replace(r, "")
        
        return value
    
    def _values_equal(self, input_value, meta_value, config):
        log = self.logger.getChild(self._values_equal.__name__)
        if type(input_value) is list:
            if any(self._values_equal(i, meta_value, config) for i in input_value):
                log.debug(f"Something in {input_value} matched '{meta_value}'")
                return True
            
        if type(meta_value) is list:
            if len(input_value) == 0 and len(meta_value) == 0:
                log.debug(f"input_value and meta_value are both empty")
                return True
            
            if input_value in meta_value:
                log.debug(f"Matched {input_value} in {meta_value}")
                return True
            
            if "match_threshold" in config:
                (best, score) = process.extractOne(input_value, meta_value) or (None, 0)
                log.debug(f"Best match for '{input_value}' in {meta_value} is '{best}' with score {score}")
                return score >= config.get("match_threshold")

            return False
            
        if input_value == meta_value:
            return True
        
        if "match_threshold" in config:
            score = fuzz.ratio(input_value, meta_value)
            log.debug(f"Matched '{input_value}' to '{meta_value}' with {score}")
            return score >= config.get("match_threshold")
        
        return False

    def compare(self, input_data, entry_data):
        log = self.logger.getChild(self.compare.__name__)
        results = { "comparisons": {} }
        for comparison, config in self.fields.items():
            input_field = self._get_field_value(comparison, config.get("input", {}), input_data)
            meta_field = self._get_field_value(comparison, config.get("meta", {}), entry_data)
            if self._values_equal(input_field, meta_field, config):
                log.info(f"Matched {comparison}: '{input_field}' to '{meta_field}'")
                results["comparisons"][comparison] = True
                if "score" in config:
                    score = results.get("score", 0)
                    results["score"] = score + config["score"]
            else:
                log.info(f"Match failed {comparison}: '{input_field}' to '{meta_field}'")
                results["comparisons"][comparison] = False

        return results

class ResultProcessor:
    def __init__(self, comparitor, data_set) -> None:
        self.comparitor = comparitor
        self.data_set = data_set
        self.stats = {}
        self.logger = logging.getLogger(__class__.__name__)

    def _store_info(self, path, check_type, input_data, entry_data):
        new_path = Path(f"./results/{check_type}/{path.name}")
        new_path.mkdir(parents=True)
        with open(new_path / "input_data.json", "w") as f:
            json.dump(input_data, f, indent=4)
        with open(new_path / "entry_data.json", "w") as f:
            json.dump(entry_data, f, indent=4)
        shutil.copy2(path, new_path)

    def process(self, path, input_data):
        log = self.logger.getChild(self.process.__name__)
        log.debug(f"({path}, {input_data})")

        entry = next((entry for entry in self.data_set if entry["Filename"] == path.name), None)

        if entry is None:
            self.failed(path, input_data, "No Dataset Entry found")
            log.warning(f"Entry for file {path.name} not found")
            return
        
        results = self.comparitor.compare(input_data, entry)
        self.stats[path.name] = results.get("score", 0)

        for comparison, result in results["comparisons"].items():
            if not result:
                self._store_info(path, comparison, input_data, entry)

        if all(results["comparisons"].values()):
            self._store_info(path, "succeeded", input_data, entry)

    def failed(self, path, input_data, response):
        log = self.logger.getChild(self.failed.__name__)
        log.debug(f"({path}, {input_data}, {response})")
    
        self._store_info(path, "failed", input_data, {"response": response})

    def print_stats(self):
        log = self.logger.getChild(self.print_stats.__name__)
        log.info(json.dumps(self.stats, indent=4, default=lambda _: "<not serializable>"))

async def post(url, path, session):
    log = logging.getLogger(__name__)
    log.debug(f"processing file {path.name}")
    try:
        if path.is_file():
            form_data = aiohttp.FormData()

            async with aiofiles.open(path, mode='rb') as f:
                file_data = await f.read()
                form_data.add_field('image', file_data, filename='image.jpg', content_type='image/jpeg')

            async with session.post(url, data=form_data) as response:
                if response.status == 200:
                    response_data = await response.json()
                    log.info(f"File: {path.name} processed successfully.")
                    return (True, path, response_data)
                elif response.status == 429:
                    log.info(f"Rate limit reached. File: {path.name}, will retry")
                    return (False, path, "REDO")
                else:
                    text = await response.text()
                    log.error(f"File: {path.name}, Response Code: {response.status} Error: {text}")
                    return (False, path, text)
        else:
            log.error(f"File {path.name} was not a file. Skipping.")
            return (False, path, "File not found.")
    except asyncio.TimeoutError as e:
        log.error(f"Timeout error, retrying {path.name}")
        return (False, path, "REDO")
    except Exception as e:
        log.error(f"Unable to get url {url} due to {e}")
        return (False, path, f'{str(e)}({type(e)})')
    
def process_results(rp, results):
    redo = []

    for (success, path, response) in results:
        if success:
            rp.process(path, response)
        elif response == "REDO":
            redo.append(path)
        else:
            rp.failed(path, {}, response)

    return redo


async def main():
    parser = argparse.ArgumentParser(description="Test Image Processing")
    parser.add_argument("--config", type=str, help="The configuration file to use", default="config.yaml")
    parser.add_argument("--directory", type=str, help="The directory containing the images to process", default="data")
    parser.add_argument("--threshold", type=int, help="The threshold for fuzzy matching", default=80)
    parser.add_argument("--concurrent", type=int, help="The number of concurrent requests to make", default=2)
    parser.add_argument("--max", type=int, help="The maximum number of files to process", default=0)
    parser.add_argument("--retries", type=int, help="Amount of times to retry rate limited files", default=10)
    parser.add_argument("--meta", type=str, help="The meta data file containing the expected results", default="data/meta.json")
    parser.add_argument('-v', '--verbose', action='count', help="Increase logging level", default=0)
    args = parser.parse_args()

    if args.verbose == 1:
        logging.getLogger().setLevel(logging.INFO)
    elif args.verbose > 1:
        logging.getLogger().setLevel(logging.DEBUG)

    # Are we running in the debugger?
    if getattr(sys, 'gettrace', lambda: None)() is not None:
        logging.getLogger().setLevel(logging.DEBUG)
        args.max = 50

    if not os.path.isfile(args.config):
        parser.error(f"Config file {args.config} not found.")

    # glob pattern to match jpg, jpeg, png, JPG, JPEG, PNG files
    paths = list(Path(args.directory).glob('*.[jpJP][npNP]*[gG$]'))
    if args.max > 0:
        paths = paths[:args.max]
    logging.debug(f"Processing {len(paths)} files.")

    conn = aiohttp.TCPConnector(limit=args.concurrent)
    # set total=None because the POST is really slow and the defeault will cause any request still waiting to be processed after "total" seconds to fail.  Also set read to 10 minutes
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=600)

    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        results = await asyncio.gather(*(post("http://localhost:5000", path, session) for path in paths))
        logging.info("Finalized all. Return is a list of len {} outputs.".format(len(results)))

        config = {}
        async with aiofiles.open(args.config, mode='r') as f:
            config = yaml.safe_load(await f.read()) or {}
            
        logging.debug(f"Config: {json.dumps(config, indent=4, default=lambda o: "<not serializable>")}")
        comparitor = Comparitor(config)

        meta_data = {}
        
        async with aiofiles.open(args.meta, mode='r') as f:
            meta_data = json.loads(await f.read())

        rp = ResultProcessor(comparitor, meta_data)

        redo = process_results(rp, results)
        retries = args.retries

        while redo and retries > 0:
            logging.info(f"Retrying {len(redo)} files.")
            results = await asyncio.gather(*(post("http://localhost:5000", path, session) for path in redo))
            redo = process_results(rp, results)
            retries -= 1

        rp.print_stats()


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stderr, level=logging.WARN)
    asyncio.run(main())
