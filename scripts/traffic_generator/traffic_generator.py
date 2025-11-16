import json
import logging
import os
import random
import requests
import sys
import time

import numpy as np

from collections import Counter
from concurrent import futures
from pathlib import Path
from pydantic import BaseModel, AnyUrl, TypeAdapter, ValidationError, UrlConstraints
from typing import List, Dict, Annotated, Tuple, Any, Optional, NewType
from tqdm import tqdm

NUM_THREADS = int(os.environ.get('NUM_THREADS', 10))
NUM_QUERIES = int(os.environ.get('NUM_QUERIES', 10))
NUM_CHUNKS_PER_QUERY = int(os.environ.get('NUM_CHUNKS_PER_QUERY', 100))
QUERY_TIMEOUT_SEC = int(os.environ.get('QUERY_TIMEOUT_SEC', 20))
MIN_INTERVAL_SEC = float(os.environ.get('MIN_INTERVAL_SEC', 60))
MAX_BLOCK_RANGE = int(os.environ.get('MAX_BLOCK_RANGE', 1_000_000))
ADD_SLOW_TEMPLATES = bool(os.environ.get('ADD_SLOW_TEMPLATES', False))

PORTAL_URL = os.environ.get('PORTAL_URL', "https://portal.sqd.dev")

LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()

TEMPLATES_DIR = Path(__file__).parent / 'query_templates'

logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

DatasetId = NewType('DatasetId', str)
WorkerId = NewType('WorkerId', str)


class Dataset(BaseModel):
    id: DatasetId
    name: str
    template_dir: str


class Config(BaseModel):
    datasets: List[Dataset]
    worker_whitelist: Optional[List[WorkerId]] = None
    worker_blacklist: Optional[List[WorkerId]] = None


class BlockRange(BaseModel):
    begin: int
    end: int

    def __hash__(self):
        return self.begin


class DatasetRanges(BaseModel):
    ranges: List[BlockRange]


class DatasetState(BaseModel):
    worker_ranges: Dict[WorkerId, DatasetRanges]


WorkerState = Dict[DatasetId, List[BlockRange]]
WorkersDict = Dict[WorkerId, WorkerState]


def strip_prefix(s: str, prefix: str) -> str:
    if s.startswith(prefix):
        return s[len(prefix):]
    return s


class TrafficGenerator:
    def __init__(self, config: Config):
        self._config = config
        self._executor = futures.ThreadPoolExecutor(max_workers=NUM_THREADS)
        self._query_templates: Dict[str, List[Any]] = read_templates()

    def run(self):
        logging.info("Starting traffic generation")
        worker_p = {}
        while True:
            try:
                start = time.time()
                self._query_workers(worker_p)
                time_to_wait = MIN_INTERVAL_SEC - (time.time() - start)
                if time_to_wait > 0:
                    #pass
                    time.sleep(time_to_wait)
            except KeyboardInterrupt:
                print("Shutting down")
                self._executor.shutdown(wait=False, cancel_futures=True)
                break

    def _query_workers(self, worker_p):
        datasets = self._get_datasets()
        if not datasets:
            logging.info("No datasets to query")
            return

        fn = lambda x: self._query_stream(datasets, x)
        samples = range(NUM_QUERIES)
        results = Counter()
        try:
            tasks = self._executor.map(fn, samples)
            for worker_id, task in zip(samples, tasks):
                res, dataset_id, block_begin = task
                results[res] += 1
        except futures.TimeoutError:
            logging.error("Querying workers timed out")
        logging.info(f"All queries finished. Results: {results}")

    def _pull_dataset(self, dataset) -> List[DatasetRanges]:
        dataset_name = dataset.name
        dataset_ranges = []
        try:
            response = requests.get(f'{PORTAL_URL}/datasets/{dataset_name}/state')
            if response.ok:
                dataset_state = DatasetState.model_validate_json(response.content)
                for worker_id, ranges in dataset_state.worker_ranges.items():
                    if ranges.ranges:
                        dataset_ranges.extend(ranges.ranges)
            else:
                logging.warning(f"Couldn't get chunks of {dataset_name}, skipping")
        except (requests.HTTPError, ValidationError) as e:
            logging.error(f"Error getting datasets: {e}")
        return sorted(set(dataset_ranges), key = lambda x: x.begin)

    def _get_datasets(self) -> WorkersDict:
        datasets = {}
        try:
            results = self._executor.map(self._pull_dataset, self._config.datasets)
            for dataset, ranges in zip(self._config.datasets, results):
                if len(ranges) == 0:
                    continue
                datasets[dataset.id] = ranges
        except futures.TimeoutError:
            logging.error("Querying datasets timed out")
        return datasets

    def _filter_workers(self, workers: WorkersDict) -> WorkersDict:
        if self._config.worker_whitelist is not None:
            workers = {w: s for w, s in workers.items() if w in self._config.worker_whitelist}
        elif self._config.worker_blacklist is not None:
            workers = {w: s for w, s in workers.items() if w not in self._config.worker_blacklist}
        return workers

    def _do_request(self, query_url, query) -> Tuple[int, int, int]:
        logging.debug(
            f"Sending stream query_url={query_url}"
            f"from={query['fromBlock']} to={query['toBlock']}")
        try:
            response = requests.post(query_url, json=query, stream=True, timeout=QUERY_TIMEOUT_SEC)
            cnt = 0
            number = -1
            if response.ok:
                for line in response.iter_lines():
                    if len(line) == 0:
                        continue
                    cnt += 1
                    number = json.loads(line).get("header", {}).get("number", -1)
            return response.status_code, cnt, number
        except:
            logging.info(f"Query timed out. query_url={query_url}")
            return 499, 0, -1

    def _query_stream(self, datasets, x) -> Optional[Tuple[int, str, int]]:
        stored_datasets = [
            (dataset.template_dir, dataset.id, dataset.name) for dataset in self._config.datasets
            if dataset.id in datasets.keys()
        ]
        if not stored_datasets:
            logging.warning(f"Worker {worker_id} has no datasets to query")
            return None

        template, dataset_id, dataset_name = random.choice(stored_datasets)
        query_url = f'{PORTAL_URL}/datasets/{dataset_name}/finalized-stream'
        query = random.choice(self._query_templates[template])
        query_id = self._query_templates[template].index(query)
        query['fromBlock'], query['toBlock'], block_start = random_range(datasets[dataset_id], chunks = NUM_CHUNKS_PER_QUERY)

        logging.debug(
            f"Sending stream query_url={query_url}"
            f"from={query['fromBlock']} to={query['toBlock']}")
        for attempt in range(10):
            code, ret_count, last_block = self._do_request(query_url, query)
            logging.debug(f"attempt = {attempt} code = {code} last = {last_block}")
            if ret_count == 0:
                break
            if last_block == query['toBlock']:
                break
            if last_block == -1:
                break
            query['fromBlock'] = last_block + 1

        logging.info(f"from={query['fromBlock']} to={query['toBlock']} ret={ret_count} last_block={last_block}")
        return code, dataset_id, block_start

def random_range(ranges: List[BlockRange], chunks: int) -> Tuple[int, int]:
    first_chunk_idx = random.randint(0, len(ranges) - chunks - 1)
    first_chunk = ranges[first_chunk_idx]
    last_chunk = ranges[first_chunk_idx + chunks]
    from_block = random.randint(first_chunk.begin, first_chunk.end)
    to_block = random.randint(last_chunk.begin, last_chunk.end)
    return from_block, to_block, first_chunk.begin


def read_templates() -> Dict[str, List[Any]]:
    result = {}
    for dataset in TEMPLATES_DIR.iterdir():
        templates_dir = TEMPLATES_DIR / dataset
        templates = []
        for template in templates_dir.glob('**/*.json'):
            if str(template).find("slow") > -1 or ADD_SLOW_TEMPLATES:
                continue
            logging.info(f"Loading template {template}")
            try:
                templates.append(json.loads(template.read_text()))
            except (IOError, json.JSONDecodeError) as e:
                logging.error(f"Template {template} invalid: {e}")
        result[dataset.name] = templates
    return result


def main(config_path: str = 'config.json'):
    try:
        logging.info(f"Reading config from {config_path}")
        config_json = Path(config_path).read_text()
        config = Config.model_validate_json(config_json)
        logging.info(
            f"Config loaded datasets={[str(dataset.name) for dataset in config.datasets]}")
    except (IOError, ValidationError) as e:
        logging.error(f"Error reading config: {e}")
        exit(1)

    TrafficGenerator(config).run()


if __name__ == '__main__':
    if len(sys.argv) > 2:
        logging.error(f"Usage: {sys.argv[0]} [<CONFIG_PATH>]")
        exit(1)
    main(*sys.argv[1:])
