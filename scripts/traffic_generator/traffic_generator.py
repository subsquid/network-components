import json
import logging
import os
import random
import requests
import sys
import time

from collections import Counter
from concurrent import futures
from pathlib import Path
from pydantic import BaseModel, AnyUrl, TypeAdapter, ValidationError, UrlConstraints
from typing import List, Dict, Annotated, Tuple, Any, Optional, NewType

NUM_THREADS = int(os.environ.get('NUM_THREADS', 10))
QUERY_TIMEOUT_SEC = int(os.environ.get('QUERY_TIMEOUT_SEC', 60))
MIN_INTERVAL_SEC = float(os.environ.get('MIN_INTERVAL_SEC', 60))
MAX_BLOCK_RANGE = int(os.environ.get('MAX_BLOCK_RANGE', 1_000_000))

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
        self._worker_timeouts: Counter[WorkerId] = Counter()

    def run(self):
        logging.info("Starting traffic generation")
        while True:
            try:
                start = time.time()
                self._query_workers()
                time_to_wait = MIN_INTERVAL_SEC - (time.time() - start)
                if time_to_wait > 0:
                    time.sleep(time_to_wait)
            except KeyboardInterrupt:
                print("Shutting down")
                self._executor.shutdown(wait=False, cancel_futures=True)
                break

    def _query_workers(self):
        workers = self._get_workers()
        if not workers:
            logging.info("No workers to query")
            return

        logging.info(f"Querying {len(workers)} workers")
        results = Counter()
        try:
            tasks = self._executor.map(self._query_worker, workers.keys(), workers.values())
            for worker_id, res in zip(workers.keys(), tasks):
                if res == 504:
                    self._worker_timeouts[worker_id] += 1
                results[res] += 1
        except futures.TimeoutError:
            logging.error("Querying workers timed out")
        logging.info(f"All queries finished. Results: {results}")

        logging.debug("Total timeouts per worker:")
        for w, c in self._worker_timeouts.items():
            logging.debug(f"{w}: {c}")

    def _get_workers(self) -> WorkersDict:
        try:
            logging.info("Getting active workers")
            workers: WorkersDict = {}
            for dataset in self._config.datasets:
                response = requests.get(f'{PORTAL_URL}/datasets/{dataset.name}/state')
                if response.ok:
                    dataset_state = DatasetState.model_validate_json(response.content)
                    for worker_id, ranges in dataset_state.worker_ranges.items():
                        if ranges.ranges:
                            workers.setdefault(worker_id, {})[dataset.id] = ranges.ranges
                else:
                    logging.warning(f"Couldn't get chunks of {dataset.name}, skipping")
            return self._filter_workers(workers)
        except (requests.HTTPError, ValidationError) as e:
            logging.error(f"Error getting workers: {e}")
            return {}

    def _filter_workers(self, workers: WorkersDict) -> WorkersDict:
        if self._config.worker_whitelist is not None:
            workers = {w: s for w, s in workers.items() if w in self._config.worker_whitelist}
        elif self._config.worker_blacklist is not None:
            workers = {w: s for w, s in workers.items() if w not in self._config.worker_blacklist}
        return workers

    def _query_worker(self, worker_id: WorkerId, state: WorkerState) -> Optional[int]:
        stored_datasets = [
            (dataset.template_dir, dataset.id, dataset.name) for dataset in self._config.datasets
            if dataset.id in state
        ]
        if not stored_datasets:
            logging.warning(f"Worker {worker_id} has no datasets to query")
            return None

        template, dataset_id, dataset_name = random.choice(stored_datasets)
        query_url = f'{PORTAL_URL}/query/{dataset_id}/{worker_id}?timeout={QUERY_TIMEOUT_SEC}s'
        query = random.choice(self._query_templates[template])
        query['fromBlock'], query['toBlock'] = random_range(state[dataset_id])

        logging.debug(
            f"Sending query worker_id={worker_id} query_url={query_url} "
            f"from={query['fromBlock']} to={query['toBlock']}")
        try:
          response = requests.post(query_url, json=query, stream=True, timeout=20)  # stream=True to discard response body
        except:
            logging.info(f"Query timed out. worker_id={worker_id}")
            return 499


        if response.status_code != 200:
            logging.info(
                f"Query failed. worker_id={worker_id} status={response.status_code} "
                f"msg='{response.text}' elapsed={response.elapsed}")
        else:
            logging.info(f"Query succeeded. worker_id={worker_id} elapsed={response.elapsed}")
        return response.status_code


def random_range(ranges: List[BlockRange]) -> Tuple[int, int]:
    r = random.choice(ranges)
    from_block = random.randint(r.begin, r.end)
    to_block = min(random.randint(from_block, r.end), from_block + MAX_BLOCK_RANGE)
    return from_block, to_block


def read_templates() -> Dict[str, List[Any]]:
    result = {}
    for dataset in TEMPLATES_DIR.iterdir():
        templates_dir = TEMPLATES_DIR / dataset
        templates = []
        for template in templates_dir.glob('**/*.json'):
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
