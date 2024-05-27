import json
import logging
import os
import random
import requests
import sys
import time

from collections import Counter
from concurrent import futures
from packaging.version import InvalidVersion, Version
from pathlib import Path
from pydantic import BaseModel, AnyUrl, TypeAdapter, ValidationError, ConfigDict, UrlConstraints
from typing import List, Dict, Annotated, Tuple, Any, Optional

NUM_THREADS = int(os.environ.get('NUM_THREADS', 10))
QUERY_TIMEOUT_SEC = int(os.environ.get('QUERY_TIMEOUT_SEC', 60))
MIN_INTERVAL_SEC = float(os.environ.get('MIN_INTERVAL_SEC', 60))
SKIP_GREYLISTED = os.environ.get('SKIP_GREYLISTED', '').lower() in ('1', 't', 'true', 'y', 'yes')
MIN_WORKER_VERSION = Version(os.environ.get('MIN_WORKER_VERSION', '1.0.0-rc3'))

GATEWAY_URL = os.environ.get('GATEWAY_URL', "http://localhost:8000")
SCHEDULER_URL = os.environ.get('WORKERS_URL', "https://scheduler.testnet.subsquid.io")

LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()

TEMPLATES_DIR = Path(__file__).parent / 'query_templates'

logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

S3Url = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["s3"])]


class Dataset(BaseModel):
    id: str
    url: S3Url


class Config(BaseModel):
    datasets: Dict[str, Dataset]


class BlockRange(BaseModel):
    begin: int
    end: int


class DatasetRanges(BaseModel):
    ranges: List[BlockRange]


class Worker(BaseModel):
    peer_id: str
    version: str
    stored_ranges: Dict[S3Url, DatasetRanges]

    model_config = ConfigDict(extra='allow')

    def get_version(self) -> Version:
        try:
            return Version(self.version)
        except InvalidVersion:
            return Version("0.0.0")


worker_list = TypeAdapter(List[Worker])


class TrafficGenerator:
    def __init__(self, config: Config):
        self._config = config
        self._executor = futures.ThreadPoolExecutor(max_workers=NUM_THREADS)
        self._query_templates: Dict[str, Any] = {
            dataset: read_templates(dataset) for dataset in config.datasets
        }
        self._worker_timeouts = Counter()

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
            tasks = self._executor.map(self._query_worker, workers)
            for worker, res in zip(workers, tasks):
                if res == 504:
                    self._worker_timeouts[worker.peer_id] += 1
                results[res] += 1
        except futures.TimeoutError:
            logging.error("Querying workers timed out")
        logging.info(f"All queries finished. Results: {results}")

        logging.debug("Total timeouts per worker:")
        for w, c in self._worker_timeouts.items():
            logging.debug(f"{w}: {c}")

    def _get_workers(self) -> List[Worker]:
        try:
            logging.info("Getting active workers")
            response = requests.get(f'{SCHEDULER_URL}/workers/pings')
            response.raise_for_status()
            workers = worker_list.validate_json(response.content)
            return self._filter_workers(workers)
        except (requests.HTTPError, ValidationError) as e:
            logging.error(f"Error getting workers: {e}")
            return []

    @staticmethod
    def _filter_workers(workers: List[Worker]) -> List[Worker]:
        if SKIP_GREYLISTED:
            response = requests.get(f'{GATEWAY_URL}/workers/greylisted')
            response.raise_for_status()
            greylisted_ids = set(response.json())
            logging.info(f"Omitting {len(greylisted_ids)} grey-listed workers")
        else:
            greylisted_ids = set()

        return [
            w for w in workers
            if w.get_version() >= MIN_WORKER_VERSION
               and w.peer_id not in greylisted_ids
        ]

    def _query_worker(self, worker: Worker) -> Optional[int]:
        worker_id = worker.peer_id
        stored_datasets = [
            (name, details.id, details.url) for name, details in self._config.datasets.items()
            if details.url in worker.stored_ranges
        ]
        if not stored_datasets:
            logging.warning(f"Worker {worker_id} has no datasets to query")
            return None

        dataset, dataset_id, dataset_url = random.choice(stored_datasets)
        query_url = f'{GATEWAY_URL}/query/{dataset_id}/{worker_id}?timeout={QUERY_TIMEOUT_SEC}s'
        query = random.choice(self._query_templates[dataset])
        query['fromBlock'], query['toBlock'] = random_range(worker.stored_ranges[dataset_url])

        logging.debug(
            f"Sending query worker_id={worker_id} query_url={query_url} "
            f"from={query['fromBlock']} to={query['toBlock']}")
        response = requests.post(query_url, json=query, stream=True)  # stream=True to discard response body

        if response.status_code != 200:
            logging.info(
                f"Query failed. worker_id={worker_id} status={response.status_code} "
                f"msg='{response.text}' elapsed={response.elapsed}")
        else:
            logging.info(f"Query succeeded. worker_id={worker_id} elapsed={response.elapsed}")
        return response.status_code


def random_range(ranges: DatasetRanges) -> Tuple[int, int]:
    r = random.choice(ranges.ranges)
    from_block = random.randint(r.begin, r.end)
    to_block = random.randint(from_block, r.end)
    return from_block, to_block


def read_templates(dataset: str) -> List[Any]:
    templates_dir = TEMPLATES_DIR / dataset
    templates = []
    for template in templates_dir.glob('**/*.json'):
        logging.info(f"Loading template {template}")
        try:
            templates.append(json.loads(template.read_text()))
        except (IOError, json.JSONDecodeError) as e:
            logging.error(f"Template {template} invalid: {e}")
    return templates


def main(config_path: str = 'config.json'):
    try:
        logging.info(f"Reading config from {config_path}")
        config_json = Path(config_path).read_text()
        config = Config.model_validate_json(config_json)
        logging.info(
            f"Config loaded datasets={list(config.datasets.keys())}")
    except (IOError, ValidationError) as e:
        logging.error(f"Error reading config: {e}")
        exit(1)

    TrafficGenerator(config).run()


if __name__ == '__main__':
    if len(sys.argv) > 2:
        logging.error(f"Usage: {sys.argv[0]} [<CONFIG_PATH>]")
        exit(1)
    main(*sys.argv[1:])
