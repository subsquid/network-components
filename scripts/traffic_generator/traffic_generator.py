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
from pydantic import BaseModel, AnyUrl, TypeAdapter, ValidationError, ConfigDict, UrlConstraints
from typing import List, Dict, Annotated, Tuple, Any, Optional

QUERY_URL = os.environ.get('QUERY_URL', "http://localhost:8000/query")
WORKERS_URL = os.environ.get('WORKERS_URL', "https://scheduler.testnet.subsquid.io/workers/pings")
MAX_THREADS = int(os.environ.get('MAX_THREADS', 256))
MIN_INTERVAL_SEC = float(os.environ.get('MIN_INTERVAL_SEC', 60))
TIMEOUT_SEC = float(os.environ.get('TIMEOUT_SEC', 180))
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()

TEMPLATES_DIR = Path(__file__).parent / 'query_templates'

logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s %(levelname)s : %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

S3Url = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["s3"])]


class Dataset(BaseModel):
    id: str
    url: S3Url


class Config(BaseModel):
    worker_versions: List[str]
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


worker_list = TypeAdapter(List[Worker])


class TrafficGenerator:
    def __init__(self, config: Config):
        self._config = config
        self._executor = futures.ThreadPoolExecutor(max_workers=MAX_THREADS)
        self._query_templates: Dict[str, Any] = {
            dataset: read_templates(dataset) for dataset in config.datasets
        }

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
                self._executor.shutdown(cancel_futures=True)
                break

    def _query_workers(self):
        workers = self._get_workers()
        if not workers:
            logging.info("No workers to query")
            return

        logging.info(f"Querying {len(workers)} workers")
        results = Counter()
        try:
            for res in self._executor.map(self._query_worker, workers, timeout=TIMEOUT_SEC):
                results[res] += 1
        except futures.TimeoutError:
            logging.error("Querying workers timed out")
        logging.info(f"All queries finished. Results: {results}")

    def _get_workers(self) -> List[Worker]:
        try:
            logging.info("Getting active workers")
            response = requests.get(WORKERS_URL)
            response.raise_for_status()
            workers = worker_list.validate_json(response.content)
            return [w for w in workers if w.version in self._config.worker_versions]
        except (requests.HTTPError, ValidationError) as e:
            logging.error(f"Error getting workers: {e}")
            return []

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
        query_url = f'{QUERY_URL}/{dataset_id}/{worker_id}'
        query = random.choice(self._query_templates[dataset])
        query['fromBlock'], query['toBlock'] = random_range(worker.stored_ranges[dataset_url].ranges)

        logging.debug(
            f"Sending query worker_id={worker_id} query_url={query_url} "
            f"from={query['fromBlock']} to={query['toBlock']}")
        response = requests.post(query_url, json=query)

        if response.status_code != 200:
            logging.warning(
                f"Query failed: worker_id={worker_id} status={response.status_code} "
                f"msg={response.text} elapsed={response.elapsed}")
        else:
            logging.debug(f"Query succeeded: worker_id={worker_id} elapsed={response.elapsed}")
        return response.status_code


def random_range(ranges: List[BlockRange]) -> Tuple[int, int]:
    r = random.choice(ranges)
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
        print(repr(config))
        logging.info(f"Config loaded worker_versions={config.worker_versions} datasets={config.datasets.keys()}")
    except (IOError, ValidationError) as e:
        logging.error(f"Error reading config: {e}")
        exit(1)

    TrafficGenerator(config).run()


if __name__ == '__main__':
    if len(sys.argv) > 2:
        logging.error(f"Usage: {sys.argv[0]} [<CONFIG_PATH>]")
        exit(1)
    main(*sys.argv[1:])
