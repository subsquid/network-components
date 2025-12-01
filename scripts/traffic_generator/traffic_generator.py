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
NUM_QUERIES = int(os.environ.get('NUM_QUERIES', 10))
NUM_CHUNKS_PER_QUERY = int(os.environ.get('NUM_CHUNKS_PER_QUERY', 100))
QUERY_TIMEOUT_SEC = int(os.environ.get('QUERY_TIMEOUT_SEC', 20))
MIN_INTERVAL_SEC = float(os.environ.get('MIN_INTERVAL_SEC', 60))

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


class DatasetHead(BaseModel):
    number: int
    hash: str


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
                    time.sleep(time_to_wait)
            except KeyboardInterrupt:
                print("Shutting down")
                self._executor.shutdown(wait=False, cancel_futures=True)
                break


    def _query_workers(self, worker_p):
        heads = self._get_heads()
        logging.info(f"Heads: {heads}")
        if not heads:
            logging.info("No datasets to query")
            return

        fn = lambda x: self._query_stream(heads)
        samples = range(NUM_QUERIES)
        results = Counter()
        try:
            tasks = self._executor.map(fn, samples)
            for worker_id, task in zip(samples, tasks):
                res, dataset_id = task
                results[res] += 1
        except futures.TimeoutError:
            logging.error("Querying workers timed out")
        logging.info(f"All queries finished. Results: {results}")


    def _pull_head(self, dataset) -> int:
        dataset_name = dataset.name
        try:
            response = requests.get(f'{PORTAL_URL}/datasets/{dataset_name}/archival-head')
            if response.ok:
                head_state = DatasetHead.model_validate_json(response.content)
                return head_state.number
            else:
                logging.warning(f"Couldn't get chunks of {dataset_name}, skipping")
        except (requests.HTTPError, ValidationError) as e:
            logging.error(f"Error getting datasets: {e}")
        return -1

    def _get_heads(self) -> Dict[str, int]:
        heads = {}
        try:
            results = self._executor.map(self._pull_head, self._config.datasets)
            for dataset, head in zip(self._config.datasets, results):
                if head == -1:
                    continue
                heads[dataset.id] = head
        except futures.TimeoutError:
            logging.error("Querying heads timed out")
        return heads


    def _do_request(self, query_url, query) -> Tuple[int, int, int]:
        try:
            headers = {"Accept-Encoding": "gzip"}
            response = requests.post(query_url, json=query, stream=True, timeout=QUERY_TIMEOUT_SEC, headers = headers)
            cnt = 0
            number = -1
            if response.ok:
                for line in response.iter_lines():
                    if len(line) == 0:
                        continue
                    cnt += 1
                    number = json.loads(line).get("header", {}).get("number", -1)
            return response.status_code, cnt, number
        except Exception as e:
            logging.info(f"Error while executing query. query_url={query_url} e={e}")
            return 499, 0, -1

    def _query_stream(self, heads) -> Optional[Tuple[int, str]]:
        stored_datasets = [
            (dataset.template_dir, dataset.id, dataset.name) for dataset in self._config.datasets
            if dataset.id in heads.keys()
        ]

        template, dataset_id, dataset_name = random.choice(stored_datasets)
        query_url = f'{PORTAL_URL}/datasets/{dataset_name}/archival-stream?max_chunks={NUM_CHUNKS_PER_QUERY}'
        query = random.choice(self._query_templates[template]).copy()
        query_id = self._query_templates[template].index(query)
        del query['toBlock']
        query['fromBlock'] = random.randrange(heads[dataset_id])

        logging.debug(
            f"Sending stream query_url={query_url}"
            f"from={query['fromBlock']}")
        code, ret_count, last_block = self._do_request(query_url, query)

        logging.info(f"from={query['fromBlock']} to={last_block} ret={ret_count}")
        return code, dataset_id


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
