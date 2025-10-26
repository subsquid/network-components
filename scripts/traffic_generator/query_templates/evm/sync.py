import json
import sys
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import subprocess

def main():
    if len(sys.argv) < 4:
        print("Usage: python sync.py <portal_url> <dataset> <query_file> [first_block]")
        sys.exit(1)

    portal_url = sys.argv[1]
    dataset = sys.argv[2]
    query_file = sys.argv[3]
    first_block = int(sys.argv[4]) if len(sys.argv) > 4 else 0

    try:
        with open(query_file, 'r') as file:
            query = json.load(file)
    except Exception as e:
        print(f"Error reading query file: {e}")
        sys.exit(1)

    stream(portal_url, dataset, query, first_block=first_block)

def stream(portal_url, dataset, query, first_block=0):
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.mount("https://", HTTPAdapter(max_retries=retries))

    if "toBlock" in query:
        del query["toBlock"]

    last_block = first_block
    while True:
        print(f"Fetching from {last_block}")
        start_time = time.time()
        query["fromBlock"] = last_block
        response = session.post(f"{portal_url}/datasets/{dataset}/stream", json=query)
        if response.status_code != 200:
            print(f"Error: received status code {response.status_code} from the server.")
            print("Response:", response.text)
            return

        print(f"Response status code: {response.status_code}")

        last_line = None
        body = response.content
        lines = body.splitlines()
        num_blocks = len(lines)
        last_line = lines[-1] if body else None

        print(f"Received {num_blocks} blocks in this response. Parsing...")

        try:
            process = subprocess.Popen(['jq', '.header.number'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate(input=last_line)
            if process.returncode != 0:
                print(f"Error running jq: {stderr.decode().strip()}")
                return

            next_block = int(stdout.decode().strip()) + 1
            elapsed_time = time.time() - start_time
            print(f"{num_blocks} blocks received, speed: {(next_block - last_block) / elapsed_time} bps")
            last_block = next_block
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")

if __name__ == "__main__":
    main()
