import pprint
import requests
import sys
from datetime import datetime

PINGS_URL = "https://scheduler.testnet.subsquid.io/workers/pings"
CHUNKS_URL = "https://scheduler.testnet.subsquid.io/chunks"


def main(worker_id):
    print("Getting active workers...")
    pings = requests.get(PINGS_URL)
    pings.raise_for_status()
    for ping in pings.json():
        if ping['peer_id'] == worker_id:
            status = {
                'last_ping': datetime.fromtimestamp(ping['last_ping'] / 1000.0).isoformat(),
                'version': ping['version'],
                'stored_bytes': ping['stored_bytes'],
                'jailed': ping['jailed'],
                'reachable': ping['last_dial_ok'],
            }
            break
    else:
        print("Worker not active")
        exit(1)

    status['assigned_chunks_count'] = 0
    status['assigned_chunks_size'] = 0
    status['downloaded_chunks_count'] = 0
    status['downloaded_chunks_size'] = 0

    print("Getting assigned chunks...")
    chunks = requests.get(CHUNKS_URL)
    for ds_chunks in chunks.json().values():
        for chunk in ds_chunks:
            if worker_id in chunk['assigned_to']:
                status['assigned_chunks_count'] += 1
                status['assigned_chunks_size'] += chunk['size_bytes']
            if worker_id in chunk['downloaded_by']:
                status['downloaded_chunks_count'] += 1
                status['downloaded_chunks_size'] += chunk['size_bytes']

    pprint.pprint(status)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <WORKER_ID>")
        exit(1)
    main(sys.argv[1])
