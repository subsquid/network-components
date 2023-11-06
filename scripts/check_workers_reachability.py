import os

import sys

import requests

PINGS_URL = "https://scheduler.testnet.subsquid.io/workers/pings"
GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://localhost:8001")


def main():
    pings = requests.get(PINGS_URL)
    pings.raise_for_status()
    for worker in pings.json():
        peer_id = worker['peer_id']
        address = worker['address']
        version = worker['version']
        query_url = f"{GATEWAY_URL}/query/czM6Ly9ldGhlcmV1bS1tYWlubmV0/{peer_id}?timeout=5s"
        query_result = requests.post(url=query_url, json={})
        print(f"{peer_id} {address} {version} {query_result.status_code}")


def main2(peers):
    for peer_id in peers:
        query_url = f"{GATEWAY_URL}/query/czM6Ly9ldGhlcmV1bS1tYWlubmV0/{peer_id}?timeout=10s"
        query_result = requests.post(url=query_url, json={})
        print(f"{peer_id} {query_result.status_code}")


if __name__ == '__main__':
    if len(sys.argv) > 1:
        main2(sys.argv[1:])
    else:
        main()
