import collections
import pprint
import requests

CHUNKS_URL = "https://scheduler.testnet.subsquid.io/chunks"


def main():
    summary = {}
    replication = collections.Counter()
    chunks = requests.get(CHUNKS_URL)
    for dataset, ds_chunks in chunks.json().items():
        summary[dataset] = {
            'num_chunks': len(ds_chunks),
            'total_size_gb': sum(chunk['size_bytes'] for chunk in ds_chunks) // (1024 ** 3)
        }
        for chunk in ds_chunks:
            rep_factor = len(chunk['downloaded_by'])
            if rep_factor < 1:
                print(f"{dataset} {chunk['begin']} {chunk['end']} has no replicas")
            replication[rep_factor] += 1

    pprint.pprint(summary)
    pprint.pprint(replication)


if __name__ == '__main__':
    main()
