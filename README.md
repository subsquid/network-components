# SQD Network components

This repo contains several components of the SQD Network.

### Archive router

It distributes data ranges among workers in the centralized setup and navigates
clients what worker to use for a specific range.

The active development branch for it is `arrowsquid`.

### Network scheduler

A replacement for the router in the decentralized setup. It distributes the
data among workers and communicates the assignments to them.

### Pings collector

A simple service with multiple replicas that collects pings from workers and
stores them in a database.

### Logs collector

Receives logs from the workers and the gateways and stores them in a database.

### Peer checker

An experimental tool that helps locating a peer in the network.

## License

This project is licensed under the AGPL v3.0 license - see the [LICENSE](LICENSE.txt) file for details.
