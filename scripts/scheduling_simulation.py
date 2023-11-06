import functools
import heapq
import itertools
import matplotlib.pyplot as plt
import numpy
import os
import random
import seaborn
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Set, Optional, Iterator

NUM_WORKERS = int(os.environ.get('NUM_WORKERS', '100'))
WORKERS_JAILED_PER_EPOCH = int(os.environ.get('WORKERS_JAILED_PER_EPOCH', '0'))
WORKER_CHURN_PER_EPOCH = int(os.environ.get('WORKER_CHURN_PER_EPOCH', '0'))
NUM_UNITS = int(os.environ.get('NUM_UNITS', '12000'))
NEW_UNITS_PER_EPOCH = int(os.environ.get('NEW_UNITS_PER_EPOCH', '0'))
UNIT_SIZE_MB = int(os.environ.get('UNIT_SIZE_MB', '2500'))
WORKER_STORAGE_MB = int(os.environ.get('WORKER_STORAGE_MB', '1000000'))
REPLICATION_FACTOR = int(os.environ.get('REPLICATION_FACTOR', '3'))
MAX_EPOCHS = int(os.environ.get('MAX_EPOCHS', '0'))
NUM_REPS = int(os.environ.get('NUM_REPS', '1'))
SCHEDULER_TYPE = os.environ.get('SCHEDULER_TYPE', 'random')
MIX_ALL_REPLICAS = 'MIX_ALL_REPLICAS' in os.environ
MIXED_UNITS_RATIO = float(os.environ.get('MIXED_UNITS_RATIO', '0.1'))
MIXING_RECENT_UNIT_WEIGHT = float(os.environ.get('MIXING_RECENT_UNIT_WEIGHT', '10'))
NUM_SQUIDS_PER_EPOCH = int(os.environ.get('NUM_SQUIDS_PER_EPOCH', '1000'))
QUALIFIED_WORKER_THRESHOLD = float(os.environ.get('QUALIFIED_WORKER_THRESHOLD', '0.25'))
SQD_SUPPLY = int(os.environ.get('SQD_SUPPLY', '1_337_000_000'))
WORKER_STAKE = int(os.environ.get('WORKER_STAKE', '100_000'))
EPOCH_LEN_HOURS = int(os.environ.get('EPOCH_LEN_HOURS', '3'))
RESPONSE_SIZE_AVG = float(os.environ.get('RESPONSE_SIZE_AVG', '5.0'))
RESPONSE_SIZE_SD = float(os.environ.get('RESPONSE_SIZE_SD', '1.0'))
OUTPUT_DIR = Path(os.environ.get('OUTPUT_DIR', './out'))

assert 0 <= MIXED_UNITS_RATIO <= 1, 'MIXED_UNITS_RATIO should be in range [0,1]'
assert MIXING_RECENT_UNIT_WEIGHT >= 1, 'MIXING_RECENT_UNIT_WEIGHT should be >= 1'
assert NUM_WORKERS * WORKER_STORAGE_MB > NUM_UNITS * REPLICATION_FACTOR * UNIT_SIZE_MB, 'Not enough worker storage'


def params_summary() -> str:
    return (
        f"NUM_WORKERS = {NUM_WORKERS}\n"
        f"WORKERS_JAILED_PER_EPOCH = {WORKERS_JAILED_PER_EPOCH}\n"
        f"WORKER_CHURN_PER_EPOCH = {WORKER_CHURN_PER_EPOCH}\n"
        f"NUM_UNITS = {NUM_UNITS}\n"
        f"NEW_UNITS_PER_EPOCH = {NEW_UNITS_PER_EPOCH}\n"
        f"UNIT_SIZE_MB = {UNIT_SIZE_MB}\n"
        f"WORKER_STORAGE_MB = {WORKER_STORAGE_MB}\n"
        f"REPLICATION_FACTOR = {REPLICATION_FACTOR}\n"
        f"MAX_EPOCHS = {MAX_EPOCHS}\n"
        f"SCHEDULER_TYPE = {SCHEDULER_TYPE}\n"
        f"MIXED_UNITS_RATIO = {MIXED_UNITS_RATIO}\n"
        f"MIXING_RECENT_UNIT_WEIGHT = {MIXING_RECENT_UNIT_WEIGHT}\n"
        f"NUM_SQUIDS_PER_EPOCH = {NUM_SQUIDS_PER_EPOCH}\n"
        f"QUALIFIED_WORKER_THRESHOLD = {QUALIFIED_WORKER_THRESHOLD}\n"
    )


Id = int


def random_id() -> Id:
    return int.from_bytes(random.randbytes(32), byteorder="big", signed=False)


def distance(unit: 'Unit', worker: 'Worker') -> int:
    return unit.id ^ worker.id


@functools.total_ordering
class Worker:
    def __init__(self, epoch_joined=0):
        self.id = random_id()
        self.epoch_joined = epoch_joined
        self.last_epoch = epoch_joined
        self.epoch_retired: 'Optional[int]' = None
        self.assigned_units: 'Set[Unit]' = set()
        self.downloaded_units: 'Set[Unit]' = set()
        self.request_count_history: [int] = [0]
        self.response_size_history: [float] = [0.0]
        self.total_rewards = 0
        self.total_downloaded_data = 0
        self.initial_sync_data = 0
        self.jailed = False

    def __eq__(self, other):
        if not isinstance(other, Worker):
            return NotImplemented
        return self.assigned_data == other.assigned_data

    def __lt__(self, other):
        if not isinstance(other, Worker):
            return NotImplemented
        return self.assigned_data < other.assigned_data

    @property
    def num_requests(self) -> int:
        return sum(self.request_count_history)

    @property
    def stored_data(self) -> int:
        return len(self.downloaded_units) * UNIT_SIZE_MB

    @property
    def assigned_data(self) -> int:
        return len(self.assigned_units) * UNIT_SIZE_MB

    @property
    def remaining_capacity(self) -> int:
        return WORKER_STORAGE_MB - self.assigned_data

    def try_assign_unit(self, unit: 'Unit') -> bool:
        if self.remaining_capacity > UNIT_SIZE_MB and not self.jailed and unit not in self.assigned_units:
            self.assigned_units.add(unit)
            unit.assigned_to[self.id] = self
            return True
        return False

    def purge_assignment(self):
        while len(self.assigned_units) > 0:
            self.assigned_units.pop().assigned_to.pop(self.id)

    def download_assigned(self) -> int:
        if self.jailed:
            return 0

        old_units = self.downloaded_units - self.assigned_units
        new_units = self.assigned_units - self.downloaded_units

        for unit in old_units:
            self.downloaded_units.remove(unit)

        download_size = len(new_units) * UNIT_SIZE_MB
        self.total_downloaded_data += download_size
        for unit in self.assigned_units:
            self.downloaded_units.add(unit)

        if self.initial_sync_data == 0:
            self.initial_sync_data = download_size

        return download_size

    def make_request(self):
        response_size = max(min(numpy.random.normal(loc=RESPONSE_SIZE_AVG, scale=RESPONSE_SIZE_SD, size=1)[0], 10), 0)
        self.request_count_history[-1] += 1
        self.response_size_history[-1] += response_size

    def jail(self):
        self.jailed = True
        self.purge_assignment()

    def release(self):
        self.jailed = False

    def retire(self):
        assert self.epoch_retired is None
        self.epoch_retired = self.last_epoch
        self.purge_assignment()

    def next_epoch(self):
        assert self.epoch_retired is None
        self.last_epoch += 1
        self.request_count_history.append(0)
        self.response_size_history.append(0.0)

    def get_reward(self, reward: int):
        assert self.epoch_retired is None
        self.total_rewards += reward

    @property
    def epochs_active(self) -> int:
        assert self.epoch_retired is None or self.epoch_retired == self.last_epoch
        return self.last_epoch - self.epoch_joined + 1

    @property
    def avg_download_gb(self) -> float:
        """ Average data downloaded per epoch, excluding initial sync, in GB """
        return (self.total_downloaded_data - self.initial_sync_data) / self.epochs_active / 1024

    @property
    def avg_requests(self) -> float:
        """ Average requests served per epoch, in thousands """
        return self.num_requests / self.epochs_active / 1000

    @property
    def avg_reward(self) -> float:
        return self.total_rewards / self.epochs_active

    @property
    def stake(self) -> int:
        return WORKER_STAKE

    @property
    def liveness(self) -> float:
        return 1.0

    @property
    def tenure(self) -> float:
        if self.epochs_active >= 10:
            return 1.0
        if self.epochs_active >= 2:
            return 0.5 + self.epochs_active // 2 * 0.1
        return 0.5


class Unit:
    def __init__(self):
        self.id = random_id()
        self.assigned_to: 'dict[Id, Worker]' = {}

    def __eq__(self, other):
        if not isinstance(other, Unit):
            return False
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    @property
    def missing_replicas(self) -> int:
        return REPLICATION_FACTOR - len(self.assigned_to)

    def remove_random_replica(self):
        assert len(self.assigned_to) > 0
        worker_id = random.choice(list(self.assigned_to.keys()))
        worker = self.assigned_to.pop(worker_id)
        worker.assigned_units.remove(self)

    def remove_all_replicas(self):
        while len(self.assigned_to) > 0:
            _, worker = self.assigned_to.popitem()
            worker.assigned_units.remove(self)

    def query(self):
        assert len(self.assigned_to) > 0
        random.choice(list(self.assigned_to.values())).make_request()


class AssignmentError(ValueError):
    def __init__(self):
        super().__init__("Not enough workers to assign unit")


class History:
    def __init__(self):
        self.download_avg: [float] = []  # Average data downloaded per worker per epoch in GB
        self.download_cv: [float] = []   # Coefficient of variation of downloaded data
        self.requests_avg: [float] = []  # Average number of requests served per worker per epoch in thousands
        self.requests_cv: [float] = []   # Coefficient of variation of number of served requests
        self.rewards_avg: [float] = []  # Average number worker award per epoch
        self.rewards_cv: [float] = []   # Coefficient of variation of worker rewards

    def update(self, workers: '[Worker]'):
        downloads = [w.avg_download_gb for w in workers]
        self.download_avg.append(numpy.mean(downloads))
        self.download_cv.append(numpy.std(downloads) / self.download_avg[-1])

        requests = [w.avg_requests for w in workers]
        self.requests_avg.append(numpy.mean(requests))
        self.requests_cv.append(numpy.std(requests) / self.requests_avg[-1])

        rewards = [w.avg_reward for w in workers]
        self.rewards_avg.append(numpy.mean(rewards))
        self.rewards_cv.append(numpy.std(rewards) / self.rewards_avg[-1])


class Scheduler(ABC):
    def __init__(self):
        self.workers: '[Worker]' = [Worker() for _ in range(NUM_WORKERS)]
        self.retired_workers: '[Worker]' = []
        self.units: '[Unit]' = [Unit() for _ in range(NUM_UNITS)]
        self.epoch = 0
        self.jailed_workers_data = 0
        self.retired_workers_data = 0
        self.history = History()

        # Perform initial assignment
        self.assign_units(initial=True)
        self.download()

    @property
    def total_downloaded_data(self) -> int:
        return sum(w.total_downloaded_data for w in (self.workers + self.retired_workers))

    @property
    def initial_sync_data(self) -> int:
        return sum(w.initial_sync_data for w in (self.workers + self.retired_workers))

    def base_apr(self) -> float:
        target_capacity = len(self.units) * UNIT_SIZE_MB * REPLICATION_FACTOR
        actual_capacity = int(len(self.workers) * WORKER_STORAGE_MB * 0.9)
        u_rate = (target_capacity - actual_capacity) / target_capacity
        if u_rate >= 1.0:
            return 0.70
        if u_rate >= 0:
            return 0.25 + int(u_rate * 10) * 0.05
        if u_rate >= -3:
            return 0.20 + int(u_rate) * 0.05
        return 0.05

    def discount_factor(self) -> float:
        stake_ratio = len(self.workers) * WORKER_STAKE / SQD_SUPPLY
        if stake_ratio >= 1.0:
            return 0.1
        if stake_ratio >= 0.25:
            return 1.0 - ((stake_ratio - 0.25) / 0.25 * 0.3)
        return 1.0

    def give_worker_rewards(self):
        r_apr = self.base_apr() * self.discount_factor()
        max_epoch_reward = r_apr * sum(w.stake for w in self.workers) * EPOCH_LEN_HOURS / 365 / 24

        total_requests = sum(w.request_count_history[-1] for w in self.workers)
        total_egress = sum(w.response_size_history[-1] for w in self.workers)
        total_staked = sum(w.stake for w in self.workers)

        for worker in self.workers:
            t_scanned = worker.request_count_history[-1] / total_requests
            t_egress = worker.response_size_history[-1] / total_egress
            traffic = numpy.sqrt(t_scanned * t_egress)
            stake_share = worker.stake / total_staked
            reward = int(max_epoch_reward * worker.liveness * worker.tenure * min((traffic / stake_share)**0.1, 1))
            worker.get_reward(reward)

    @abstractmethod
    def assign_units(self, initial=False, mid_epoch=False):
        raise NotImplementedError

    def download(self):
        for worker in self.workers:
            worker.download_assigned()

    def jail_random_worker(self):
        worker: 'Worker' = random.choice([w for w in self.workers if not w.jailed])
        self.jailed_workers_data += worker.stored_data
        worker.jail()
        self.assign_units(mid_epoch=True)
        self.download()

    def retire_random_worker(self):
        worker = self.workers.pop(random.randint(0, len(self.workers)-1))
        worker.retire()
        self.retired_workers_data += worker.stored_data
        self.retired_workers.append(worker)

    def run_squids(self):
        for _ in range(NUM_SQUIDS_PER_EPOCH):
            # Each squid starts with a random unit and queries all subsequent ones
            start_unit = random.randint(0, len(self.units) - 1)
            for unit in itertools.islice(self.units, start_unit, None):
                unit.query()

    def run_epoch(self):
        # release jailed workers
        for worker in self.workers:
            worker.release()

        # some workers leave, some workers join
        for _ in range(WORKER_CHURN_PER_EPOCH):
            self.retire_random_worker()
        for _ in range(WORKER_CHURN_PER_EPOCH):
            self.workers.append(Worker(epoch_joined=self.epoch))

        # assign units, download data, run squids
        self.assign_units()
        self.download()
        self.run_squids()

        # some workers get jailed during epoch
        for _ in range(WORKERS_JAILED_PER_EPOCH):
            self.jail_random_worker()

        # new data units appear
        for _ in range(NEW_UNITS_PER_EPOCH):
            self.units.append(Unit())

        self.give_worker_rewards()
        self.history.update(self.workers + self.retired_workers)

        self.epoch += 1
        for worker in self.workers:
            worker.next_epoch()

    def summary(self) -> 'Summary':
        min_epochs = int(self.epoch * QUALIFIED_WORKER_THRESHOLD)
        qualified_workers = [
            worker for worker in (self.workers + self.retired_workers)
            if worker.epochs_active > min_epochs
        ]
        return Summary(
            last_epoch=self.epoch,
            downloaded_data_gb=(self.total_downloaded_data - self.initial_sync_data) // 1024,
            jailed_workers_data_gb=self.jailed_workers_data // 1024,
            retired_workers_data_gb=self.retired_workers_data // 1024,
            new_chunks_data_gb=self.epoch * NEW_UNITS_PER_EPOCH * UNIT_SIZE_MB * REPLICATION_FACTOR // 1024,
            avg_worker_download={w.id: w.avg_download_gb for w in qualified_workers},
            avg_worker_requests={w.id: w.avg_requests for w in qualified_workers},
            avg_worker_rewards={w.id: w.avg_reward for w in qualified_workers},
            history=self.history,
        )

    def run_simulation(self):
        summary = self.summary()
        while MAX_EPOCHS == 0 or self.epoch < MAX_EPOCHS:
            try:
                print(f"Epoch {self.epoch}")
                self.run_epoch()
                summary = self.summary()
            except (AssignmentError, KeyboardInterrupt):
                break

        return summary


class XorDistanceScheduler(Scheduler):
    def assign_units(self, initial=False, mid_epoch=False):
        if not initial and not mid_epoch:
            for worker in self.workers:
                worker.purge_assignment()

        for unit in self.units:
            if unit.missing_replicas > 0:
                for worker in sorted(self.workers, key=lambda w: distance(unit, w)):
                    worker.try_assign_unit(unit)
                    if unit.missing_replicas == 0:
                        break
                else:
                    raise AssignmentError()


class RandomScheduler(Scheduler):

    def assign_units(self, initial=False, mid_epoch=False):
        # Random mixing at the beginning of each epoch
        if not initial and not mid_epoch:
            num_mixed_units = int(len(self.units) * MIXED_UNITS_RATIO)
            assigned_units = [u for u in self.units if u.missing_replicas < REPLICATION_FACTOR]
            # earliest units have weight 1, most recent have `MIXING_RECENT_UNIT_WEIGHT`
            unit_weights = numpy.linspace(1.0, MIXING_RECENT_UNIT_WEIGHT, num=len(assigned_units))
            probabilities = unit_weights / sum(unit_weights)
            for unit in numpy.random.choice(assigned_units, size=num_mixed_units, replace=False, p=probabilities):
                if MIX_ALL_REPLICAS:
                    unit.remove_all_replicas()
                else:
                    unit.remove_random_replica()

        # Workers shuffled and ordered by number of assigned units
        random.shuffle(self.workers)
        heapq.heapify(self.workers)

        for unit in self.units:
            tried_workers = []
            while len(self.workers) > 0 and unit.missing_replicas > 0:
                worker: 'Worker' = heapq.heappop(self.workers)
                worker.try_assign_unit(unit)
                tried_workers.append(worker)
            for worker in tried_workers:
                heapq.heappush(self.workers, worker)
            if unit.missing_replicas > 0:
                raise AssignmentError()


def get_scheduler(scheduler_type: str) -> 'Scheduler':
    if scheduler_type.lower() == 'xor':
        return XorDistanceScheduler()
    if scheduler_type.lower() == 'random':
        return RandomScheduler()
    raise ValueError(f"Unknown scheduler: {scheduler_type}")


@dataclass
class Summary:
    last_epoch: int
    downloaded_data_gb: int  # Total data downloaded by all workers excluding initial sync of each worker
    jailed_workers_data_gb: int  # Total data that was held by all jailed workers at the time they were jailed
    retired_workers_data_gb: int  # Total data that was held by all retired workers at the time they were retired
    new_chunks_data_gb: int  # Total size of new chunks which appeared throughout the simulation
    avg_worker_download: dict['Id', float]  # Average size of data downloaded per epoch for each worker
    avg_worker_requests: dict['Id', float]  # Average number of received requests per epoch for each worker
    avg_worker_rewards: dict['Id', float]  # Average SQD received per epoch for each worker
    history: 'History'

    @property
    def downloaded_per_epoch(self) -> int:
        return self.downloaded_data_gb // self.last_epoch

    @property
    def reshuffled_per_epoch(self) -> int:
        return (self.downloaded_data_gb - self.jailed_workers_data_gb - self.retired_workers_data_gb - self.new_chunks_data_gb) // self.last_epoch

    def to_string(self) -> str:
        return (
            f"Run for {self.last_epoch} epochs.\n"
            f"Downloaded a total of {self.downloaded_data_gb} GB (excluding initial sync).\n"
            f"That amounts to {self.downloaded_per_epoch} GB/epoch.\n"
            f"Jailed workers data: {self.jailed_workers_data_gb} GB.\n"
            f"Retired workers data: {self.retired_workers_data_gb} GB.\n"
            f"New chunks data: {self.new_chunks_data_gb} GB.\n"
            f"Unnecessary reshuffled {self.reshuffled_per_epoch} GB/epoch.\n"
            f"Download variance coefficient: {self.history.download_cv[-1]}.\n"
            f"Requests variance coefficient: {self.history.requests_cv[-1]}.\n"
            f"Rewards variance coefficient: {self.history.rewards_cv[-1]}.\n"
        )

    def save_plot(self, plot_path: Path):
        print(f"Saving plot to {plot_path}")

        fig, ax = plt.subplots(3, 3, figsize=(12, 12))

        seaborn.histplot(data=self.avg_worker_download, kde=True, ax=ax[0, 0])
        ax[0, 0].set_xlabel("average data downloaded per epoch [GB]")
        ax[0, 0].set_ylabel("# workers")

        seaborn.histplot(data=self.avg_worker_requests, kde=True, ax=ax[0, 1])
        ax[0, 1].set_xlabel("average requests served per epoch [thousands]")
        ax[0, 1].set_ylabel("# workers")

        seaborn.histplot(data=self.avg_worker_rewards, kde=True, ax=ax[0, 2])
        ax[0, 2].set_xlabel("average reward per epoch [SQD]")
        ax[0, 2].set_ylabel("# workers")

        seaborn.lineplot(data=self.history.download_avg, ax=ax[1, 0])
        ax[1, 0].set_xlabel("epoch")
        ax[1, 0].set_ylabel("avg data downloaded p/worker p/epoch [GB]")

        seaborn.lineplot(data=self.history.requests_avg, ax=ax[1, 1])
        ax[1, 1].set_xlabel("epoch")
        ax[1, 1].set_ylabel("avg requests served p/worker p/epoch [thousands]")

        seaborn.lineplot(data=self.history.rewards_avg, ax=ax[1, 2])
        ax[1, 2].set_xlabel("epoch")
        ax[1, 2].set_ylabel("avg worker reward p/epoch [SQD]")

        seaborn.lineplot(data=self.history.download_cv, ax=ax[2, 0])
        ax[2, 0].set_xlabel("epoch")
        ax[2, 0].set_ylabel("variance coefficient of data downloaded")

        seaborn.lineplot(data=self.history.requests_cv, ax=ax[2, 1])
        ax[2, 1].set_xlabel("epoch")
        ax[2, 1].set_ylabel("variance coefficient of requests served")

        seaborn.lineplot(data=self.history.rewards_cv, ax=ax[2, 2])
        ax[2, 2].set_xlabel("epoch")
        ax[2, 2].set_ylabel("variance coefficient of worker rewards")

        plt.tight_layout()
        plt.savefig(plot_path, format="png")
        plt.close()


def run_simulation() -> 'Iterator[Summary]':
    for i in range(NUM_REPS):
        print(f"Running round {i+1} / {NUM_REPS}")
        scheduler = get_scheduler(SCHEDULER_TYPE)
        yield scheduler.run_simulation()


# def aggregate_summaries(summaries: ['Summary']) -> 'Summary':
#     total_epochs = sum(s.last_epoch for s in summaries)
#     return Summary(
#         last_epoch=total_epochs // len(summaries),
#         downloaded_data_gb=sum(s.downloaded_data_gb * s.last_epoch for s in summaries) // total_epochs,
#         jailed_workers_data_gb=sum(s.jailed_workers_data_gb * s.last_epoch for s in summaries) // total_epochs,
#         retired_workers_data_gb=sum(s.retired_workers_data_gb * s.last_epoch for s in summaries) // total_epochs,
#         new_chunks_data_gb=sum(s.new_chunks_data_gb * s.last_epoch for s in summaries) // total_epochs,
#         avg_worker_download={w: d for s in summaries for w, d in s.avg_worker_download.items()},
#         avg_worker_requests={w: d for s in summaries for w, d in s.avg_worker_requests.items()},
#     )


def main():
    print("Starting simulation.\n")
    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
    print(params_summary())

    summary = next(run_simulation())

    print(f"\nSimulation ended. Average results from {NUM_REPS} rounds:\n{summary.to_string()}")

    timestamp = datetime.utcnow().isoformat(timespec='seconds')
    with open(OUTPUT_DIR / f"{timestamp}_results.txt", "w") as results_file:
        results_file.write(params_summary())
        results_file.write(summary.to_string())
    summary.save_plot(OUTPUT_DIR / f"{timestamp}_worker_distribution.png")


if __name__ == '__main__':
    main()
