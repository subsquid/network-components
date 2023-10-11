import functools
import heapq
import matplotlib.pyplot as plt
import os
import random
import seaborn
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Set, Optional, Iterator

NUM_WORKERS = int(os.environ.get('NUM_WORKERS', '100'))
WORKERS_JAILED_PER_EPOCH = int(os.environ.get('WORKERS_JAILED_PER_EPOCH', '5'))
WORKER_CHURN_PER_EPOCH = int(os.environ.get('WORKER_CHURN_PER_EPOCH', '2'))
NUM_UNITS = int(os.environ.get('NUM_UNITS', '5000'))
NEW_UNITS_PER_EPOCH = int(os.environ.get('NEW_UNITS_PER_EPOCH', '10'))
UNIT_SIZE_MB = int(os.environ.get('UNIT_SIZE_MB', '2500'))
WORKER_STORAGE_MB = int(os.environ.get('WORKER_STORAGE_MB', '500000'))
REPLICATION_FACTOR = int(os.environ.get('REPLICATION_FACTOR', '3'))
MAX_EPOCHS = int(os.environ.get('MAX_EPOCHS', '0'))
NUM_REPS = int(os.environ.get('NUM_REPS', '1'))
SCHEDULER_TYPE = os.environ.get('SCHEDULER_TYPE', 'xor')
MIXED_UNITS_RATIO = float(os.environ.get('MIXED_UNITS_RATIO', '0.05'))
OUTPUT_DIR = Path(os.environ.get('OUTPUT_DIR', './out'))

assert 0 <= MIXED_UNITS_RATIO <= 1, 'MIXED_UNITS_RATIO should be in range [0,1]'

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
        self.epoch_retired: 'Optional[int]' = None
        self.assigned_units: 'Set[Unit]' = set()
        self.downloaded_units: 'Set[Unit]' = set()
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

    def jail(self):
        self.jailed = True
        self.purge_assignment()

    def release(self):
        self.jailed = False

    def retire(self, epoch):
        assert self.epoch_retired is None
        self.epoch_retired = epoch
        self.purge_assignment()


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
        _, worker = self.assigned_to.popitem()
        worker.assigned_units.remove(self)


class AssignmentError(ValueError):
    def __init__(self):
        super().__init__("Not enough workers to assign unit")


class Scheduler(ABC):
    def __init__(self):
        self.workers: '[Worker]' = [Worker() for _ in range(NUM_WORKERS)]
        self.retired_workers: '[Worker]' = []
        self.units: '[Unit]' = [Unit() for _ in range(NUM_UNITS)]
        self.epoch = 0
        self.jailed_workers_data = 0
        self.retired_workers_data = 0

        # Perform initial assignment
        self.assign_units(initial=True)
        self.download()

    @property
    def total_downloaded_data(self) -> int:
        return sum(w.total_downloaded_data for w in (self.workers + self.retired_workers))

    @property
    def initial_sync_data(self) -> int:
        return sum(w.initial_sync_data for w in (self.workers + self.retired_workers))

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
        worker.retire(self.epoch)
        self.retired_workers_data += worker.stored_data
        self.retired_workers.append(worker)

    def run_epoch(self):
        # release jailed workers
        for worker in self.workers:
            worker.release()

        # some workers leave, some workers join
        for _ in range(WORKER_CHURN_PER_EPOCH):
            self.retire_random_worker()
        for _ in range(WORKER_CHURN_PER_EPOCH):
            self.workers.append(Worker(epoch_joined=self.epoch))

        # assign units & download data
        self.assign_units()
        self.download()

        # some workers get jailed during epoch
        for _ in range(WORKERS_JAILED_PER_EPOCH):
            self.jail_random_worker()

        # new data units appear
        for _ in range(NEW_UNITS_PER_EPOCH):
            self.units.append(Unit())

        self.epoch += 1

    def summary(self) -> 'Summary':
        def epochs_active(worker: 'Worker') -> int:
            last_epoch = worker.epoch_retired if worker.epoch_retired is not None else self.epoch
            return last_epoch - worker.epoch_joined + 1
        avg_worker_download = {
            worker.id: (worker.total_downloaded_data - worker.initial_sync_data) // epochs_active(worker) // 1024
            for worker in self.workers + self.retired_workers
        }
        return Summary(
            last_epoch=self.epoch,
            downloaded_data_gb=(self.total_downloaded_data - self.initial_sync_data) // 1024,
            jailed_workers_data_gb=self.jailed_workers_data // 1024,
            retired_workers_data_gb=self.retired_workers_data // 1024,
            new_chunks_data_gb=self.epoch * NEW_UNITS_PER_EPOCH * UNIT_SIZE_MB * REPLICATION_FACTOR // 1024,
            avg_worker_download=avg_worker_download
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
            for unit in random.sample(assigned_units, k=num_mixed_units):
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
    avg_worker_download: dict['Id', int]  # Average size of data downloaded per epoch for each worker

    @property
    def downloaded_per_epoch(self) -> int:
        return self.downloaded_data_gb // self.last_epoch

    @property
    def reshuffled_per_epoch(self) -> int:
        return (self.downloaded_data_gb - self.jailed_workers_data_gb - self.retired_workers_data_gb - self.new_chunks_data_gb) // self.last_epoch

    def to_string(self) -> str:
        return f"""
    Run for {self.last_epoch} epochs.
    Downloaded a total of {self.downloaded_data_gb} GB (excluding initial sync).
    That amounts to {self.downloaded_per_epoch} GB/epoch.
    Jailed workers data: {self.jailed_workers_data_gb} GB.
    Retired workers data: {self.retired_workers_data_gb} GB.
    New chunks data: {self.new_chunks_data_gb} GB.
    Unnecessary reshuffled {self.reshuffled_per_epoch} GB/epoch.
    """


def run_simulation() -> 'Iterator[Summary]':
    for i in range(NUM_REPS):
        print(f"Running round {i+1} / {NUM_REPS}")
        scheduler = get_scheduler(SCHEDULER_TYPE)
        yield scheduler.run_simulation()


def aggregate_summaries(summaries: ['Summary']) -> 'Summary':
    total_epochs = sum(s.last_epoch for s in summaries)
    return Summary(
        last_epoch=total_epochs // len(summaries),
        downloaded_data_gb=sum(s.downloaded_data_gb * s.last_epoch for s in summaries) // total_epochs,
        jailed_workers_data_gb=sum(s.jailed_workers_data_gb * s.last_epoch for s in summaries) // total_epochs,
        retired_workers_data_gb=sum(s.retired_workers_data_gb * s.last_epoch for s in summaries) // total_epochs,
        new_chunks_data_gb=sum(s.new_chunks_data_gb * s.last_epoch for s in summaries) // total_epochs,
        avg_worker_download={w: d for s in summaries for w, d in s.avg_worker_download.items()}
    )


def main():
    print(f"""Starting simulation. 
        NUM_WORKERS = {NUM_WORKERS}
        WORKERS_JAILED_PER_EPOCH = {WORKERS_JAILED_PER_EPOCH}
        WORKER_CHURN_PER_EPOCH = {WORKER_CHURN_PER_EPOCH}
        NUM_UNITS = {NUM_UNITS}
        NEW_UNITS_PER_EPOCH = {NEW_UNITS_PER_EPOCH}
        UNIT_SIZE_MB = {UNIT_SIZE_MB}
        WORKER_STORAGE_MB = {WORKER_STORAGE_MB}
        REPLICATION_FACTOR = {REPLICATION_FACTOR}
        MAX_EPOCHS = {MAX_EPOCHS}
        SCHEDULER_TYPE = {SCHEDULER_TYPE}
    """)
    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

    summary = aggregate_summaries(list(run_simulation()))

    print(f"Simulation ended. Average results from {NUM_REPS} rounds:\n{summary.to_string()}")

    plot_path = OUTPUT_DIR / f"worker_download_distribution_{datetime.utcnow().isoformat(timespec='seconds')}.png"
    print(f"Saving plot to {plot_path}")
    seaborn.displot(data=summary.avg_worker_download, kde=True)
    plt.xlabel("average data downloaded per epoch [GB]")
    plt.ylabel("# workers")
    plt.tight_layout()
    plt.savefig(plot_path, format="png")
    plt.close()


if __name__ == '__main__':
    main()
