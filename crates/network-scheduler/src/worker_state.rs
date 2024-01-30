use crate::cli::Config;
use crate::data_chunk::DataChunk;
use crate::scheduling_unit::{SchedulingUnit, UnitId};
use contract_client::Address;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, TimestampMilliSeconds};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::time::{Duration, SystemTime};
use subsquid_messages::{PingV2 as Ping, RangeSet};
use subsquid_network_transport::PeerId;

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerState {
    pub peer_id: PeerId,
    pub address: Address,
    #[serde_as(as = "TimestampMilliSeconds")]
    #[serde(default = "SystemTime::now")]
    pub last_ping: SystemTime,
    pub version: Option<String>,
    pub jailed: bool,
    pub assigned_units: HashSet<UnitId>,
    pub assigned_bytes: u64, // Can be outdated, source of truth is assigned_units
    pub stored_ranges: HashMap<String, RangeSet>, // dataset -> ranges
    pub stored_bytes: u64,
    #[serde(default)]
    pub num_missing_chunks: u32,
    #[serde_as(as = "TimestampMilliSeconds")]
    #[serde(default = "SystemTime::now")]
    pub last_assignment: SystemTime,
    #[serde_as(as = "TimestampMilliSeconds")]
    #[serde(default = "SystemTime::now")]
    pub last_successful_dial: SystemTime,
    #[serde_as(as = "TimestampMilliSeconds")]
    #[serde(default = "SystemTime::now")]
    pub last_dial_time: SystemTime,
    #[serde(default = "def_true")]
    pub last_dial_ok: bool,
}

fn def_true() -> bool {
    true
}

impl WorkerState {
    pub fn new(peer_id: PeerId, address: Address) -> Self {
        Self {
            peer_id,
            address,
            last_ping: SystemTime::now(),
            version: None,
            jailed: false,
            assigned_units: HashSet::new(),
            stored_ranges: HashMap::new(),
            stored_bytes: 0,
            assigned_bytes: 0,
            num_missing_chunks: 0,
            last_assignment: SystemTime::now(),
            last_successful_dial: SystemTime::now(),
            last_dial_time: SystemTime::now(),
            last_dial_ok: false,
        }
    }

    fn time_since_last_ping(&self) -> Duration {
        self.last_ping.elapsed().expect("Time doesn't go backwards")
    }

    /// Register ping msg from a worker.
    pub fn ping(&mut self, msg: Ping) {
        self.last_ping = SystemTime::now();
        self.version = msg.version;
        self.stored_ranges = msg
            .stored_ranges
            .into_iter()
            .map(|r| (r.url, r.ranges.into()))
            .collect();
        self.stored_bytes = msg.stored_bytes.unwrap_or_default();
    }

    pub fn is_active(&self) -> bool {
        self.time_since_last_ping() < Config::get().worker_inactive_timeout
    }

    pub fn is_unreachable(&self) -> bool {
        // Worker is considered unreachable if it hasn't been successfully dialed
        // for at least `worker_unreachable_timeout`
        !self.last_dial_ok
            && self
                .last_successful_dial
                .elapsed()
                .expect("time doesn't go backwards")
                > Config::get().worker_unreachable_timeout
    }

    pub fn remaining_capacity(&self) -> u64 {
        Config::get()
            .worker_storage_bytes
            .saturating_sub(self.assigned_bytes)
    }

    pub fn try_assign_unit(&mut self, unit_id: UnitId, unit_size: u64) -> bool {
        if unit_size > self.remaining_capacity() {
            return false; // Not enough capacity
        }
        if self.assigned_units.insert(unit_id) {
            self.assigned_bytes += unit_size;
            return true; // Successfully assigned
        }
        false // Unit was already assigned before
    }

    pub fn remove_unit(&mut self, unit_id: &UnitId, unit_size: u64) {
        if self.assigned_units.remove(unit_id) {
            self.assigned_bytes -= unit_size;
        }
    }

    /// Assigned unit's size has increased. Unassing the unit if it doesn't fit anymore.
    /// Return true iff the unit remained assigned.
    pub fn try_expand_unit(&mut self, unit_id: &UnitId, old_size: u64, new_size: u64) -> bool {
        let size_diff = new_size - old_size;
        if self.remaining_capacity() > size_diff {
            self.assigned_bytes += size_diff;
            true
        } else {
            assert!(self.assigned_units.remove(unit_id));
            self.assigned_bytes -= old_size;
            false
        }
    }

    pub fn assigned_chunks<'a>(
        &'a self,
        units_map: &'a HashMap<UnitId, SchedulingUnit>,
    ) -> impl Iterator<Item = DataChunk> + 'a {
        self.assigned_units.iter().flat_map(|unit_id| {
            units_map
                .get(unit_id)
                .expect("Unknown scheduling unit")
                .clone()
        })
    }

    fn count_missing_chunks<'a>(&'a self, units: &'a HashMap<UnitId, SchedulingUnit>) -> u32 {
        self.assigned_chunks(units)
            .map(|chunk| match self.stored_ranges.get(&chunk.dataset_url) {
                Some(range_set) if range_set.includes(chunk.block_range) => 0,
                _ => 1,
            })
            .sum()
    }

    /// Check if the worker is making progress with downloading missing chunks.
    /// Returns true iff the worker is fully synced or making progress.
    pub fn check_download_progress<'a>(
        &'a mut self,
        units: &'a HashMap<UnitId, SchedulingUnit>,
    ) -> bool {
        assert!(!self.jailed);
        if self
            .last_assignment
            .elapsed()
            .is_ok_and(|d| d < Config::get().worker_stale_timeout)
        {
            return true;
        }

        let num_missing_chunks = self.count_missing_chunks(units);
        if num_missing_chunks == 0 {
            log::debug!("Worker {} is fully synced", self.peer_id);
            self.num_missing_chunks = num_missing_chunks;
            true
        } else if num_missing_chunks < self.num_missing_chunks {
            log::debug!(
                "Worker {} is making progress {} -> {} chunks missing",
                self.peer_id,
                self.num_missing_chunks,
                num_missing_chunks
            );
            self.num_missing_chunks = num_missing_chunks;
            true
        } else {
            log::debug!(
                "Worker {} has not downloaded any chunks since last check",
                self.peer_id
            );
            false
        }
    }

    pub fn reset_download_progress<'a>(&'a mut self, units: &'a HashMap<UnitId, SchedulingUnit>) {
        self.num_missing_chunks = self.count_missing_chunks(units);
        self.last_assignment = SystemTime::now();
    }

    /// Jail the worker, unassign all units and return their IDs.
    pub fn jail(&mut self) -> Vec<UnitId> {
        log::info!("Jailing worker {}", self.peer_id);
        self.jailed = true;
        self.assigned_bytes = 0;
        self.num_missing_chunks = 0;
        self.assigned_units.drain().collect()
    }

    pub fn release(&mut self) {
        log::info!("Releasing worker {}", self.peer_id);
        self.jailed = false;
    }
}

impl Display for WorkerState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {} units assigned ({} bytes)",
            self.peer_id,
            self.assigned_units.len(),
            self.assigned_bytes,
        )
    }
}