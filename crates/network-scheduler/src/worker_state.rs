use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::time::{Duration, SystemTime};

use log::{debug, error};
use serde::{Deserialize, Serialize};
use serde_partial::SerializePartial;
use serde_with::{serde_as, TimestampMilliSeconds};

use dashmap::DashMap;
use sqd_contract_client::Address;
use sqd_messages::Heartbeat;
use sqd_network_transport::PeerId;

use crate::cli::Config;
use crate::scheduling_unit::{SchedulingUnit, UnitId};

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, SerializePartial)]
pub struct WorkerState {
    pub peer_id: PeerId,
    pub address: Address,
    #[serde_as(as = "Option<TimestampMilliSeconds>")]
    pub last_ping: Option<SystemTime>,
    pub version: Option<String>,
    pub jailed: bool,
    pub assigned_units: HashSet<UnitId>,
    pub assigned_bytes: u64, // Can be outdated, source of truth is assigned_units
    pub stored_bytes: u64,
    pub num_missing_chunks: u32,
    #[serde(skip)]
    pub num_missing_chunks_on_heartbeat: u32,
    #[serde_as(as = "Option<TimestampMilliSeconds>")]
    pub last_assignment: Option<SystemTime>,
    #[serde_as(as = "Option<TimestampMilliSeconds>")]
    pub last_dial_time: Option<SystemTime>,
    pub last_dial_ok: bool,
    #[serde_as(as = "Option<TimestampMilliSeconds>")]
    #[serde(default)]
    pub unreachable_since: Option<SystemTime>,
    #[serde(default)]
    pub jail_reason: Option<JailReason>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum JailReason {
    Inactive,
    Unreachable,
    Stale,
}

impl Display for JailReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            JailReason::Inactive => write!(
                f,
                "Worker didn't send pings for over {} seconds",
                Config::get().worker_inactive_timeout.as_secs()
            ),
            JailReason::Unreachable => write!(f, "Worker could not be reached on a public address"),
            JailReason::Stale => write!(
                f,
                "Worker didn't download any of the assigned chunks trough {} seconds",
                Config::get().worker_stale_timeout.as_secs()
            ),
        }
    }
}

impl WorkerState {
    pub fn new(peer_id: PeerId, address: Address) -> Self {
        Self {
            peer_id,
            address,
            last_ping: None,
            version: None,
            jailed: false,
            assigned_units: HashSet::new(),
            stored_bytes: 0,
            assigned_bytes: 0,
            num_missing_chunks: 0,
            num_missing_chunks_on_heartbeat: 0,
            last_assignment: None,
            last_dial_time: None,
            last_dial_ok: false,
            unreachable_since: None,
            jail_reason: None,
        }
    }

    fn time_since_last_ping(&self) -> Option<Duration> {
        self.last_ping
            .map(|t| t.elapsed().expect("Time doesn't go backwards"))
    }

    /// Register heartbeat msg from a worker.
    pub fn heartbeat(&mut self, msg: Heartbeat) {
        let Some(missing_chunks) = msg.missing_chunks else {
            error!("Got no missing chunks info from {}", self.peer_id);
            return;
        };
        self.last_ping = Some(SystemTime::now());
        self.version = Some(msg.version);
        self.num_missing_chunks_on_heartbeat = missing_chunks.ones() as u32;
        debug!(
            "Got {} missing chunks for {}",
            self.num_missing_chunks_on_heartbeat, self.peer_id
        );
        self.stored_bytes = msg.stored_bytes.unwrap_or(0);
    }

    pub fn dialed(&mut self, reachable: bool) {
        let now = SystemTime::now();
        self.last_dial_time = Some(now);
        self.last_dial_ok = reachable;
        if reachable {
            self.unreachable_since = None
        } else if self.unreachable_since.is_none() {
            self.unreachable_since = Some(now)
        }
    }

    pub fn is_active(&self) -> bool {
        self.time_since_last_ping()
            .is_some_and(|t| t < Config::get().worker_inactive_timeout)
    }

    pub fn ever_been_active(&self) -> bool {
        self.last_ping.is_some()
    }

    pub fn is_unreachable(&self) -> bool {
        // Worker is considered unreachable if it hasn't been successfully dialed
        // for at least `worker_unreachable_timeout`
        self.unreachable_since.is_some_and(|t| {
            t.elapsed().expect("time doesn't go backwards")
                > Config::get().worker_unreachable_timeout
        })
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

    /// Assigned unit's size has increased. Unassign the unit if it doesn't fit anymore.
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

    /// Check if the worker is making progress with downloading missing chunks.
    /// Returns true iff the worker is fully synced or making progress.
    pub fn check_download_progress(&mut self, _units: &DashMap<UnitId, SchedulingUnit>) -> bool {
        assert!(!self.jailed);
        let Some(last_assignment) = self.last_assignment.as_ref() else {
            return true; // worker doesn't have any assignment
        };
        if last_assignment
            .elapsed()
            .is_ok_and(|d| d < Config::get().worker_stale_timeout)
        {
            return true;
        }

        let current_missing_chunks = self.num_missing_chunks_on_heartbeat;
        if current_missing_chunks == 0 {
            log::debug!("Worker {} is fully synced", self.peer_id);
            self.num_missing_chunks = current_missing_chunks;
            true
        } else if current_missing_chunks < self.num_missing_chunks {
            log::debug!(
                "Worker {} is making progress {} -> {} chunks missing",
                self.peer_id,
                self.num_missing_chunks,
                current_missing_chunks
            );
            self.num_missing_chunks = current_missing_chunks;
            true
        } else {
            log::debug!(
                "Worker {} has not downloaded any chunks since last check",
                self.peer_id
            );
            false
        }
    }

    pub fn reset_download_progress(&mut self, units: &DashMap<UnitId, SchedulingUnit>) {
        self.num_missing_chunks = self
            .assigned_units
            .iter()
            .map(|unit| units.get(unit).map_or(0, |u| u.num_chunks()))
            .sum::<usize>() as u32;
        self.last_assignment = Some(SystemTime::now());
    }

    /// Jail the worker, unassign all units and return their IDs.
    pub fn jail(&mut self, reason: JailReason) -> Vec<UnitId> {
        log::info!("Jailing worker {}", self.peer_id);
        self.jailed = true;
        self.jail_reason = Some(reason);
        self.assigned_bytes = 0;
        self.num_missing_chunks = 0;
        self.assigned_units.drain().collect()
    }

    pub fn release(&mut self) {
        log::info!("Releasing worker {}", self.peer_id);
        self.jailed = false;
        self.jail_reason = None;
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
