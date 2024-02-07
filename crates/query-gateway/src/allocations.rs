use std::path::Path;

use rusqlite::Transaction;
use tokio_rusqlite::Connection;

use crate::metrics;
use contract_client::Allocation;
use subsquid_network_transport::PeerId;

pub struct AllocationsManager {
    db_conn: Connection,
}

impl AllocationsManager {
    pub async fn new(db_path: impl AsRef<Path>) -> anyhow::Result<Self> {
        log::info!("Initializing allocations manager");
        let db_conn = Connection::open(&db_path).await?;
        db_conn
            .call(|conn| {
                conn.trace(Some(|s| log::trace!("SQL trace: {s}")));
                let tx = conn.transaction()?;
                tx.execute(sql::ALLOCATIONS_TABLE, ())?;
                tx.commit()?;
                Ok(())
            })
            .await?;

        Ok(Self { db_conn })
    }

    async fn db_exec<T, F>(&self, f: F) -> anyhow::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Transaction) -> rusqlite::Result<T> + Send + 'static,
    {
        self.db_conn
            .call(move |conn| {
                let mut tx = conn.transaction()?;
                let res = f(&mut tx)?;
                tx.commit()?;
                Ok(res)
            })
            .await
            .map_err(Into::into)
    }

    pub async fn try_spend_cus(&self, worker_id: PeerId, cus: u32) -> anyhow::Result<bool> {
        log::debug!("Spending {cus} compute units allocated to worker {worker_id}");
        let worker_id = worker_id.to_string();
        self.db_exec(move |tx| {
            let updated = tx.execute(sql::SPEND_CUS, (&worker_id, cus))? > 0;
            if updated {
                metrics::spend_comp_units(&worker_id, cus);
            }
            Ok(updated)
        })
        .await
    }

    pub async fn get_last_epoch(&self) -> anyhow::Result<u32> {
        self.db_exec(|tx| tx.query_row(sql::GET_EPOCH, (), |row| row.get(0)))
            .await
    }

    pub async fn update_allocations(
        &self,
        allocations: Vec<Allocation>,
        epoch: u32,
    ) -> anyhow::Result<()> {
        log::info!("Updating allocations");
        let allocations: Vec<(String, u32)> = allocations
            .into_iter()
            .map(|a| (a.worker_peer_id.to_string(), a.computation_units.as_u32()))
            .collect();

        let allocations = self
            .db_exec(move |tx| {
                tx.execute(sql::RESET_ALLOCATIONS, ())?;
                let mut update_stmt = tx.prepare(sql::UPDATE_ALLOCATION)?;
                for (worker_id, comp_units) in &allocations {
                    update_stmt.execute((worker_id, comp_units, epoch))?;
                }
                Ok(allocations)
            })
            .await?;

        metrics::new_epoch(epoch);
        metrics::update_allocations(allocations);
        Ok(())
    }

    /// Return total (available, allocated, spent) compute units
    pub async fn compute_units_summary(&self) -> anyhow::Result<(u32, u32)> {
        let (allocated, spent) = self
            .db_exec(|tx| tx.query_row(sql::GET_SUMMARY, (), |row| row.try_into()))
            .await?;
        Ok((allocated, spent))
    }
}

mod sql {

    pub const ALLOCATIONS_TABLE: &str = "
    CREATE TABLE IF NOT EXISTS worker_allocations(
        peer_id STRING PRIMARY KEY,
        allocated_cus INTEGER NOT NULL DEFAULT 0,
        spent_cus INTEGER NOT NULL DEFAULT 0,
        epoch INTEGER NOT NULL
    )";

    pub const SPEND_CUS: &str = "
    UPDATE worker_allocations
    SET spent_cus = spent_cus + ?2
    WHERE peer_id = ?1 AND allocated_cus - spent_cus >= ?2
    ";

    pub const RESET_ALLOCATIONS: &str = "DELETE FROM worker_allocations";

    pub const UPDATE_ALLOCATION: &str = "
    INSERT INTO worker_allocations (peer_id, allocated_cus, spent_cus, epoch)
    VALUES (?1, ?2, 0, ?3)
    ";

    pub const GET_EPOCH: &str = "SELECT COALESCE(MAX(epoch), 0) FROM worker_allocations";

    pub const GET_SUMMARY: &str =
        "SELECT COALESCE(sum(allocated_cus), 0), COALESCE(sum(spent_cus), 0) FROM worker_allocations";
}
