use crate::config::Config;
use contract_client::{Allocation, AllocationsClient, Worker};
use rusqlite::Transaction;
use std::collections::HashSet;
use std::path::Path;
use subsquid_network_transport::PeerId;
use tokio_rusqlite::Connection;

pub struct AllocationsManager {
    client: Box<dyn AllocationsClient>,
    db_conn: Connection,
    worker_peer_ids: HashSet<PeerId>,
    worker_onchain_ids: HashSet<u32>,
}

impl AllocationsManager {
    pub async fn new(
        client: Box<dyn AllocationsClient>,
        db_path: impl AsRef<Path>,
    ) -> anyhow::Result<Self> {
        log::info!("Initializing allocations manager");
        let db_conn = Connection::open(&db_path).await?;
        db_conn
            .call(|conn| {
                conn.trace(Some(|s| log::trace!("SQL trace: {s}")));
                let tx = conn.transaction()?;
                tx.execute(sql::ALLOCATIONS_TABLE, ())?;
                tx.execute(sql::LAST_BLOCK_TABLE, ())?;
                tx.commit()?;
                Ok(())
            })
            .await?;

        Ok(Self {
            client,
            db_conn,
            worker_peer_ids: Default::default(),
            worker_onchain_ids: Default::default(),
        })
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

    pub async fn spend_cus(&self, worker_id: PeerId, cus: u32) -> anyhow::Result<()> {
        log::info!("Spending {cus} compute units allocated to worker {worker_id}");
        anyhow::ensure!(
            self.worker_peer_ids.contains(&worker_id),
            "{worker_id} not in active workers set"
        );
        let worker_id = worker_id.to_string();
        let updated = self
            .db_exec(move |tx| tx.execute(sql::SPEND_CUS, (worker_id, cus)))
            .await?;
        anyhow::ensure!(updated > 0, "Not enough remaining computation units");
        Ok(())
    }

    pub async fn update_workers(&mut self, workers: Vec<Worker>) -> anyhow::Result<()> {
        let worker_ids: Vec<(PeerId, u32)> = workers
            .iter()
            .map(|w| (w.peer_id, w.onchain_id.as_u32()))
            .collect();
        let (peer_ids, onchain_ids) = worker_ids.iter().cloned().unzip();
        self.db_exec(move |tx| {
            let mut insert_stmt = tx.prepare(sql::UPDATE_WORKERS)?;
            for (peer_id, onchain_id) in worker_ids {
                insert_stmt.execute((peer_id.to_string(), onchain_id))?;
            }
            Ok(())
        })
        .await?;
        self.worker_peer_ids = peer_ids;
        self.worker_onchain_ids = onchain_ids;
        Ok(())
    }

    async fn save_allocations(
        &self,
        allocations: Vec<Allocation>,
        last_block: u64,
    ) -> anyhow::Result<()> {
        self.db_exec(move |tx| {
            let mut update_stmt = tx.prepare(sql::ALLOCATE_CUS)?;
            for a in allocations {
                update_stmt
                    .execute((a.worker_onchain_id.as_u32(), a.computation_units.as_u32()))?;
            }
            tx.execute(sql::UPDATE_LAST_BLOCK, (last_block,))?;
            Ok(())
        })
        .await
    }

    async fn get_last_block(&self) -> anyhow::Result<Option<u64>> {
        self.db_exec(|tx| tx.query_row(sql::GET_LAST_BLOCK, (), |row| row.get(0)))
            .await
    }

    /// Get onchain IDs of workers that need to be allocated more compute units
    async fn get_worker_ids_to_allocate(&self) -> anyhow::Result<Vec<u32>> {
        // Select workers which have less than minimum remaining CUs
        let min_cus = Config::get().compute_units.minimum;
        let mut worker_ids = self
            .db_exec(move |tx| {
                let mut select_stmt = tx.prepare(sql::GET_WORKERS_TO_ALLOCATE)?;
                let worker_ids = select_stmt
                    .query_map((min_cus,), |row| row.get(0))?
                    .collect::<rusqlite::Result<Vec<u32>>>()?;
                Ok(worker_ids)
            })
            .await?;

        // Discard workers which don't belong to the active worker set
        worker_ids.retain(|id| self.worker_onchain_ids.contains(id));
        Ok(worker_ids)
    }

    pub async fn update_allocations(&self) -> anyhow::Result<()> {
        log::info!("Updating allocations");
        // Save allocations from blockchain into database
        let from_block = self.get_last_block().await?.map(|x| x + 1);
        let (allocations, last_block) = self.client.get_allocations(from_block).await?;
        log::info!("{} allocations retrieved from chain", allocations.len());
        log::debug!("Allocations retrieved: {allocations:?}");
        self.save_allocations(allocations, last_block).await?;

        // Make new allocations if necessary
        let worker_ids_to_allocate = self.get_worker_ids_to_allocate().await?;
        let cus_to_allocate = Config::get().compute_units.allocate.into();
        log::info!("{} workers need allocation", worker_ids_to_allocate.len());
        let allocations = worker_ids_to_allocate
            .into_iter()
            .map(|id| Allocation {
                worker_onchain_id: id.into(),
                computation_units: cus_to_allocate,
            })
            .collect();
        self.client.allocate_cus(allocations).await?;
        Ok(())
    }
}

mod sql {

    pub const ALLOCATIONS_TABLE: &str = "
    CREATE TABLE IF NOT EXISTS worker_allocations(
        peer_id STRING PRIMARY KEY,
        onchain_id INTEGER NOT NULL UNIQUE,
        allocated_cus INTEGER NOT NULL DEFAULT 0,
        spent_cus INTEGER NOT NULL DEFAULT 0
    )";

    pub const LAST_BLOCK_TABLE: &str = "
    CREATE TABLE IF NOT EXISTS last_scanned_block AS SELECT NULL AS block_no
    ";

    pub const GET_LAST_BLOCK: &str = "SELECT block_no FROM last_scanned_block";

    pub const UPDATE_LAST_BLOCK: &str = "UPDATE last_scanned_block SET block_no = ?1";

    pub const SPEND_CUS: &str = "
    UPDATE worker_allocations
    SET spent_cus = spent_cus + ?2
    WHERE peer_id = ?1 AND allocated_cus - spent_cus >= ?2
    ";

    pub const UPDATE_WORKERS: &str = "
    INSERT OR IGNORE INTO worker_allocations (peer_id, onchain_id) VALUES (?1, ?2)
    ";

    pub const ALLOCATE_CUS: &str = "
    UPDATE worker_allocations
    SET allocated_cus = allocated_cus + ?2
    WHERE onchain_id = ?1
    ";

    pub const GET_WORKERS_TO_ALLOCATE: &str = "
    SELECT onchain_id FROM worker_allocations
    WHERE (allocated_cus - spent_cus) < ?1
    ";
}
