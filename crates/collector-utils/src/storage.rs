use std::collections::HashMap;

use async_trait::async_trait;
use clickhouse::{Client, Row};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use sqd_messages::signatures::sha3_256;
use sqd_messages::{query_error, query_executed, query_finished, Heartbeat, QueryExecuted, QueryFinished};
use sqd_network_transport::{protocol, PeerId};

use crate::cli::ClickhouseArgs;
use crate::{base64, timestamp_now_ms};

lazy_static! {
    static ref LOGS_TABLE: String =
        std::env::var("LOGS_TABLE").unwrap_or("worker_query_logs".to_string());
    static ref PINGS_TABLE: String =
        std::env::var("PINGS_TABLE").unwrap_or("worker_pings_v2".to_string());
    static ref PORTAL_LOGS_TABLE: String =
        std::env::var("PORTAL_LOGS_TABLE").unwrap_or("portal_logs".to_string());
    static ref LOGS_TABLE_DEFINITION: String = format!(
        "
        CREATE TABLE IF NOT EXISTS {}
        (
            client_id String NOT NULL,
            worker_id String NOT NULL,
            query_id String NOT NULL,
            dataset String NOT NULL,
            dataset_id String NOT NULL DEFAULT '',
            request_id String NOT NULL,
            from_block Nullable(UInt64),
            to_block Nullable(UInt64),
            chunk_id String NOT NULL DEFAULT '',
            query String NOT NULL,
            query_hash String NOT NULL,
            exec_time UInt32 NOT NULL DEFAULT 0,
            exec_time_ms UInt32 NOT NULL,
            result Enum8(
                'ok' = 1,
                'bad_request' = 2,
                'server_error' = 3,
                'not_found' = 4,
                'server_overloaded' = 5,
                'too_many_requests' = 6
            ) NOT NULL,
            num_read_chunks UInt32 NOT NULL DEFAULT 0,
            output_size UInt32 NOT NULL DEFAULT 0,
            output_hash String NOT NULL DEFAULT '',
            last_block Nullable(UInt64),
            error_msg String NOT NULL DEFAULT '',
            client_signature String NOT NULL,
            client_timestamp DateTime64(3) NOT NULL CODEC(DoubleDelta, ZSTD),
            worker_timestamp DateTime64(3) NOT NULL CODEC(DoubleDelta, ZSTD),
            collector_timestamp DateTime64(3) NOT NULL CODEC(DoubleDelta, ZSTD),
            worker_version LowCardinality(String) NOT NULL DEFAULT ''
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(worker_timestamp)
        ORDER BY (worker_timestamp, worker_id);
    ",
        &*LOGS_TABLE
    );
    static ref PINGS_TABLE_DEFINITION: String = format!(
        "
        CREATE TABLE IF NOT EXISTS {}
        (
            timestamp DateTime64(3) NOT NULL CODEC(DoubleDelta, ZSTD),
            worker_id String NOT NULL,
            stored_bytes UInt64 NOT NULL CODEC(Delta, ZSTD),
            version LowCardinality(TEXT) NOT NULL
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (timestamp, worker_id);
    ",
        &*PINGS_TABLE
    );
    static ref PORTAL_LOGS_TABLE_DEFINITION: String = format!(
        "
        CREATE TABLE IF NOT EXISTS {}
        (
            query_id String NOT NULL,
            worker_id String NOT NULL,
            result_hash String NOT NULL DEFAULT '',
            worker_signature String NOT NULL,
            total_time UInt32 NOT NULL,
            collector_timestamp DateTime64(3) NOT NULL CODEC(DoubleDelta, ZSTD),
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(collector_timestamp)
        ORDER BY (collector_timestamp, worker_id);
    ",
        &*PORTAL_LOGS_TABLE
    );
}

#[async_trait]
pub trait Storage {
    async fn store_logs<T: Iterator<Item = QueryExecutedRow> + Sized + Send>(
        &self,
        query_logs: T,
    ) -> anyhow::Result<()>;

    async fn store_pings<T: Iterator<Item = PingRow> + Sized + Send>(
        &self,
        pings: T,
    ) -> anyhow::Result<()>;

    /// Get timestamp of the last stored log for each worker
    async fn get_last_stored(&self) -> anyhow::Result<HashMap<String, u64>>;

    async fn store_portal_logs<T: Iterator<Item = QueryFinishedRow> + Sized + Send>(
        &self,
        portal_logs: T,
    ) -> anyhow::Result<()>;
}

pub struct ClickhouseStorage(Client);

#[derive(Debug, Clone, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
enum QueryResult {
    Ok = 1,
    BadRequest = 2,
    ServerError = 3,
    NotFound = 4,
    ServerOverloaded = 5,
    TooManyRequests = 6,
}

#[derive(Row, Debug, Clone, Serialize, Deserialize)]
pub struct QueryExecutedRow {
    client_id: String,
    worker_id: String,
    query_id: String,
    dataset: String,
    dataset_id: String,
    request_id: String,
    from_block: Option<u64>,
    to_block: Option<u64>,
    chunk_id: String,
    query: String,
    #[serde(with = "serde_bytes")]
    query_hash: Vec<u8>,
    exec_time: u32,
    exec_time_ms: u32,
    result: QueryResult,
    num_read_chunks: u32,
    output_size: u32,
    #[serde(with = "serde_bytes")]
    output_hash: Vec<u8>,
    last_block: Option<u64>,
    error_msg: String,
    #[serde(with = "serde_bytes")]
    client_signature: Vec<u8>,
    pub client_timestamp: u64,
    pub worker_timestamp: u64,
    collector_timestamp: u64,
    worker_version: String,
}

impl QueryExecutedRow {
    pub fn try_from(
        query_executed: QueryExecuted,
        worker_id: PeerId,
    ) -> Result<Self, &'static str> {
        let query = query_executed.query.ok_or("Query field missing")?;
        let result = query_executed.result.ok_or("Result field missing")?;
        let client_id = query_executed
            .client_id
            .parse()
            .map_err(|_| "Invalid client_id")?;
        let collector_timestamp = timestamp_now_ms();

        if query.timestamp_ms.abs_diff(query_executed.timestamp_ms) as u128
            > protocol::MAX_TIME_LAG.as_millis()
        {
            return Err("Invalid worker timestamp");
        }
        if !query.verify_signature(client_id, worker_id) {
            return Err("Invalid client signature");
        }

        let query_result;
        let num_read_chunks;
        let output_size;
        let output_hash;
        let error_msg;
        let last_block;
        match result {
            query_executed::Result::Ok(ok) => {
                query_result = QueryResult::Ok;
                num_read_chunks = 1;
                output_size = ok.uncompressed_data_size as u32;
                output_hash = ok.data_hash;
                error_msg = Default::default();
                last_block = Some(ok.last_block);
            }
            query_executed::Result::Err(err) => {
                num_read_chunks = 0;
                output_size = 0;
                output_hash = Default::default();
                last_block = None;
                match err.err.ok_or("Unknown error type")? {
                    query_error::Err::BadRequest(err_msg) => {
                        query_result = QueryResult::BadRequest;
                        error_msg = err_msg;
                    }
                    query_error::Err::NotFound(err_msg) => {
                        query_result = QueryResult::NotFound;
                        error_msg = err_msg;
                    }
                    query_error::Err::ServerError(err_msg) => {
                        query_result = QueryResult::ServerError;
                        error_msg = err_msg;
                    }
                    query_error::Err::ServerOverloaded(()) => {
                        query_result = QueryResult::ServerOverloaded;
                        error_msg = Default::default();
                    }
                    query_error::Err::TooManyRequests(()) => {
                        query_result = QueryResult::TooManyRequests;
                        error_msg = Default::default();
                    }
                }
            }
        }

        Ok(Self {
            client_id: query_executed.client_id,
            worker_id: worker_id.to_string(),
            query_id: query.query_id,
            dataset: base64(&query.dataset),
            dataset_id: query.dataset,
            request_id: query.request_id,
            from_block: query.block_range.map(|r| r.begin),
            to_block: query.block_range.map(|r| r.end),
            chunk_id: query.chunk_id,
            query_hash: sha3_256(query.query.as_bytes()).to_vec(),
            query: query.query,
            exec_time: query_executed.exec_time_micros,
            exec_time_ms: query_executed.exec_time_micros / 1000,
            result: query_result,
            num_read_chunks,
            output_size,
            output_hash,
            last_block,
            error_msg,
            client_signature: query.signature,
            client_timestamp: query.timestamp_ms,
            worker_timestamp: query_executed.timestamp_ms,
            collector_timestamp,
            worker_version: query_executed.worker_version,
        })
    }
}

#[derive(Row, Debug, Clone, Serialize, Deserialize)]
pub struct PingRow {
    timestamp: u64,
    worker_id: String,
    stored_bytes: u64,
    version: String,
}

impl PingRow {
    pub fn new(heartbeat: Heartbeat, worker_id: String) -> Result<Self, &'static str> {
        Ok(Self {
            stored_bytes: heartbeat.stored_bytes(),
            worker_id,
            version: heartbeat.version,
            timestamp: timestamp_now_ms(),
        })
    }
}

#[derive(Row, Debug, Deserialize)]
struct TimestampRow {
    worker_id: String,
    timestamp: u64,
}

#[derive(Row, Debug, Clone, Serialize, Deserialize)]
pub struct QueryFinishedRow {
    query_id: String,
    worker_id: String,
    #[serde(with = "serde_bytes")]
    result_hash: Vec<u8>,
    #[serde(with = "serde_bytes")]
    worker_signature: Vec<u8>,
    total_time: u32,
    collector_timestamp: u64,
}

impl QueryFinishedRow {
    pub fn try_from(
        query_finished: QueryFinished,
    ) -> Result<Self, &'static str> {
        let QueryFinished {
            worker_id,
            query_id,
            total_time_micros: total_time,
            worker_signature,
            result
        } = query_finished;
        let result_hash = match result {
            Some(query_finished::Result::Ok(ok)) => ok.data_hash,
            Some(_) => Default::default(),
            None => Default::default(),
        };
        let collector_timestamp = timestamp_now_ms();

        Ok(Self {
            query_id,
            worker_id,
            result_hash,
            worker_signature,
            total_time,
            collector_timestamp,
        })
    }
}


impl ClickhouseStorage {
    pub async fn new(args: ClickhouseArgs) -> anyhow::Result<Self> {
        let client = Client::default()
            .with_url(args.clickhouse_url)
            .with_database(args.clickhouse_database)
            .with_user(args.clickhouse_user)
            .with_password(args.clickhouse_password);
        client.query(&LOGS_TABLE_DEFINITION).execute().await?;
        client.query(&PINGS_TABLE_DEFINITION).execute().await?;
        Ok(Self(client))
    }
}

#[async_trait]
impl Storage for ClickhouseStorage {
    async fn store_logs<T: Iterator<Item = QueryExecutedRow> + Sized + Send>(
        &self,
        query_logs: T,
    ) -> anyhow::Result<()> {
        log::debug!("Storing logs in clickhouse");
        let mut insert = self.0.insert(&LOGS_TABLE)?;
        for row in query_logs {
            log::trace!("Storing query log {:?}", row);
            insert.write(&row).await?;
        }
        insert.end().await?;
        Ok(())
    }

    async fn store_pings<T: Iterator<Item = PingRow> + Sized + Send>(
        &self,
        pings: T,
    ) -> anyhow::Result<()> {
        log::debug!("Storing pings in clickhouse");
        let mut insert = self.0.insert(&PINGS_TABLE)?;
        for row in pings {
            log::trace!("Storing ping {:?}", row);
            insert.write(&row).await?;
        }
        insert.end().await?;
        Ok(())
    }

    async fn get_last_stored(&self) -> anyhow::Result<HashMap<String, u64>> {
        log::debug!("Retrieving latest timestamps from clickhouse");
        let mut cursor = self
            .0
            .query(&format!(
                "SELECT worker_id, MAX(worker_timestamp) FROM {} GROUP BY worker_id",
                &*LOGS_TABLE
            ))
            .fetch::<TimestampRow>()?;
        let mut result = HashMap::new();
        while let Some(row) = cursor.next().await? {
            result.insert(row.worker_id, row.timestamp);
        }
        log::debug!("Retrieved timestamps: {:?}", result);
        Ok(result)
    }

    async fn store_portal_logs<T: Iterator<Item = QueryFinishedRow> + Sized + Send>(
        &self,
        portal_logs: T,
    ) -> anyhow::Result<()> {
        log::debug!("Storing portal logs in clickhouse");
        let mut insert = self.0.insert(&PORTAL_LOGS_TABLE)?;
        for row in portal_logs {
            log::trace!("Storing portal log {:?}", row);
            insert.write(&row).await?;
        }
        insert.end().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use sqd_messages::{Query, QueryOkSummary};
    use sqd_network_transport::{Keypair, PeerId};

    use super::*;

    // To run this test, start a local clickhouse instance first
    // docker run --rm \
    //   -e CLICKHOUSE_DB=logs_db \
    //   -e CLICKHOUSE_USER=user \
    //   -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
    //   -e CLICKHOUSE_PASSWORD=password \
    //   --network=host \
    //   --ulimit nofile=262144:262144 \
    //   clickhouse/clickhouse-server
    // And set `STORAGE_TEST` env variable to a non-empty value
    #[test_with::env(STORAGE_TEST)]
    #[tokio::test]
    async fn test_storage() {
        let storage = ClickhouseStorage::new(ClickhouseArgs {
            clickhouse_url: "http://localhost:8123/".to_string(),
            clickhouse_database: "logs_db".to_string(),
            clickhouse_user: "user".to_string(),
            clickhouse_password: "password".to_string(),
        })
        .await
        .expect("Cannot connect to clickhouse");

        // Clean up database
        storage
            .0
            .query(&format!("TRUNCATE TABLE {}", &*LOGS_TABLE))
            .execute()
            .await
            .unwrap();
        storage
            .0
            .query(&format!("TRUNCATE TABLE {}", &*PINGS_TABLE))
            .execute()
            .await
            .unwrap();

        let client_keypair = Keypair::from_protobuf_encoding(&[
            8, 1, 18, 64, 246, 13, 52, 78, 165, 229, 195, 19, 180, 208, 225, 55, 240, 115, 159, 6,
            9, 123, 239, 172, 245, 55, 141, 57, 41, 185, 78, 191, 141, 74, 8, 242, 152, 79, 38,
            199, 39, 192, 209, 175, 147, 85, 150, 22, 192, 22, 89, 173, 61, 11, 207, 219, 48, 43,
            48, 151, 232, 105, 234, 80, 19, 205, 172, 92,
        ])
        .unwrap();
        let worker_keypair = Keypair::from_protobuf_encoding(&[
            8, 1, 18, 64, 212, 50, 184, 182, 239, 153, 10, 166, 254, 122, 105, 16, 51, 223, 126,
            105, 10, 134, 204, 224, 42, 135, 92, 76, 32, 60, 197, 56, 128, 22, 131, 84, 233, 166,
            242, 11, 16, 14, 160, 254, 4, 185, 170, 32, 157, 3, 144, 53, 230, 39, 150, 221, 142, 2,
            37, 101, 100, 63, 24, 116, 110, 6, 156, 78,
        ])
        .unwrap();

        let client_id = PeerId::from_public_key(&client_keypair.public());
        let worker_id = PeerId::from_public_key(&worker_keypair.public());

        let mut query = Query {
            query_id: "b14371f9-2463-49cb-9e60-f2f62283b1af".to_string(),
            dataset: "dataset".to_string(),
            request_id: "my-request-id".to_string(),
            query: r#"{"from": "0"}"#.to_string(),
            block_range: Some(sqd_messages::Range {
                begin: 808650,
                end: 808800,
            }),
            chunk_id: "0000000000/0000808640-0000816499-b0486318".to_string(),
            timestamp_ms: 123456789000,
            signature: vec![],
        };
        query.sign(&client_keypair, worker_id).unwrap();

        let query_log = QueryExecuted {
            client_id: client_id.to_string(),
            query: Some(query),
            exec_time_micros: 2137000,
            timestamp_ms: 123456789500,
            result: Some(query_executed::Result::Ok(QueryOkSummary {
                uncompressed_data_size: 666,
                data_hash: vec![0xbe, 0xbe, 0xf0, 0x00],
                last_block: 808800,
            })),
            worker_version: "1.0.0".to_string(),
        };

        storage
            .store_logs(std::iter::once(
                QueryExecutedRow::try_from(query_log.clone(), worker_id).unwrap(),
            ))
            .await
            .unwrap();

        // Verify the last stored timestamp
        let last_stored = storage.get_last_stored().await.unwrap();
        assert_eq!(last_stored.get(&worker_id.to_string()), Some(&123456789500));

        // Check pings storing
        let ping = Heartbeat {
            version: "1.0.0".to_string(),
            stored_bytes: Some(1024),
            assignment_id: Default::default(),
            missing_chunks: Default::default(),
        };
        let ts = timestamp_now_ms();
        storage
            .store_pings(std::iter::once(
                PingRow::new(ping.clone(), worker_id.to_string()).unwrap(),
            ))
            .await
            .unwrap();

        let mut cursor = storage
            .0
            .query(&format!("SELECT * FROM {}", &*PINGS_TABLE))
            .fetch::<PingRow>()
            .unwrap();
        let row = cursor.next().await.unwrap().unwrap();
        assert_eq!(ping.version, row.version);
        assert_eq!(ping.stored_bytes.unwrap(), row.stored_bytes);
        assert!(row.timestamp >= ts);
        assert!(row.timestamp <= timestamp_now_ms());
    }
}
