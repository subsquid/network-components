use std::collections::HashMap;

use async_trait::async_trait;
use clickhouse::{Client, Row};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use subsquid_messages::{query_executed, InputAndOutput, Ping, Query, QueryExecuted, SizeAndHash};

use crate::cli::ClickhouseArgs;
use crate::timestamp_now_ms;

lazy_static! {
    static ref LOGS_TABLE: String =
        std::env::var("LOGS_TABLE").unwrap_or("worker_query_logs".to_string());
    static ref PINGS_TABLE: String =
        std::env::var("PINGS_TABLE").unwrap_or("worker_pings_v2".to_string());
    static ref LOGS_TABLE_DEFINITION: String = format!(
        "
        CREATE TABLE IF NOT EXISTS {}
        (
            client_id String NOT NULL,
            worker_id String NOT NULL,
            query_id String NOT NULL,
            dataset String NOT NULL,
            query String NOT NULL,
            profiling Boolean NOT NULL,
            client_state_json String NOT NULL,
            query_hash String NOT NULL,
            exec_time_ms UInt32 NOT NULL,
            result Enum8('ok' = 1, 'bad_request' = 2, 'server_error' = 3) NOT NULL,
            num_read_chunks UInt32 NOT NULL DEFAULT 0,
            output_size UInt32 NOT NULL DEFAULT 0,
            output_hash String NOT NULL DEFAULT '',
            error_msg String NOT NULL DEFAULT '',
            seq_no UInt64 NOT NULL,
            client_signature String NOT NULL,
            worker_signature String NOT NULL,
            worker_timestamp DateTime64(3) NOT NULL CODEC(DoubleDelta, ZSTD),
            collector_timestamp DateTime64(3) NOT NULL CODEC(DoubleDelta, ZSTD)
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
}

#[async_trait]
pub trait Storage {
    async fn store_logs<'a, T: Iterator<Item = QueryExecutedRow> + Sized + Send>(
        &self,
        query_logs: T,
    ) -> anyhow::Result<()>;

    async fn store_pings<'a, T: Iterator<Item = PingRow> + Sized + Send>(
        &self,
        pings: T,
    ) -> anyhow::Result<()>;

    /// Get the sequence number & timestamp of the last stored log for each worker
    async fn get_last_stored(&self) -> anyhow::Result<HashMap<String, (u64, u64)>>;
}

pub struct ClickhouseStorage(Client);

#[derive(Debug, Clone, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
enum QueryResult {
    Ok = 1,
    BadRequest = 2,
    ServerError = 3,
}

#[derive(Row, Debug, Clone, Serialize, Deserialize)]
pub struct QueryExecutedRow {
    client_id: String,
    worker_id: String,
    query_id: String,
    dataset: String,
    query: String,
    profiling: bool,
    client_state_json: String,
    #[serde(with = "serde_bytes")]
    query_hash: Vec<u8>,
    exec_time_ms: u32,
    result: QueryResult,
    num_read_chunks: u32,
    output_size: u32,
    #[serde(with = "serde_bytes")]
    output_hash: Vec<u8>,
    error_msg: String,
    pub seq_no: u64,
    #[serde(with = "serde_bytes")]
    client_signature: Vec<u8>,
    #[serde(with = "serde_bytes")]
    worker_signature: Vec<u8>,
    pub worker_timestamp: u64,
    collector_timestamp: u64,
}

impl TryFrom<QueryExecuted> for QueryExecutedRow {
    type Error = &'static str;

    fn try_from(query_executed: QueryExecuted) -> Result<Self, Self::Error> {
        let query = query_executed.query.ok_or("Query field missing")?;
        let result = query_executed.result.ok_or("Result field missing")?;
        let collector_timestamp = timestamp_now_ms();
        let (result, num_read_chunks, output_size, output_hash, error_msg) = match result {
            query_executed::Result::Ok(res) => {
                let output = res.output.ok_or("Output field missing")?;
                (
                    QueryResult::Ok,
                    res.num_read_chunks,
                    output.size,
                    output.sha3_256,
                    "".to_string(),
                )
            }
            query_executed::Result::BadRequest(err_msg) => (
                QueryResult::BadRequest,
                Some(0u32),
                Some(0u32),
                vec![],
                err_msg,
            ),
            query_executed::Result::ServerError(err_msg) => (
                QueryResult::ServerError,
                Some(0u32),
                Some(0u32),
                vec![],
                err_msg,
            ),
        };
        Ok(Self {
            client_id: query_executed.client_id,
            worker_id: query_executed.worker_id,
            query_id: query.query_id.ok_or("query_id field missing")?,
            dataset: query.dataset.ok_or("dataset field missing")?,
            query: query.query.ok_or("query field missing")?,
            profiling: query.profiling.ok_or("profiling field missing")?,
            client_state_json: query.client_state_json.unwrap(),
            query_hash: query_executed.query_hash,
            exec_time_ms: query_executed
                .exec_time_ms
                .ok_or("exec_time field missing")?,
            result,
            num_read_chunks: num_read_chunks.ok_or("num_read_chunks field missing")?,
            output_size: output_size.ok_or("output_size field missing")?,
            output_hash,
            error_msg,
            seq_no: query_executed.seq_no.ok_or("seq_no field missing")?,
            client_signature: query.signature,
            worker_signature: query_executed.signature,
            worker_timestamp: query_executed
                .timestamp_ms
                .ok_or("timestamp field missing")?,
            collector_timestamp,
        })
    }
}

impl From<QueryExecutedRow> for QueryExecuted {
    fn from(row: QueryExecutedRow) -> Self {
        let result = match row.result {
            QueryResult::Ok => query_executed::Result::Ok(InputAndOutput {
                num_read_chunks: Some(row.num_read_chunks),
                output: Some(SizeAndHash {
                    size: Some(row.output_size),
                    sha3_256: row.output_hash,
                }),
            }),
            QueryResult::BadRequest => query_executed::Result::BadRequest(row.error_msg),
            QueryResult::ServerError => query_executed::Result::ServerError(row.error_msg),
        };
        QueryExecuted {
            client_id: row.client_id,
            worker_id: row.worker_id,
            query: Some(Query {
                query_id: Some(row.query_id),
                dataset: Some(row.dataset),
                query: Some(row.query),
                profiling: Some(row.profiling),
                client_state_json: Some(row.client_state_json),
                signature: row.client_signature,
            }),
            query_hash: row.query_hash,
            exec_time_ms: Some(row.exec_time_ms),
            seq_no: Some(row.seq_no),
            timestamp_ms: Some(row.worker_timestamp),
            signature: row.worker_signature,
            result: Some(result),
        }
    }
}

#[derive(Row, Debug, Clone, Serialize, Deserialize)]
pub struct PingRow {
    timestamp: u64,
    worker_id: String,
    stored_bytes: u64,
    version: String,
}

impl TryFrom<Ping> for PingRow {
    type Error = &'static str;

    fn try_from(ping: Ping) -> Result<Self, Self::Error> {
        Ok(Self {
            stored_bytes: ping.stored_bytes(),
            worker_id: ping.worker_id.ok_or("worker_id missing")?,
            version: ping.version.ok_or("version missing")?,
            timestamp: timestamp_now_ms(),
        })
    }
}

#[derive(Row, Debug, Deserialize)]
struct SeqNoRow {
    worker_id: String,
    seq_no: u64,
    timestamp: u64,
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
    async fn store_logs<'a, T: Iterator<Item = QueryExecutedRow> + Sized + Send>(
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

    async fn store_pings<'a, T: Iterator<Item = PingRow> + Sized + Send>(
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

    async fn get_last_stored(&self) -> anyhow::Result<HashMap<String, (u64, u64)>> {
        log::debug!("Retrieving latest sequence from clickhouse");
        let mut cursor = self
            .0
            .query(&format!(
                "SELECT worker_id, MAX(seq_no), MAX(worker_timestamp) FROM {} GROUP BY worker_id",
                &*LOGS_TABLE
            ))
            .fetch::<SeqNoRow>()?;
        let mut result = HashMap::new();
        while let Some(row) = cursor.next().await? {
            result.insert(row.worker_id, (row.seq_no, row.timestamp));
        }
        log::debug!("Retrieved sequence numbers: {:?}", result);
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use subsquid_messages::signatures::SignedMessage;
    use subsquid_messages::{InputAndOutput, Query, SizeAndHash};
    use subsquid_network_transport::{Keypair, PeerId};

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
            query_id: Some("query_id".to_string()),
            dataset: Some("dataset".to_string()),
            query: Some("{\"from\": \"0xdeadbeef\"}".to_string()),
            profiling: Some(false),
            client_state_json: Some("".to_string()),
            signature: vec![],
        };
        query.sign(&client_keypair);

        let mut query_log = QueryExecuted {
            client_id: client_id.to_string(),
            worker_id: worker_id.to_string(),
            query: Some(query),
            query_hash: vec![0xde, 0xad, 0xbe, 0xef],
            exec_time_ms: Some(2137),
            seq_no: Some(69),
            timestamp_ms: Some(123456789000),
            signature: vec![],
            result: Some(query_executed::Result::Ok(InputAndOutput {
                num_read_chunks: Some(10),
                output: Some(SizeAndHash {
                    size: Some(666),
                    sha3_256: vec![0xbe, 0xbe, 0xf0, 0x00],
                }),
            })),
        };
        query_log.sign(&worker_keypair);

        storage
            .store_logs(std::iter::once(query_log.clone().try_into().unwrap()))
            .await
            .unwrap();

        // Verify the last stored sequence number and timestamp
        let last_stored = storage.get_last_stored().await.unwrap();
        assert_eq!(
            last_stored.get(&worker_id.to_string()),
            Some(&(69, 123456789000))
        );

        // Verify the signatures
        let mut cursor = storage
            .0
            .query(&format!("SELECT * FROM {}", &*LOGS_TABLE))
            .fetch::<QueryExecutedRow>()
            .unwrap();
        let row = cursor.next().await.unwrap().unwrap();
        let mut saved_log: QueryExecuted = row.into();
        assert_eq!(query_log, saved_log);
        assert!(saved_log.verify_signature(&worker_id));

        // Check pings storing
        let ping = Ping {
            worker_id: Some("worker_id".to_string()),
            version: Some("1.0.0".to_string()),
            stored_bytes: Some(1024),
            stored_ranges: vec![],
            signature: vec![],
        };
        let ts = timestamp_now_ms();
        storage
            .store_pings(std::iter::once(ping.clone().try_into().unwrap()))
            .await
            .unwrap();

        let mut cursor = storage
            .0
            .query(&format!("SELECT * FROM {}", &*PINGS_TABLE))
            .fetch::<PingRow>()
            .unwrap();
        let row = cursor.next().await.unwrap().unwrap();
        assert_eq!(ping.worker_id.unwrap(), row.worker_id);
        assert_eq!(ping.version.unwrap(), row.version);
        assert_eq!(ping.stored_bytes.unwrap(), row.stored_bytes);
        assert!(row.timestamp >= ts);
        assert!(row.timestamp <= timestamp_now_ms());
    }
}
