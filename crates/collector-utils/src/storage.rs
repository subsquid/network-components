use async_trait::async_trait;
use clickhouse::{Client, Row};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use sqd_messages::Heartbeat;

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
    async fn store_pings<'a, T: Iterator<Item = PingRow> + Sized + Send>(
        &self,
        pings: T,
    ) -> anyhow::Result<()>;
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
}

#[cfg(test)]
mod tests {
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

        let _client_keypair = Keypair::from_protobuf_encoding(&[
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

        let worker_id = PeerId::from_public_key(&worker_keypair.public());

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
