use crate::cli::ClickhouseArgs;
use crate::utils::timestamp_now_ms;
use async_trait::async_trait;
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::collections::HashMap;
use subsquid_messages::{query_executed, QueryExecuted};

const LOGS_TABLE: &str = "worker_query_logs";
const LOGS_TABLE_DEFINITION: &str = "
CREATE TABLE IF NOT EXISTS worker_query_logs
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
    num_read_chunks UInt32 NULL,
    output_size UInt32 NULL,
    output_hash String NULL,
    error_msg String NULL,
    seq_no UInt64 NOT NULL,
    client_signature String NOT NULL,
    worker_signature String NOT NULL,
    worker_timestamp_ms UInt64 NOT NULL,
    collector_timestamp_ms UInt64 NOT NULL
)
ENGINE = MergeTree
ORDER BY (worker_id, seq_no);
";

#[async_trait]
pub trait LogsStorage {
    async fn store_logs<'a, T: Iterator<Item = &'a QueryExecuted> + Sized + Send>(
        &self,
        query_logs: T,
    ) -> anyhow::Result<()>;

    /// Get the sequence number & timestamp of the last stored log for each worker
    async fn get_last_stored(&self) -> anyhow::Result<HashMap<String, (u64, u64)>>;
}

pub struct ClickhouseStorage(Client);

#[derive(Debug, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
enum QueryResult {
    Ok = 1,
    BadRequest = 2,
    ServerError = 3,
}

#[derive(Row, Debug, Serialize, Deserialize)]
struct QueryExecutedRow<'a> {
    client_id: &'a str,
    worker_id: &'a str,
    query_id: &'a str,
    dataset: &'a str,
    query: &'a str,
    profiling: bool,
    client_state_json: &'a str,
    #[serde(with = "serde_bytes")]
    query_hash: &'a [u8],
    exec_time_ms: u32,
    result: QueryResult,
    num_read_chunks: Option<u32>,
    output_size: Option<u32>,
    #[serde(with = "serde_bytes")]
    output_hash: Option<&'a [u8]>,
    error_msg: Option<&'a str>,
    seq_no: u64,
    #[serde(with = "serde_bytes")]
    client_signature: &'a [u8],
    #[serde(with = "serde_bytes")]
    worker_signature: &'a [u8],
    worker_timestamp_ms: u64,
    collector_timestamp_ms: u64,
}

impl<'a> TryFrom<&'a QueryExecuted> for QueryExecutedRow<'a> {
    type Error = &'static str;

    fn try_from(query_executed: &'a QueryExecuted) -> Result<Self, Self::Error> {
        let query = query_executed.query.as_ref().ok_or("Query field missing")?;
        let result = query_executed
            .result
            .as_ref()
            .ok_or("Result field missing")?;
        let collector_timestamp_ms = timestamp_now_ms();
        let (result, num_read_chunks, output_size, output_hash, error_msg) = match result {
            query_executed::Result::Ok(res) => {
                let output = res.output.as_ref().ok_or("Output field missing")?;
                (
                    QueryResult::Ok,
                    Some(res.num_read_chunks),
                    Some(output.size),
                    Some(output.sha3_256.as_slice()),
                    None,
                )
            }
            query_executed::Result::BadRequest(err_msg) => (
                QueryResult::BadRequest,
                None,
                None,
                None,
                Some(err_msg.as_str()),
            ),
            query_executed::Result::ServerError(err_msg) => (
                QueryResult::ServerError,
                None,
                None,
                None,
                Some(err_msg.as_str()),
            ),
        };
        Ok(Self {
            client_id: &query_executed.client_id,
            worker_id: &query_executed.worker_id,
            query_id: &query.query_id,
            dataset: &query.dataset,
            query: &query.query,
            profiling: query.profiling,
            client_state_json: &query.client_state_json,
            query_hash: &query_executed.query_hash,
            exec_time_ms: query_executed.exec_time_ms,
            result,
            num_read_chunks,
            output_size,
            output_hash,
            error_msg,
            seq_no: query_executed.seq_no,
            client_signature: &query.signature,
            worker_signature: &query_executed.signature,
            worker_timestamp_ms: query_executed.timestamp_ms,
            collector_timestamp_ms,
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
        client.query(LOGS_TABLE_DEFINITION).execute().await?;
        Ok(Self(client))
    }
}

#[async_trait]
impl LogsStorage for ClickhouseStorage {
    async fn store_logs<'a, T: Iterator<Item = &'a QueryExecuted> + Sized + Send>(
        &self,
        query_logs: T,
    ) -> anyhow::Result<()> {
        log::debug!("Storing logs in clickhouse");
        let mut insert = self.0.insert(LOGS_TABLE)?;
        let rows: Vec<QueryExecutedRow> = query_logs
            .filter_map(|log| match log.try_into() {
                Ok(log) => Some(log),
                Err(e) => {
                    log::error!("Invalid log message: {e}");
                    None
                }
            })
            .collect();
        for row in rows {
            log::debug!("Storing query log {:?}", row);
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
                "SELECT worker_id, MAX(seq_no), MAX(worker_timestamp_ms) FROM {LOGS_TABLE} GROUP BY worker_id"
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
    use super::*;
    use subsquid_messages::{InputAndOutput, Query, SizeAndHash};

    // To run this test, start a local clickhouse instance first
    // docker run --rm \
    //   -e CLICKHOUSE_DB=logs_db \
    //   -e CLICKHOUSE_USER=user \
    //   -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
    //   -e CLICKHOUSE_PASSWORD=password \
    //   --network=host \
    //   --ulimit nofile=262144:262144 \
    //   clickhouse/clickhouse-server
    //
    #[tokio::test]
    async fn test_storage() {
        let storage = ClickhouseStorage::new(ClickhouseArgs {
            clickhouse_url: "http://localhost:8123/".to_string(),
            clickhouse_database: "logs_db".to_string(),
            clickhouse_user: "user".to_string(),
            clickhouse_password: "password".to_string(),
        })
        .await
        .unwrap();

        storage
            .store_logs(
                vec![QueryExecuted {
                    client_id: "client".to_string(),
                    worker_id: "worker".to_string(),
                    query: Some(Query {
                        query_id: "query_id".to_string(),
                        dataset: "dataset".to_string(),
                        query: "{\"from\": \"0xdeadbeef\"}".to_string(),
                        profiling: false,
                        client_state_json: "{}".to_string(),
                        signature: vec![],
                    }),
                    query_hash: vec![0xde, 0xad, 0xbe, 0xef],
                    exec_time_ms: 2137,
                    seq_no: 69,
                    timestamp_ms: 123456789000,
                    signature: vec![],
                    result: Some(query_executed::Result::Ok(InputAndOutput {
                        num_read_chunks: 10,
                        output: Some(SizeAndHash {
                            size: 666,
                            sha3_256: vec![0xbe, 0xbe, 0xf0, 0x00],
                        }),
                    })),
                }]
                .iter(),
            )
            .await
            .unwrap();

        let last_stored = storage.get_last_stored().await.unwrap();
        assert_eq!(*last_stored.get("worker").unwrap(), (69, 123456789000));
    }
}
