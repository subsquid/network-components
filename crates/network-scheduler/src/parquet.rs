use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Field, Row};
use sqd_messages::assignments;
use std::fs::File;

pub fn read_chunk_summary(blocks_file: File) -> anyhow::Result<assignments::ChunkSummary> {
    let Some(last_row) = read_last_row(blocks_file)? else {
        anyhow::bail!("Empty blocks file");
    };
    let mut hash = None;
    let mut slot = None;
    let mut number = None;
    for column in last_row.into_columns() {
        match column {
            (name, Field::Str(s)) if name == "hash" => {
                hash = Some(s);
            }
            (name, Field::Int(n)) if name == "number" => {
                number = Some(n as u64);
            }
            (name, Field::Long(n)) if name == "slot" => {
                slot = Some(n as u64);
            }
            _ => {}
        }
    }
    Ok(assignments::ChunkSummary {
        last_block_hash: hash.ok_or(anyhow::anyhow!("No hash of last block found"))?,
        last_block_number: slot
            .or(number)
            .ok_or(anyhow::anyhow!("No number of last block found",))?,
    })
}

fn read_last_row(parquet: File) -> anyhow::Result<Option<Row>> {
    let reader = SerializedFileReader::new(parquet)?;

    let metadata = reader.metadata();
    let num_row_groups = metadata.num_row_groups();
    if num_row_groups == 0 {
        return Ok(None);
    }

    let row_group_reader = reader.get_row_group(num_row_groups - 1)?;
    let row_iter = row_group_reader.get_row_iter(None)?;
    row_iter.last().transpose().map_err(Into::into)
}
