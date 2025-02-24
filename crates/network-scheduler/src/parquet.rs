use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Field;
use parquet::schema::types::Type;
use sqd_messages::assignments;
use std::fs::File;

pub fn read_chunk_summary(blocks_file: File) -> anyhow::Result<assignments::ChunkSummary> {
    let reader = SerializedFileReader::new(blocks_file)?;
    let mut iter = read_blocks(&reader)?;
    let mut last_block = iter
        .next()
        .ok_or_else(|| anyhow::anyhow!("No blocks found"))??;
    for block in iter {
        let block = block?;
        if block.number > last_block.number {
            last_block = block;
        }
    }
    Ok(assignments::ChunkSummary {
        last_block_hash: last_block.hash,
        last_block_number: last_block.slot.unwrap_or(last_block.number),
    })
}

struct BlockSummary {
    hash: String,
    number: u64,
    slot: Option<u64>,
}

fn read_blocks(
    reader: &impl FileReader,
) -> anyhow::Result<impl Iterator<Item = anyhow::Result<BlockSummary>> + '_> {
    let metadata = reader.metadata();
    let mut fields = metadata.file_metadata().schema().get_fields().to_vec();
    fields.retain(|f| matches!(f.name(), "number" | "hash" | "slot"));
    let projection = Type::group_type_builder("schema")
        .with_fields(fields)
        .build()
        .unwrap();

    let iter = reader.get_row_iter(Some(projection))?.map(|r| {
        let mut hash = None;
        let mut slot = None;
        let mut number = None;
        for column in r?.into_columns() {
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
        Ok(BlockSummary {
            hash: hash.ok_or(anyhow::anyhow!("No hash of block found"))?,
            number: slot
                .or(number)
                .ok_or(anyhow::anyhow!("No number of block found"))?,
            slot,
        })
    });
    Ok(iter)
}
