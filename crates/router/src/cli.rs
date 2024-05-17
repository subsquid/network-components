use clap::Parser;

use crate::dataset::Dataset;

fn parse_dataset(s: &str) -> Result<Dataset, String> {
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{}`", s))?;

    let name = s[..pos].to_string();
    if name.is_empty() {
        return Err(format!("invalid KEY=value: the KEY is empty in `{}`", s));
    }

    let value = &s[pos + 1..];
    if !value.starts_with("s3://") {
        return Err(format!("invalid S3 URL: `{}`", value));
    }

    let mut split = s[pos + 6..].split(':');
    let url = format!("s3://{}", split.next().unwrap());
    let start_block = match split.next() {
        Some(value) => {
            match value.parse::<u32>() {
                Ok(value) => Some(value),
                Err(_) => return Err(format!("invalid START_BLOCK: `{}`", value))
            }
        },
        None => None,
    };

    Ok(Dataset::new(name, url, start_block))
}

#[derive(Parser)]
pub struct Cli {
    /// Add dataset `NAME` pointing to S3 `URL`
    #[clap(short, long, value_parser = parse_dataset, value_name = "NAME=URL[:START_BLOCK]")]
    pub dataset: Vec<Dataset>,

    /// Add managed worker
    #[clap(short, long, value_name = "ID")]
    pub worker: Vec<String>,

    /// Data replication factor
    #[clap(short, long, value_name = "N")]
    pub replication: usize,

    /// Size of a data scheduling unit (in chunks)
    #[clap(short = 'u', long, value_name = "N")]
    pub scheduling_unit: usize,

    /// Scheduling interval (in seconds)
    #[clap(short = 'i', long, default_value = "300", value_name = "N")]
    pub scheduling_interval: u64,
}
