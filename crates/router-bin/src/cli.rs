use clap::Parser;

fn parse_dataset(s: &str) -> Result<(String, String), String> {
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{}`", s))?;

    let name = s[..pos].to_string();
    if name.len() == 0 {
        return Err(format!("invalid KEY=value: the KEY is empty in `{}`", s))
    }

    let url = s[pos + 1..].to_string();
    if !url.starts_with("s3://") {
        return Err(format!("invalid S3 URL: `{}`", url))
    }

    Ok((name, url))
}

#[derive(Parser)]
pub struct Cli {
    /// Add dataset `NAME` pointing to S3 `URL`
    #[clap(short, long, value_parser = parse_dataset, value_name = "NAME=URL")]
    pub dataset: Vec<(String, String)>,

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
    #[clap(short = 'i', long, default_value = "300", value_name="N")]
    pub scheduling_interval: u64,
}
