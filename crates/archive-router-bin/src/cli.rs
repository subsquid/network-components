use clap::Parser;

#[derive(Parser)]
pub struct Cli {
    /// A path to s3 dataset
    #[clap(short, long)]
    pub dataset: String,

    /// A replication factor for each data range
    #[clap(short, long)]
    pub replication: usize,

    /// Size of a data schedule unit
    pub chunk_size: usize,

    /// Interval of distribution data ranges among available workers (in seconds)
    #[clap(long)]
    pub scheduling_interval: u64,

    /// Interval of dataset syncronization (in seconds)
    #[clap(long)]
    pub sync_interval: u64,
}
