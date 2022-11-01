use clap::Parser;

#[derive(Parser)]
pub struct Cli {
    /// A path to s3 dataset
    #[clap(short, long)]
    pub dataset: String,

    /// A replication factor for each data range
    #[clap(short, long)]
    pub replication: usize,

    /// Interval of distribution data ranges among available workers (in seconds)
    #[clap(short = 'i', long)]
    pub scheduling_interval: u64,
}
