use clap::Parser;

#[derive(Parser)]
pub struct Cli {
    /// A path to s3 dataset
    #[clap(short, long)]
    pub dataset: Vec<String>,

    /// An identifier of a managed worker
    #[clap(short, long)]
    pub worker: Vec<String>,

    /// A replication factor for each data range
    #[clap(short, long)]
    pub replication: usize,

    /// Size of a data schedule unit
    #[clap(short, long)]
    pub chunk_size: usize,

    /// Interval of distribution data ranges among available workers (in seconds)
    #[clap(short = 'i', long)]
    pub scheduling_interval: u64,
}
