#[derive(Debug)]
pub enum Error {
    ReadDatasetError(Box<dyn std::error::Error>),
    InvalidLayoutError(String),
}
