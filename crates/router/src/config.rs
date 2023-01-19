use archive_router_controller::controller;
use serde::Deserialize;
use url::Url;

#[derive(Deserialize, Debug)]
pub struct Config;

impl controller::Config for Config {
    type WorkerId = String;
    type WorkerUrl = Url;
    type Dataset = String;
}
