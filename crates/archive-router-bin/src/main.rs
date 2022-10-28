use archive_router::ArchiveRouter;
use archive_router_api::Server;

#[tokio::main]
async fn main() {
    let dataset = "s3://eth/main".to_string();
    let router = ArchiveRouter::new(dataset);
    Server::new(router).run().await;
}
