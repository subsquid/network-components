use router_controller::controller::Controller;
use std::sync::Arc;
use tracing::info;

pub fn start(controller: Arc<Controller>, rpc_url: &str) -> anyhow::Result<()> {
    let client = contract_client::Client::new(rpc_url)?;
    tokio::spawn(async move {
        let mut worker_stream = client.active_workers_stream().await;
        while let Some(workers) = worker_stream.recv().await {
            info!("Active worker set updated: {workers:?}");
            let workers = workers.into_iter().map(|w| w.peer_id.to_string());
            controller.update_managed_workers(workers);
        }
    });
    Ok(())
}
