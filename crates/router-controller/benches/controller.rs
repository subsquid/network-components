#![feature(test)]

extern crate test;

use std::ops::Deref;
use std::collections::HashMap;
use test::Bencher;

use router_controller::controller::{ControllerBuilder, PingMessage};
use router_controller::data_chunk::DataChunk;

#[bench]
fn bench_get_worker(b: &mut Bencher) {
    let controller = ControllerBuilder::new()
        .set_data_management_unit(1)
        .set_data_replication(2)
        .set_workers(["worker-0".to_string(), "worker-1".to_string()])
        .set_datasets([("ethereum-mainnet".to_string(), "s3://ethereum-mainnet".to_string())])
        .build();

    controller.ping(PingMessage {
        worker_id: "worker-0".to_string(),
        worker_url: "http://worker-0:8000".to_string(),
        state: HashMap::new(),
        pause: false,
    });
    controller.ping(PingMessage {
        worker_id: "worker-1".to_string(),
        worker_url: "http://worker-1:8000".to_string(),
        state: HashMap::new(),
        pause: false,
    });

    let chunks = vec![DataChunk::new(0, 0, 100, "0x".to_string())];
    controller.update_dataset(&"s3://ethereum-mainnet".to_string(), chunks).unwrap();
    controller.schedule();

    let worker_0_state = controller.ping(PingMessage {
        worker_id: "worker-0".to_string(),
        worker_url: "http://worker-0:8000".to_string(),
        state: HashMap::new(),
        pause: false,
    });
    let worker_1_state = controller.ping(PingMessage {
        worker_id: "worker-1".to_string(),
        worker_url: "http://worker-1:8000".to_string(),
        state: HashMap::new(),
        pause: false,
    });

    controller.ping(PingMessage {
        worker_id: "worker-0".to_string(),
        worker_url: "http://worker-0:8000".to_string(),
        state: worker_0_state.deref().clone(),
        pause: false,
    });
    controller.ping(PingMessage {
        worker_id: "worker-1".to_string(),
        worker_url: "http://worker-1:8000".to_string(),
        state: worker_1_state.deref().clone(),
        pause: false,
    });

    b.iter(|| {
        for block_num in 0..100 {
            controller.get_worker("ethereum-mainnet", block_num);
        }
    });
}
