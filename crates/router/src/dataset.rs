use std::sync::Arc;

use crate::storage::Storage;

#[derive(Clone)]
pub struct Dataset {
    name: String,
    url: String,
    start_block: Option<u32>,
    storage: Option<Arc<dyn Storage + Sync + Send>>,
}

impl Dataset {
    pub fn new(name: String, url: String, start_block: Option<u32>) -> Dataset {
        Dataset {
            name,
            url,
            start_block,
            storage: None,
        }
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn url(&self) -> &String {
        &self.url
    }

    pub fn start_block(&self) -> &Option<u32> {
        &self.start_block
    }

    pub fn set_storage(&mut self, storage: Arc<dyn Storage + Sync + Send>) {
        self.storage = Some(storage)
    }

    pub fn storage(&self) -> &Arc<dyn Storage + Sync + Send> {
        self.storage.as_ref().expect("storage wasn't set on the dataset")
    }
}

impl From<&Dataset> for (String, (String, Option<u32>)) {
    fn from(value: &Dataset) -> Self {
        (value.name.clone(), (value.url.clone(), value.start_block))
    }
}
