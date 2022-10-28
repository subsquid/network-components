use archive_router::ArchiveRouter;

pub struct Server {
    router: ArchiveRouter,
}

impl Server {
    pub fn new(router: ArchiveRouter) -> Self {
        Server { router }
    }

    pub async fn run(&self) {}
}
