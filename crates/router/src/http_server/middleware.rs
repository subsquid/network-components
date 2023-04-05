use axum::http::Request;
use axum::middleware::Next;
use axum::response::IntoResponse;
use tracing::info;

pub async fn logging<B>(req: Request<B>, next: Next<B>) -> impl IntoResponse {
    let method = req.method().to_string();
    let uri = req.uri().to_string();
    let version = format!("{:?}", req.version());

    let response = next.run(req).await;
    let status = response.status().to_string();
    info!(method, uri, version, status);

    response
}
