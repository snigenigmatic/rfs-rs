mod command;
mod config;
mod metrics;
mod observability;
mod persistence;
mod protocol;
mod server;
mod store;

#[tokio::main]
async fn main() {
    let config = config::Config::from_args();

    observability::init_tracing();
    metrics::init_metrics();

    if let Err(err) = server::run(config).await {
        tracing::error!(error = %err, "server exited with error");
    }
}
