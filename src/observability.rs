use tracing_subscriber::{fmt, layer::SubscriberExt, EnvFilter, Registry};

/// Initialize structured logging with a reasonable default filter.
pub fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,tokio=info,rfs_rs=debug"));

    let subscriber = Registry::default()
        .with(env_filter)
        .with(fmt::layer().with_target(true).with_thread_ids(true));

    tracing::subscriber::set_global_default(subscriber)
        .expect("failed to install tracing subscriber");
}
