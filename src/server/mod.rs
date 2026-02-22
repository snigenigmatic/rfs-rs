use std::io;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::Semaphore;

use crate::config::Config;
use crate::persistence::aof::{self, AofWriter, FsyncPolicy};
use crate::server::connection::handle_connection;
use crate::store::{SharedStore, new_shared};

pub mod connection;

pub async fn run(config: Config) -> io::Result<()> {
    let store: SharedStore = new_shared();

    // AOF: replay on startup, then open writer.
    let aof = if let Some(ref path) = config.aof_path {
        let policy = FsyncPolicy::from_str(&config.aof_fsync);
        match aof::replay_aof(path, &store) {
            Ok(n) => tracing::info!(commands = n, path = %path.display(), "AOF replay complete"),
            Err(e) => tracing::warn!(error = %e, "AOF replay failed, starting fresh"),
        }
        match AofWriter::open(path, policy) {
            Ok(w) => {
                tracing::info!(path = %path.display(), ?policy, "AOF writer opened");
                Some(w)
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to open AOF file");
                None
            }
        }
    } else {
        None
    };

    let listener = TcpListener::bind(config.bind).await?;
    let limiter = Arc::new(Semaphore::new(config.max_connections));

    tracing::info!(addr = %config.bind, "server listening");

    // Spawn periodic eviction task.
    {
        let store = store.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                interval.tick().await;
                if let Ok(mut guard) = store.write() {
                    let evicted = guard.evict_expired();
                    if evicted > 0 {
                        tracing::debug!(evicted, "expired keys evicted");
                    }
                }
            }
        });
    }

    loop {
        let (socket, addr) = listener.accept().await?;
        tracing::debug!(?addr, "accepted connection");

        let permit = limiter
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed");
        let store = store.clone();
        let aof = aof.clone();

        tokio::spawn(async move {
            let _permit = permit;
            if let Err(err) = handle_connection(socket, store, aof).await {
                tracing::warn!(error = %err, "connection handler exited with error");
            }
        });
    }
}
