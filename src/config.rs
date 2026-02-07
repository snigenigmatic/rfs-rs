use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;

/// CLI configuration for the Redis-like server.
#[derive(Debug, Clone, Parser)]
pub struct Config {
    /// Address to bind for the main server, e.g. 127.0.0.1:6379
    #[arg(long, env = "RFS_BIND", default_value = "127.0.0.1:6379")]
    pub bind: SocketAddr,

    /// Optional address to expose Prometheus metrics, e.g. 127.0.0.1:9900
    #[arg(long, env = "RFS_METRICS_BIND")]
    pub metrics_bind: Option<SocketAddr>,

    /// Maximum simultaneous client connections
    #[arg(long, env = "RFS_MAX_CONNECTIONS", default_value_t = 1024)]
    pub max_connections: usize,

    /// Path to append-only file. If set, enables AOF persistence.
    #[arg(long, env = "RFS_AOF_PATH")]
    pub aof_path: Option<PathBuf>,

    /// Fsync policy: "always", "everysec", or "no"
    #[arg(long, env = "RFS_AOF_FSYNC", default_value = "everysec")]
    pub aof_fsync: String,
}

impl Config {
    pub fn from_args() -> Self {
        Self::parse()
    }
}
