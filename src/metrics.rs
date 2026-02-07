use metrics_exporter_prometheus::PrometheusBuilder;

/// Install a global Prometheus recorder. HTTP serving can be added later.
pub fn init_metrics() {
    if let Err(err) = PrometheusBuilder::new().install_recorder() {
        tracing::warn!(error = %err, "failed to install prometheus metrics recorder");
    }
}
