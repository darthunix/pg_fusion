use pgrx::prelude::*;
use runtime_metrics::{MetricValue, RuntimeMetrics};

use crate::shmem::attach_runtime_metrics;

#[pg_extern]
fn pg_fusion_metrics() -> TableIterator<
    'static,
    (
        name!(component, String),
        name!(metric, String),
        name!(kind, String),
        name!(unit, String),
        name!(value, i64),
        name!(reset_epoch, i64),
    ),
> {
    let metrics = attach_runtime_metrics();
    let rows = metrics
        .snapshot()
        .into_iter()
        .map(metric_value_to_row)
        .collect::<Vec<_>>();
    TableIterator::new(rows)
}

#[pg_extern]
fn pg_fusion_metrics_reset() -> i64 {
    saturating_i64(attach_runtime_metrics().reset())
}

fn metric_value_to_row(
    MetricValue {
        descriptor,
        value,
        reset_epoch,
    }: MetricValue,
) -> (String, String, String, String, i64, i64) {
    (
        descriptor.component.to_string(),
        descriptor.metric.to_string(),
        descriptor.kind.as_str().to_string(),
        descriptor.unit.as_str().to_string(),
        saturating_i64(value),
        saturating_i64(reset_epoch),
    )
}

fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[allow(dead_code)]
fn _assert_metrics_is_copy(metrics: RuntimeMetrics) -> RuntimeMetrics {
    metrics
}
