"""Prometheus instruments for the RDC-BigQuery bridge.

All components import the global instances defined here and update them inline
at the same call sites where they already update their internal counters.
The HTTP server exposes them at /metrics in Prometheus exposition format.
"""

from prometheus_client import Counter, Gauge, Histogram

ROWS_INGESTED = Counter(
    "rdc_rows_ingested_total",
    "Rows produced by the row assembler and enqueued for the BigQuery loader.",
    ["source"],
)

ROWS_COMMITTED = Counter(
    "rdc_rows_committed_total",
    "Rows successfully committed to BigQuery.",
    ["table"],
)

BATCHES_COMMITTED = Counter(
    "rdc_batches_committed_total",
    "Batches successfully committed to BigQuery.",
    ["table"],
)

COMMIT_ERRORS = Counter(
    "rdc_commit_errors_total",
    "BigQuery commit errors by table.",
    ["table"],
)

COMMIT_LATENCY = Histogram(
    "rdc_commit_latency_seconds",
    "Time spent committing a batch to BigQuery (includes retries).",
    ["table"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

LAST_COMMIT_TIMESTAMP = Gauge(
    "rdc_last_commit_timestamp_seconds",
    "Unix timestamp of the last successful BigQuery commit. "
    "Use (time() - this) for staleness alerts.",
    ["table"],
)

QUEUE_DEPTH = Gauge(
    "rdc_queue_depth",
    "Current depth of internal asyncio queues.",
    ["queue"],
)

DEVICE_MAPPINGS_ACTIVE = Gauge(
    "rdc_device_mappings_active",
    "Current number of active device-to-ticket mappings.",
)

DEVICE_MAPPING_UPDATES = Counter(
    "rdc_device_mapping_updates_total",
    "Device-to-ticket mapping updates.",
)

DEVICE_MAPPING_REMOVALS = Counter(
    "rdc_device_mapping_removals_total",
    "Device-to-ticket mapping removals.",
)

REDIS_CONNECTED = Gauge(
    "rdc_redis_connected",
    "1 if Redis is currently connected, 0 otherwise.",
)

LOADER_RUNNING = Gauge(
    "rdc_loader_running",
    "1 if the BigQuery loader for the labelled table is running, 0 otherwise.",
    ["table"],
)

BUILD_INFO = Gauge(
    "rdc_build_info",
    "Build/version info. Value is always 1; useful labels are attached.",
    ["version"],
)
