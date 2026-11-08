"""Prometheus metrics collectors and definitions.

Follows strict cardinality controls:
- NO high-cardinality labels (no IDs, no secrets, no URLs, no raw error messages).
- Bounded labels only: event_type (domain events), status, outcome, error_category.
"""
from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    CollectorRegistry,
    generate_latest,
    CONTENT_TYPE_LATEST,
    REGISTRY,
)

# Shared or custom registry
registry = REGISTRY

# 1. Ingestion metrics
EVENTS_RECEIVED_TOTAL = Counter(
    "webhook_events_received_total",
    "Total number of events ingested via API",
    ["event_type"],
    registry=registry,
)

# 2. Delivery lifecycle metrics
DELIVERIES_TOTAL = Counter(
    "webhook_deliveries_total",
    "Total deliveries transitioning to terminal or scheduled status",
    ["status"],
    registry=registry,
)

DELIVERY_ATTEMPTS_TOTAL = Counter(
    "webhook_delivery_attempts_total",
    "Total outgoing HTTP delivery attempts executed",
    ["outcome", "error_category"],
    registry=registry,
)

DELIVERY_DURATION_SECONDS = Histogram(
    "webhook_delivery_duration_seconds",
    "Duration of webhook HTTP request executions in seconds",
    registry=registry,
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

# 3. Resilience and failure metrics
DELIVERY_RETRIES_TOTAL = Counter(
    "webhook_delivery_retries_total",
    "Total number of delivery retries scheduled with backoff",
    registry=registry,
)

DEAD_LETTERS_TOTAL = Counter(
    "webhook_dead_letters_total",
    "Total deliveries moved to dead-letter queue",
    registry=registry,
)

RATE_LIMIT_DEFERRED_TOTAL = Counter(
    "webhook_rate_limit_deferred_total",
    "Total delivery attempts deferred due to destination rate limiting",
    registry=registry,
)

CIRCUIT_BREAKER_DEFERRED_TOTAL = Counter(
    "webhook_circuit_breaker_deferred_total",
    "Total delivery attempts deferred because destination circuit breaker was OPEN",
    registry=registry,
)

# 4. Operational state gauges
OUTBOX_PENDING_GAUGE = Gauge(
    "webhook_outbox_pending",
    "Number of pending outbox entries waiting for dispatch",
    registry=registry,
)

QUEUE_DEPTH_GAUGE = Gauge(
    "webhook_queue_depth",
    "Approximate number of delivery tasks queued in Celery broker",
    registry=registry,
)


def get_metrics_content() -> tuple[bytes, str]:
    """Generate Prometheus exposition format payload."""
    return generate_latest(registry), CONTENT_TYPE_LATEST

