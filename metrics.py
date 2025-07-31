"""
Shared Prometheus metrics for the UWS Orchestrator.
This module defines all metrics in one place to avoid duplication.
"""

from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

# HTTP request metrics
REQUEST_COUNT = Counter(
    "orchestrator_requests_total",
    "Total orchestrator requests",
    ["method", "endpoint", "status"],
)
REQUEST_LATENCY = Histogram(
    "orchestrator_request_duration_seconds", "Orchestrator request latency"
)

# Infrastructure metrics
ACTIVE_NODES = Gauge("active_nodes", "Number of active nodes")
ACTIVE_CONTAINERS = Gauge("active_containers", "Number of active containers")
WEBSOCKET_CONNECTIONS = Gauge(
    "websocket_connections", "Number of active WebSocket connections"
)

# Service-specific metrics
ACTIVE_BUCKET_SERVICES = Gauge(
    "active_bucket_services", "Number of active bucket services"
)
ACTIVE_DB_SERVICES = Gauge("active_db_services", "Number of active database services")
ACTIVE_NOSQL_SERVICES = Gauge(
    "active_nosql_services", "Number of active NoSQL services"
)
ACTIVE_QUEUE_SERVICES = Gauge(
    "active_queue_services", "Number of active queue services"
)
ACTIVE_SECRETS_SERVICES = Gauge(
    "active_secrets_services", "Number of active secrets services"
)
