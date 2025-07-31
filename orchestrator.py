import asyncio
import time
import structlog
from datetime import datetime
from fastapi import (
    FastAPI,
    Request,
    Response,
    UploadFile,
    File,
    Depends,
    WebSocket,
)
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from sqlalchemy.orm import Session
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from metrics import REQUEST_COUNT, REQUEST_LATENCY

# Import our new modules
from database import get_db, Node, Container
from models import ContainerLaunchRequest
from health import health_check_loop
from service_health import service_health_check_loop
from node_manager import register_node, list_nodes, manual_health_check
from container_manager import (
    launch_container,
    get_user_containers,
    get_container_status,
    get_container_ports,
    start_container,
    stop_container,
    restart_container,
    delete_container,
    list_all_containers,
    get_templates,
)
from service_launcher import (
    launch_bucket_service,
    launch_db_service,
    launch_nosql_service,
    launch_queue_service,
    launch_secrets_service,
)
from service_manager import (
    # Bucket services
    list_bucket_services,
    get_bucket_service,
    list_bucket_files,
    upload_to_bucket,
    download_from_bucket,
    delete_from_bucket,
    check_bucket_service_health,
    remove_bucket_service,
    # DB services
    list_db_services,
    get_db_service,
    check_db_service_health,
    remove_db_service,
    execute_db_sql_query,
    get_db_tables,
    get_db_table_schema,
    update_db_service_config,
    get_db_service_stats,
    # NoSQL services
    list_nosql_services,
    get_nosql_service,
    check_nosql_service_health,
    remove_nosql_service,
    # NoSQL collection management
    get_nosql_collections,
    create_nosql_collection,
    save_nosql_entity,
    query_nosql_collection,
    scan_nosql_collection,
    get_nosql_entity,
    update_nosql_entity,
    delete_nosql_entity,
    # Queue services
    list_queue_services,
    get_queue_service,
    check_queue_service_health,
    remove_queue_service,
    # Queue operations
    add_queue_message,
    read_queue_messages,
    delete_queue_message,
    # Secrets services
    list_secrets_services,
    get_secrets_service,
    check_secrets_service_health,
    remove_secrets_service,
    # Secrets operations
    create_secret,
    get_secret,
    list_secrets,
    delete_secret,
)
from monitoring import monitoring_service
from billing import billing_service
from websocket_proxy import terminal_proxy

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Initialize rate limiter
limiter = Limiter(key_func=get_remote_address)

app = FastAPI(
    title="UWS Orchestrator",
    description="Unicorn Web Services Orchestrator API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Add rate limiter to app state
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Security middleware
app.add_middleware(
    TrustedHostMiddleware, allowed_hosts=["*"]  # Configure properly for production
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request/Response middleware for logging and metrics
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()

    # Process request
    response = await call_next(request)

    # Calculate response time
    response_time = time.time() - start_time

    # Log request
    logger.info(
        "HTTP request",
        method=request.method,
        url=str(request.url),
        status_code=response.status_code,
        response_time=response_time,
        user_agent=request.headers.get("user-agent"),
        client_ip=request.client.host if request.client else None,
    )

    # Update metrics
    REQUEST_COUNT.labels(
        method=request.method, endpoint=request.url.path, status=response.status_code
    ).inc()
    REQUEST_LATENCY.observe(response_time)

    return response


# Application startup
@app.on_event("startup")
async def startup():
    logger.info("Starting UWS Orchestrator")
    asyncio.create_task(health_check_loop())
    asyncio.create_task(service_health_check_loop())

    # Start monitoring and billing services
    await monitoring_service.start_monitoring()
    await billing_service.start_billing_service()


# Node management endpoints
app.post("/register_node/{node_id}")(register_node)
app.get("/nodes")(list_nodes)
app.get("/health_check/{node_id}")(manual_health_check)

# Container management endpoints
app.post("/launch")(launch_container)
app.get("/user/{user_id}/containers")(get_user_containers)
app.get("/containers/{container_id}/status")(get_container_status)
app.get("/containers/{container_id}/ports")(get_container_ports)
app.post("/containers/{container_id}/start")(start_container)
app.post("/containers/{container_id}/stop")(stop_container)
app.post("/containers/{container_id}/restart")(restart_container)
app.delete("/containers/{container_id}")(delete_container)
app.get("/containers")(list_all_containers)
app.get("/templates")(get_templates)

# Service launcher endpoints
app.post("/launchBucket")(launch_bucket_service)
app.post("/launchDB")(launch_db_service)
app.post("/launchNoSQL")(launch_nosql_service)
app.post("/launchQueue")(launch_queue_service)
app.post("/launchSecrets")(launch_secrets_service)

# Bucket service management endpoints
app.get("/bucket-services")(list_bucket_services)
app.get("/bucket-services/{service_id}")(get_bucket_service)
app.get("/bucket-services/{service_id}/files")(list_bucket_files)
app.post("/bucket-services/{service_id}/upload")(upload_to_bucket)
app.get("/bucket-services/{service_id}/download/{filename}")(download_from_bucket)
app.delete("/bucket-services/{service_id}/delete/{filename}")(delete_from_bucket)
app.get("/bucket-services/{service_id}/health")(check_bucket_service_health)
app.delete("/bucket-services/{service_id}")(remove_bucket_service)

# DB service management endpoints
app.get("/db-services")(list_db_services)
app.get("/db-services/{service_id}")(get_db_service)
app.get("/db-services/{service_id}/health")(check_db_service_health)
app.delete("/db-services/{service_id}")(remove_db_service)

# DB service SQL and configuration endpoints
app.post("/db-services/{service_id}/sql/query")(execute_db_sql_query)
app.get("/db-services/{service_id}/sql/tables")(get_db_tables)
app.get("/db-services/{service_id}/sql/schema/{table_name}")(get_db_table_schema)
app.put("/db-services/{service_id}/config")(update_db_service_config)
app.get("/db-services/{service_id}/stats")(get_db_service_stats)

# NoSQL service management endpoints
app.get("/nosql-services")(list_nosql_services)
app.get("/nosql-services/{service_id}")(get_nosql_service)
app.get("/nosql-services/{service_id}/health")(check_nosql_service_health)
app.delete("/nosql-services/{service_id}")(remove_nosql_service)

# NoSQL collection management endpoints
app.get("/nosql-services/{service_id}/collections")(get_nosql_collections)
app.post("/nosql-services/{service_id}/collections/{collection_name}")(
    create_nosql_collection
)
app.post("/nosql-services/{service_id}/collections/{collection_name}/save")(
    save_nosql_entity
)
app.get("/nosql-services/{service_id}/collections/{collection_name}/query")(
    query_nosql_collection
)
app.get("/nosql-services/{service_id}/collections/{collection_name}/scan")(
    scan_nosql_collection
)
app.get(
    "/nosql-services/{service_id}/collections/{collection_name}/entity/{entity_id}"
)(get_nosql_entity)
app.put(
    "/nosql-services/{service_id}/collections/{collection_name}/entity/{entity_id}"
)(update_nosql_entity)
app.delete(
    "/nosql-services/{service_id}/collections/{collection_name}/entity/{entity_id}"
)(delete_nosql_entity)

# Queue service management endpoints
app.get("/queue-services")(list_queue_services)
app.get("/queue-services/{service_id}")(get_queue_service)
app.get("/queue-services/{service_id}/health")(check_queue_service_health)
app.delete("/queue-services/{service_id}")(remove_queue_service)

# Queue operations endpoints
app.post("/queue-services/{service_id}/messages")(add_queue_message)
app.get("/queue-services/{service_id}/messages")(read_queue_messages)
app.delete("/queue-services/{service_id}/messages/{message_id}")(delete_queue_message)

# Secrets service management endpoints
app.get("/secrets-services")(list_secrets_services)
app.get("/secrets-services/{service_id}")(get_secrets_service)
app.get("/secrets-services/{service_id}/health")(check_secrets_service_health)
app.delete("/secrets-services/{service_id}")(remove_secrets_service)

# Secrets operations endpoints
app.post("/secrets-services/{service_id}/secrets")(create_secret)
app.get("/secrets-services/{service_id}/secrets")(list_secrets)
app.get("/secrets-services/{service_id}/secrets/{secret_name}")(get_secret)
app.delete("/secrets-services/{service_id}/secrets/{secret_name}")(delete_secret)

# Monitoring endpoints
app.get("/monitoring/metrics/{service_id}")(monitoring_service.get_service_metrics)
app.get("/monitoring/alerts")(monitoring_service.get_active_alerts)
app.post("/monitoring/alerts/rules")(monitoring_service.create_alert_rule)
app.get("/monitoring/dashboards")(monitoring_service.get_all_dashboards)
app.post("/monitoring/dashboards")(monitoring_service.create_dashboard)
app.get("/monitoring/dashboards/{dashboard_id}")(monitoring_service.get_dashboard)
app.put("/monitoring/dashboards/{dashboard_id}")(monitoring_service.update_dashboard)
app.delete("/monitoring/dashboards/{dashboard_id}")(monitoring_service.delete_dashboard)

# Billing endpoints
app.get("/billing/usage")(billing_service.get_current_usage)
app.get("/billing/cost-breakdown")(billing_service.get_cost_breakdown)
app.get("/billing/history")(billing_service.get_billing_history)
app.get("/billing/invoices/{invoice_id}")(billing_service.get_invoice_details)
app.get("/billing/spending-limits")(billing_service.get_spending_limits)
app.post("/billing/spending-limits")(billing_service.set_spending_limit)
app.get("/billing/forecast")(billing_service.get_cost_forecast)
app.post("/billing/alerts")(billing_service.create_billing_alert)


# Core endpoints
@app.get("/health")
async def health_endpoint():
    """Enhanced health check endpoint"""
    db = next(get_db())
    try:
        total_nodes = db.query(Node).count()
        healthy_nodes = db.query(Node).filter(Node.is_healthy == True).count()
        total_containers = db.query(Container).count()
        running_containers = (
            db.query(Container).filter(Container.status == "running").count()
        )

        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "nodes": {"total": total_nodes, "healthy": healthy_nodes},
            "containers": {"total": total_containers, "running": running_containers},
        }
    except Exception as e:
        logger.error("Health check failed", error=str(e))
        return {
            "status": "unhealthy",
            "timestamp": datetime.utcnow().isoformat(),
            "error": str(e),
        }
    finally:
        db.close()


@app.get("/metrics")
async def metrics_endpoint():
    """Prometheus metrics endpoint"""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "name": "UWS Orchestrator",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs",
        "health": "/health",
        "metrics": "/metrics",
    }


# WebSocket endpoint
@app.websocket("/ws/terminal/{node_id}/{container_id}")
async def websocket_terminal_proxy(
    websocket: WebSocket, node_id: str, container_id: str
):
    """WebSocket proxy for terminal connections"""
    await terminal_proxy(websocket, node_id, container_id)
