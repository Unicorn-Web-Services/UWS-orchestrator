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
from prometheus_client import (
    Counter,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

# Import our new modules
from database import get_db, Node, Container
from models import ContainerLaunchRequest
from health import health_check_loop
from node_manager import register_node, list_nodes, manual_health_check
from container_manager import (
    launch_container,
    get_user_containers,
    get_container_status,
    get_container_ports,
    start_container,
    stop_container,
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
    # DB services
    list_db_services,
    get_db_service,
    check_db_service_health,
    # NoSQL services
    list_nosql_services,
    get_nosql_service,
    check_nosql_service_health,
    # Queue services
    list_queue_services,
    get_queue_service,
    check_queue_service_health,
    # Secrets services
    list_secrets_services,
    get_secrets_service,
    check_secrets_service_health,
)
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

# Prometheus metrics
REQUEST_COUNT = Counter(
    "orchestrator_requests_total",
    "Total orchestrator requests",
    ["method", "endpoint", "status"],
)
REQUEST_LATENCY = Histogram(
    "orchestrator_request_duration_seconds", "Orchestrator request latency"
)

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

# DB service management endpoints
app.get("/db-services")(list_db_services)
app.get("/db-services/{service_id}")(get_db_service)
app.get("/db-services/{service_id}/health")(check_db_service_health)

# NoSQL service management endpoints
app.get("/nosql-services")(list_nosql_services)
app.get("/nosql-services/{service_id}")(get_nosql_service)
app.get("/nosql-services/{service_id}/health")(check_nosql_service_health)

# Queue service management endpoints
app.get("/queue-services")(list_queue_services)
app.get("/queue-services/{service_id}")(get_queue_service)
app.get("/queue-services/{service_id}/health")(check_queue_service_health)

# Secrets service management endpoints
app.get("/secrets-services")(list_secrets_services)
app.get("/secrets-services/{service_id}")(get_secrets_service)
app.get("/secrets-services/{service_id}/health")(check_secrets_service_health)


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
async def websocket_terminal_proxy(websocket: WebSocket, node_id: str, container_id: str):
    """WebSocket proxy for terminal connections"""
    await terminal_proxy(websocket, node_id, container_id)