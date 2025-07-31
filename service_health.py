import asyncio
import httpx
import structlog
from datetime import datetime
from sqlalchemy.orm import Session

from database import get_db, BucketService, DBService, NoSQLService, QueueService, SecretsService, Container, Node
from auth import get_auth_headers
from metrics import ACTIVE_BUCKET_SERVICES, ACTIVE_DB_SERVICES, ACTIVE_NOSQL_SERVICES, ACTIVE_QUEUE_SERVICES, ACTIVE_SECRETS_SERVICES

logger = structlog.get_logger()

SERVICE_HEALTH_CHECK_INTERVAL = 30  # Check services every 30 seconds


async def check_service_health(service_url: str, timeout: float = 10.0) -> bool:
    """Check if a service is healthy by calling its health endpoint"""
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(f"{service_url}/health")
            return resp.status_code == 200
    except Exception as e:
        logger.debug("Service health check failed", url=service_url, error=str(e))
        return False


async def restart_failed_service(service_type: str, service_id: str, db: Session) -> bool:
    """Restart a failed service by relaunching its container"""
    try:
        logger.info(f"Attempting to restart {service_type} service", service_id=service_id)
        
        # Get service info
        service_models = {
            "bucket": BucketService,
            "db": DBService,
            "nosql": NoSQLService,
            "queue": QueueService,
            "secrets": SecretsService,
        }
        
        service_model = service_models.get(service_type)
        if not service_model:
            logger.error(f"Unknown service type: {service_type}")
            return False
            
        service = db.query(service_model).filter(service_model.service_id == service_id).first()
        if not service:
            logger.error(f"Service not found: {service_id}")
            return False
            
        # Get associated container and node
        container = db.query(Container).filter(Container.container_id == service.container_id).first()
        if not container:
            logger.error(f"Container not found for service: {service_id}")
            return False
            
        node = db.query(Node).filter(Node.node_id == container.node_id).first()
        if not node or not node.is_healthy:
            logger.error(f"Node not available for service: {service_id}")
            return False
            
        # Try to restart the container on the node
        async with httpx.AsyncClient() as client:
            # First try to start the existing container
            resp = await client.post(
                f"{node.url}/containers/{container.container_id}/start",
                headers=get_auth_headers(),
                timeout=60.0,
            )
            
            if resp.status_code == 200:
                logger.info(f"Successfully restarted container for {service_type} service", 
                           service_id=service_id, container_id=container.container_id)
                
                # Update service status
                service.is_healthy = True
                service.status = "running"
                service.last_health_check = datetime.utcnow()
                container.status = "running"
                db.commit()
                return True
            else:
                # If start fails, try to recreate the service
                logger.warning(f"Container start failed, attempting to recreate {service_type} service",
                             service_id=service_id)
                
                # This would require calling the service launcher
                # For now, mark as failed and let manual intervention handle it
                service.is_healthy = False
                service.status = "failed"
                db.commit()
                return False
                
    except Exception as e:
        logger.error(f"Failed to restart {service_type} service", 
                    service_id=service_id, error=str(e))
        return False


async def health_check_services():
    """Check health of all services and restart failed ones"""
    db = next(get_db())
    try:
        logger.info("Starting service health check cycle")
        
        # Check bucket services
        bucket_services = db.query(BucketService).all()
        healthy_buckets = 0
        
        for service in bucket_services:
            service_url = f"http://{service.ip_address}:{service.port}"
            is_healthy = await check_service_health(service_url)
            
            if is_healthy != service.is_healthy:
                logger.info(f"Bucket service health changed",
                           service_id=service.service_id, 
                           was_healthy=service.is_healthy,
                           now_healthy=is_healthy)
                
            service.is_healthy = is_healthy
            service.last_health_check = datetime.utcnow()
            
            if is_healthy:
                healthy_buckets += 1
                service.status = "running"
            else:
                service.status = "unhealthy"
                # Attempt auto-restart
                logger.warning(f"Bucket service unhealthy, attempting restart",
                             service_id=service.service_id)
                restart_success = await restart_failed_service("bucket", service.service_id, db)
                if restart_success:
                    healthy_buckets += 1
                    
        ACTIVE_BUCKET_SERVICES.set(healthy_buckets)
        
        # Check DB services
        db_services = db.query(DBService).all()
        healthy_dbs = 0
        
        for service in db_services:
            service_url = f"http://{service.ip_address}:{service.port}"
            is_healthy = await check_service_health(service_url)
            
            service.is_healthy = is_healthy
            service.last_health_check = datetime.utcnow()
            
            if is_healthy:
                healthy_dbs += 1
                service.status = "running"
            else:
                service.status = "unhealthy"
                restart_success = await restart_failed_service("db", service.service_id, db)
                if restart_success:
                    healthy_dbs += 1
                    
        ACTIVE_DB_SERVICES.set(healthy_dbs)
        
        # Check NoSQL services
        nosql_services = db.query(NoSQLService).all()
        healthy_nosql = 0
        
        for service in nosql_services:
            service_url = f"http://{service.ip_address}:{service.port}"
            is_healthy = await check_service_health(service_url)
            
            service.is_healthy = is_healthy
            service.last_health_check = datetime.utcnow()
            
            if is_healthy:
                healthy_nosql += 1
                service.status = "running"
            else:
                service.status = "unhealthy"
                restart_success = await restart_failed_service("nosql", service.service_id, db)
                if restart_success:
                    healthy_nosql += 1
                    
        ACTIVE_NOSQL_SERVICES.set(healthy_nosql)
        
        # Check Queue services
        queue_services = db.query(QueueService).all()
        healthy_queues = 0
        
        for service in queue_services:
            service_url = f"http://{service.ip_address}:{service.port}"
            is_healthy = await check_service_health(service_url)
            
            service.is_healthy = is_healthy
            service.last_health_check = datetime.utcnow()
            
            if is_healthy:
                healthy_queues += 1
                service.status = "running"
            else:
                service.status = "unhealthy"
                restart_success = await restart_failed_service("queue", service.service_id, db)
                if restart_success:
                    healthy_queues += 1
                    
        ACTIVE_QUEUE_SERVICES.set(healthy_queues)
        
        # Check Secrets services
        secrets_services = db.query(SecretsService).all()
        healthy_secrets = 0
        
        for service in secrets_services:
            service_url = f"http://{service.ip_address}:{service.port}"
            is_healthy = await check_service_health(service_url)
            
            service.is_healthy = is_healthy
            service.last_health_check = datetime.utcnow()
            
            if is_healthy:
                healthy_secrets += 1
                service.status = "running"
            else:
                service.status = "unhealthy"
                restart_success = await restart_failed_service("secrets", service.service_id, db)
                if restart_success:
                    healthy_secrets += 1
                    
        ACTIVE_SECRETS_SERVICES.set(healthy_secrets)
        
        db.commit()
        
        logger.info("Service health check complete",
                   bucket_services=healthy_buckets,
                   db_services=healthy_dbs,
                   nosql_services=healthy_nosql,
                   queue_services=healthy_queues,
                   secrets_services=healthy_secrets)
        
    except Exception as e:
        logger.error("Service health check error", error=str(e))
    finally:
        db.close()


async def service_health_check_loop():
    """Periodic health check for all services"""
    while True:
        await health_check_services()
        await asyncio.sleep(SERVICE_HEALTH_CHECK_INTERVAL)