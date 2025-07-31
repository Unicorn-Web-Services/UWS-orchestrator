import uuid
import asyncio
import httpx
import structlog
from datetime import datetime
from typing import Optional
from pydantic import BaseModel
from fastapi import HTTPException, Request, Depends
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.orm import Session

from database import (
    get_db,
    Node,
    Container,
    BucketService,
    DBService,
    NoSQLService,
    QueueService,
    SecretsService,
)
from auth import get_auth_headers

logger = structlog.get_logger()

# Initialize rate limiter
limiter = Limiter(key_func=get_remote_address)

# Import shared metrics
from metrics import ACTIVE_CONTAINERS


def extract_host_port(port_bindings):
    """Extract host port from Docker port binding structure"""
    if isinstance(port_bindings, list) and len(port_bindings) > 0:
        first_binding = port_bindings[0]
        if isinstance(first_binding, dict) and "HostPort" in first_binding:
            return int(first_binding["HostPort"])
    elif isinstance(port_bindings, int):
        return port_bindings
    return None


async def wait_for_service_container(
    selected_node: Node,
    container_id: str,
    service_port: str,
    service_type: str,
    max_retries: int = 60,
):
    """Wait for a service container to be ready and return port information"""
    logger.info(f"Waiting for {service_type} service container to be ready...")
    retry_count = 0

    async with httpx.AsyncClient() as client:
        while retry_count < max_retries:
            try:
                # Get container ports to find the IP
                ports_resp = await client.get(
                    f"{selected_node.url}/containers/{container_id}/ports",
                    headers=get_auth_headers(),
                    timeout=10.0,
                )

                if ports_resp.status_code == 200:
                    ports_data = ports_resp.json()
                    logger.info("Retrieved ports data", ports_data=ports_data)

                    # Extract IP from the node URL
                    node_ip = selected_node.url.split("://")[1].split(":")[0]

                    # Find the service port - be more flexible with port matching
                    service_port_num = None
                    ports_dict = ports_data.get("ports", {})

                    # Handle case where ports_data itself might be the ports dict
                    if not ports_dict and isinstance(ports_data, dict):
                        # Check if ports_data directly contains port mappings
                        for key, value in ports_data.items():
                            if key.endswith("/tcp") or key.endswith("/udp"):
                                ports_dict = ports_data
                                break

                    logger.info(
                        "Looking for service port",
                        service_port=service_port,
                        available_ports=list(ports_dict.keys()),
                        ports_structure=ports_dict,
                    )

                    # Strategy 1: Try exact match
                    if service_port in ports_dict:
                        port_bindings = ports_dict[service_port]
                        service_port_num = extract_host_port(port_bindings)
                        logger.info(
                            f"Found exact match for {service_port}: {service_port_num}"
                        )

                    # Strategy 2: Try without the /tcp suffix
                    elif not service_port_num:
                        service_port_base = service_port.replace("/tcp", "").replace(
                            "/udp", ""
                        )
                        for container_port, port_bindings in ports_dict.items():
                            container_port_base = container_port.replace(
                                "/tcp", ""
                            ).replace("/udp", "")
                            if container_port_base == service_port_base:
                                service_port_num = extract_host_port(port_bindings)
                                logger.info(
                                    f"Found match by port number {service_port_base}: {container_port} -> {service_port_num}"
                                )
                                break

                    # Strategy 3: Use any available port as fallback
                    if not service_port_num:
                        logger.info(
                            f"No exact match for {service_port}, trying any available port"
                        )
                        for container_port, port_bindings in ports_dict.items():
                            extracted_port = extract_host_port(port_bindings)
                            if extracted_port:
                                service_port_num = extracted_port
                                logger.info(
                                    f"Using fallback port {container_port} -> {service_port_num}"
                                )
                                break

                    if service_port_num:
                        logger.info(
                            f"Successfully found service port: {service_port_num}"
                        )
                        return node_ip, service_port_num
                    else:
                        logger.warning(f"No usable ports found in: {ports_dict}")
                else:
                    logger.warning(
                        f"Failed to get ports, status: {ports_resp.status_code}, response: {ports_resp.text}"
                    )

                retry_count += 1
                await asyncio.sleep(1)

            except Exception as e:
                logger.warning(f"Retry {retry_count + 1} failed: {str(e)}")
                retry_count += 1
                await asyncio.sleep(1)

    return None, None


@limiter.limit("10/minute")
async def launch_bucket_service(request: Request, db: Session = Depends(get_db)):
    """Launch bucket service through orchestrator"""
    logger.info("Launching bucket service")

    # Find healthy nodes
    healthy_nodes = db.query(Node).filter(Node.is_healthy == True).all()

    if not healthy_nodes:
        logger.error("No healthy nodes available")
        raise HTTPException(status_code=503, detail="No healthy nodes available")

    # Simple load balancing - select the first healthy node
    selected_node = healthy_nodes[0]

    try:
        # Forward request to the node
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{selected_node.url}/launchBucket",
                headers=get_auth_headers(),
                timeout=60.0,
            )

            if resp.status_code != 200:
                logger.error(
                    "Failed to launch bucket service on node",
                    node_id=selected_node.node_id,
                    status_code=resp.status_code,
                )
                raise HTTPException(
                    status_code=resp.status_code,
                    detail="Failed to launch bucket service",
                )

            result = resp.json()

            # Get container_id from response or generate one if missing
            container_id = result.get("container_id") or result.get("container_id")
            if not container_id:
                container_id = f"container-{uuid.uuid4().hex[:8]}"
                logger.warning(
                    "Container ID not provided by node, generated one",
                    generated_id=container_id,
                    node_id=selected_node.node_id,
                )

            # Store container information in database
            container = Container(
                container_id=container_id,
                user_id="system",  # System service
                node_id=selected_node.node_id,
                image="bucket-service",
                name="bucket-service",
                status="running",
                created_at=datetime.utcnow(),
            )
            db.add(container)
            db.commit()

            # Wait for container to be ready
            node_ip, bucket_port = await wait_for_service_container(
                selected_node, container_id, "8000/tcp", "bucket"
            )

            if bucket_port:
                # Create bucket service record
                service_id = f"bucket-{uuid.uuid4().hex[:8]}"
                bucket_service = BucketService(
                    service_id=service_id,
                    container_id=container_id,
                    node_id=selected_node.node_id,
                    ip_address=node_ip,
                    port=bucket_port,
                    status="running",
                    is_healthy=True,
                    created_at=datetime.utcnow(),
                    last_health_check=datetime.utcnow(),
                )
                db.add(bucket_service)
                db.commit()

                # Update metrics
                active_containers = (
                    db.query(Container).filter(Container.status == "running").count()
                )
                ACTIVE_CONTAINERS.set(active_containers)

                # Return the result with service information
                result["container_id"] = container_id
                result["service_id"] = service_id
                result["ip_address"] = node_ip
                result["port"] = bucket_port
                result["service_url"] = f"http://{node_ip}:{bucket_port}"
                logger.info(
                    "Bucket service launched successfully",
                    service_id=service_id,
                    container_id=container_id,
                )
                return result

            # If we get here, the container didn't become ready in time
            logger.error("Container did not become ready in time")
            raise HTTPException(
                status_code=500,
                detail="Bucket service container did not become ready in time",
            )

    except Exception as e:
        logger.error("Failed to launch bucket service", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to launch bucket service: {str(e)}"
        )


class DBServiceLaunchRequest(BaseModel):
    """Request model for launching DB service with configuration"""
    instance_name: Optional[str] = None
    max_cpu_percent: Optional[int] = 90
    max_ram_mb: Optional[int] = 2048
    max_disk_gb: Optional[int] = 10
    database_name: Optional[str] = "main"

@limiter.limit("10/minute")
async def launch_db_service(
    launch_request: Optional[DBServiceLaunchRequest] = None,
    request: Request = None, 
    db: Session = Depends(get_db)
):
    """Launch database service through orchestrator with resource limits configuration"""
    if launch_request is None:
        launch_request = DBServiceLaunchRequest()
    
    logger.info(
        "Launching database service",
        instance_name=launch_request.instance_name,
        resource_limits={
            "cpu": launch_request.max_cpu_percent,
            "ram": launch_request.max_ram_mb,
            "disk": launch_request.max_disk_gb
        }
    )

    # Find healthy nodes
    healthy_nodes = db.query(Node).filter(Node.is_healthy == True).all()

    if not healthy_nodes:
        logger.error("No healthy nodes available")
        raise HTTPException(status_code=503, detail="No healthy nodes available")

    # Simple load balancing - select the first healthy node
    selected_node = healthy_nodes[0]

    try:
        # Prepare resource limits for the node
        launch_config = {
            "resource_limits": {
                "max_cpu_percent": launch_request.max_cpu_percent,
                "max_ram_mb": launch_request.max_ram_mb,
                "max_disk_gb": launch_request.max_disk_gb
            },
            "instance_name": launch_request.instance_name,
            "database_name": launch_request.database_name
        }
        
        # Forward request to the node with resource limits
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{selected_node.url}/launchDB",
                json=launch_config,
                headers=get_auth_headers(),
                timeout=60.0,
            )

            if resp.status_code != 200:
                logger.error(
                    "Failed to launch database service on node",
                    node_id=selected_node.node_id,
                    status_code=resp.status_code,
                )
                raise HTTPException(
                    status_code=resp.status_code,
                    detail="Failed to launch database service",
                )

            result = resp.json()

            # Get container_id from response or generate one if missing
            container_id = result.get("container_id") or result.get("id")
            if not container_id:
                container_id = f"container-{uuid.uuid4().hex[:8]}"
                logger.warning(
                    "Container ID not provided by node, generated one",
                    generated_id=container_id,
                    node_id=selected_node.node_id,
                )

            # Store container information in database
            container = Container(
                container_id=container_id,
                user_id="system",  # System service
                node_id=selected_node.node_id,
                image="database-service",
                name="database-service",
                status="running",
                created_at=datetime.utcnow(),
            )
            db.add(container)
            db.commit()

            # Wait for container to be ready
            node_ip, db_port = await wait_for_service_container(
                selected_node, container_id, "8010/tcp", "database"
            )

            if db_port:
                # Create DB service record with resource limits
                service_id = f"db-{uuid.uuid4().hex[:8]}"
                db_service = DBService(
                    service_id=service_id,
                    container_id=container_id,
                    node_id=selected_node.node_id,
                    ip_address=node_ip,
                    port=db_port,
                    status="running",
                    is_healthy=True,
                    created_at=datetime.utcnow(),
                    last_health_check=datetime.utcnow(),
                    max_cpu_percent=launch_request.max_cpu_percent,
                    max_ram_mb=launch_request.max_ram_mb,
                    max_disk_gb=launch_request.max_disk_gb,
                    database_name=launch_request.database_name,
                    instance_name=launch_request.instance_name,
                )
                db.add(db_service)
                db.commit()

                # Update metrics
                active_containers = (
                    db.query(Container).filter(Container.status == "running").count()
                )
                ACTIVE_CONTAINERS.set(active_containers)

                # Return the result with service information
                result["container_id"] = container_id
                result["service_id"] = service_id
                result["ip_address"] = node_ip
                result["port"] = db_port
                result["service_url"] = f"http://{node_ip}:{db_port}"
                logger.info(
                    "DB service launched successfully",
                    service_id=service_id,
                    container_id=container_id,
                )
                return result

            # If we get here, the container didn't become ready in time
            logger.error("DB service container did not become ready in time")
            raise HTTPException(
                status_code=500,
                detail="DB service container did not become ready in time",
            )

    except Exception as e:
        logger.error("Failed to launch database service", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to launch database service: {str(e)}"
        )


@limiter.limit("10/minute")
async def launch_nosql_service(request: Request, db: Session = Depends(get_db)):
    """Launch NoSQL service through orchestrator"""
    logger.info("Launching NoSQL service")

    # Find healthy nodes
    healthy_nodes = db.query(Node).filter(Node.is_healthy == True).all()

    if not healthy_nodes:
        logger.error("No healthy nodes available")
        raise HTTPException(status_code=503, detail="No healthy nodes available")

    # Simple load balancing - select the first healthy node
    selected_node = healthy_nodes[0]

    try:
        # Forward request to the node
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{selected_node.url}/launchNoSQL",
                headers=get_auth_headers(),
                timeout=60.0,
            )

            if resp.status_code != 200:
                logger.error(
                    "Failed to launch NoSQL service on node",
                    node_id=selected_node.node_id,
                    status_code=resp.status_code,
                )
                raise HTTPException(
                    status_code=resp.status_code,
                    detail="Failed to launch NoSQL service",
                )

            result = resp.json()

            # Get container_id from response or generate one if missing
            container_id = result.get("container_id") or result.get("id")
            if not container_id:
                container_id = f"container-{uuid.uuid4().hex[:8]}"
                logger.warning(
                    "Container ID not provided by node, generated one",
                    generated_id=container_id,
                    node_id=selected_node.node_id,
                )

            # Store container information in database
            container = Container(
                container_id=container_id,
                user_id="system",  # System service
                node_id=selected_node.node_id,
                image="nosql-service",
                name="nosql-service",
                status="running",
                created_at=datetime.utcnow(),
            )
            db.add(container)
            db.commit()

            # Wait for container to be ready
            node_ip, nosql_port = await wait_for_service_container(
                selected_node, container_id, "8020/tcp", "NoSQL"
            )

            if nosql_port:
                # Create NoSQL service record
                service_id = f"nosql-{uuid.uuid4().hex[:8]}"
                nosql_service = NoSQLService(
                    service_id=service_id,
                    container_id=container_id,
                    node_id=selected_node.node_id,
                    ip_address=node_ip,
                    port=nosql_port,
                    status="running",
                    is_healthy=True,
                    created_at=datetime.utcnow(),
                    last_health_check=datetime.utcnow(),
                )
                db.add(nosql_service)
                db.commit()

                # Update metrics
                active_containers = (
                    db.query(Container).filter(Container.status == "running").count()
                )
                ACTIVE_CONTAINERS.set(active_containers)

                # Return the result with service information
                result["container_id"] = container_id
                result["service_id"] = service_id
                result["ip_address"] = node_ip
                result["port"] = nosql_port
                result["service_url"] = f"http://{node_ip}:{nosql_port}"
                logger.info(
                    "NoSQL service launched successfully",
                    service_id=service_id,
                    container_id=container_id,
                )
                return result

            # If we get here, the container didn't become ready in time
            logger.error("NoSQL service container did not become ready in time")
            raise HTTPException(
                status_code=500,
                detail="NoSQL service container did not become ready in time",
            )

    except Exception as e:
        logger.error("Failed to launch NoSQL service", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to launch NoSQL service: {str(e)}"
        )


@limiter.limit("10/minute")
async def launch_queue_service(request: Request, db: Session = Depends(get_db)):
    """Launch queue service through orchestrator"""
    logger.info("Launching queue service")

    # Find healthy nodes
    healthy_nodes = db.query(Node).filter(Node.is_healthy == True).all()

    if not healthy_nodes:
        logger.error("No healthy nodes available")
        raise HTTPException(status_code=503, detail="No healthy nodes available")

    # Simple load balancing - select the first healthy node
    selected_node = healthy_nodes[0]

    try:
        # Forward request to the node
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{selected_node.url}/launchQueue",
                headers=get_auth_headers(),
                timeout=60.0,
            )

            if resp.status_code != 200:
                logger.error(
                    "Failed to launch queue service on node",
                    node_id=selected_node.node_id,
                    status_code=resp.status_code,
                )
                raise HTTPException(
                    status_code=resp.status_code,
                    detail="Failed to launch queue service",
                )

            result = resp.json()

            # Get container_id from response or generate one if missing
            container_id = result.get("container_id") or result.get("id")
            if not container_id:
                container_id = f"container-{uuid.uuid4().hex[:8]}"
                logger.warning(
                    "Container ID not provided by node, generated one",
                    generated_id=container_id,
                    node_id=selected_node.node_id,
                )

            # Store container information in database
            container = Container(
                container_id=container_id,
                user_id="system",  # System service
                node_id=selected_node.node_id,
                image="queue-service",
                name="queue-service",
                status="running",
                created_at=datetime.utcnow(),
            )
            db.add(container)
            db.commit()

            # Wait for container to be ready
            node_ip, queue_port = await wait_for_service_container(
                selected_node, container_id, "8030/tcp", "Queue"
            )

            if queue_port:
                # Create Queue service record
                service_id = f"queue-{uuid.uuid4().hex[:8]}"
                queue_service = QueueService(
                    service_id=service_id,
                    container_id=container_id,
                    node_id=selected_node.node_id,
                    ip_address=node_ip,
                    port=queue_port,
                    status="running",
                    is_healthy=True,
                    created_at=datetime.utcnow(),
                    last_health_check=datetime.utcnow(),
                )
                db.add(queue_service)
                db.commit()

                # Update metrics
                active_containers = (
                    db.query(Container).filter(Container.status == "running").count()
                )
                ACTIVE_CONTAINERS.set(active_containers)

                # Return the result with service information
                result["container_id"] = container_id
                result["service_id"] = service_id
                result["ip_address"] = node_ip
                result["port"] = queue_port
                result["service_url"] = f"http://{node_ip}:{queue_port}"
                logger.info(
                    "Queue service launched successfully",
                    service_id=service_id,
                    container_id=container_id,
                )
                return result

            # If we get here, the container didn't become ready in time
            logger.error("Queue service container did not become ready in time")
            raise HTTPException(
                status_code=500,
                detail="Queue service container did not become ready in time",
            )

    except Exception as e:
        logger.error("Failed to launch queue service", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to launch queue service: {str(e)}"
        )


@limiter.limit("10/minute")
async def launch_secrets_service(request: Request, db: Session = Depends(get_db)):
    """Launch secrets service through orchestrator"""
    logger.info("Launching secrets service")

    # Find healthy nodes
    healthy_nodes = db.query(Node).filter(Node.is_healthy == True).all()

    if not healthy_nodes:
        logger.error("No healthy nodes available")
        raise HTTPException(status_code=503, detail="No healthy nodes available")

    # Simple load balancing - select the first healthy node
    selected_node = healthy_nodes[0]

    try:
        # Forward request to the node
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{selected_node.url}/launchSecrets",
                headers=get_auth_headers(),
                timeout=60.0,
            )

            if resp.status_code != 200:
                logger.error(
                    "Failed to launch secrets service on node",
                    node_id=selected_node.node_id,
                    status_code=resp.status_code,
                )
                raise HTTPException(
                    status_code=resp.status_code,
                    detail="Failed to launch secrets service",
                )

            result = resp.json()

            # Get container_id from response or generate one if missing
            container_id = result.get("container_id") or result.get("id")
            if not container_id:
                container_id = f"container-{uuid.uuid4().hex[:8]}"
                logger.warning(
                    "Container ID not provided by node, generated one",
                    generated_id=container_id,
                    node_id=selected_node.node_id,
                )

            # Store container information in database
            container = Container(
                container_id=container_id,
                user_id="system",  # System service
                node_id=selected_node.node_id,
                image="secrets-service",
                name="secrets-service",
                status="running",
                created_at=datetime.utcnow(),
            )
            db.add(container)
            db.commit()

            # Wait for container to be ready
            node_ip, secrets_port = await wait_for_service_container(
                selected_node, container_id, "8040/tcp", "Secrets"
            )

            if secrets_port:
                # Create Secrets service record
                service_id = f"secrets-{uuid.uuid4().hex[:8]}"
                secrets_service = SecretsService(
                    service_id=service_id,
                    container_id=container_id,
                    node_id=selected_node.node_id,
                    ip_address=node_ip,
                    port=secrets_port,
                    status="running",
                    is_healthy=True,
                    created_at=datetime.utcnow(),
                    last_health_check=datetime.utcnow(),
                )
                db.add(secrets_service)
                db.commit()

                # Update metrics
                active_containers = (
                    db.query(Container).filter(Container.status == "running").count()
                )
                ACTIVE_CONTAINERS.set(active_containers)

                # Return the result with service information
                result["container_id"] = container_id
                result["service_id"] = service_id
                result["ip_address"] = node_ip
                result["port"] = secrets_port
                result["service_url"] = f"http://{node_ip}:{secrets_port}"
                logger.info(
                    "Secrets service launched successfully",
                    service_id=service_id,
                    container_id=container_id,
                )
                return result

            # If we get here, the container didn't become ready in time
            logger.error("Secrets service container did not become ready in time")
            raise HTTPException(
                status_code=500,
                detail="Secrets service container did not become ready in time",
            )

    except Exception as e:
        logger.error("Failed to launch secrets service", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to launch secrets service: {str(e)}"
        )
