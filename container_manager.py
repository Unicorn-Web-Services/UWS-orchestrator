import uuid
import httpx
import structlog
from datetime import datetime
from fastapi import HTTPException, Request, Depends
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.orm import Session

from database import get_db, Node, Container
from models import ContainerLaunchRequest
from auth import get_auth_headers

logger = structlog.get_logger()

# Initialize rate limiter
limiter = Limiter(key_func=get_remote_address)

# Import shared metrics
from metrics import ACTIVE_CONTAINERS


@limiter.limit("20/minute")
async def launch_container(
    launch_request: ContainerLaunchRequest,
    request: Request,
    db: Session = Depends(get_db),
):
    """Launch a container on the best available node with enhanced logging"""
    logger.info(
        "Launching container",
        user_id=launch_request.user_id,
        config=launch_request.config.dict(),
    )

    # Find healthy nodes
    healthy_nodes = db.query(Node).filter(Node.is_healthy == True).all()

    if not healthy_nodes:
        logger.error("No healthy nodes available")
        raise HTTPException(status_code=503, detail="No healthy nodes available")

    # Simple load balancing - select the first healthy node
    # In production, implement more sophisticated load balancing
    selected_node = healthy_nodes[0]

    try:
        # Launch container on selected node
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{selected_node.url}/launch",
                json=launch_request.config.dict(),
                headers=get_auth_headers(),
                timeout=60.0,
            )

            if resp.status_code != 200:
                logger.error(
                    "Failed to launch container on node",
                    node_id=selected_node.node_id,
                    status_code=resp.status_code,
                )
                raise HTTPException(
                    status_code=resp.status_code, detail="Failed to launch container"
                )

            result = resp.json()

            # Get container_id from response or generate one if missing
            # Server returns "id" but we need "container_id"
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
                user_id=launch_request.user_id,
                node_id=selected_node.node_id,
                image=launch_request.config.image,
                name=launch_request.config.name,
                status="running",
                created_at=datetime.utcnow(),
            )
            db.add(container)
            db.commit()

            # Update metrics
            active_containers = (
                db.query(Container).filter(Container.status == "running").count()
            )
            ACTIVE_CONTAINERS.set(active_containers)

            logger.info(
                "Container launched successfully",
                container_id=container_id,
                node_id=selected_node.node_id,
            )

            # Return the result with our container_id
            result["container_id"] = container_id
            return result

    except Exception as e:
        logger.error("Failed to launch container", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to launch container: {str(e)}"
        )


@limiter.limit("30/minute")
async def get_user_containers(
    user_id: str, request: Request, db: Session = Depends(get_db)
):
    """Get all containers for a specific user with enhanced logging"""
    logger.info("Getting user containers", user_id=user_id)

    containers = db.query(Container).filter(Container.user_id == user_id).all()
    container_data = []

    for container in containers:
        container_data.append(
            {
                "container_id": container.container_id,
                "image": container.image,
                "name": container.name,
                "status": container.status,
                "node_id": container.node_id,
                "created_at": (
                    container.created_at.isoformat() if container.created_at else None
                ),
            }
        )

    logger.info("Retrieved user containers", user_id=user_id, count=len(container_data))
    return {"containers": container_data}


@limiter.limit("30/minute")
async def get_container_status(
    container_id: str, request: Request, db: Session = Depends(get_db)
):
    """Get container status through orchestrator"""
    logger.info("Getting container status", container_id=container_id)

    # Get container info from database
    container = (
        db.query(Container).filter(Container.container_id == container_id).first()
    )
    if not container:
        raise HTTPException(status_code=404, detail="Container not found")

    # Get node info
    node = db.query(Node).filter(Node.node_id == container.node_id).first()
    if not node or not node.is_healthy:
        raise HTTPException(status_code=503, detail="Container node is not available")

    try:
        # Forward request to the node
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{node.url}/containers/{container_id}/status",
                headers=get_auth_headers(),
                timeout=30.0,
            )

            if resp.status_code != 200:
                logger.error(
                    "Failed to get container status from node",
                    node_id=node.node_id,
                    container_id=container_id,
                    status_code=resp.status_code,
                )
                raise HTTPException(
                    status_code=resp.status_code,
                    detail="Failed to get container status",
                )

            result = resp.json()
            logger.info("Retrieved container status", container_id=container_id)
            return result

    except Exception as e:
        logger.error("Failed to get container status", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to get container status: {str(e)}"
        )


@limiter.limit("30/minute")
async def get_container_ports(
    container_id: str, request: Request, db: Session = Depends(get_db)
):
    """Get container ports through orchestrator"""
    logger.info("Getting container ports", container_id=container_id)

    # Get container info from database
    container = (
        db.query(Container).filter(Container.container_id == container_id).first()
    )
    if not container:
        raise HTTPException(status_code=404, detail="Container not found")

    # Get node info
    node = db.query(Node).filter(Node.node_id == container.node_id).first()
    if not node or not node.is_healthy:
        raise HTTPException(status_code=503, detail="Container node is not available")

    try:
        # Forward request to the node
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{node.url}/containers/{container_id}/ports",
                headers=get_auth_headers(),
                timeout=30.0,
            )

            if resp.status_code != 200:
                logger.error(
                    "Failed to get container ports from node",
                    node_id=node.node_id,
                    container_id=container_id,
                    status_code=resp.status_code,
                )
                raise HTTPException(
                    status_code=resp.status_code, detail="Failed to get container ports"
                )

            result = resp.json()
            logger.info(
                "Retrieved container ports",
                container_id=container_id,
                ports_result=result,
            )
            return result

    except Exception as e:
        logger.error("Failed to get container ports", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to get container ports: {str(e)}"
        )


@limiter.limit("20/minute")
async def start_container(
    container_id: str, request: Request, db: Session = Depends(get_db)
):
    """Start container through orchestrator"""
    logger.info("Starting container", container_id=container_id)

    # Get container info from database
    container = (
        db.query(Container).filter(Container.container_id == container_id).first()
    )
    if not container:
        raise HTTPException(status_code=404, detail="Container not found")

    # Get node info
    node = db.query(Node).filter(Node.node_id == container.node_id).first()
    if not node or not node.is_healthy:
        raise HTTPException(status_code=503, detail="Container node is not available")

    try:
        # Forward request to the node
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{node.url}/containers/{container_id}/start",
                headers=get_auth_headers(),
                timeout=60.0,
            )

            if resp.status_code != 200:
                logger.error(
                    "Failed to start container on node",
                    node_id=node.node_id,
                    container_id=container_id,
                    status_code=resp.status_code,
                )
                raise HTTPException(
                    status_code=resp.status_code, detail="Failed to start container"
                )

            result = resp.json()

            # Update container status in database
            container.status = "running"
            db.commit()

            logger.info("Container started successfully", container_id=container_id)
            return result

    except Exception as e:
        logger.error("Failed to start container", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to start container: {str(e)}"
        )


@limiter.limit("20/minute")
async def stop_container(
    container_id: str, request: Request, db: Session = Depends(get_db)
):
    """Stop container through orchestrator"""
    logger.info("Stopping container", container_id=container_id)

    # Get container info from database
    container = (
        db.query(Container).filter(Container.container_id == container_id).first()
    )
    if not container:
        raise HTTPException(status_code=404, detail="Container not found")

    # Get node info
    node = db.query(Node).filter(Node.node_id == container.node_id).first()
    if not node or not node.is_healthy:
        raise HTTPException(status_code=503, detail="Container node is not available")

    try:
        # Forward request to the node
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{node.url}/containers/{container_id}/stop",
                headers=get_auth_headers(),
                timeout=60.0,
            )

            if resp.status_code != 200:
                logger.error(
                    "Failed to stop container on node",
                    node_id=node.node_id,
                    container_id=container_id,
                    status_code=resp.status_code,
                )
                raise HTTPException(
                    status_code=resp.status_code, detail="Failed to stop container"
                )

            result = resp.json()

            # Update container status in database
            container.status = "stopped"
            db.commit()

            logger.info("Container stopped successfully", container_id=container_id)
            return result

    except Exception as e:
        logger.error("Failed to stop container", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to stop container: {str(e)}"
        )


@limiter.limit("30/minute")
async def list_all_containers(
    request: Request, db: Session = Depends(get_db), all: bool = False
):
    """List all containers through orchestrator"""
    logger.info("Listing all containers", all=all)

    # Get all containers from database
    containers = db.query(Container).all()
    container_data = []

    for container in containers:
        container_data.append(
            {
                "container_id": container.container_id,
                "image": container.image,
                "name": container.name,
                "status": container.status,
                "node_id": container.node_id,
                "created_at": (
                    container.created_at.isoformat() if container.created_at else None
                ),
            }
        )

    logger.info("Retrieved all containers", count=len(container_data))
    return container_data


@limiter.limit("30/minute")
async def get_templates(request: Request, db: Session = Depends(get_db)):
    """Get templates through orchestrator"""
    logger.info("Getting templates")

    # Default templates to return if nodes don't support templates endpoint
    default_templates = {
        "templates": [
            {
                "name": "python-web",
                "description": "Python web application with Flask",
                "image": "python:3.9-slim",
                "ports": {"5000/tcp": 5000},
                "env": {"FLASK_APP": "app.py"},
                "cpu": 0.2,
                "memory": "512m",
            },
            {
                "name": "node-web",
                "description": "Node.js web application",
                "image": "node:16-alpine",
                "ports": {"3000/tcp": 3000},
                "env": {"NODE_ENV": "production"},
                "cpu": 0.2,
                "memory": "512m",
            },
            {
                "name": "nginx",
                "description": "Nginx web server",
                "image": "nginx:alpine",
                "ports": {"80/tcp": 8080},
                "env": {},
                "cpu": 0.1,
                "memory": "256m",
            },
        ]
    }

    # Find healthy nodes
    healthy_nodes = db.query(Node).filter(Node.is_healthy == True).all()

    if not healthy_nodes:
        logger.warning("No healthy nodes available, returning default templates")
        return default_templates

    # Try to get templates from the first healthy node
    selected_node = healthy_nodes[0]

    try:
        # Forward request to the node
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{selected_node.url}/templates",
                headers=get_auth_headers(),
                timeout=30.0,
            )

            if resp.status_code == 200:
                result = resp.json()
                logger.info("Retrieved templates from node successfully")
                return result
            elif resp.status_code == 404:
                logger.info(
                    "Node does not support templates endpoint, returning default templates",
                    node_id=selected_node.node_id,
                )
                return default_templates
            else:
                logger.warning(
                    "Failed to get templates from node, returning default templates",
                    node_id=selected_node.node_id,
                    status_code=resp.status_code,
                )
                return default_templates

    except Exception as e:
        logger.warning(
            "Failed to get templates from node, returning default templates",
            error=str(e),
            node_id=selected_node.node_id,
        )
        return default_templates


@limiter.limit("20/minute")
async def restart_container(
    container_id: str, request: Request, db: Session = Depends(get_db)
):
    """Restart container through orchestrator (stop then start)"""
    logger.info("Restarting container", container_id=container_id)

    # Get container info from database
    container = (
        db.query(Container).filter(Container.container_id == container_id).first()
    )
    if not container:
        raise HTTPException(status_code=404, detail="Container not found")

    # Get node info
    node = db.query(Node).filter(Node.node_id == container.node_id).first()
    if not node or not node.is_healthy:
        raise HTTPException(status_code=503, detail="Container node is not available")

    try:
        # Forward restart request to the node
        async with httpx.AsyncClient() as client:
            # Try restart endpoint first (if node supports it)
            resp = await client.post(
                f"{node.url}/containers/{container_id}/restart",
                headers=get_auth_headers(),
                timeout=60.0,
            )

            # If restart endpoint doesn't exist, do stop then start
            if resp.status_code == 404:
                logger.info("Node doesn't support restart endpoint, doing stop then start")
                
                # Stop container
                stop_resp = await client.post(
                    f"{node.url}/containers/{container_id}/stop",
                    headers=get_auth_headers(),
                    timeout=60.0,
                )
                
                if stop_resp.status_code != 200:
                    logger.error("Failed to stop container during restart", 
                               container_id=container_id, status_code=stop_resp.status_code)
                    # Continue anyway, maybe it was already stopped
                
                # Start container
                start_resp = await client.post(
                    f"{node.url}/containers/{container_id}/start",
                    headers=get_auth_headers(),
                    timeout=60.0,
                )
                
                if start_resp.status_code != 200:
                    logger.error("Failed to start container during restart", 
                               container_id=container_id, status_code=start_resp.status_code)
                    raise HTTPException(
                        status_code=start_resp.status_code, detail="Failed to restart container"
                    )
                
                result = start_resp.json()
            else:
                if resp.status_code != 200:
                    logger.error(
                        "Failed to restart container on node",
                        node_id=node.node_id,
                        container_id=container_id,
                        status_code=resp.status_code,
                    )
                    raise HTTPException(
                        status_code=resp.status_code, detail="Failed to restart container"
                    )
                result = resp.json()

            # Update container status in database
            container.status = "running"
            db.commit()

            logger.info("Container restarted successfully", container_id=container_id)
            return result

    except Exception as e:
        logger.error("Failed to restart container", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to restart container: {str(e)}"
        )


@limiter.limit("10/minute")
async def delete_container(
    container_id: str, request: Request, db: Session = Depends(get_db)
):
    """Delete container entirely (remove from node and database)"""
    logger.info("Deleting container", container_id=container_id)

    # Get container info from database
    container = (
        db.query(Container).filter(Container.container_id == container_id).first()
    )
    if not container:
        raise HTTPException(status_code=404, detail="Container not found")

    # Get node info
    node = db.query(Node).filter(Node.node_id == container.node_id).first()
    
    # Try to remove from node if node is available
    if node and node.is_healthy:
        try:
            async with httpx.AsyncClient() as client:
                # First try to stop the container
                stop_resp = await client.post(
                    f"{node.url}/containers/{container_id}/stop",
                    headers=get_auth_headers(),
                    timeout=60.0,
                )
                
                # Try to delete/remove the container from the node
                delete_resp = await client.delete(
                    f"{node.url}/containers/{container_id}",
                    headers=get_auth_headers(),
                    timeout=60.0,
                )
                
                if delete_resp.status_code not in [200, 404]:
                    logger.warning(
                        "Failed to remove container from node, continuing with database cleanup",
                        node_id=node.node_id,
                        container_id=container_id,
                        status_code=delete_resp.status_code,
                    )
                else:
                    logger.info("Container removed from node successfully", 
                              container_id=container_id, node_id=node.node_id)

        except Exception as e:
            logger.warning(
                "Failed to communicate with node for container deletion, continuing with database cleanup",
                error=str(e),
                container_id=container_id,
                node_id=node.node_id if node else None,
            )
    else:
        logger.warning(
            "Node not available for container deletion, cleaning up database only",
            container_id=container_id,
            node_id=container.node_id,
        )

    try:
        # Remove container from database
        db.delete(container)
        db.commit()
        
        # Update metrics
        active_containers = (
            db.query(Container).filter(Container.status == "running").count()
        )
        ACTIVE_CONTAINERS.set(active_containers)

        logger.info("Container deleted successfully", container_id=container_id)
        return {"message": f"Container {container_id} deleted successfully"}

    except Exception as e:
        logger.error("Failed to delete container from database", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to delete container: {str(e)}"
        )
