import structlog
from datetime import datetime
from fastapi import HTTPException, Request, Depends
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.orm import Session

from database import get_db, Node
from health import health_check_node, ACTIVE_NODES

logger = structlog.get_logger()

# Initialize rate limiter
limiter = Limiter(key_func=get_remote_address)


@limiter.limit("10/minute")
async def register_node(
    node_id: str, url: str, request: Request, db: Session = Depends(get_db)
):
    """Register or update a node in the database with enhanced logging"""
    logger.info("Registering node", node_id=node_id, url=url)

    # If the URL contains 0.0.0.0, replace it with the actual client IP
    if "0.0.0.0" in url:
        client_ip = request.client.host

        if client_ip in ["127.0.0.1", "::1"] and "x-forwarded-for" in request.headers:
            client_ip = request.headers["x-forwarded-for"].split(",")[0].strip()

        url = url.replace("0.0.0.0", client_ip)

    # Check if node already exists
    existing_node = db.query(Node).filter(Node.node_id == node_id).first()

    if existing_node:
        # Update existing node
        existing_node.url = url
        existing_node.last_seen = datetime.utcnow()
        existing_node.is_healthy = True
        logger.info("Updated existing node", node_id=node_id)
    else:
        # Create new node
        new_node = Node(
            node_id=node_id,
            url=url,
            is_healthy=True,
            last_seen=datetime.utcnow(),
            last_health_check=datetime.utcnow(),
        )
        db.add(new_node)
        logger.info("Created new node", node_id=node_id)

    db.commit()

    # Update metrics
    active_nodes = db.query(Node).filter(Node.is_healthy == True).count()
    ACTIVE_NODES.set(active_nodes)

    return {"status": "success", "node_id": node_id, "url": url}


@limiter.limit("30/minute")
async def list_nodes(request: Request, db: Session = Depends(get_db)):
    """List all registered nodes with enhanced logging"""
    logger.info("Listing nodes")

    nodes = db.query(Node).all()
    node_data = []

    for node in nodes:
        node_data.append(
            {
                "node_id": node.node_id,
                "url": node.url,
                "is_healthy": node.is_healthy,
                "last_seen": node.last_seen.isoformat() if node.last_seen else None,
                "last_health_check": (
                    node.last_health_check.isoformat()
                    if node.last_health_check
                    else None
                ),
            }
        )

    logger.info("Retrieved nodes", count=len(node_data))
    return {"nodes": node_data}


@limiter.limit("10/minute")
async def manual_health_check(
    node_id: str, request: Request, db: Session = Depends(get_db)
):
    """Manually check health of a specific node"""
    logger.info("Manual health check", node_id=node_id)

    node = db.query(Node).filter(Node.node_id == node_id).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")

    is_healthy = await health_check_node(node, db)
    return {"node_id": node_id, "healthy": is_healthy}